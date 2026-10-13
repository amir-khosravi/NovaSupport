"""
NovaSupport LangGraph Orchestrator — ticket resolution workflow.

Graph topology (Phase 7: parallel context fetching):

  START
    │
    ▼
  triage ────────────────────────────────────────────────────────────┐
    │ requires_order_lookup=True                                       │ False
    ▼                                                                 │
  fetch_context  ← 3 commands sent in parallel:                      │
    │               GET_ORDER + GET_SHIPMENT + GET_CUSTOMER           │
    ▼                                                                 │
  retrieve_policy ◄─────────────────────────────────────────────────┘
    │
    ▼
  resolve
    │
    ▼
  generate_response
    │
    ▼
  quality_check
    │ validation_passed=True       │ False
    ▼                              ▼
  complete_ticket             escalate_ticket
    │                              │
    └──────────────────────────────┘
                   │
                  END

Parallelism pattern in fetch_context_node:
  1. Generate 3 correlation IDs
  2. Subscribe all 3 Futures BEFORE sending any command
  3. Fire all 3 commands with asyncio.gather (no inter-send await)
  4. Await all 3 Futures with asyncio.gather (concurrent wait)
  Result: total time ≈ max(handler latencies) instead of sum

WorkflowContext (passed via LangGraph config["configurable"]["ctx"]) holds:
  - producer: KafkaEventProducer
  - result_bus: ResultBus
  - repo: WorkflowRepository | None  (Phase 12 — None disables DB persistence)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from apps.orchestrator.result_bus import ResultBus
from apps.orchestrator.state import WorkflowState
from apps.orchestrator.step_events import emit_step
from config.settings import settings
from observability.metrics import inc_workflows_completed, inc_workflows_escalated
from persistence.workflow_repository import WorkflowRecord, WorkflowRepository
from streaming.producer import KafkaEventProducer
from streaming.schemas import AgentCommand, AgentResult, TaskType, new_id

logger = logging.getLogger(__name__)

COMMAND_TIMEOUT = 30.0  # seconds to wait for a worker result


# ── Dependency container ──────────────────────────────────────────────────────

@dataclass
class WorkflowContext:
    """Holds shared infrastructure injected into every graph node."""
    producer: KafkaEventProducer
    result_bus: ResultBus
    repo: WorkflowRepository | None = None  # None = DB persistence disabled
    redis: object = None                    # None = step events disabled


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_command(
    state: WorkflowState,
    task_type: TaskType,
    payload: dict,
    correlation_id: str,
) -> AgentCommand:
    return AgentCommand(
        workflow_id=state["workflow_id"],
        ticket_id=state["ticket_id"],
        task_type=task_type,
        payload=payload,
        correlation_id=correlation_id,
    )


async def _dispatch_and_wait(
    ctx: WorkflowContext,
    state: WorkflowState,
    task_type: TaskType,
    payload: dict,
) -> AgentResult:
    """
    Send a single AgentCommand and block until the result arrives.
    Used for sequential nodes (triage, retrieve_policy, resolve, etc.).
    """
    corr_id = new_id()
    cmd = _make_command(state, task_type, payload, corr_id)

    # Subscribe BEFORE sending — never miss a fast result
    future = ctx.result_bus.subscribe(corr_id)

    await ctx.producer.send(settings.kafka.command_topic, cmd, key=state["workflow_id"])
    logger.info("Dispatched  task_type=%-25s  corr_id=%s", task_type.value, corr_id)

    try:
        result = await asyncio.wait_for(future, timeout=COMMAND_TIMEOUT)
    except asyncio.TimeoutError:
        ctx.result_bus.unsubscribe(corr_id)
        raise asyncio.TimeoutError(
            f"No result for {task_type.value} within {COMMAND_TIMEOUT}s (corr_id={corr_id})"
        )

    logger.info("Received    task_type=%-25s  status=%s", task_type.value, result.status)
    return result


async def _dispatch_parallel(
    ctx: WorkflowContext,
    state: WorkflowState,
    tasks: list[tuple[TaskType, dict]],
) -> list[AgentResult]:
    """
    Send N commands simultaneously and wait for all N results in parallel.

    Steps:
      1. Generate N correlation IDs
      2. Subscribe N Futures (all before any send)
      3. Send N commands with asyncio.gather (concurrent fire)
      4. Await N Futures with asyncio.gather (concurrent wait)

    Total latency ≈ max(handler latency) instead of sum(handler latency).
    """
    corr_ids = [new_id() for _ in tasks]

    # Subscribe all BEFORE sending any — avoids race where fast worker
    # returns a result before we've subscribed
    futures = [ctx.result_bus.subscribe(cid) for cid in corr_ids]

    # Fire all commands at once
    commands = [
        _make_command(state, task_type, payload, cid)
        for (task_type, payload), cid in zip(tasks, corr_ids)
    ]
    await asyncio.gather(*[
        ctx.producer.send(settings.kafka.command_topic, cmd, key=state["workflow_id"])
        for cmd in commands
    ])

    type_names = [t.value for t, _ in tasks]
    logger.info(
        "Dispatched parallel [%s]  wf=%s",
        ", ".join(type_names), state["workflow_id"],
    )

    # Wait for all results concurrently
    try:
        results = await asyncio.gather(*[
            asyncio.wait_for(f, timeout=COMMAND_TIMEOUT)
            for f in futures
        ])
    except asyncio.TimeoutError:
        # Cancel remaining subscriptions
        for cid, fut in zip(corr_ids, futures):
            if not fut.done():
                ctx.result_bus.unsubscribe(cid)
        raise asyncio.TimeoutError(
            f"Parallel dispatch timeout for [{', '.join(type_names)}] "
            f"after {COMMAND_TIMEOUT}s"
        )

    logger.info(
        "Completed  parallel [%s]  wf=%s",
        ", ".join(type_names), state["workflow_id"],
    )
    return list(results)


def _ctx(config: RunnableConfig) -> WorkflowContext:
    return config["configurable"]["ctx"]


# ── Graph nodes ───────────────────────────────────────────────────────────────

async def triage_node(state: WorkflowState, config: RunnableConfig) -> dict:
    ctx = _ctx(config)

    # Phase 12 — persist initial workflow record before any work starts
    if ctx.repo is not None:
        await ctx.repo.save(WorkflowRecord(
            workflow_id=state["workflow_id"],
            ticket_id=state["ticket_id"],
            customer_id=state.get("customer_id"),
            order_id=state.get("order_id"),
            status="running",
            state_json={"message": state.get("message", "")},
        ))

    result = await _dispatch_and_wait(
        ctx, state, TaskType.TRIAGE_TICKET,
        payload={
            "message": state["message"],
            "language": state.get("language", "en"),
        },
    )
    data = result.result
    requires_order_lookup = data.get("requires_order_lookup", False)

    # Detect missing required info early — route directly to escalation
    escalation_reason: str | None = None
    if requires_order_lookup:
        missing = []
        if not state.get("customer_id"):
            missing.append("Customer ID")
        if not state.get("order_id"):
            missing.append("Order ID")
        if missing:
            escalation_reason = f"missing_required_info:{','.join(missing)}"

    await emit_step(ctx.redis, state["workflow_id"], "triage", "🎯 Ticket Triage", {
        "category": data.get("category", "general_inquiry"),
        "priority": data.get("priority", "low"),
        "requires_order_lookup": requires_order_lookup,
    })
    return {
        "triage_category": data.get("category", "general_inquiry"),
        "triage_priority": data.get("priority", "low"),
        "requires_order_lookup": requires_order_lookup,
        "requires_policy_retrieval": data.get("requires_policy_retrieval", False),
        "escalation_reason": escalation_reason,
    }


async def fetch_context_node(state: WorkflowState, config: RunnableConfig) -> dict:
    """
    Phase 7 — Parallel fetch: GET_ORDER + GET_SHIPMENT + GET_CUSTOMER
    all dispatched simultaneously.

    Latency: ~max(0.2, 0.2, 0.1) = ~0.2s  instead of  0.2 + 0.2 + 0.1 = 0.5s
    """
    ctx = _ctx(config)
    t0 = time.monotonic()

    order_id = state.get("order_id")
    customer_id = state.get("customer_id")

    results = await _dispatch_parallel(ctx, state, [
        (TaskType.GET_ORDER,    {"order_id": order_id, "customer_id": customer_id}),
        (TaskType.GET_SHIPMENT, {"order_id": order_id}),
        (TaskType.GET_CUSTOMER, {"customer_id": customer_id}),
    ])

    order_data, shipment_data, customer_data = [r.result for r in results]
    elapsed = time.monotonic() - t0

    logger.info(
        "fetch_context  parallel=3  elapsed=%.3fs  wf=%s",
        elapsed, state["workflow_id"],
    )
    await emit_step(ctx.redis, state["workflow_id"], "fetch_context", "🗄️ Context Fetched (MCP → PostgreSQL)", {
        "shipping_method": order_data.get("shipping_method", "standard"),
        "days_delayed": shipment_data.get("days_delayed", 0),
        "shipment_status": shipment_data.get("shipment_status", "unknown"),
        "customer_name": customer_data.get("name", ""),
        "customer_tier": customer_data.get("tier", "standard"),
        "parallel_fetch_ms": round(elapsed * 1000),
    })

    return {
        # order
        "order_shipping_method": order_data.get("shipping_method", "standard"),
        "order_status": order_data.get("order_status", "unknown"),
        "order_total": order_data.get("order_total", 0.0),
        "shipping_fee": order_data.get("shipping_fee", 0.0),
        # shipment
        "shipment_status": shipment_data.get("shipment_status", "unknown"),
        "days_delayed": shipment_data.get("days_delayed", 0),
        "carrier": shipment_data.get("carrier", "unknown"),
        "tracking_number": shipment_data.get("tracking_number", ""),
        # customer
        "customer_name": customer_data.get("name", ""),
        "customer_tier": customer_data.get("tier", "standard"),
        "customer_email": customer_data.get("email", ""),
    }


async def retrieve_policy_node(state: WorkflowState, config: RunnableConfig) -> dict:
    ctx = _ctx(config)
    result = await _dispatch_and_wait(
        ctx, state, TaskType.RETRIEVE_POLICY,
        payload={
            "category": state.get("triage_category", "general_inquiry"),
            "shipping_method": state.get("order_shipping_method", "standard"),
        },
    )
    data = result.result
    sources = data.get("sources", [])
    top = sources[0] if sources else {}
    await emit_step(ctx.redis, state["workflow_id"], "retrieve_policy", "📚 Policy Retrieval (RAG → Qdrant)", {
        "policy_id": top.get("policy_id", ""),
        "passage": (top.get("passage", "")[:120] + "…") if top.get("passage") else "",
        "num_sources": len(sources),
    })
    return {
        "policy_sources": sources,
    }


async def resolve_node(state: WorkflowState, config: RunnableConfig) -> dict:
    ctx = _ctx(config)
    result = await _dispatch_and_wait(
        ctx, state, TaskType.RESOLVE_TICKET,
        payload={
            "category": state.get("triage_category", "general_inquiry"),
            "message": state.get("message", ""),
            "order_id": state.get("order_id"),
            "days_delayed": state.get("days_delayed", 0),
            "shipping_fee": state.get("shipping_fee", 0.0),
            "shipping_method": state.get("order_shipping_method", "standard"),
            "order_total": state.get("order_total", 0.0),
            "shipment_status": state.get("shipment_status", "unknown"),
            "policy_sources": state.get("policy_sources", []),
            "customer_tier": state.get("customer_tier", "standard"),
        },
    )
    data = result.result
    await emit_step(ctx.redis, state["workflow_id"], "resolve", "⚖️ Resolution Decision (LLM)", {
        "action": data.get("recommended_action", "follow_up"),
        "refund_amount": data.get("refund_amount", 0.0),
        "confidence": round(data.get("confidence", 0.0), 2),
    })
    return {
        "recommended_action": data.get("recommended_action", "follow_up"),
        "refund_amount": data.get("refund_amount", 0.0),
        "resolution_notes": data.get("resolution_notes", ""),
        "resolution_confidence": data.get("confidence", 0.0),
    }


async def generate_response_node(state: WorkflowState, config: RunnableConfig) -> dict:
    ctx = _ctx(config)
    result = await _dispatch_and_wait(
        ctx, state, TaskType.GENERATE_RESPONSE,
        payload={
            "order_id": state.get("order_id"),
            "category": state.get("triage_category", "general_inquiry"),
            "recommended_action": state.get("recommended_action", "follow_up"),
            "days_delayed": state.get("days_delayed", 0),
            "shipping_fee": state.get("shipping_fee", 0.0),
            "refund_amount": state.get("refund_amount", 0.0),
            "resolution_notes": state.get("resolution_notes", ""),
            "customer_name": state.get("customer_name", ""),
            "customer_tier": state.get("customer_tier", "standard"),
        },
    )
    data = result.result
    response_text = data.get("response_text", "")
    await emit_step(ctx.redis, state["workflow_id"], "generate_response", "✍️ Response Generated (LLM)", {
        "preview": (response_text[:150] + "…") if len(response_text) > 150 else response_text,
    })
    return {
        "response_text": response_text,
    }


async def quality_check_node(state: WorkflowState, config: RunnableConfig) -> dict:
    ctx = _ctx(config)
    result = await _dispatch_and_wait(
        ctx, state, TaskType.QUALITY_CHECK,
        payload={
            "recommended_action": state.get("recommended_action", "follow_up"),
            "confidence": state.get("resolution_confidence", 0.0),
            "response_text": state.get("response_text", ""),
        },
    )
    data = result.result
    await emit_step(ctx.redis, state["workflow_id"], "quality_check", "✅ Quality Check (LLM)", {
        "status": data.get("quality_status", "needs_review"),
        "passed": data.get("validation_passed", False),
    })
    return {
        "quality_status": data.get("quality_status", "needs_review"),
        "validation_passed": data.get("validation_passed", False),
    }


async def complete_ticket_node(state: WorkflowState, config: RunnableConfig) -> dict:
    """Terminal success node — persists final outcome to PostgreSQL."""
    ctx = _ctx(config)
    final_response = state.get("response_text", "")
    final_action = state.get("recommended_action", "follow_up")

    logger.info(
        "Workflow COMPLETED  wf=%s  ticket=%s  action=%s  refund=%.2f  customer_tier=%s",
        state["workflow_id"],
        state["ticket_id"],
        final_action,
        state.get("refund_amount", 0.0),
        state.get("customer_tier", "unknown"),
    )

    inc_workflows_completed(action=final_action)

    await emit_step(ctx.redis, state["workflow_id"], "complete", "🎉 Workflow Complete", {
        "final_action": final_action,
        "refund_amount": float(state.get("refund_amount", 0.0)),
        "final_response": final_response,
    }, done=True)

    # Phase 12 — persist completed outcome
    if ctx.repo is not None:
        await ctx.repo.save(WorkflowRecord(
            workflow_id=state["workflow_id"],
            ticket_id=state["ticket_id"],
            customer_id=state.get("customer_id"),
            order_id=state.get("order_id"),
            status="completed",
            category=state.get("triage_category"),
            recommended_action=final_action,
            refund_amount=float(state.get("refund_amount", 0.0)),
            final_response=final_response,
            escalation_reason=None,
            state_json={
                "days_delayed": state.get("days_delayed", 0),
                "shipping_fee": state.get("shipping_fee", 0.0),
                "customer_tier": state.get("customer_tier", "standard"),
                "resolution_confidence": state.get("resolution_confidence", 0.0),
            },
        ))

    return {
        "final_response": final_response,
        "final_action": final_action,
        "escalation_reason": None,
        "completed": True,
    }


async def escalate_ticket_node(state: WorkflowState, config: RunnableConfig) -> dict:
    """Terminal escalation node. Persists to PostgreSQL with a context-specific message."""
    ctx = _ctx(config)

    existing_reason = state.get("escalation_reason") or ""

    if existing_reason.startswith("missing_required_info:"):
        missing_fields = existing_reason.split(":", 1)[1].replace(",", " and ")
        reason = f"Missing required information: {missing_fields}"
        final_response = (
            f"We're unable to process your request because your {missing_fields} "
            f"{'was' if ',' not in missing_fields else 'were'} not provided. "
            f"Please contact us again with your complete details — "
            f"a specialist will assist you within 24 hours."
        )
    else:
        confidence = state.get("resolution_confidence", 0.0)
        action = state.get("recommended_action", "unknown")
        reason = f"Specialist review required: confidence={confidence:.0%}, action={action}"
        final_response = (
            "Your request has been forwarded to our specialist team for careful review. "
            "We will contact you with a resolution within 24 hours."
        )

    logger.warning(
        "Workflow ESCALATED  wf=%s  ticket=%s  reason=%s",
        state["workflow_id"], state["ticket_id"], reason,
    )

    inc_workflows_escalated()

    await emit_step(ctx.redis, state["workflow_id"], "escalate", "👤 Connecting to Human Agent", {
        "reason": reason,
        "final_response": final_response,
    }, done=True)

    # Phase 12 — persist escalation outcome
    if ctx.repo is not None:
        await ctx.repo.save(WorkflowRecord(
            workflow_id=state["workflow_id"],
            ticket_id=state["ticket_id"],
            customer_id=state.get("customer_id"),
            order_id=state.get("order_id"),
            status="escalated",
            category=state.get("triage_category"),
            recommended_action=state.get("recommended_action"),
            refund_amount=0.0,
            final_response=final_response,
            escalation_reason=reason,
            state_json={
                "resolution_confidence": state.get("resolution_confidence", 0.0),
            },
        ))

    return {
        "final_response": final_response,
        "final_action": "escalated",
        "escalation_reason": reason,
        "completed": True,
    }


# ── Routing functions ─────────────────────────────────────────────────────────

def route_after_triage(state: WorkflowState) -> str:
    """Missing required info → escalate early; needs order data → fetch_context; else → retrieve_policy."""
    if state.get("escalation_reason"):
        return "escalate_ticket"
    if state.get("requires_order_lookup", False):
        return "fetch_context"
    return "retrieve_policy"


def route_after_quality(state: WorkflowState) -> str:
    """Approve → complete; reject → escalate."""
    if state.get("validation_passed", False):
        return "complete_ticket"
    return "escalate_ticket"


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    """Assemble and return the compiled LangGraph workflow."""
    graph = StateGraph(WorkflowState)

    # Nodes
    graph.add_node("triage", triage_node)
    graph.add_node("fetch_context", fetch_context_node)   # replaces fetch_order + fetch_shipment
    graph.add_node("retrieve_policy", retrieve_policy_node)
    graph.add_node("resolve", resolve_node)
    graph.add_node("generate_response", generate_response_node)
    graph.add_node("quality_check", quality_check_node)
    graph.add_node("complete_ticket", complete_ticket_node)
    graph.add_node("escalate_ticket", escalate_ticket_node)

    # Edges
    graph.add_edge(START, "triage")

    graph.add_conditional_edges(
        "triage",
        route_after_triage,
        {
            "fetch_context": "fetch_context",
            "retrieve_policy": "retrieve_policy",
            "escalate_ticket": "escalate_ticket",
        },
    )

    graph.add_edge("fetch_context", "retrieve_policy")
    graph.add_edge("retrieve_policy", "resolve")
    graph.add_edge("resolve", "generate_response")
    graph.add_edge("generate_response", "quality_check")

    graph.add_conditional_edges(
        "quality_check",
        route_after_quality,
        {"complete_ticket": "complete_ticket", "escalate_ticket": "escalate_ticket"},
    )

    graph.add_edge("complete_ticket", END)
    graph.add_edge("escalate_ticket", END)

    return graph.compile()


# Module-level compiled graph
workflow = build_graph()
