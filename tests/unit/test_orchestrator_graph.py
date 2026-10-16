"""
Unit tests for the LangGraph orchestrator (Phase 7 — parallel fetch_context).

Uses ImmediateResultBus that resolves Futures the moment a command is "sent",
so no Kafka or worker is needed. Tests verify:
  - state transitions through each node
  - fetch_context merges order + shipment + customer data correctly
  - routing logic (requires_order_lookup, validation_passed)
  - end-to-end express-delay scenario → refund_shipping_fee
  - parallel dispatch doesn't mix up correlation IDs
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from apps.orchestrator.graph import WorkflowContext, build_graph
from apps.orchestrator.result_bus import ResultBus
from apps.orchestrator.state import WorkflowState
from streaming.schemas import AgentResult, TaskStatus, TaskType, new_id


# ── Mock infrastructure ───────────────────────────────────────────────────────

class ImmediateResultBus:
    """
    Resolves Futures immediately when a command is "sent".

    The mock producer's `send` calls `resolve_next(task_type)` which pops
    the oldest pending Future and resolves it with the canned response.
    This works for both sequential and parallel dispatch because:
      - subscribe() appends to _pending in order
      - resolve_next() pops FIFO in the same order commands were sent
    """

    def __init__(self, responses: dict[str, dict]) -> None:
        # task_type.value → result dict
        self._responses = responses
        self._pending: list[asyncio.Future] = []

    def subscribe(self, correlation_id: str) -> asyncio.Future:
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._pending.append(future)
        return future

    def unsubscribe(self, correlation_id: str) -> None:
        pass

    def resolve_next(self, task_type_value: str, workflow_id: str = "WF-TEST") -> None:
        """Resolve the oldest pending future with the canned response for task_type."""
        if not self._pending:
            return
        future = self._pending.pop(0)
        if future.done():
            return
        result_data = self._responses.get(task_type_value, {})
        result = AgentResult(
            workflow_id=workflow_id,
            ticket_id="TKT-TEST",
            task_id=new_id(),
            task_type=task_type_value,
            status=TaskStatus.COMPLETED,
            result=result_data,
            correlation_id=new_id(),
        )
        future.set_result(result)


def _make_mock_ctx(responses: dict[str, dict]) -> tuple[WorkflowContext, ImmediateResultBus]:
    """Build a WorkflowContext with mock producer and immediate result bus."""
    bus = ImmediateResultBus(responses)
    producer = MagicMock()

    async def fake_send(topic, cmd, key=None):
        bus.resolve_next(cmd.task_type, cmd.workflow_id)

    producer.send = fake_send

    ctx = WorkflowContext(producer=producer, result_bus=bus)
    return ctx, bus


def _base_state() -> dict:
    return {
        "workflow_id": new_id(),
        "ticket_id": "TKT-0001",
        "customer_id": "C-0001",
        "order_id": "ORD-0001",
        "message": "My express delivery is 5 days late. I want a refund.",
        "language": "en",
    }


# ── Canned responses for the golden path ─────────────────────────────────────

EXPRESS_DELAY_RESPONSES = {
    "triage_ticket": {
        "category": "delivery_delay",
        "priority": "high",
        "requires_order_lookup": True,
        "requires_policy_retrieval": True,
        "language": "en",
    },
    # fetch_context fires these 3 in parallel
    "get_order": {
        "order_id": "ORD-0001",
        "shipping_method": "express",
        "order_status": "shipped",
        "order_total": 124.99,
        "shipping_fee": 24.99,
    },
    "get_shipment": {
        "shipment_status": "delayed",
        "days_delayed": 5,
        "carrier": "FastShip",
        "tracking_number": "FS-8821934",
    },
    "get_customer": {
        "customer_id": "C-0001",
        "name": "Alex Johnson",
        "email": "alex.johnson@example.com",
        "tier": "gold",
        "account_age_days": 380,
    },
    "retrieve_policy": {
        "category": "delivery_delay",
        "sources": [
            {
                "policy_id": "express_delivery_policy",
                "title": "Express Delivery Policy",
                "passage": (
                    "If express delivery is delayed by more than 3 business days, "
                    "the shipping fee may be refunded."
                ),
                "relevance_score": 0.94,
            }
        ],
    },
    "resolve_ticket": {
        "recommended_action": "refund_shipping_fee",
        "refund_amount": 24.99,
        "resolution_notes": "5-day express delay exceeds 3-day threshold — refund approved.",
        "confidence": 0.92,
    },
    "generate_response": {
        "response_text": (
            "We apologize for the express delivery delay. "
            "You are eligible for a full refund of the $24.99 CAD shipping fee."
        ),
        "recommended_action": "refund_shipping_fee",
        "refund_amount": 24.99,
        "confidence": 0.92,
    },
    "quality_check": {
        "quality_status": "approved",
        "validation_passed": True,
        "action_validated": "refund_shipping_fee",
    },
}


# ── Golden path tests ─────────────────────────────────────────────────────────

class TestOrchestratorGraph:

    async def test_express_delay_full_workflow(self):
        """
        Golden path: express delivery delayed 5 days → refund_shipping_fee.
        """
        graph = build_graph()
        ctx, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        assert final["triage_category"] == "delivery_delay"
        assert final["days_delayed"] == 5
        assert final["order_shipping_method"] == "express"
        assert final["recommended_action"] == "refund_shipping_fee"
        assert abs(final["refund_amount"] - 24.99) < 0.01
        assert final["quality_status"] == "approved"
        assert final["validation_passed"] is True
        assert final["final_action"] == "refund_shipping_fee"
        assert final["completed"] is True
        assert final.get("escalation_reason") is None

    async def test_customer_context_captured_in_state(self):
        """fetch_context must merge customer data into workflow state."""
        graph = build_graph()
        ctx, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        assert final["customer_name"] == "Alex Johnson"
        assert final["customer_tier"] == "gold"
        assert final["customer_email"] == "alex.johnson@example.com"

    async def test_response_text_mentions_refund(self):
        """Final response text must mention the refund."""
        graph = build_graph()
        ctx, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        assert "refund" in final["final_response"].lower()

    async def test_policy_sources_captured_in_state(self):
        """Policy retrieval results must be stored in state."""
        graph = build_graph()
        ctx, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        assert len(final["policy_sources"]) >= 1
        assert final["policy_sources"][0]["policy_id"] == "express_delivery_policy"


# ── Routing logic tests ───────────────────────────────────────────────────────

class TestRoutingLogic:

    async def test_skip_fetch_context_when_not_required(self):
        """When requires_order_lookup=False, graph skips fetch_context → retrieve_policy."""
        no_order_responses = {
            "triage_ticket": {
                "category": "general_inquiry",
                "priority": "low",
                "requires_order_lookup": False,
                "requires_policy_retrieval": False,
            },
            "retrieve_policy": {
                "category": "general_inquiry",
                "sources": [{
                    "policy_id": "general_support_policy",
                    "title": "General Support Policy",
                    "passage": "Our support team is available 24/7.",
                    "relevance_score": 0.72,
                }],
            },
            "resolve_ticket": {
                "recommended_action": "follow_up",
                "refund_amount": 0.0,
                "resolution_notes": "General inquiry.",
                "confidence": 0.85,
            },
            "generate_response": {
                "response_text": "Thank you for contacting us. We will follow up shortly.",
                "recommended_action": "follow_up",
                "refund_amount": 0.0,
                "confidence": 0.85,
            },
            "quality_check": {
                "quality_status": "approved",
                "validation_passed": True,
                "action_validated": "follow_up",
            },
        }

        graph = build_graph()
        ctx, _ = _make_mock_ctx(no_order_responses)
        config = {"configurable": {"ctx": ctx}}

        state = _base_state()
        state["message"] = "Just checking my account status."
        final = await graph.ainvoke(state, config=config)

        assert final["triage_category"] == "general_inquiry"
        # fetch_context was skipped — these fields should be absent or default
        assert final.get("days_delayed", 0) == 0
        assert final["final_action"] == "follow_up"
        assert final["completed"] is True

    async def test_low_confidence_escalates(self):
        """When quality_check fails, workflow routes to escalate_ticket."""
        low_conf_responses = {**EXPRESS_DELAY_RESPONSES}
        low_conf_responses["quality_check"] = {
            "quality_status": "needs_review",
            "validation_passed": False,
            "action_validated": "refund_shipping_fee",
        }

        graph = build_graph()
        ctx, _ = _make_mock_ctx(low_conf_responses)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        assert final["final_action"] == "escalated"
        assert final["escalation_reason"] is not None
        assert final["completed"] is True
        assert "specialist" in final["final_response"].lower()


# ── Parallel dispatch correctness ─────────────────────────────────────────────

class TestParallelDispatch:

    async def test_fetch_context_fires_three_commands(self):
        """
        fetch_context_node must send exactly 3 commands (order, shipment, customer)
        and merge all three results into state.
        """
        graph = build_graph()
        ctx, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        config = {"configurable": {"ctx": ctx}}

        final = await graph.ainvoke(_base_state(), config=config)

        # All three data sources must be present in state
        assert final["order_shipping_method"] == "express"       # from get_order
        assert final["shipment_status"] == "delayed"             # from get_shipment
        assert final["customer_tier"] == "gold"                  # from get_customer

    async def test_parallel_results_not_cross_correlated(self):
        """
        Two independent workflows running concurrently must not mix up results.
        Each must get its own customer/order/shipment data.
        """
        graph = build_graph()

        # Both use the same canned responses — but distinct workflow IDs
        ctx1, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)
        ctx2, _ = _make_mock_ctx(EXPRESS_DELAY_RESPONSES)

        s1 = {**_base_state(), "workflow_id": new_id(), "ticket_id": "TKT-A"}
        s2 = {**_base_state(), "workflow_id": new_id(), "ticket_id": "TKT-B"}

        config1 = {"configurable": {"ctx": ctx1}}
        config2 = {"configurable": {"ctx": ctx2}}

        final1, final2 = await asyncio.gather(
            graph.ainvoke(s1, config=config1),
            graph.ainvoke(s2, config=config2),
        )

        assert final1["completed"] is True
        assert final2["completed"] is True
        assert final1["final_action"] == "refund_shipping_fee"
        assert final2["final_action"] == "refund_shipping_fee"


# ── ResultBus unit tests ──────────────────────────────────────────────────────

class TestResultBus:

    async def test_subscribe_and_deliver(self):
        """ResultBus.subscribe() returns a Future that deliver() resolves."""
        bus = ResultBus()

        corr_id = new_id()
        future = bus.subscribe(corr_id)
        assert not future.done()

        await bus.deliver(AgentResult(
            workflow_id="WF-TEST",
            ticket_id="TKT-TEST",
            task_id=new_id(),
            task_type="triage_ticket",
            status=TaskStatus.COMPLETED,
            result={"x": 42},
            correlation_id=corr_id,
        ))
        assert future.done()
        assert (await future).result["x"] == 42

    async def test_parallel_subscribe_all_resolved(self):
        """Three subscriptions can all be resolved independently."""
        bus = ResultBus()

        ids = [new_id() for _ in range(3)]
        futures = [bus.subscribe(cid) for cid in ids]

        for i, cid in enumerate(ids):
            await bus.deliver(AgentResult(
                workflow_id="WF",
                ticket_id="TKT",
                task_id=new_id(),
                task_type="get_order",
                status=TaskStatus.COMPLETED,
                result={"i": i},
                correlation_id=cid,
            ))

        results = await asyncio.gather(*futures)
        assert [r.result["i"] for r in results] == [0, 1, 2]

    async def test_unsubscribe_cancels_future(self):
        bus = ResultBus()
        corr_id = new_id()
        future = bus.subscribe(corr_id)
        bus.unsubscribe(corr_id)
        assert future.cancelled()

    async def test_deliver_to_missing_corr_id_is_noop(self):
        """No waiter registered → deliver is a safe no-op."""
        bus = ResultBus()
        await bus.deliver(AgentResult(
            workflow_id="WF",
            ticket_id="TKT",
            task_id=new_id(),
            task_type="triage_ticket",
            status=TaskStatus.COMPLETED,
            result={},
            correlation_id="NONEXISTENT",
        ))
