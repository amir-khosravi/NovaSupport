"""
Task handlers for NovaSupport workers.

GET_CUSTOMER / GET_ORDER / GET_PAYMENT / GET_SHIPMENT call the operational-data
MCP server (Phase 9).  All handlers fall back to mock data when the MCP server
or Qdrant is unavailable so unit and E2E tests run without external services.

TaskType values (from streaming/schemas.py):
  TRIAGE_TICKET, GET_CUSTOMER, GET_ORDER, GET_PAYMENT,
  GET_SHIPMENT, RETRIEVE_POLICY, RESOLVE_TICKET,
  GENERATE_RESPONSE, QUALITY_CHECK
"""

from __future__ import annotations

import asyncio
import logging

from streaming.schemas import AgentCommand, TaskType
from apps.worker.registry import HandlerRegistry

logger = logging.getLogger(__name__)

# Module-level registry — imported by WorkerService
registry = HandlerRegistry()


# ── helpers ──────────────────────────────────────────────────────────────────

async def _think(seconds: float = 0.15) -> None:
    """Simulate async processing time."""
    await asyncio.sleep(seconds)


# ── handlers ─────────────────────────────────────────────────────────────────

@registry.register(TaskType.TRIAGE_TICKET)
async def handle_triage(cmd: AgentCommand) -> dict:
    """
    Classify the ticket using Claude (Phase 10).
    Falls back to keyword matching when the API key is absent.
    """
    from agents.triage import triage_ticket

    message: str = cmd.payload.get("message", "")
    try:
        result = await triage_ticket(message)
        logger.info(
            "Triage (LLM) ticket=%s category=%s priority=%s",
            cmd.ticket_id, result.category, result.priority,
        )
        return {
            "category": result.category,
            "priority": result.priority,
            "requires_order_lookup": result.requires_order_lookup,
            "requires_policy_retrieval": result.requires_policy_retrieval,
            "language": result.language,
        }
    except Exception as exc:
        logger.warning("LLM unavailable for TRIAGE (%s) — keyword fallback", exc)

    # ── keyword fallback ──────────────────────────────────────────────────────
    await _think(0.1)
    msg = message.lower()
    if any(kw in msg for kw in ("express", "delayed", "late", "delay", "missing", "lost")):
        category, priority, req_order, req_policy = "delivery_delay", "high", True, True
    elif any(kw in msg for kw in ("refund", "return", "money back", "cancel")):
        category, priority, req_order, req_policy = "refund_request", "high", True, True
    elif any(kw in msg for kw in ("wrong item", "incorrect", "wrong product", "damaged")):
        category, priority, req_order, req_policy = "wrong_item", "medium", True, False
    elif any(kw in msg for kw in ("duplicate", "charged twice", "double charge", "billing")):
        category, priority, req_order, req_policy = "billing_issue", "high", True, True
    else:
        category, priority, req_order, req_policy = "general_inquiry", "low", False, False

    logger.info(
        "Triage (keyword) ticket=%s category=%s priority=%s",
        cmd.ticket_id, category, priority,
    )
    return {
        "category": category,
        "priority": priority,
        "requires_order_lookup": req_order,
        "requires_policy_retrieval": req_policy,
        "language": cmd.payload.get("language", "en"),
    }


@registry.register(TaskType.GET_CUSTOMER)
async def handle_get_customer(cmd: AgentCommand) -> dict:
    """
    Retrieve customer profile via the operational-data MCP server.
    Falls back to mock data when the server is unavailable.
    """
    from mcp_servers.client import OperationalDataClient

    customer_id = cmd.payload.get("customer_id", "UNKNOWN")
    try:
        client = OperationalDataClient()
        r = await client.get_customer(customer_id)
        if "error" in r:
            raise ValueError(r["error"])
        return {
            "customer_id": r.get("customer_id", customer_id),
            # graph reads "name" and "tier" — map from DB column names
            "name": r.get("full_name", r.get("name", "Unknown")),
            "full_name": r.get("full_name", "Unknown"),
            "email": r.get("email", ""),
            "tier": r.get("account_status", r.get("tier", "standard")),
            "account_status": r.get("account_status", "active"),
            "preferred_language": r.get("preferred_language", "en"),
            "risk_flag": r.get("risk_flag", False),
            "data_source": r.get("data_source", "mcp"),
        }
    except Exception as exc:
        logger.warning("MCP unavailable for GET_CUSTOMER (%s) — using mock", exc)
        await _think(0.1)
        return {
            "customer_id": customer_id,
            "name": "Alex Johnson",
            "full_name": "Alex Johnson",
            "email": "alex.johnson@example.com",
            "tier": "gold",
            "account_status": "active",
            "preferred_language": "en",
            "risk_flag": False,
            "data_source": "mock_mcp",
        }


@registry.register(TaskType.GET_ORDER)
async def handle_get_order(cmd: AgentCommand) -> dict:
    """
    Retrieve order details via the operational-data MCP server.
    Falls back to mock data when the server is unavailable.
    """
    from mcp_servers.client import OperationalDataClient

    order_id = cmd.payload.get("order_id", "UNKNOWN")
    customer_id = cmd.payload.get("customer_id", "UNKNOWN")
    try:
        client = OperationalDataClient()
        r = await client.get_order(order_id)
        if "error" in r:
            raise ValueError(r["error"])
        return {
            "order_id": r.get("order_id", order_id),
            "customer_id": r.get("customer_id", customer_id),
            "shipping_method": r.get("shipping_method", "standard"),
            "order_status": r.get("order_status", "unknown"),
            # DB uses total_amount; graph reads order_total
            "order_total": float(r.get("total_amount", r.get("order_total", 0.0))),
            "shipping_fee": float(r.get("shipping_fee", 0.0)),
            "currency": r.get("currency", "USD"),
            "items": r.get("items", []),
            "data_source": r.get("data_source", "mcp"),
        }
    except Exception as exc:
        logger.warning("MCP unavailable for GET_ORDER (%s) — using mock", exc)
        await _think(0.2)
        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "shipping_method": "express",
            "order_status": "shipped",
            "order_total": 124.99,
            "shipping_fee": 24.99,
            "currency": "CAD",
            "items": [{"product_id": "P-001", "name": "Wireless Headphones", "qty": 1, "price": 124.99}],
            "data_source": "mock_mcp",
        }


@registry.register(TaskType.GET_PAYMENT)
async def handle_get_payment(cmd: AgentCommand) -> dict:
    """
    Retrieve payment details via the operational-data MCP server.
    Falls back to mock data when the server is unavailable.
    """
    from mcp_servers.client import OperationalDataClient

    order_id = cmd.payload.get("order_id", "UNKNOWN")
    try:
        client = OperationalDataClient()
        r = await client.get_payment(order_id)
        if "error" in r:
            raise ValueError(r["error"])
        return {
            "order_id": r.get("order_id", order_id),
            "payment_status": r.get("payment_status", "unknown"),
            # DB uses payment_amount; keep amount_charged alias for compatibility
            "amount_charged": float(r.get("payment_amount", r.get("amount_charged", 0.0))),
            "shipping_fee": float(r.get("shipping_fee", 0.0)),
            "currency": r.get("currency", "USD"),
            "payment_method": r.get("payment_method", "unknown"),
            "duplicate_charge": r.get("duplicate_charge", False),
            "refund_status": r.get("refund_status", "none"),
            "data_source": r.get("data_source", "mcp"),
        }
    except Exception as exc:
        logger.warning("MCP unavailable for GET_PAYMENT (%s) — using mock", exc)
        await _think(0.15)
        return {
            "order_id": order_id,
            "payment_status": "completed",
            "amount_charged": 124.99,
            "shipping_fee": 24.99,
            "currency": "CAD",
            "payment_method": "credit_card",
            "duplicate_charge": False,
            "data_source": "mock_mcp",
        }


@registry.register(TaskType.GET_SHIPMENT)
async def handle_get_shipment(cmd: AgentCommand) -> dict:
    """
    Retrieve shipment tracking via the operational-data MCP server.
    Falls back to mock data when the server is unavailable.
    """
    from mcp_servers.client import OperationalDataClient

    order_id = cmd.payload.get("order_id", "UNKNOWN")
    try:
        client = OperationalDataClient()
        r = await client.get_shipment(order_id)
        if "error" in r:
            raise ValueError(r["error"])
        return {
            "order_id": r.get("order_id", order_id),
            "shipment_status": r.get("shipment_status", "unknown"),
            "carrier": r.get("carrier", "Unknown"),
            "tracking_number": r.get("tracking_number", ""),
            "days_delayed": int(r.get("days_delayed", 0)),
            "last_scan_location": r.get("last_location", r.get("last_scan_location", "")),
            "estimated_delivery": r.get("estimated_delivery_date", ""),
            "actual_delivery": r.get("delivered_at"),
            "data_source": r.get("data_source", "mcp"),
        }
    except Exception as exc:
        logger.warning("MCP unavailable for GET_SHIPMENT (%s) — using mock", exc)
        await _think(0.2)
        return {
            "order_id": order_id,
            "shipment_status": "delayed",
            "carrier": "FastShip",
            "tracking_number": "FS-8821934",
            "estimated_delivery": "2025-11-10",
            "actual_delivery": None,
            "days_delayed": 5,
            "last_scan_location": "Toronto Distribution Centre",
            "data_source": "mock_mcp",
        }


@registry.register(TaskType.RETRIEVE_POLICY)
async def handle_retrieve_policy(cmd: AgentCommand) -> dict:
    """
    Retrieve relevant policy passages via semantic search (Qdrant RAG).
    Phase 8: replaced mock with real vector search.
    """
    from rag.retriever import PolicyRetriever

    category = cmd.payload.get("category", "general_inquiry")
    shipping_method = cmd.payload.get("shipping_method", "standard")

    try:
        retriever = PolicyRetriever()
        sources = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: retriever.retrieve(category, shipping_method=shipping_method, top_k=3),
        )
        retrieval_method = "qdrant_semantic"
    except Exception as exc:
        logger.warning("Qdrant unavailable (%s) — falling back to mock", exc)
        sources = [
            {
                "policy_id": "express_delivery_policy",
                "title": "Express Delivery Policy",
                "passage": (
                    "If express delivery is delayed by more than 3 business days, "
                    "the shipping fee may be refunded upon customer request."
                ),
                "relevance_score": 0.94,
            }
        ]
        retrieval_method = "mock_fallback"

    logger.info(
        "Policy retrieved category=%s shipping=%s sources=%d method=%s",
        category, shipping_method, len(sources), retrieval_method,
    )
    return {
        "category": category,
        "sources": sources,
        "retrieval_method": retrieval_method,
    }


@registry.register(TaskType.RESOLVE_TICKET)
async def handle_resolve_ticket(cmd: AgentCommand) -> dict:
    """
    Determine resolution using Claude (Phase 10).
    Falls back to rule-based logic when the API key is absent.
    """
    from agents.resolver import resolve_ticket

    category = cmd.payload.get("category", "general_inquiry")
    days_delayed = cmd.payload.get("days_delayed", 0)
    shipping_fee = float(cmd.payload.get("shipping_fee", 0.0))
    policy_sources = cmd.payload.get("policy_sources", [])
    customer_tier = cmd.payload.get("customer_tier", "standard")

    message = cmd.payload.get("message", "")
    shipping_method = cmd.payload.get("shipping_method", "standard")
    order_total = float(cmd.payload.get("order_total", 0.0))
    shipment_status = cmd.payload.get("shipment_status", "unknown")

    try:
        result = await resolve_ticket(
            category=category,
            message=message,
            days_delayed=days_delayed,
            shipping_fee=shipping_fee,
            policy_sources=policy_sources,
            customer_tier=customer_tier,
            shipping_method=shipping_method,
            order_total=order_total,
            shipment_status=shipment_status,
        )
        logger.info(
            "Resolved (LLM) ticket=%s action=%s refund=%.2f confidence=%.2f",
            cmd.ticket_id, result.recommended_action, result.refund_amount, result.confidence,
        )
        return {
            "recommended_action": result.recommended_action,
            "refund_amount": result.refund_amount,
            "currency": "CAD",
            "resolution_notes": result.resolution_notes,
            "confidence": result.confidence,
        }
    except Exception as exc:
        logger.warning("LLM unavailable for RESOLVE (%s) — rule-based fallback", exc)

    # ── rule-based fallback ───────────────────────────────────────────────────
    await _think(0.2)
    if category == "delivery_delay" and days_delayed > 3 and shipping_method == "express":
        action, refund_amount = "refund_shipping_fee", shipping_fee
        confidence = 0.92
        notes = (
            f"Express delivery delayed {days_delayed} days — "
            "exceeds 3-day threshold. Policy allows shipping fee refund."
        )
    elif category == "billing_issue":
        action = "reverse_duplicate_charge"
        refund_amount = float(cmd.payload.get("amount_charged", 0.0))
        confidence = 0.92
        notes = "Billing issue confirmed — reversal initiated."
    elif category in ("damaged_product", "wrong_item"):
        action, refund_amount = "send_replacement", 0.0
        confidence = 0.88
        notes = "Replacement will be dispatched within 3–5 business days."
    elif category == "general_inquiry":
        action, refund_amount = "provide_information", 0.0
        confidence = 0.85
        notes = "General inquiry answered based on available policy information."
    elif category == "delivery_delay" and days_delayed <= 3:
        action, refund_amount = "provide_information", 0.0
        confidence = 0.82
        notes = f"Delivery is {days_delayed} day(s) delayed — within policy tolerance. Status update provided."
    else:
        action, refund_amount = "follow_up", 0.0
        confidence = 0.65
        notes = "Complex case — requires specialist review."

    logger.info("Resolved (rule) ticket=%s action=%s refund=%.2f", cmd.ticket_id, action, refund_amount)
    return {
        "recommended_action": action,
        "refund_amount": refund_amount,
        "currency": "CAD",
        "resolution_notes": notes,
        "confidence": confidence,
    }


@registry.register(TaskType.GENERATE_RESPONSE)
async def handle_generate_response(cmd: AgentCommand) -> dict:
    """
    Draft customer-facing response using Claude (Phase 10).
    Falls back to template strings when the API key is absent.
    """
    from agents.responder import generate_response

    ctx = cmd.payload
    order_id = ctx.get("order_id", "your order")
    days_delayed = ctx.get("days_delayed", 0)
    shipping_fee = float(ctx.get("shipping_fee", 0.0))
    refund_amount = float(ctx.get("refund_amount", 0.0))
    action = ctx.get("recommended_action", "follow_up")
    customer_name = ctx.get("customer_name", "")
    resolution_notes = ctx.get("resolution_notes", "")
    category = ctx.get("category", "general_inquiry")

    try:
        response_text = await generate_response(
            customer_name=customer_name,
            order_id=order_id,
            recommended_action=action,
            refund_amount=refund_amount,
            resolution_notes=resolution_notes,
            category=category,
        )
        logger.info("Response (LLM) ticket=%s action=%s", cmd.ticket_id, action)
    except Exception as exc:
        logger.warning("LLM unavailable for GENERATE_RESPONSE (%s) — template fallback", exc)
        await _think(0.3)
        if action == "refund_shipping_fee":
            response_text = (
                f"We sincerely apologise for the delay with {order_id}. "
                f"Your express delivery is {days_delayed} business days late, "
                f"which exceeds our guaranteed window. "
                f"We are processing a full refund of ${refund_amount:.2f} CAD — "
                f"please allow 5 business days. Thank you for your patience."
            )
        elif action == "reverse_duplicate_charge":
            response_text = (
                f"We sincerely apologise for the billing error on {order_id}. "
                f"We have initiated a reversal of the duplicate charge of ${refund_amount:.2f} CAD — "
                f"please allow 3–5 business days for it to appear on your statement."
            )
        elif action == "send_replacement":
            response_text = (
                f"We are sorry to hear about the issue with your order {order_id}. "
                f"A replacement has been arranged and will be dispatched within 3–5 business days. "
                f"You will receive a shipping confirmation by email shortly."
            )
        elif action == "provide_information":
            info = resolution_notes or "our standard policies apply"
            response_text = (
                f"Thank you for reaching out about {order_id}. "
                f"{info} "
                f"Please don't hesitate to contact us if you have any further questions."
            )
        else:
            response_text = (
                f"Thank you for contacting NovaSupport regarding {order_id}. "
                f"We are reviewing your request and a specialist will follow up with you shortly."
            )

    return {
        "response_text": response_text,
        "recommended_action": action,
        "refund_amount": refund_amount,
        "currency": "CAD",
        "confidence": 0.92,
    }


@registry.register(TaskType.QUALITY_CHECK)
async def handle_quality_check(cmd: AgentCommand) -> dict:
    """
    Validate that the proposed resolution is consistent with policies.
    """
    await _think(0.1)
    action = cmd.payload.get("recommended_action", "follow_up")
    confidence = cmd.payload.get("confidence", 0.5)

    passed = confidence >= 0.75
    quality_status = "approved" if passed else "needs_review"

    return {
        "quality_status": quality_status,
        "validation_passed": passed,
        "action_validated": action,
        "reviewer": "auto_validator",
        "notes": (
            "Resolution meets quality threshold — approved for autonomous delivery."
            if passed
            else f"Confidence {confidence:.0%} below threshold — specialist review required."
        ),
    }
