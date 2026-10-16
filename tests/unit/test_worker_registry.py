"""
Unit tests for the worker HandlerRegistry and all mock handlers.

No Kafka needed — tests run against in-memory registry.
"""

import pytest

from streaming.schemas import AgentCommand, TaskType, new_id
from apps.worker.registry import HandlerRegistry
from apps.worker.handlers import registry as default_registry


def _make_command(task_type: TaskType, payload: dict | None = None) -> AgentCommand:
    wf = new_id()
    return AgentCommand(
        workflow_id=wf,
        ticket_id="TKT-0001",
        task_type=task_type,
        payload=payload or {},
        correlation_id=new_id(),
    )


# ── HandlerRegistry mechanics ─────────────────────────────────────────────────

class TestHandlerRegistry:

    def test_register_and_dispatch(self):
        reg = HandlerRegistry()

        @reg.register(TaskType.TRIAGE_TICKET)
        async def my_handler(cmd: AgentCommand) -> dict:
            return {"ok": True}

        assert "triage_ticket" in reg.registered_types

    def test_duplicate_registration_raises(self):
        reg = HandlerRegistry()

        @reg.register(TaskType.QUALITY_CHECK)
        async def h1(cmd: AgentCommand) -> dict:
            return {}

        with pytest.raises(ValueError, match="already registered"):
            @reg.register(TaskType.QUALITY_CHECK)
            async def h2(cmd: AgentCommand) -> dict:
                return {}

    async def test_dispatch_calls_handler(self):
        reg = HandlerRegistry()

        @reg.register(TaskType.TRIAGE_TICKET)
        async def h(cmd: AgentCommand) -> dict:
            return {"called": True, "task_id": cmd.task_id}

        cmd = _make_command(TaskType.TRIAGE_TICKET)
        result = await reg.dispatch(cmd)
        assert result["called"] is True
        assert result["task_id"] == cmd.task_id

    async def test_dispatch_unknown_type_raises(self):
        reg = HandlerRegistry()
        cmd = _make_command(TaskType.TRIAGE_TICKET)
        with pytest.raises(KeyError, match="No handler registered"):
            await reg.dispatch(cmd)


# ── Default registry has all 9 handlers ──────────────────────────────────────

class TestDefaultRegistry:

    def test_all_task_types_registered(self):
        registered = set(default_registry.registered_types)
        expected = {t.value for t in TaskType}
        assert registered == expected, f"Missing: {expected - registered}"


# ── Individual handler outputs ────────────────────────────────────────────────

class TestTriageHandler:

    async def test_express_delay_classified_correctly(self):
        cmd = _make_command(
            TaskType.TRIAGE_TICKET,
            {"message": "My express order is 5 days delayed.", "language": "en"},
        )
        result = await default_registry.dispatch(cmd)
        assert result["category"] == "delivery_delay"
        assert result["priority"] == "high"
        assert result["requires_order_lookup"] is True
        assert result["requires_policy_retrieval"] is True

    async def test_refund_message_classified_correctly(self):
        cmd = _make_command(
            TaskType.TRIAGE_TICKET,
            {"message": "I want a refund for my order.", "language": "en"},
        )
        result = await default_registry.dispatch(cmd)
        assert result["category"] == "refund_request"

    async def test_generic_message_is_low_priority(self):
        cmd = _make_command(
            TaskType.TRIAGE_TICKET,
            {"message": "Just checking in on my account.", "language": "en"},
        )
        result = await default_registry.dispatch(cmd)
        assert result["category"] == "general_inquiry"
        assert result["priority"] == "low"


class TestGetOrderHandler:

    async def test_returns_order_dict(self):
        cmd = _make_command(
            TaskType.GET_ORDER,
            {"order_id": "ORD-0001", "customer_id": "C-0001"},
        )
        result = await default_registry.dispatch(cmd)
        assert result["order_id"] == "ORD-0001"
        assert result["shipping_method"] == "express"
        assert "items" in result


class TestGetShipmentHandler:

    async def test_returns_shipment_with_delay(self):
        cmd = _make_command(
            TaskType.GET_SHIPMENT,
            {"order_id": "ORD-0001"},
        )
        result = await default_registry.dispatch(cmd)
        assert result["shipment_status"] == "delayed"
        # days_delayed comes from real DB when MCP server is running, or mock (5) otherwise
        assert isinstance(result["days_delayed"], int)
        assert result["days_delayed"] > 0


class TestRetrievePolicyHandler:
    # Full retriever tests (with live Qdrant) are in tests/integration/test_rag_pipeline.py.
    # This unit test only verifies the handler returns valid structure.

    async def test_delivery_delay_returns_valid_structure(self):
        """Handler returns sources list with required fields (Qdrant or fallback)."""
        cmd = _make_command(
            TaskType.RETRIEVE_POLICY,
            {"category": "delivery_delay", "shipping_method": "express"},
        )
        result = await default_registry.dispatch(cmd)
        assert len(result["sources"]) >= 1
        assert "retrieval_method" in result
        first = result["sources"][0]
        assert "policy_id" in first
        assert "passage" in first
        assert "relevance_score" in first


class TestResolveTicketHandler:

    async def test_delayed_order_triggers_refund_action(self):
        cmd = _make_command(
            TaskType.RESOLVE_TICKET,
            {
                "order_id": "ORD-0001",
                "category": "delivery_delay",
                "days_delayed": 5,
                "shipping_fee": 24.99,
            },
        )
        result = await default_registry.dispatch(cmd)
        assert result["recommended_action"] == "refund_shipping_fee"
        assert result["refund_amount"] == pytest.approx(24.99)

    async def test_on_time_order_no_refund(self):
        cmd = _make_command(
            TaskType.RESOLVE_TICKET,
            {
                "order_id": "ORD-0002",
                "category": "delivery_delay",
                "days_delayed": 1,
                "shipping_fee": 9.99,
            },
        )
        result = await default_registry.dispatch(cmd)
        assert result["recommended_action"] == "follow_up"
        assert result["refund_amount"] == pytest.approx(0.0)


class TestGenerateResponseHandler:

    async def test_refund_action_produces_refund_text(self):
        cmd = _make_command(
            TaskType.GENERATE_RESPONSE,
            {
                "order_id": "ORD-0001",
                "recommended_action": "refund_shipping_fee",
                "days_delayed": 5,
                "shipping_fee": 24.99,
                "refund_amount": 24.99,
            },
        )
        result = await default_registry.dispatch(cmd)
        assert "refund" in result["response_text"].lower()
        assert result["recommended_action"] == "refund_shipping_fee"

    async def test_followup_action_produces_generic_text(self):
        cmd = _make_command(
            TaskType.GENERATE_RESPONSE,
            {
                "order_id": "ORD-0002",
                "recommended_action": "follow_up",
                "days_delayed": 1,
                "shipping_fee": 9.99,
                "refund_amount": 0.0,
            },
        )
        result = await default_registry.dispatch(cmd)
        assert result["recommended_action"] == "follow_up"


class TestQualityCheckHandler:

    async def test_high_confidence_passes(self):
        cmd = _make_command(
            TaskType.QUALITY_CHECK,
            {"recommended_action": "refund_shipping_fee", "confidence": 0.92},
        )
        result = await default_registry.dispatch(cmd)
        assert result["validation_passed"] is True
        assert result["quality_status"] == "approved"

    async def test_low_confidence_fails(self):
        cmd = _make_command(
            TaskType.QUALITY_CHECK,
            {"recommended_action": "refund_shipping_fee", "confidence": 0.50},
        )
        result = await default_registry.dispatch(cmd)
        assert result["validation_passed"] is False
        assert result["quality_status"] == "needs_review"
