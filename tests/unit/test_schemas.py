"""Unit tests for event schemas."""

import json
from datetime import datetime, timezone

import pytest

from streaming.schemas import (
    AgentCommand,
    AgentResult,
    PolicySource,
    TaskStatus,
    TaskType,
    TicketCategory,
    TicketCompletedEvent,
    TicketCreatedEvent,
    TicketEscalatedEvent,
    deserialize_command,
    deserialize_result,
    deserialize_ticket_created,
    new_id,
    serialize,
)


# ──────────────────────────────────────────────────────────────────────────────
# TicketCreatedEvent
# ──────────────────────────────────────────────────────────────────────────────

class TestTicketCreatedEvent:
    def test_defaults_are_populated(self):
        evt = TicketCreatedEvent(
            ticket_id="TKT-0001",
            customer_id="C-0001",
            message="My order is late.",
        )
        assert evt.event_id != ""
        assert evt.workflow_id != ""
        assert evt.schema_version == "1.0"
        assert evt.language == "en"
        assert evt.order_id is None
        assert isinstance(evt.timestamp, datetime)

    def test_event_type_is_fixed(self):
        evt = TicketCreatedEvent(
            ticket_id="TKT-0001",
            customer_id="C-0001",
            message="test",
        )
        assert evt.event_type == "ticket.created"

    def test_with_order_id(self):
        evt = TicketCreatedEvent(
            ticket_id="TKT-0001",
            customer_id="C-0001",
            order_id="ORD-0001",
            message="My express delivery is 5 days late.",
        )
        assert evt.order_id == "ORD-0001"

    def test_timestamp_gets_utc_when_naive(self):
        naive = datetime(2025, 1, 1, 12, 0, 0)
        evt = TicketCreatedEvent(
            ticket_id="T1",
            customer_id="C1",
            message="test",
            timestamp=naive,
        )
        assert evt.timestamp.tzinfo == timezone.utc


# ──────────────────────────────────────────────────────────────────────────────
# AgentCommand
# ──────────────────────────────────────────────────────────────────────────────

class TestAgentCommand:
    def test_basic_command(self):
        cmd = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.TRIAGE_TICKET,
            payload={"message": "My order is late."},
            correlation_id=new_id(),
        )
        assert cmd.task_type == "triage_ticket"
        assert cmd.attempt == 1
        assert cmd.task_id != ""
        assert cmd.event_type == "agent.command"

    def test_attempt_must_be_positive(self):
        with pytest.raises(Exception):
            AgentCommand(
                workflow_id="WF-001",
                ticket_id="TKT-0001",
                task_type=TaskType.GET_ORDER,
                correlation_id=new_id(),
                attempt=0,
            )

    def test_all_task_types_are_valid(self):
        for task_type in TaskType:
            cmd = AgentCommand(
                workflow_id="WF-001",
                ticket_id="TKT-0001",
                task_type=task_type,
                correlation_id=new_id(),
            )
            assert cmd.task_type == task_type.value

    def test_causation_id_optional(self):
        cmd = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.GET_ORDER,
            correlation_id=new_id(),
            causation_id="some-prior-event-id",
        )
        assert cmd.causation_id == "some-prior-event-id"


# ──────────────────────────────────────────────────────────────────────────────
# AgentResult
# ──────────────────────────────────────────────────────────────────────────────

class TestAgentResult:
    def test_successful_result(self):
        result = AgentResult(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_id="TASK-001",
            task_type=TaskType.TRIAGE_TICKET,
            status=TaskStatus.COMPLETED,
            result={"category": "delivery_delay", "priority": "high"},
            correlation_id="CORR-001",
        )
        assert result.is_success() is True
        assert result.is_failure() is False
        assert result.error is None

    def test_failed_result(self):
        result = AgentResult(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_id="TASK-001",
            task_type=TaskType.GET_ORDER,
            status=TaskStatus.FAILED,
            error="Order not found in database",
            correlation_id="CORR-001",
        )
        assert result.is_failure() is True
        assert result.is_success() is False
        assert result.result is None


# ──────────────────────────────────────────────────────────────────────────────
# TicketCompletedEvent
# ──────────────────────────────────────────────────────────────────────────────

class TestTicketCompletedEvent:
    def test_with_policy_sources(self):
        source = PolicySource(
            content="If express delivery is delayed by more than 3 business days, the shipping fee may be refunded.",
            source="express_delivery_policy.md",
            section="Compensation Rules",
            score=0.91,
        )
        evt = TicketCompletedEvent(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            resolution={"action": "refund_shipping_fee", "amount": 12.99},
            final_response="We apologize for the delay. A refund of $12.99 has been processed.",
            quality_status="approved",
            sources=[source],
        )
        assert len(evt.sources) == 1
        assert evt.sources[0].score == 0.91
        assert evt.quality_status == "approved"


# ──────────────────────────────────────────────────────────────────────────────
# Serialization round-trip
# ──────────────────────────────────────────────────────────────────────────────

class TestSerialization:
    def test_ticket_created_roundtrip(self):
        original = TicketCreatedEvent(
            ticket_id="TKT-0001",
            customer_id="C-0001",
            order_id="ORD-0001",
            message="My order is late.",
        )
        raw = serialize(original)
        assert isinstance(raw, bytes)
        restored = deserialize_ticket_created(raw)
        assert restored.ticket_id == original.ticket_id
        assert restored.workflow_id == original.workflow_id
        assert restored.event_id == original.event_id

    def test_command_roundtrip(self):
        corr_id = new_id()
        original = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.TRIAGE_TICKET,
            payload={"message": "test"},
            correlation_id=corr_id,
        )
        raw = serialize(original)
        restored = deserialize_command(raw)
        assert restored.correlation_id == corr_id
        assert restored.task_type == "triage_ticket"

    def test_result_roundtrip(self):
        original = AgentResult(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_id="TASK-001",
            task_type=TaskType.GET_ORDER,
            status=TaskStatus.COMPLETED,
            result={"order_status": "in_transit", "shipping_method": "express"},
            correlation_id="CORR-001",
        )
        raw = serialize(original)
        restored = deserialize_result(raw)
        assert restored.result["shipping_method"] == "express"
        assert restored.is_success() is True

    def test_serialized_json_is_valid(self):
        cmd = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.RETRIEVE_POLICY,
            correlation_id=new_id(),
        )
        raw = serialize(cmd)
        parsed = json.loads(raw)
        assert parsed["event_type"] == "agent.command"
        assert parsed["task_type"] == "retrieve_policy"
        assert "timestamp" in parsed
        assert "schema_version" in parsed

    def test_correlation_id_preserved_through_command_result_cycle(self):
        """The correlation_id set on a command must appear unchanged in its result."""
        corr_id = new_id()
        cmd = AgentCommand(
            workflow_id="WF-001",
            ticket_id="TKT-0001",
            task_type=TaskType.TRIAGE_TICKET,
            correlation_id=corr_id,
        )
        # Worker creates result using cmd's correlation_id
        result = AgentResult(
            workflow_id=cmd.workflow_id,
            ticket_id=cmd.ticket_id,
            task_id=cmd.task_id,
            task_type=cmd.task_type,
            status=TaskStatus.COMPLETED,
            result={"category": "delivery_delay"},
            correlation_id=cmd.correlation_id,  # <- key contract
            causation_id=cmd.event_id,
        )
        assert result.correlation_id == corr_id
        assert result.causation_id == cmd.event_id


# ──────────────────────────────────────────────────────────────────────────────
# Enums coverage
# ──────────────────────────────────────────────────────────────────────────────

class TestEnums:
    def test_all_task_types_exist(self):
        expected = {
            "triage_ticket", "get_customer", "get_order", "get_payment",
            "get_shipment", "retrieve_policy", "resolve_ticket",
            "generate_response", "quality_check",
        }
        actual = {t.value for t in TaskType}
        assert expected == actual

    def test_ticket_categories(self):
        assert TicketCategory.DELIVERY_DELAY.value == "delivery_delay"
        assert TicketCategory.DUPLICATE_CHARGE.value == "duplicate_charge"

    def test_task_status_terminal_states(self):
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
