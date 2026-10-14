"""
Event schemas for the NovaSupport event-driven pipeline.

All messages that flow through Kafka use these Pydantic models.
Three layers:
  1. Enums  — controlled vocabularies for task types, statuses, categories
  2. Base   — shared fields every event carries
  3. Events — concrete message types for each topic
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator
from ulid import ULID


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def new_id() -> str:
    """Generate a time-sortable unique ID (ULID)."""
    return str(ULID())


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ──────────────────────────────────────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    TICKET_CREATED   = "ticket.created"
    AGENT_COMMAND    = "agent.command"
    AGENT_RESULT     = "agent.result"
    TICKET_COMPLETED = "ticket.completed"
    TICKET_ESCALATED = "ticket.escalated"


class TaskType(str, Enum):
    TRIAGE_TICKET     = "triage_ticket"
    GET_CUSTOMER      = "get_customer"
    GET_ORDER         = "get_order"
    GET_PAYMENT       = "get_payment"
    GET_SHIPMENT      = "get_shipment"
    RETRIEVE_POLICY   = "retrieve_policy"
    RESOLVE_TICKET    = "resolve_ticket"
    GENERATE_RESPONSE = "generate_response"
    QUALITY_CHECK     = "quality_check"


class TaskStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    RETRYING  = "retrying"


class TicketCategory(str, Enum):
    DELIVERY_DELAY   = "delivery_delay"
    CANCEL_ORDER     = "cancel_order"
    REQUEST_REFUND   = "request_refund"
    PAYMENT_PROBLEM  = "payment_problem"
    DUPLICATE_CHARGE = "duplicate_charge"
    DAMAGED_PRODUCT  = "damaged_product"
    WRONG_PRODUCT    = "wrong_product"
    ACCOUNT_ISSUE    = "account_issue"
    PRODUCT_QUESTION = "product_question"
    UNKNOWN          = "unknown"


class Priority(str, Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"


class WorkflowStatus(str, Enum):
    STARTED    = "started"
    RUNNING    = "running"
    WAITING    = "waiting"
    COMPLETED  = "completed"
    ESCALATED  = "escalated"
    FAILED     = "failed"


# ──────────────────────────────────────────────────────────────────────────────
# Base event — every Kafka message inherits this
# ──────────────────────────────────────────────────────────────────────────────

class BaseEvent(BaseModel):
    """Fields that every event in the system must carry."""

    event_id: str = Field(default_factory=new_id)
    event_type: EventType
    schema_version: str = "1.0"
    timestamp: datetime = Field(default_factory=utcnow)

    model_config = {"use_enum_values": True}

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Topic: ticket.created
# ──────────────────────────────────────────────────────────────────────────────

class TicketCreatedEvent(BaseEvent):
    """
    Published by the FastAPI layer when a customer submits a support ticket.
    This is the entry point of every workflow.
    """

    event_type: EventType = EventType.TICKET_CREATED

    workflow_id: str = Field(default_factory=new_id)
    ticket_id: str
    customer_id: str
    order_id: str | None = None
    message: str
    language: str = "en"
    metadata: dict[str, Any] = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Topic: agent.commands
# ──────────────────────────────────────────────────────────────────────────────

class AgentCommand(BaseEvent):
    """
    Published by the orchestrator to request one unit of work from the worker pool.
    Workers consume these from agent.commands and execute the matching handler.

    correlation_id: copied into the AgentResult so the orchestrator can match
                    which result belongs to which command.
    causation_id:   the event_id that caused this command to be created
                    (usually the TicketCreatedEvent or a previous AgentResult).
    """

    event_type: EventType = EventType.AGENT_COMMAND

    workflow_id: str
    ticket_id: str
    task_id: str = Field(default_factory=new_id)
    task_type: TaskType
    payload: dict[str, Any] = Field(default_factory=dict)
    attempt: int = 1
    correlation_id: str = Field(default_factory=new_id)
    causation_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("attempt")
    @classmethod
    def attempt_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("attempt must be >= 1")
        return v


# ──────────────────────────────────────────────────────────────────────────────
# Topic: agent.results
# ──────────────────────────────────────────────────────────────────────────────

class AgentResult(BaseEvent):
    """
    Published by the worker after executing a task.
    The orchestrator reads this to advance the workflow state.

    status = completed → result contains the task output
    status = failed    → error contains the failure description
    """

    event_type: EventType = EventType.AGENT_RESULT

    workflow_id: str
    ticket_id: str
    task_id: str
    task_type: TaskType
    status: TaskStatus
    result: dict[str, Any] | None = None
    error: str | None = None
    attempt: int = 1
    correlation_id: str
    causation_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("result", "error", mode="before")
    @classmethod
    def validate_result_or_error(cls, v: Any) -> Any:
        return v

    def is_success(self) -> bool:
        return self.status == TaskStatus.COMPLETED

    def is_failure(self) -> bool:
        return self.status == TaskStatus.FAILED


# ──────────────────────────────────────────────────────────────────────────────
# Topic: ticket.completed
# ──────────────────────────────────────────────────────────────────────────────

class PolicySource(BaseModel):
    """A single RAG retrieval result included as evidence in the final response."""
    content: str
    source: str
    section: str
    score: float


class TicketCompletedEvent(BaseEvent):
    """
    Published by the orchestrator when the workflow reaches a terminal state.
    Contains the final customer-facing response and all supporting evidence.
    """

    event_type: EventType = EventType.TICKET_COMPLETED

    workflow_id: str
    ticket_id: str
    resolution: dict[str, Any]
    final_response: str
    quality_status: str  # "approved" | "rejected"
    sources: list[PolicySource] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Topic: ticket.escalated
# ──────────────────────────────────────────────────────────────────────────────

class TicketEscalatedEvent(BaseEvent):
    """
    Published when the workflow cannot be resolved automatically and a human
    agent must take over. Includes everything the human needs to continue.
    """

    event_type: EventType = EventType.TICKET_ESCALATED

    workflow_id: str
    ticket_id: str
    customer_id: str
    escalation_reason: str
    attempted_resolutions: list[str] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Serialization helpers
# ──────────────────────────────────────────────────────────────────────────────

def serialize(event: BaseEvent) -> bytes:
    """Serialize an event to UTF-8 JSON bytes for Kafka producer."""
    return event.model_dump_json().encode("utf-8")


def deserialize_command(data: bytes) -> AgentCommand:
    """Deserialize raw Kafka bytes into an AgentCommand."""
    return AgentCommand.model_validate_json(data)


def deserialize_result(data: bytes) -> AgentResult:
    """Deserialize raw Kafka bytes into an AgentResult."""
    return AgentResult.model_validate_json(data)


def deserialize_ticket_created(data: bytes) -> TicketCreatedEvent:
    """Deserialize raw Kafka bytes into a TicketCreatedEvent."""
    return TicketCreatedEvent.model_validate_json(data)
