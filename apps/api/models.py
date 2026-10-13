"""Pydantic request / response models for the FastAPI intake service."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class SubmitTicketRequest(BaseModel):
    customer_id: str = Field(min_length=1, description="Customer account ID")
    order_id: str | None = Field(None, description="Related order ID (optional)")
    message: str = Field(min_length=1, max_length=2000, description="Customer's support message")
    language: str = Field("en", description="Preferred response language")

    @field_validator("message", "customer_id", mode="before")
    @classmethod
    def sanitize(cls, v: object) -> object:
        """Strip whitespace and remove null bytes to prevent injection vectors."""
        if isinstance(v, str):
            return v.strip().replace("\x00", "")
        return v


class TicketAcceptedResponse(BaseModel):
    workflow_id: str
    ticket_id: str
    status: str = "accepted"
    detail: str = "Ticket submitted. You will receive a response shortly."


class WorkflowStatusResponse(BaseModel):
    workflow_id: str
    ticket_id: str
    status: str                          # running | completed | escalated | failed
    category: str | None = None
    recommended_action: str | None = None
    refund_amount: float = 0.0
    final_response: str | None = None
    escalation_reason: str | None = None


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    service: str = "novasupport-api"
