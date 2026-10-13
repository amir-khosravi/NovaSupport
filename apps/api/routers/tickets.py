"""
Ticket endpoints — the primary intake path for NovaSupport.

POST /api/v1/tickets        — submit a new support ticket
GET  /api/v1/tickets/{id}/status — poll workflow outcome
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from apps.api.deps import get_producer, get_repo
from apps.api.models import SubmitTicketRequest, TicketAcceptedResponse, WorkflowStatusResponse
from apps.api.security import check_rate_limit
from config.settings import settings
from observability.metrics import inc_tickets_submitted
from persistence.workflow_repository import WorkflowRepository
from streaming.producer import KafkaEventProducer
from streaming.schemas import TicketCreatedEvent, new_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["tickets"])


@router.post(
    "/tickets",
    status_code=202,
    response_model=TicketAcceptedResponse,
    summary="Submit a support ticket",
)
async def submit_ticket(
    body: SubmitTicketRequest,
    producer: KafkaEventProducer = Depends(get_producer),
    _security: None = Depends(check_rate_limit),
) -> TicketAcceptedResponse:
    """
    Accepts a customer support ticket, publishes a TicketCreatedEvent to Kafka,
    and returns 202 Accepted with the workflow_id for status polling.
    """
    ticket_id = f"TKT-{new_id()}"
    event = TicketCreatedEvent(
        ticket_id=ticket_id,
        customer_id=body.customer_id,
        order_id=body.order_id,
        message=body.message,
        language=body.language,
    )

    try:
        await producer.send(
            settings.kafka.ticket_topic, event, key=event.workflow_id
        )
    except Exception as exc:
        logger.error("Failed to publish TicketCreatedEvent ticket=%s: %s", ticket_id, exc)
        raise HTTPException(status_code=503, detail="Event stream unavailable. Please retry.") from exc

    inc_tickets_submitted(language=body.language)
    logger.info(
        "Ticket accepted  ticket_id=%s  workflow_id=%s  customer=%s",
        ticket_id, event.workflow_id, body.customer_id,
    )
    return TicketAcceptedResponse(
        workflow_id=event.workflow_id,
        ticket_id=ticket_id,
    )


@router.get(
    "/tickets/{ticket_id}/status",
    response_model=WorkflowStatusResponse,
    summary="Poll ticket workflow status",
)
async def get_ticket_status(
    ticket_id: str,
    repo: WorkflowRepository = Depends(get_repo),
) -> WorkflowStatusResponse:
    """
    Returns the current workflow state for a ticket.
    Status values: running | completed | escalated | failed
    """
    record = await repo.get_by_ticket_id(ticket_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_id!r} not found.")

    return WorkflowStatusResponse(
        workflow_id=record.workflow_id,
        ticket_id=record.ticket_id,
        status=record.status,
        category=record.category,
        recommended_action=record.recommended_action,
        refund_amount=record.refund_amount,
        final_response=record.final_response,
        escalation_reason=record.escalation_reason,
    )
