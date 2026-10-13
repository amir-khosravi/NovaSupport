"""
Ticket triage agent — classifies customer messages using Claude.

Replaces keyword matching with semantic understanding so edge cases
like "my parcel seems lost" are correctly identified as delivery_delay.
"""

from __future__ import annotations

from typing import Literal

from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field


class TriageResult(BaseModel):
    category: Literal[
        "delivery_delay",
        "refund_request",
        "wrong_item",
        "billing_issue",
        "general_inquiry",
    ] = Field(description="Best-fit category for this support ticket")
    priority: Literal["high", "medium", "low"] = Field(
        description="high=urgent/financial; medium=standard issue; low=informational"
    )
    requires_order_lookup: bool = Field(
        description="True when order/shipment details are needed to resolve the ticket"
    )
    requires_policy_retrieval: bool = Field(
        description="True when company policies must be checked (refunds, delays, damages)"
    )
    language: str = Field(description="2-letter ISO language code of the customer message")


_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a customer support classifier for NovaSupport, an e-commerce platform.

Classify the incoming customer message into exactly one category:
- delivery_delay: order is late, missing, stuck in transit, or tracking not updated
- refund_request: customer wants money back, cancellation refund, or return credit
- wrong_item: received incorrect, damaged, or missing product
- billing_issue: duplicate charge, overcharge, payment error, or invoice problem
- general_inquiry: account questions, product info, how-to, or anything else

Priority rules:
- high: express delivery issue, financial loss, urgent language, or VIP context
- medium: standard complaint needing investigation
- low: question or informational request

Lookup rules:
- requires_order_lookup: true for delivery, wrong-item, billing, refund (need order data)
- requires_policy_retrieval: true for delivery delays, refund requests, billing issues

Return the 2-letter language code detected (e.g. "en", "fr", "es").""",
    ),
    ("human", "Customer message:\n{message}"),
])


def _llm():
    from langchain_anthropic import ChatAnthropic
    from config.settings import settings

    if not settings.llm.anthropic_api_key or settings.llm.anthropic_api_key.startswith("your-"):
        raise ValueError("ANTHROPIC_API_KEY not configured")
    return ChatAnthropic(
        model=settings.llm.model,
        api_key=settings.llm.anthropic_api_key,
        temperature=0,
        max_tokens=512,
    )


async def triage_ticket(message: str) -> TriageResult:
    """Classify a support ticket. Raises ValueError if API key is missing."""
    chain = _PROMPT | _llm().with_structured_output(TriageResult)
    return await chain.ainvoke({"message": message})
