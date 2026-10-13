"""
Resolution agent — determines the correct action for a support ticket.

Combines order context, shipment data, and policy passages to reason about
what should be done. Aims to resolve autonomously in most cases — escalation
(follow_up, confidence < 0.75) should be rare.
"""

from __future__ import annotations

from typing import Literal

from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field


class ResolverResult(BaseModel):
    recommended_action: Literal[
        "refund_shipping_fee",
        "reverse_duplicate_charge",
        "send_replacement",
        "full_refund",
        "provide_information",
        "follow_up",
    ] = Field(description="The resolution action to take")
    refund_amount: float = Field(
        description="Amount to refund in local currency. 0.0 unless a financial refund applies."
    )
    resolution_notes: str = Field(
        description=(
            "Internal explanation of the resolution. For provide_information, include the "
            "key answer/information to share with the customer."
        )
    )
    confidence: float = Field(
        description="Confidence 0.0–1.0. Must be >= 0.75 for actions you're confident in."
    )


_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a customer support resolution specialist for NovaSupport.
Your goal is to RESOLVE issues autonomously using the FACTS provided — do not rely solely on the category label.
Only use follow_up when the situation is truly impossible to decide from the available information.

DECISION RULES — look at the FACTS and apply the FIRST match:

RULE 1 — Express shipping refund:
  IF shipping_method = express AND days_delayed > 3 AND the customer mentions shipping delay or wants a shipping refund:
  → recommended_action: refund_shipping_fee
  → refund_amount: exactly the shipping_fee value
  → confidence: 0.92
  (This applies regardless of whether category is "delivery_delay", "refund_request", etc.)

RULE 2 — Duplicate / incorrect charge:
  IF the customer reports being charged twice or an incorrect amount:
  → recommended_action: reverse_duplicate_charge
  → refund_amount: the incorrectly charged amount
  → confidence: 0.95

RULE 3 — Damaged or wrong item:
  IF the customer received a damaged product or the wrong item:
  → recommended_action: send_replacement
  → refund_amount: 0.0
  → resolution_notes: explain replacement will be dispatched within 3–5 business days
  → confidence: 0.88

RULE 4 — General question or policy inquiry (no order fault):
  IF the customer is asking a how-to or policy question without reporting an order problem:
  → recommended_action: provide_information
  → refund_amount: 0.0
  → resolution_notes: write a complete, helpful answer using the policy passages
  → confidence: 0.85

RULE 5 — Standard shipping delay or delay within policy:
  IF shipping is NOT express OR days_delayed <= 3:
  → recommended_action: provide_information
  → resolution_notes: explain current status and expected delivery update
  → confidence: 0.82

RULE 6 — Other clear, resolvable issue:
  → choose the most appropriate action, set confidence >= 0.80

RULE 7 — LAST RESORT only:
  IF none of the above apply AND the situation is genuinely ambiguous or requires legal review:
  → recommended_action: follow_up, confidence: 0.65
  This should be RARE. Most tickets can be resolved with rules 1–6.

Policy passages:
{policy_text}

Customer tier: {customer_tier}""",
    ),
    (
        "human",
        """Ticket facts:
- Customer message: "{message}"
- Triage category: {category}
- Days delayed: {days_delayed}
- Shipping method: {shipping_method}
- Shipping fee: ${shipping_fee:.2f}
- Order total: ${order_total:.2f}
- Shipment status: {shipment_status}
- Customer tier: {customer_tier}

Apply the decision rules based on the FACTS above and determine the resolution.""",
    ),
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


def _format_policy(sources: list[dict]) -> str:
    if not sources:
        return "No policy passages retrieved."
    return "\n".join(
        f"[{s.get('policy_id', 'policy')}] {s.get('passage', '')}"
        for s in sources[:3]
    )


async def resolve_ticket(
    category: str,
    message: str,
    days_delayed: int,
    shipping_fee: float,
    policy_sources: list[dict],
    customer_tier: str = "standard",
    shipping_method: str = "standard",
    order_total: float = 0.0,
    shipment_status: str = "unknown",
) -> ResolverResult:
    """Determine resolution action using Claude. Raises ValueError if API key missing."""
    chain = _PROMPT | _llm().with_structured_output(ResolverResult)
    return await chain.ainvoke({
        "category": category,
        "message": message,
        "days_delayed": days_delayed,
        "shipping_fee": shipping_fee,
        "shipping_method": shipping_method,
        "order_total": order_total,
        "shipment_status": shipment_status,
        "policy_text": _format_policy(policy_sources),
        "customer_tier": customer_tier,
    })
