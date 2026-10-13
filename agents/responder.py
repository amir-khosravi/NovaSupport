"""
Response generation agent — drafts the customer-facing message.

Takes the resolution decision and ticket context, produces a professional,
empathetic reply personalised to the customer's name and situation.
"""

from __future__ import annotations

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate


_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a customer support agent at NovaSupport, an e-commerce platform.

Write a professional, empathetic 2–3 sentence reply to the customer.

Guidelines:
- Address the customer by name if provided
- Be specific: mention the order ID and relevant details when you have them
- refund_shipping_fee: confirm the exact refund amount and 5 business-day processing time
- reverse_duplicate_charge: apologise and confirm the reversal timeline (3–5 days)
- send_replacement: confirm a replacement is being arranged within 3–5 business days
- full_refund: confirm the full refund amount and 5 business-day processing time
- provide_information: answer their question clearly and helpfully using the context notes
- follow_up: acknowledge the issue, apologise, and say a specialist will contact them within 24 hours
- Tone: warm, professional, apologetic where appropriate
- Do NOT add subject lines, sign-offs, or email headers — just the body text""",
    ),
    (
        "human",
        """Customer name: {customer_name}
Order ID: {order_id}
Issue category: {category}
Resolution action: {recommended_action}
Refund amount: ${refund_amount:.2f}
Context and notes: {resolution_notes}

Write the customer reply now.""",
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
        temperature=0.3,
        max_tokens=300,
    )


async def generate_response(
    customer_name: str,
    order_id: str,
    recommended_action: str,
    refund_amount: float,
    resolution_notes: str = "",
    category: str = "general_inquiry",
) -> str:
    """Generate customer-facing response text. Raises ValueError if API key missing."""
    chain = _PROMPT | _llm() | StrOutputParser()
    return await chain.ainvoke({
        "customer_name": customer_name or "valued customer",
        "order_id": order_id or "your order",
        "category": category,
        "recommended_action": recommended_action,
        "refund_amount": refund_amount,
        "resolution_notes": resolution_notes,
    })
