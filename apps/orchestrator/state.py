"""
LangGraph workflow state for NovaSupport.

WorkflowState is the single source of truth for a ticket's lifecycle.
Each graph node reads from it and returns a partial update dict.
LangGraph merges updates automatically after each node completes.

State lifecycle:
  ticket fields set at intake → triage fills category/priority →
  fetch nodes fill order/shipment → policy fills sources →
  resolve fills resolution → generate fills response →
  quality fills quality_status → complete fills final_*
"""

from __future__ import annotations

from typing import TypedDict


class WorkflowState(TypedDict, total=False):
    # ── ticket intake (set once, never mutated) ───────────────────────────────
    workflow_id: str
    ticket_id: str
    customer_id: str
    order_id: str | None
    message: str
    language: str

    # ── triage (TRIAGE_TICKET) ────────────────────────────────────────────────
    triage_category: str          # e.g. "delivery_delay"
    triage_priority: str          # "high" | "medium" | "low"
    requires_order_lookup: bool
    requires_policy_retrieval: bool

    # ── customer context (GET_CUSTOMER) ──────────────────────────────────────
    customer_name: str
    customer_tier: str            # "gold" | "silver" | "standard"
    customer_email: str

    # ── order context (GET_ORDER + GET_SHIPMENT) ──────────────────────────────
    order_shipping_method: str    # "express" | "standard"
    order_status: str
    order_total: float
    shipping_fee: float

    shipment_status: str          # "delayed" | "in_transit" | "delivered"
    days_delayed: int
    carrier: str
    tracking_number: str

    # ── policy retrieval (RETRIEVE_POLICY) ───────────────────────────────────
    policy_sources: list[dict]    # [{policy_id, title, passage, relevance_score}]

    # ── resolution (RESOLVE_TICKET) ──────────────────────────────────────────
    recommended_action: str       # "refund_shipping_fee" | "follow_up" | ...
    refund_amount: float
    resolution_notes: str
    resolution_confidence: float

    # ── response generation (GENERATE_RESPONSE) ──────────────────────────────
    response_text: str

    # ── quality check (QUALITY_CHECK) ────────────────────────────────────────
    quality_status: str           # "approved" | "needs_review"
    validation_passed: bool

    # ── terminal state ────────────────────────────────────────────────────────
    final_response: str
    final_action: str
    escalation_reason: str | None
    workflow_error: str | None
    completed: bool
