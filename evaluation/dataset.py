"""
Evaluation dataset for NovaSupport.

Each EvalCase defines:
  - The ticket submitted by the customer
  - The expected triage category (what the LLM *should* classify it as)
  - The expected resolution action
  - Keywords that *must* appear in the final response
  - Whether the case uses real DB data (needs seeded postgres + MCP server)
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EvalCase:
    case_id: str
    message: str
    expected_category: str
    expected_action: str
    customer_id: str = "C-0001"
    order_id: str | None = None
    language: str = "en"
    # Soft keywords — response_quality is measured by how many appear
    response_keywords: list[str] = field(default_factory=list)
    # If True, test requires running postgres + MCP server
    requires_db: bool = False
    # A description of what this case is testing
    description: str = ""


EVAL_DATASET: list[EvalCase] = [
    # ── Delivery delay cases ──────────────────────────────────────────────────

    EvalCase(
        case_id="EVAL-001",
        description="Express delivery 5 days late — should trigger shipping refund",
        customer_id="C-0001",
        order_id="ORD-0001",
        message=(
            "My express delivery is 5 days late. "
            "I paid extra for express shipping and it still hasn't arrived. "
            "I want a refund for the shipping fee."
        ),
        expected_category="delivery_delay",
        expected_action="refund_shipping_fee",
        response_keywords=["refund", "shipping", "apologize"],
        requires_db=True,
    ),

    EvalCase(
        case_id="EVAL-002",
        description="Express delivery delay — phrased as a question",
        customer_id="C-0001",
        order_id="ORD-0001",
        message=(
            "Where is my order? It was supposed to arrive via express 3 days ago "
            "and I still have nothing. Can you look into this?"
        ),
        expected_category="delivery_delay",
        expected_action="refund_shipping_fee",
        response_keywords=["delayed", "order"],
        requires_db=True,
    ),

    EvalCase(
        case_id="EVAL-003",
        description="General delivery inquiry — no order attached",
        message=(
            "I ordered something a week ago and it's late. "
            "How long does standard shipping usually take?"
        ),
        expected_category="delivery_delay",
        expected_action="provide_info",
        response_keywords=["delivery", "shipping"],
        requires_db=False,
    ),

    # ── Billing / refund cases ─────────────────────────────────────────────────

    EvalCase(
        case_id="EVAL-004",
        description="Duplicate charge on credit card",
        message=(
            "I was charged twice for the same order. "
            "I can see two identical charges on my credit card statement. "
            "Please refund the duplicate charge immediately."
        ),
        expected_category="billing_dispute",
        expected_action="process_refund",
        response_keywords=["charge", "refund"],
        requires_db=False,
    ),

    EvalCase(
        case_id="EVAL-005",
        description="General billing question — how do refunds work",
        message=(
            "How long does it take to get a refund? "
            "I returned a product last week and haven't received my money back."
        ),
        expected_category="billing_dispute",
        expected_action="provide_info",
        response_keywords=["refund", "days"],
        requires_db=False,
    ),

    # ── Product issue cases ────────────────────────────────────────────────────

    EvalCase(
        case_id="EVAL-006",
        description="Damaged product on arrival",
        message=(
            "The item I received was completely damaged. "
            "The box was crushed and the product inside is broken. "
            "I need a replacement or a full refund."
        ),
        expected_category="product_issue",
        expected_action="replacement",
        response_keywords=["damaged", "replacement", "refund"],
        requires_db=False,
    ),

    EvalCase(
        case_id="EVAL-007",
        description="Wrong item received",
        message=(
            "You sent me the wrong product. I ordered a blue jacket size M "
            "but received a red one in size XL. Please send the correct item."
        ),
        expected_category="product_issue",
        expected_action="replacement",
        response_keywords=["wrong", "correct", "replace"],
        requires_db=False,
    ),

    # ── General inquiry cases ─────────────────────────────────────────────────

    EvalCase(
        case_id="EVAL-008",
        description="General return policy question",
        message=(
            "What is your return policy? "
            "I bought a product last month and I'm not happy with it."
        ),
        expected_category="general_inquiry",
        expected_action="provide_info",
        response_keywords=["return", "policy"],
        requires_db=False,
    ),

    EvalCase(
        case_id="EVAL-009",
        description="Account / order status question",
        message=(
            "Can you tell me the status of my account? "
            "I'm trying to see my order history."
        ),
        expected_category="general_inquiry",
        expected_action="provide_info",
        response_keywords=["account", "order"],
        requires_db=False,
    ),

    EvalCase(
        case_id="EVAL-010",
        description="Aggressive tone — should still classify correctly",
        customer_id="C-0001",
        order_id="ORD-0001",
        message=(
            "This is absolutely unacceptable! My express order is LATE again! "
            "I want my money back NOW. This is the worst service I've ever had!"
        ),
        expected_category="delivery_delay",
        expected_action="refund_shipping_fee",
        response_keywords=["apologize", "refund"],
        requires_db=True,
    ),
]


# Subset that doesn't need a running database (safe for CI)
EVAL_DATASET_NO_DB = [c for c in EVAL_DATASET if not c.requires_db]

# Full dataset (requires postgres + MCP server + seeded data)
EVAL_DATASET_FULL = EVAL_DATASET
