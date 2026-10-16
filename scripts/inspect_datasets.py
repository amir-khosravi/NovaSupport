"""
Phase 1 — Step 2: Inspect downloaded datasets.

Shows column types, missing values, sample rows, and intent/category mapping
so we know exactly what to keep before building the normalized schema.

Run: python scripts/inspect_datasets.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

RAW_DIR = ROOT / "data" / "raw"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def section(title: str) -> None:
    print(f"\n{'-'*60}")
    print(f"  {title}")
    print(f"{'-'*60}")


def inspect_df(df: pd.DataFrame, name: str) -> None:
    section(f"{name}  ({len(df):,} rows x {len(df.columns)} cols)")

    # Column types and nulls
    print("\n[Column overview]")
    print(f"{'Column':<45} {'Type':<15} {'Nulls':>8} {'Unique':>8}")
    print("-" * 80)
    for col in df.columns:
        nulls = df[col].isna().sum()
        unique = df[col].nunique()
        dtype = str(df[col].dtype)
        print(f"  {col:<43} {dtype:<15} {nulls:>8} {unique:>8}")

    # Sample rows
    print("\n[Sample rows (first 3)]")
    for i, row in df.head(3).iterrows():
        print(f"\n  Row {i}:")
        for col in df.columns:
            val = str(row[col])
            if len(val) > 120:
                val = val[:117] + "..."
            print(f"    {col:<40}: {val}")

    # Text length stats for string columns
    text_cols = [c for c in df.columns if df[c].dtype == object]
    if text_cols:
        print("\n[Text length stats (chars)]")
        print(f"  {'Column':<40} {'min':>6} {'mean':>8} {'max':>8}")
        print("  " + "-" * 65)
        for col in text_cols:
            lengths = df[col].dropna().astype(str).str.len()
            print(f"  {col:<40} {lengths.min():>6} {lengths.mean():>8.0f} {lengths.max():>8}")


def inspect_bitext(df: pd.DataFrame) -> None:
    section("Bitext — Intent → NovaSupport category mapping")

    # Show how bitext intents map to our system intents
    nova_map = {
        "track_order":              "delivery_delay",
        "delivery_options":         "delivery_delay",
        "change_shipping_address":  "delivery_delay",
        "check_cancellation_fee":   "cancel_order",
        "cancel_order":             "cancel_order",
        "track_refund":             "request_refund",
        "check_refund_policy":      "request_refund",
        "payment_issue":            "payment_problem",
        "check_payment_methods":    "payment_problem",
        "place_order":              "product_question",
        "review":                   "product_question",
        "complaint":                "damaged_product",
        "get_invoice":              "payment_problem",
        "contact_customer_service": "account_issue",
        "contact_human_agent":      "account_issue",
        "create_account":           "account_issue",
        "newsletter_subscription":  "account_issue",
    }

    print("\n  Bitext Intent                  →  NovaSupport Category")
    print("  " + "-" * 55)
    counts = df["intent"].value_counts()
    for intent, count in counts.items():
        mapped = nova_map.get(intent, "— (unmapped)")
        print(f"  {intent:<35} {mapped:<25} ({count} rows)")

    mapped_count = df["intent"].isin(nova_map).sum()
    pct = mapped_count / len(df) * 100
    print(f"\n  Mapped rows: {mapped_count:,} / {len(df):,}  ({pct:.1f}%)")


def inspect_ecommerce(df: pd.DataFrame) -> None:
    section("E-commerce QA — Issue areas and categories")

    for col in ["issue_area", "issue_category", "customer_sentiment", "product_category"]:
        if col in df.columns:
            print(f"\n  [{col}]")
            for val, count in df[col].value_counts().items():
                print(f"    {str(val):<50} {count}")

    # Show one full conversation
    if "conversation" in df.columns:
        section("E-commerce QA — Sample conversation")
        sample = df["conversation"].dropna().iloc[0]
        lines = str(sample).split("\n")[:20]
        for line in lines:
            print(f"  {line}")
        if len(str(sample).split("\n")) > 20:
            print("  ... (truncated)")


def main() -> None:
    print("NovaSupport — Phase 1: Dataset Inspection")

    bitext_path = RAW_DIR / "bitext_support.csv"
    ecommerce_path = RAW_DIR / "ecommerce_qa.csv"

    if not bitext_path.exists():
        print("ERROR: Run scripts/download_datasets.py first.")
        sys.exit(1)

    bitext_df = pd.read_csv(bitext_path)
    inspect_df(bitext_df, "bitext_support.csv")
    inspect_bitext(bitext_df)

    if ecommerce_path.exists():
        ecommerce_df = pd.read_csv(ecommerce_path)
        inspect_df(ecommerce_df, "ecommerce_qa.csv")
        inspect_ecommerce(ecommerce_df)

    section("Conclusions")
    print("""
  bitext_support.csv
    KEEP : instruction, intent, response, category
    USE FOR : triage evaluation, intent classification, few-shot examples
    ACTION  : map intents to NovaSupport categories in prepare_tickets.py

  ecommerce_qa.csv
    KEEP : issue_area, issue_category, customer_sentiment, conversation, qa
    USE FOR : resolution and response evaluation
    ACTION  : extract customer message from conversation field

  Next step: scripts/prepare_tickets.py
    → clean text, apply intent mapping, produce data/processed/tickets.csv
""")


if __name__ == "__main__":
    main()
