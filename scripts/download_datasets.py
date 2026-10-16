"""
Phase 1 — Step 1: Download public datasets from Hugging Face.

Downloads:
  1. bitext/Bitext-customer-support-llm-chatbot-training-dataset
     → customer support messages with intent labels (cancel_order, track_order, etc.)
  2. rjac/e-commerce-customer-support-qa
     → e-commerce specific QA pairs

Saves small samples to data/raw/ for inspection and further processing.
Run: python scripts/download_datasets.py
"""

import json
import sys
from pathlib import Path

# Add project root to path so we can import config
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from datasets import load_dataset  # noqa: E402
import pandas as pd  # noqa: E402

RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# How many rows to keep per dataset (spec: 1,000-5,000)
SAMPLE_SIZE = 2000


def download_bitext_support() -> pd.DataFrame:
    """
    Download the Bitext customer support chatbot training dataset.

    Fields:
      - instruction : customer message
      - intent      : label like cancel_order, track_order, refund_request
      - response    : expected agent reply
      - category    : broad category (ORDER, REFUND, SHIPPING, ...)
      - tags        : style/tone tags

    Source: https://huggingface.co/datasets/bitext/Bitext-customer-support-llm-chatbot-training-dataset
    """
    print("Downloading bitext customer support dataset...")
    ds = load_dataset(
        "bitext/Bitext-customer-support-llm-chatbot-training-dataset",
        split="train",
        trust_remote_code=False,
    )
    print(f"  Total rows: {len(ds)}")

    df = ds.to_pandas()

    # Keep only the columns we care about
    keep = [c for c in ["instruction", "intent", "response", "category", "tags"] if c in df.columns]
    df = df[keep].copy()

    # Sample
    if len(df) > SAMPLE_SIZE:
        df = df.sample(n=SAMPLE_SIZE, random_state=42).reset_index(drop=True)

    out = RAW_DIR / "bitext_support.csv"
    df.to_csv(out, index=False)
    print(f"  Saved {len(df)} rows -> {out.relative_to(ROOT)}")
    return df


def download_ecommerce_qa() -> pd.DataFrame:
    """
    Download the e-commerce customer support QA dataset.

    Source: https://huggingface.co/datasets/rjac/e-commerce-customer-support-qa
    """
    print("Downloading e-commerce QA dataset...")
    try:
        ds = load_dataset(
            "rjac/e-commerce-customer-support-qa",
            split="train",
            trust_remote_code=False,
        )
        print(f"  Total rows: {len(ds)}")

        df = ds.to_pandas()
        if len(df) > SAMPLE_SIZE:
            df = df.sample(n=SAMPLE_SIZE, random_state=42).reset_index(drop=True)

        out = RAW_DIR / "ecommerce_qa.csv"
        df.to_csv(out, index=False)
        print(f"  Saved {len(df)} rows -> {out.relative_to(ROOT)}")
        return df

    except Exception as exc:
        print(f"  WARNING: Could not download e-commerce QA dataset: {exc}")
        print("  Skipping — will use bitext dataset only.")
        return pd.DataFrame()


def print_summary(name: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    print(f"\n{'='*50}")
    print(f"Dataset: {name}")
    print(f"  Rows   : {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    if "intent" in df.columns:
        top_intents = df["intent"].value_counts().head(15)
        print(f"\n  Top intents ({len(df['intent'].unique())} total):")
        for intent, count in top_intents.items():
            print(f"    {intent:<40} {count}")

    if "category" in df.columns:
        print(f"\n  Categories:")
        for cat, count in df["category"].value_counts().items():
            print(f"    {cat:<30} {count}")

    print(f"{'='*50}")


def save_manifest() -> None:
    """Write a simple JSON manifest listing what was downloaded."""
    files = list(RAW_DIR.glob("*.csv"))
    manifest = {
        "datasets": [
            {
                "file": f.name,
                "rows": len(pd.read_csv(f)),
                "columns": list(pd.read_csv(f, nrows=0).columns),
            }
            for f in sorted(files)
        ]
    }
    out = RAW_DIR / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest saved -> {out.relative_to(ROOT)}")


def main() -> None:
    print("NovaSupport — Phase 1: Dataset Download")
    print(f"Output directory: {RAW_DIR.relative_to(ROOT)}\n")

    bitext_df = download_bitext_support()
    ecommerce_df = download_ecommerce_qa()

    print_summary("bitext_support", bitext_df)
    print_summary("ecommerce_qa", ecommerce_df)

    save_manifest()
    print("\nDone. Next step: scripts/inspect_datasets.py")


if __name__ == "__main__":
    main()
