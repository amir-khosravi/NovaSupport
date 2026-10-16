"""
Evaluation report generator.

Prints a formatted summary table to stdout and optionally saves JSON.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from evaluation.dataset import EvalCase
    from evaluation.metrics import EvalResult, EvalSummary


def print_report(
    cases: list["EvalCase"],
    results: list["EvalResult"],
    summary: "EvalSummary",
) -> None:
    case_map = {c.case_id: c for c in cases}

    print("\n" + "=" * 80)
    print("  NovaSupport Evaluation Report")
    print("=" * 80)

    print(f"\n{'ID':<12} {'Category':>15} {'Action':>22} {'Cat✓':>5} {'Act✓':>5} {'Qual':>5} {'Time':>6}")
    print("-" * 80)

    for r in results:
        case = case_map[r.case_id]
        if r.succeeded:
            cat_ok = "✓" if r.category_correct else "✗"
            act_ok = "✓" if r.action_correct else "✗"
            qual = f"{r.response_quality:.0%}"
            t = f"{r.elapsed_seconds:.1f}s"
            cat = r.triage_category[:15]
            act = r.final_action[:22]
        else:
            cat_ok = act_ok = "!"
            qual = "—"
            t = f"{r.elapsed_seconds:.1f}s"
            cat = "ERROR"
            act = (r.error or "")[:22]

        print(f"{r.case_id:<12} {cat:>15} {act:>22} {cat_ok:>5} {act_ok:>5} {qual:>5} {t:>6}")

    print("-" * 80)
    print(f"\n  Cases             : {summary.total}")
    print(f"  Completion rate   : {summary.completion_rate:.0%}  ({summary.succeeded}/{summary.total})")
    print(f"  Triage accuracy   : {summary.triage_accuracy:.0%}")
    print(f"  Action accuracy   : {summary.action_accuracy:.0%}")
    print(f"  Response quality  : {summary.avg_response_quality:.0%}")
    print(f"  Avg latency       : {summary.avg_latency_seconds:.1f}s")

    if summary.failures:
        print(f"\n  Failed cases: {', '.join(summary.failures)}")

    print("=" * 80 + "\n")


def save_json_report(
    cases: list["EvalCase"],
    results: list["EvalResult"],
    summary: "EvalSummary",
    path: str | Path = "evaluation_report.json",
) -> None:
    case_map = {c.case_id: c for c in cases}

    report = {
        "summary": {
            "total": summary.total,
            "succeeded": summary.succeeded,
            "completion_rate": round(summary.completion_rate, 4),
            "triage_accuracy": round(summary.triage_accuracy, 4),
            "action_accuracy": round(summary.action_accuracy, 4),
            "avg_response_quality": round(summary.avg_response_quality, 4),
            "avg_latency_seconds": round(summary.avg_latency_seconds, 2),
            "failures": summary.failures,
        },
        "cases": [
            {
                "case_id": r.case_id,
                "description": case_map[r.case_id].description,
                "succeeded": r.succeeded,
                "elapsed_seconds": round(r.elapsed_seconds, 2),
                "expected_category": case_map[r.case_id].expected_category,
                "actual_category": r.triage_category,
                "category_correct": r.category_correct,
                "expected_action": case_map[r.case_id].expected_action,
                "actual_action": r.final_action,
                "action_correct": r.action_correct,
                "response_quality": round(r.response_quality, 4),
                "final_response": r.final_response[:200] if r.final_response else "",
                "error": r.error,
            }
            for r in results
        ],
    }

    Path(path).write_text(json.dumps(report, indent=2))
    print(f"Report saved to {path}")
