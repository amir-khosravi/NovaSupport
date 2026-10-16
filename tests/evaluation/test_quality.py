"""
LLM Quality Evaluation Tests — Phase 16.

These tests run the full LangGraph workflow against a live infrastructure
(Kafka + Workers) and verify that quality thresholds are met.

Requirements:
  docker compose up -d redpanda postgres redis qdrant
  python scripts/seed_database.py
  python -m rag.indexer        (load policy vectors)

Run:
  pytest tests/evaluation/ -v -m evaluation
  pytest tests/evaluation/ -v -m "evaluation and not db"   # skip DB-dependent cases

Thresholds (must pass for CI gate):
  - completion_rate  >= 100%   (no crashes or timeouts)
  - triage_accuracy  >= 75%    (LLM classifies correctly ≥ 3/4 cases)
  - action_accuracy  >= 75%
  - response_quality >= 0.70   (≥ 70% of expected keywords in responses)
"""

from __future__ import annotations

import pytest

from evaluation.dataset import EVAL_DATASET_FULL, EVAL_DATASET_NO_DB
from evaluation.metrics import EvalSummary, score_result, summarise
from evaluation.report import print_report, save_json_report
from evaluation.runner import EvalRunner

pytestmark = pytest.mark.evaluation

# ── Quality thresholds ────────────────────────────────────────────────────────

THRESHOLD_COMPLETION   = 1.00   # all cases must finish without error
THRESHOLD_TRIAGE_ACC   = 0.75   # 75% correct triage classification
THRESHOLD_ACTION_ACC   = 0.75   # 75% correct resolution action
THRESHOLD_RESPONSE_Q   = 0.70   # 70% of expected keywords in response


# ── Session-scoped fixture: run all cases once ────────────────────────────────

@pytest.fixture(scope="module")
async def eval_summary_no_db():
    """
    Run EVAL_DATASET_NO_DB through the full workflow and return scored summary.
    This fixture is module-scoped — cases run once, all tests share the results.
    """
    cases = EVAL_DATASET_NO_DB
    async with EvalRunner(worker_count=3, case_timeout=90.0) as runner:
        results = await runner.run(cases)

    summary = summarise(cases, results)
    print_report(cases, results, summary)
    save_json_report(cases, results, summary, "evaluation_report_no_db.json")
    return summary


@pytest.fixture(scope="module")
async def eval_summary_full():
    """
    Run EVAL_DATASET_FULL (includes DB-dependent cases).
    Requires postgres + MCP server + seeded data.
    """
    cases = EVAL_DATASET_FULL
    async with EvalRunner(worker_count=3, case_timeout=90.0) as runner:
        results = await runner.run(cases)

    summary = summarise(cases, results)
    print_report(cases, results, summary)
    save_json_report(cases, results, summary, "evaluation_report_full.json")
    return summary


# ── Tests: no-DB dataset (safe for CI) ───────────────────────────────────────

class TestQualityNoDb:
    """Quality gates for cases that don't need a running database."""

    async def test_all_cases_complete_without_error(self, eval_summary_no_db: EvalSummary):
        summary = eval_summary_no_db
        assert summary.completion_rate >= THRESHOLD_COMPLETION, (
            f"Some cases failed: {summary.failures}\n"
            f"Completion rate: {summary.completion_rate:.0%} < {THRESHOLD_COMPLETION:.0%}"
        )

    async def test_triage_accuracy_meets_threshold(self, eval_summary_no_db: EvalSummary):
        summary = eval_summary_no_db
        assert summary.triage_accuracy >= THRESHOLD_TRIAGE_ACC, (
            f"Triage accuracy {summary.triage_accuracy:.0%} < {THRESHOLD_TRIAGE_ACC:.0%}\n"
            "Check: are LLM prompts classifying tickets correctly?"
        )

    async def test_action_accuracy_meets_threshold(self, eval_summary_no_db: EvalSummary):
        summary = eval_summary_no_db
        assert summary.action_accuracy >= THRESHOLD_ACTION_ACC, (
            f"Action accuracy {summary.action_accuracy:.0%} < {THRESHOLD_ACTION_ACC:.0%}\n"
            "Check: are resolution handlers recommending the right actions?"
        )

    async def test_response_quality_meets_threshold(self, eval_summary_no_db: EvalSummary):
        summary = eval_summary_no_db
        assert summary.avg_response_quality >= THRESHOLD_RESPONSE_Q, (
            f"Response quality {summary.avg_response_quality:.0%} < {THRESHOLD_RESPONSE_Q:.0%}\n"
            "Check: are response keywords appearing in generated text?"
        )

    async def test_latency_under_60_seconds_per_case(self, eval_summary_no_db: EvalSummary):
        summary = eval_summary_no_db
        assert summary.avg_latency_seconds < 60.0, (
            f"Average latency {summary.avg_latency_seconds:.1f}s exceeds 60s/case.\n"
            "Check: LLM timeouts, Kafka connection speed, handler performance."
        )


# ── Tests: full dataset (requires DB) ────────────────────────────────────────

@pytest.mark.db
class TestQualityFull:
    """Quality gates for the full dataset including DB-dependent cases."""

    async def test_all_cases_complete_without_error(self, eval_summary_full: EvalSummary):
        summary = eval_summary_full
        assert summary.completion_rate >= THRESHOLD_COMPLETION, (
            f"Failures: {summary.failures}"
        )

    async def test_triage_accuracy_meets_threshold(self, eval_summary_full: EvalSummary):
        summary = eval_summary_full
        assert summary.triage_accuracy >= THRESHOLD_TRIAGE_ACC, (
            f"Triage accuracy {summary.triage_accuracy:.0%} < threshold"
        )

    async def test_action_accuracy_meets_threshold(self, eval_summary_full: EvalSummary):
        summary = eval_summary_full
        assert summary.action_accuracy >= THRESHOLD_ACTION_ACC, (
            f"Action accuracy {summary.action_accuracy:.0%} < threshold"
        )

    async def test_express_delay_always_triggers_refund(self, eval_summary_full: EvalSummary):
        """
        EVAL-001 and EVAL-010 are express delay cases with ORD-0001.
        Both must resolve to refund_shipping_fee — this is the core product demo.
        """
        report_path = "evaluation_report_full.json"
        import json, pathlib
        if not pathlib.Path(report_path).exists():
            pytest.skip("Full report not yet generated")

        report = json.loads(pathlib.Path(report_path).read_text())
        express_cases = [c for c in report["cases"] if c["case_id"] in ("EVAL-001", "EVAL-010")]

        for case in express_cases:
            assert case["action_correct"], (
                f"{case['case_id']}: expected refund_shipping_fee, "
                f"got {case['actual_action']!r}"
            )
