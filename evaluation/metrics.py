"""
Evaluation metrics for NovaSupport quality assessment.

Metrics:
  - triage_accuracy      : % of cases where LLM category == expected_category
  - action_accuracy      : % of cases where final_action == expected_action
  - response_quality     : avg fraction of expected keywords found in response
  - completion_rate      : % of cases that finished without error/timeout
  - avg_latency_seconds  : mean elapsed time per case
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from evaluation.dataset import EvalCase


@dataclass
class EvalResult:
    case_id: str
    elapsed_seconds: float
    final_state: dict
    error: str | None = None

    # Computed after scoring
    category_correct: bool = False
    action_correct: bool = False
    response_quality: float = 0.0  # 0.0–1.0

    @property
    def succeeded(self) -> bool:
        return self.error is None and self.final_state.get("completed", False)

    @property
    def triage_category(self) -> str:
        return self.final_state.get("triage_category", "")

    @property
    def final_action(self) -> str:
        return self.final_state.get("final_action", "")

    @property
    def final_response(self) -> str:
        return self.final_state.get("final_response", "")


def score_result(case: "EvalCase", result: EvalResult) -> EvalResult:
    """Populate category_correct, action_correct, response_quality on a result."""
    if not result.succeeded:
        return result

    result.category_correct = result.triage_category == case.expected_category

    # action_correct: exact match OR the final action is semantically equivalent
    # (e.g. "refund_shipping_fee" counts as a "process_refund" if expected)
    action = result.final_action
    expected = case.expected_action
    result.action_correct = (action == expected) or _action_compatible(action, expected)

    # response_quality: fraction of expected keywords present in response
    if case.response_keywords:
        response_lower = result.final_response.lower()
        hits = sum(1 for kw in case.response_keywords if kw.lower() in response_lower)
        result.response_quality = hits / len(case.response_keywords)
    else:
        result.response_quality = 1.0 if result.succeeded else 0.0

    return result


def _action_compatible(actual: str, expected: str) -> bool:
    """Allow semantically equivalent actions to be considered correct."""
    refund_group = {"refund_shipping_fee", "process_refund", "full_refund", "partial_refund"}
    replacement_group = {"replacement", "exchange", "resend"}
    info_group = {"provide_info", "follow_up", "escalate"}
    for group in (refund_group, replacement_group, info_group):
        if actual in group and expected in group:
            return True
    return False


# ── Aggregate metrics ─────────────────────────────────────────────────────────

def triage_accuracy(results: list[EvalResult]) -> float:
    scored = [r for r in results if r.succeeded]
    if not scored:
        return 0.0
    return sum(1 for r in scored if r.category_correct) / len(scored)


def action_accuracy(results: list[EvalResult]) -> float:
    scored = [r for r in results if r.succeeded]
    if not scored:
        return 0.0
    return sum(1 for r in scored if r.action_correct) / len(scored)


def avg_response_quality(results: list[EvalResult]) -> float:
    scored = [r for r in results if r.succeeded]
    if not scored:
        return 0.0
    return sum(r.response_quality for r in scored) / len(scored)


def completion_rate(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(1 for r in results if r.succeeded) / len(results)


def avg_latency(results: list[EvalResult]) -> float:
    completed = [r for r in results if r.succeeded]
    if not completed:
        return 0.0
    return sum(r.elapsed_seconds for r in completed) / len(completed)


@dataclass
class EvalSummary:
    total: int
    succeeded: int
    triage_accuracy: float
    action_accuracy: float
    avg_response_quality: float
    avg_latency_seconds: float
    failures: list[str] = field(default_factory=list)  # case_ids that errored

    @property
    def completion_rate(self) -> float:
        return self.succeeded / self.total if self.total else 0.0


def summarise(
    cases: list["EvalCase"],
    results: list[EvalResult],
) -> EvalSummary:
    """Score all results and produce an EvalSummary."""
    from evaluation.metrics import score_result

    case_map = {c.case_id: c for c in cases}
    scored = [score_result(case_map[r.case_id], r) for r in results]

    return EvalSummary(
        total=len(scored),
        succeeded=sum(1 for r in scored if r.succeeded),
        triage_accuracy=triage_accuracy(scored),
        action_accuracy=action_accuracy(scored),
        avg_response_quality=avg_response_quality(scored),
        avg_latency_seconds=avg_latency(scored),
        failures=[r.case_id for r in scored if not r.succeeded],
    )
