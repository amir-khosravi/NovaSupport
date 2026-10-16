"""
Prometheus metrics for NovaSupport.

All metrics are registered once at import time using a lazy singleton pattern
to safely survive module re-imports across test runs.

Metric naming convention: novasupport_<subsystem>_<name>_<unit>

Instrumented callsites:
  apps/api/routers/tickets.py   → inc_tickets_submitted()
  apps/worker/service.py        → inc_dlq(), inc_worker_retries(), record_task_duration()
  apps/orchestrator/graph.py    → inc_workflows_completed(), inc_workflows_escalated()
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram, REGISTRY, CollectorRegistry


def _get_or_create_counter(name: str, help_text: str, labelnames: tuple = ()) -> Counter:
    """Return existing counter or create new one — safe for test re-runs."""
    try:
        return Counter(name, help_text, list(labelnames))
    except ValueError:
        # Already registered in this process (e.g. test isolation)
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]


def _get_or_create_histogram(
    name: str, help_text: str, labelnames: tuple = (), buckets=None
) -> Histogram:
    try:
        kwargs = {"labelnames": list(labelnames)}
        if buckets is not None:
            kwargs["buckets"] = buckets
        return Histogram(name, help_text, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]


# ── counters ──────────────────────────────────────────────────────────────────

TICKETS_SUBMITTED = _get_or_create_counter(
    "novasupport_tickets_submitted_total",
    "Number of support tickets submitted via the API",
    labelnames=("language",),
)

WORKFLOWS_COMPLETED = _get_or_create_counter(
    "novasupport_workflows_completed_total",
    "Number of workflows that reached the complete_ticket node",
    labelnames=("action",),
)

WORKFLOWS_ESCALATED = _get_or_create_counter(
    "novasupport_workflows_escalated_total",
    "Number of workflows that were escalated to a human agent",
)

DLQ_MESSAGES = _get_or_create_counter(
    "novasupport_dlq_messages_total",
    "Number of messages sent to the Dead Letter Queue",
    labelnames=("task_type",),
)

WORKER_RETRIES = _get_or_create_counter(
    "novasupport_worker_retries_total",
    "Number of retry attempts made by the worker (excluding the first attempt)",
    labelnames=("task_type",),
)

# ── histograms ────────────────────────────────────────────────────────────────

TASK_DURATION = _get_or_create_histogram(
    "novasupport_task_duration_seconds",
    "Handler execution time per task type",
    labelnames=("task_type",),
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


# ── public helper functions ───────────────────────────────────────────────────

def inc_tickets_submitted(language: str = "en") -> None:
    if TICKETS_SUBMITTED is not None:
        TICKETS_SUBMITTED.labels(language=language).inc()


def inc_workflows_completed(action: str = "unknown") -> None:
    if WORKFLOWS_COMPLETED is not None:
        WORKFLOWS_COMPLETED.labels(action=action).inc()


def inc_workflows_escalated() -> None:
    if WORKFLOWS_ESCALATED is not None:
        WORKFLOWS_ESCALATED.inc()


def inc_dlq(task_type: str = "unknown") -> None:
    if DLQ_MESSAGES is not None:
        DLQ_MESSAGES.labels(task_type=task_type).inc()


def inc_worker_retries(task_type: str = "unknown") -> None:
    if WORKER_RETRIES is not None:
        WORKER_RETRIES.labels(task_type=task_type).inc()


def record_task_duration(task_type: str, seconds: float) -> None:
    if TASK_DURATION is not None:
        TASK_DURATION.labels(task_type=task_type).observe(seconds)
