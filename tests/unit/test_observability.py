"""
Unit tests for Phase 14 Observability.

Tests cover:
  - configure_logging() runs without error in both modes
  - Each metric helper increments the correct Prometheus counter/histogram
  - /metrics endpoint returns 200 with Prometheus text format
  - /health/ready returns correct structure with mocked DB and producer
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY


# ── logging ───────────────────────────────────────────────────────────────────

class TestConfigureLogging:

    def test_configure_logging_development_does_not_raise(self):
        from observability.logging import configure_logging
        configure_logging(env="development", level="WARNING")

    def test_configure_logging_production_does_not_raise(self):
        from observability.logging import configure_logging
        configure_logging(env="production", level="INFO")

    def test_stdlib_logger_still_works_after_configure(self):
        import logging
        from observability.logging import configure_logging
        configure_logging(env="development", level="WARNING")
        logger = logging.getLogger("test.observability")
        # should not raise
        logger.info("test message after structlog configure")


# ── metrics helpers ───────────────────────────────────────────────────────────

class TestMetricHelpers:

    def _sample_value(self, sample_name: str, labels: dict | None = None) -> float:
        """
        Read the current value of a Prometheus sample by its full name
        (e.g. 'novasupport_tickets_submitted_total' for a Counter).
        """
        for metric in REGISTRY.collect():
            for sample in metric.samples:
                if sample.name == sample_name:
                    if labels is None or all(
                        sample.labels.get(k) == v for k, v in labels.items()
                    ):
                        return sample.value
        return 0.0

    def test_inc_tickets_submitted_increments_counter(self):
        from observability.metrics import inc_tickets_submitted, TICKETS_SUBMITTED
        before = self._sample_value("novasupport_tickets_submitted_total", {"language": "en"})
        inc_tickets_submitted(language="en")
        after = self._sample_value("novasupport_tickets_submitted_total", {"language": "en"})
        assert after == before + 1.0

    def test_inc_workflows_completed_increments_with_action_label(self):
        from observability.metrics import inc_workflows_completed
        before = self._sample_value(
            "novasupport_workflows_completed_total", {"action": "refund_shipping_fee"}
        )
        inc_workflows_completed(action="refund_shipping_fee")
        after = self._sample_value(
            "novasupport_workflows_completed_total", {"action": "refund_shipping_fee"}
        )
        assert after == before + 1.0

    def test_inc_workflows_escalated_increments(self):
        from observability.metrics import inc_workflows_escalated
        before = self._sample_value("novasupport_workflows_escalated_total")
        inc_workflows_escalated()
        after = self._sample_value("novasupport_workflows_escalated_total")
        assert after == before + 1.0

    def test_inc_dlq_increments_with_task_type_label(self):
        from observability.metrics import inc_dlq
        before = self._sample_value(
            "novasupport_dlq_messages_total", {"task_type": "triage_ticket"}
        )
        inc_dlq(task_type="triage_ticket")
        after = self._sample_value(
            "novasupport_dlq_messages_total", {"task_type": "triage_ticket"}
        )
        assert after == before + 1.0

    def test_inc_worker_retries_increments(self):
        from observability.metrics import inc_worker_retries
        before = self._sample_value(
            "novasupport_worker_retries_total", {"task_type": "get_order"}
        )
        inc_worker_retries(task_type="get_order")
        after = self._sample_value(
            "novasupport_worker_retries_total", {"task_type": "get_order"}
        )
        assert after == before + 1.0

    def test_record_task_duration_observes_histogram(self):
        from observability.metrics import record_task_duration
        # Should not raise — observe() always succeeds on a Histogram
        record_task_duration(task_type="resolve_ticket", seconds=0.42)

    def test_metric_helpers_do_not_raise_on_none_metric(self):
        """All helpers must be no-ops when the metric object is None."""
        import observability.metrics as m
        original = m.TICKETS_SUBMITTED
        m.TICKETS_SUBMITTED = None
        try:
            m.inc_tickets_submitted()  # should not raise
        finally:
            m.TICKETS_SUBMITTED = original


# ── API endpoints ─────────────────────────────────────────────────────────────

@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send = AsyncMock()
    p.start = AsyncMock()
    p.stop = AsyncMock()
    return p


@pytest.fixture
def mock_repo():
    r = AsyncMock()
    r._pool = AsyncMock()
    r._pool.fetchrow = AsyncMock(return_value=MagicMock())
    r.get_by_ticket_id = AsyncMock(return_value=None)
    r.close = AsyncMock()
    return r


@pytest.fixture
async def client(mock_producer, mock_repo):
    from apps.api.main import app
    from apps.api.deps import get_producer, get_repo

    app.dependency_overrides[get_producer] = lambda: mock_producer
    app.dependency_overrides[get_repo] = lambda: mock_repo

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

    app.dependency_overrides.clear()


class TestMetricsEndpoint:

    async def test_returns_200(self, client):
        response = await client.get("/metrics")
        assert response.status_code == 200

    async def test_content_type_is_prometheus_text(self, client):
        response = await client.get("/metrics")
        assert "text/plain" in response.headers["content-type"]

    async def test_contains_novasupport_metrics(self, client):
        response = await client.get("/metrics")
        assert b"novasupport_" in response.content

    async def test_contains_tickets_submitted_metric(self, client):
        response = await client.get("/metrics")
        assert b"novasupport_tickets_submitted_total" in response.content


class TestHealthReady:

    async def test_returns_200_when_db_ok(self, client, mock_repo):
        mock_repo._pool.fetchrow = AsyncMock(return_value=MagicMock())
        response = await client.get("/health/ready")
        assert response.status_code == 200

    async def test_body_contains_checks(self, client):
        response = await client.get("/health/ready")
        body = response.json()
        assert "checks" in body
        assert "postgres" in body["checks"]
        assert "kafka" in body["checks"]

    async def test_returns_503_when_db_down(self, client, mock_repo):
        mock_repo._pool.fetchrow = AsyncMock(side_effect=ConnectionError("DB down"))
        response = await client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["status"] == "not_ready"

    async def test_returns_503_when_db_pool_is_none(self, client, mock_repo):
        mock_repo._pool = None
        response = await client.get("/health/ready")
        assert response.status_code == 503
