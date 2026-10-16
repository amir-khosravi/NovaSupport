"""
Unit tests for Phase 13 FastAPI intake service.

All Kafka and DB calls are mocked — no external services required.
Uses httpx.AsyncClient with ASGITransport for realistic HTTP-level testing.
Lifespan startup degrades gracefully (logs warnings), dependency_overrides
ensure mock objects are injected into every request.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from apps.api.deps import get_producer, get_repo
from apps.api.main import app
from persistence.workflow_repository import WorkflowRecord


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_producer() -> AsyncMock:
    p = AsyncMock()
    p.send = AsyncMock()
    p.start = AsyncMock()
    p.stop = AsyncMock()
    return p


@pytest.fixture
def mock_repo() -> AsyncMock:
    r = AsyncMock()
    r.get_by_ticket_id = AsyncMock(return_value=None)
    r.save = AsyncMock()
    r.close = AsyncMock()
    return r


@pytest.fixture
async def client(mock_producer, mock_repo):
    """
    HTTP test client with mocked Kafka producer and DB repository.
    dependency_overrides bypass app.state so lifespan failures are irrelevant.
    """
    app.dependency_overrides[get_producer] = lambda: mock_producer
    app.dependency_overrides[get_repo] = lambda: mock_repo

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c

    app.dependency_overrides.clear()


# ── POST /api/v1/tickets ──────────────────────────────────────────────────────

class TestSubmitTicket:

    async def test_returns_202_accepted(self, client, mock_producer):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "My express order is 5 days late.",
        })
        assert response.status_code == 202

    async def test_response_contains_workflow_and_ticket_ids(self, client, mock_producer):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "My express order is 5 days late.",
        })
        body = response.json()
        assert "workflow_id" in body
        assert "ticket_id" in body
        assert body["status"] == "accepted"

    async def test_ticket_id_has_tkt_prefix(self, client, mock_producer):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "Wrong item delivered.",
        })
        assert response.json()["ticket_id"].startswith("TKT-")

    async def test_publishes_event_to_kafka(self, client, mock_producer):
        await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-002",
            "order_id": "ORD-0001",
            "message": "I was charged twice.",
        })
        mock_producer.send.assert_called_once()
        # First arg is topic, second is the event
        topic_arg = mock_producer.send.call_args[0][0]
        from config.settings import settings
        assert topic_arg == settings.kafka.ticket_topic

    async def test_published_event_contains_customer_and_message(self, client, mock_producer):
        await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-003",
            "message": "Package never arrived.",
            "language": "fr",
        })
        event_arg = mock_producer.send.call_args[0][1]
        assert event_arg.customer_id == "CUST-003"
        assert event_arg.message == "Package never arrived."
        assert event_arg.language == "fr"

    async def test_returns_503_when_kafka_unavailable(self, client, mock_producer):
        mock_producer.send.side_effect = ConnectionError("Kafka down")
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "Help needed.",
        })
        assert response.status_code == 503

    async def test_rejects_empty_message(self, client):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "",
        })
        assert response.status_code == 422

    async def test_rejects_empty_customer_id(self, client):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "",
            "message": "Help please.",
        })
        assert response.status_code == 422

    async def test_order_id_is_optional(self, client, mock_producer):
        response = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "General question about returns.",
            # no order_id
        })
        assert response.status_code == 202
        event_arg = mock_producer.send.call_args[0][1]
        assert event_arg.order_id is None

    async def test_two_tickets_get_different_ids(self, client, mock_producer):
        r1 = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001", "message": "Issue 1"
        })
        r2 = await client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001", "message": "Issue 2"
        })
        assert r1.json()["ticket_id"] != r2.json()["ticket_id"]
        assert r1.json()["workflow_id"] != r2.json()["workflow_id"]


# ── GET /api/v1/tickets/{id}/status ──────────────────────────────────────────

class TestGetTicketStatus:

    async def test_returns_404_when_not_found(self, client, mock_repo):
        mock_repo.get_by_ticket_id = AsyncMock(return_value=None)
        response = await client.get("/api/v1/tickets/TKT-MISSING/status")
        assert response.status_code == 404

    async def test_returns_200_with_running_status(self, client, mock_repo):
        mock_repo.get_by_ticket_id = AsyncMock(return_value=WorkflowRecord(
            workflow_id="WF-001",
            ticket_id="TKT-001",
            status="running",
        ))
        response = await client.get("/api/v1/tickets/TKT-001/status")
        assert response.status_code == 200
        assert response.json()["status"] == "running"

    async def test_returns_completed_workflow_fields(self, client, mock_repo):
        mock_repo.get_by_ticket_id = AsyncMock(return_value=WorkflowRecord(
            workflow_id="WF-002",
            ticket_id="TKT-002",
            status="completed",
            category="delivery_delay",
            recommended_action="refund_shipping_fee",
            refund_amount=24.99,
            final_response="Your refund has been processed.",
        ))
        response = await client.get("/api/v1/tickets/TKT-002/status")
        body = response.json()
        assert body["status"] == "completed"
        assert body["category"] == "delivery_delay"
        assert body["recommended_action"] == "refund_shipping_fee"
        assert body["refund_amount"] == pytest.approx(24.99)
        assert "refund" in body["final_response"].lower()

    async def test_returns_escalated_workflow_with_reason(self, client, mock_repo):
        mock_repo.get_by_ticket_id = AsyncMock(return_value=WorkflowRecord(
            workflow_id="WF-003",
            ticket_id="TKT-003",
            status="escalated",
            escalation_reason="Quality check failed: confidence=0.40",
        ))
        response = await client.get("/api/v1/tickets/TKT-003/status")
        body = response.json()
        assert body["status"] == "escalated"
        assert body["escalation_reason"] is not None

    async def test_queries_repo_with_correct_ticket_id(self, client, mock_repo):
        mock_repo.get_by_ticket_id = AsyncMock(return_value=None)
        await client.get("/api/v1/tickets/TKT-SPECIFIC/status")
        mock_repo.get_by_ticket_id.assert_called_once_with("TKT-SPECIFIC")


# ── GET /health ───────────────────────────────────────────────────────────────

class TestHealth:

    async def test_returns_200(self, client):
        response = await client.get("/health")
        assert response.status_code == 200

    async def test_returns_ok_status(self, client):
        response = await client.get("/health")
        assert response.json()["status"] == "ok"

    async def test_returns_version(self, client):
        from config.settings import settings
        response = await client.get("/health")
        assert response.json()["version"] == settings.app.version

    async def test_returns_service_name(self, client):
        response = await client.get("/health")
        assert response.json()["service"] == "novasupport-api"
