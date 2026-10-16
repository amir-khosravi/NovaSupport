"""
API Smoke Tests — Phase 16.

Tests the full HTTP intake path:
  POST /api/v1/tickets  →  Kafka  →  Orchestrator  →  Workers
  GET  /api/v1/tickets/{id}/status  →  poll until completed

Requirements:
  All services running: docker compose up -d
  (or: make up)

Run:
  pytest tests/evaluation/test_api_smoke.py -v -m smoke
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

# ── Config ────────────────────────────────────────────────────────────────────

API_BASE = "http://localhost:8000"
POLL_INTERVAL = 2.0      # seconds between status polls
SMOKE_TIMEOUT = 120.0    # max seconds to wait for ticket to complete

pytestmark = pytest.mark.smoke


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _poll_status(
    client: httpx.AsyncClient,
    ticket_id: str,
    timeout: float = SMOKE_TIMEOUT,
) -> dict:
    """Poll GET /api/v1/tickets/{id}/status until status != 'running'."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.get(f"/api/v1/tickets/{ticket_id}/status")
        if resp.status_code == 404:
            # Not yet persisted — keep polling
            await asyncio.sleep(POLL_INTERVAL)
            continue
        resp.raise_for_status()
        data = resp.json()
        if data["status"] != "running":
            return data
        await asyncio.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Ticket {ticket_id} still 'running' after {timeout}s")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
async def http_client():
    async with httpx.AsyncClient(base_url=API_BASE, timeout=30.0) as client:
        yield client


# ── Smoke tests ───────────────────────────────────────────────────────────────

class TestApiHealth:

    async def test_liveness_probe(self, http_client: httpx.AsyncClient):
        resp = await http_client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_readiness_probe(self, http_client: httpx.AsyncClient):
        resp = await http_client.get("/health/ready")
        # Accept 200 (all healthy) or 503 (degraded but running)
        assert resp.status_code in (200, 503)
        data = resp.json()
        assert "status" in data
        assert "checks" in data

    async def test_metrics_endpoint(self, http_client: httpx.AsyncClient):
        resp = await http_client.get("/metrics")
        assert resp.status_code == 200
        assert "novasupport_tickets_submitted_total" in resp.text


class TestTicketSubmission:

    async def test_submit_ticket_returns_202(self, http_client: httpx.AsyncClient):
        resp = await http_client.post("/api/v1/tickets", json={
            "customer_id": "SMOKE-001",
            "message": "I have a question about my delivery.",
        })
        assert resp.status_code == 202
        data = resp.json()
        assert "workflow_id" in data
        assert "ticket_id" in data
        assert data["status"] == "accepted"

    async def test_empty_message_rejected(self, http_client: httpx.AsyncClient):
        resp = await http_client.post("/api/v1/tickets", json={
            "customer_id": "SMOKE-001",
            "message": "",
        })
        assert resp.status_code == 422

    async def test_empty_customer_id_rejected(self, http_client: httpx.AsyncClient):
        resp = await http_client.post("/api/v1/tickets", json={
            "customer_id": "",
            "message": "Hello",
        })
        assert resp.status_code == 422

    async def test_message_too_long_rejected(self, http_client: httpx.AsyncClient):
        resp = await http_client.post("/api/v1/tickets", json={
            "customer_id": "SMOKE-001",
            "message": "x" * 2001,
        })
        assert resp.status_code == 422

    async def test_null_bytes_stripped(self, http_client: httpx.AsyncClient):
        """Null bytes in message must not cause a 500 — they are stripped silently."""
        resp = await http_client.post("/api/v1/tickets", json={
            "customer_id": "SMOKE-001",
            "message": "Hello\x00World",
        })
        assert resp.status_code == 202

    async def test_unknown_ticket_returns_404(self, http_client: httpx.AsyncClient):
        resp = await http_client.get("/api/v1/tickets/TKT-DOES-NOT-EXIST/status")
        assert resp.status_code == 404


class TestFullPipelineSmoke:
    """
    End-to-end smoke: submit a ticket, wait for it to complete, check outcome.
    Requires all services running (Kafka, orchestrator, workers, postgres).
    """

    async def test_general_inquiry_completes(self, http_client: httpx.AsyncClient):
        """A general inquiry ticket must reach completed or escalated status."""
        submit = await http_client.post("/api/v1/tickets", json={
            "customer_id": "C-SMOKE-001",
            "message": "What is your return policy for electronics?",
        })
        assert submit.status_code == 202
        ticket_id = submit.json()["ticket_id"]

        result = await _poll_status(http_client, ticket_id)

        assert result["status"] in ("completed", "escalated"), (
            f"Unexpected status: {result['status']!r}"
        )
        assert result["ticket_id"] == ticket_id

    async def test_delivery_delay_completes_with_category(
        self, http_client: httpx.AsyncClient
    ):
        """A delivery delay ticket must complete and classify as delivery_delay."""
        submit = await http_client.post("/api/v1/tickets", json={
            "customer_id": "C-SMOKE-002",
            "message": "My express delivery is a week late. I need a shipping refund.",
        })
        assert submit.status_code == 202
        ticket_id = submit.json()["ticket_id"]

        result = await _poll_status(http_client, ticket_id)

        assert result["status"] in ("completed", "escalated")
        assert result["category"] == "delivery_delay", (
            f"Expected delivery_delay, got {result['category']!r}"
        )

    async def test_two_concurrent_tickets_dont_cross(
        self, http_client: httpx.AsyncClient
    ):
        """Two simultaneous tickets must each get their own final responses."""
        submit1, submit2 = await asyncio.gather(
            http_client.post("/api/v1/tickets", json={
                "customer_id": "C-CONC-1",
                "message": "My delivery is late.",
            }),
            http_client.post("/api/v1/tickets", json={
                "customer_id": "C-CONC-2",
                "message": "I was charged twice for my order.",
            }),
        )
        assert submit1.status_code == 202
        assert submit2.status_code == 202

        tid1 = submit1.json()["ticket_id"]
        tid2 = submit2.json()["ticket_id"]
        wid1 = submit1.json()["workflow_id"]
        wid2 = submit2.json()["workflow_id"]

        # Workflow IDs must be distinct
        assert wid1 != wid2

        result1, result2 = await asyncio.gather(
            _poll_status(http_client, tid1),
            _poll_status(http_client, tid2),
        )

        # Each ticket must refer to its own workflow
        assert result1["workflow_id"] == wid1
        assert result2["workflow_id"] == wid2
        assert result1["ticket_id"] == tid1
        assert result2["ticket_id"] == tid2
