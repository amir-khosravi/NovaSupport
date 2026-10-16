"""
Unit tests for Phase 12 WorkflowRepository.

All tests use a mock asyncpg pool — no real database required.
Tests verify:
  - save() builds the correct SQL and parameters
  - get() maps DB rows back to WorkflowRecord correctly
  - list_by_status() returns records in order
  - graceful no-op when pool is None (DB unavailable)
  - upsert semantics: save() can be called multiple times for same workflow_id
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from persistence.workflow_repository import WorkflowRecord, WorkflowRepository


# ── helpers ───────────────────────────────────────────────────────────────────

def _mock_pool() -> MagicMock:
    pool = MagicMock()
    pool.execute = AsyncMock(return_value=None)
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetch = AsyncMock(return_value=[])
    pool.close = AsyncMock()
    return pool


def _make_record(
    workflow_id: str = "WF-001",
    ticket_id: str = "TKT-001",
    status: str = "running",
    **kwargs,
) -> WorkflowRecord:
    return WorkflowRecord(
        workflow_id=workflow_id,
        ticket_id=ticket_id,
        status=status,
        **kwargs,
    )


def _make_db_row(record: WorkflowRecord) -> MagicMock:
    """Simulate an asyncpg Record (dict-like)."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "workflow_id": record.workflow_id,
        "ticket_id": record.ticket_id,
        "customer_id": record.customer_id,
        "order_id": record.order_id,
        "status": record.status,
        "category": record.category,
        "recommended_action": record.recommended_action,
        "refund_amount": record.refund_amount,
        "final_response": record.final_response,
        "escalation_reason": record.escalation_reason,
        "state_json": record.state_json,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }[key]
    return row


# ── save() ────────────────────────────────────────────────────────────────────

class TestSave:

    async def test_save_calls_execute_once(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.save(_make_record())
        pool.execute.assert_called_once()

    async def test_save_passes_correct_workflow_id(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.save(_make_record(workflow_id="WF-SPECIAL"))
        args = pool.execute.call_args[0]
        # First positional arg is SQL, second is workflow_id
        assert args[1] == "WF-SPECIAL"

    async def test_save_passes_ticket_id(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.save(_make_record(ticket_id="TKT-XYZ"))
        args = pool.execute.call_args[0]
        assert args[2] == "TKT-XYZ"

    async def test_save_passes_status(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.save(_make_record(status="completed"))
        args = pool.execute.call_args[0]
        # status is $5 (index 5 in args tuple, which starts at 1)
        assert args[5] == "completed"

    async def test_save_passes_refund_amount(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.save(_make_record(refund_amount=24.99, status="completed"))
        args = pool.execute.call_args[0]
        assert args[8] == 24.99

    async def test_save_can_be_called_multiple_times(self):
        """Upsert is idempotent — calling twice for same workflow is fine."""
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        rec = _make_record(workflow_id="WF-DUP")
        await repo.save(rec)
        await repo.save(rec)
        assert pool.execute.call_count == 2

    async def test_save_silently_handles_db_error(self):
        """If execute raises, save() logs a warning and does not re-raise."""
        pool = _mock_pool()
        pool.execute = AsyncMock(side_effect=ConnectionError("DB down"))
        repo = WorkflowRepository(pool)
        # should not raise
        await repo.save(_make_record())

    async def test_save_noop_when_pool_is_none(self):
        """No-op mode — pool=None means no calls are made."""
        repo = WorkflowRepository(None)
        # should not raise, no pool to call
        await repo.save(_make_record())


# ── get() ─────────────────────────────────────────────────────────────────────

class TestGet:

    async def test_get_returns_none_when_not_found(self):
        pool = _mock_pool()
        pool.fetchrow = AsyncMock(return_value=None)
        repo = WorkflowRepository(pool)
        result = await repo.get("MISSING")
        assert result is None

    async def test_get_returns_record_when_found(self):
        record = _make_record(
            workflow_id="WF-FOUND",
            ticket_id="TKT-001",
            status="completed",
            category="delivery_delay",
            recommended_action="refund_shipping_fee",
            refund_amount=24.99,
        )
        pool = _mock_pool()
        pool.fetchrow = AsyncMock(return_value=_make_db_row(record))
        repo = WorkflowRepository(pool)

        result = await repo.get("WF-FOUND")

        assert result is not None
        assert result.workflow_id == "WF-FOUND"
        assert result.status == "completed"
        assert result.category == "delivery_delay"
        assert result.recommended_action == "refund_shipping_fee"
        assert result.refund_amount == pytest.approx(24.99)

    async def test_get_passes_workflow_id_to_query(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.get("WF-QUERY-TEST")
        call_args = pool.fetchrow.call_args[0]
        assert "WF-QUERY-TEST" in call_args

    async def test_get_returns_none_on_db_error(self):
        pool = _mock_pool()
        pool.fetchrow = AsyncMock(side_effect=ConnectionError("DB down"))
        repo = WorkflowRepository(pool)
        result = await repo.get("WF-001")
        assert result is None

    async def test_get_returns_none_when_pool_is_none(self):
        repo = WorkflowRepository(None)
        result = await repo.get("WF-001")
        assert result is None


# ── list_by_status() ──────────────────────────────────────────────────────────

class TestListByStatus:

    async def test_returns_empty_list_when_no_rows(self):
        pool = _mock_pool()
        pool.fetch = AsyncMock(return_value=[])
        repo = WorkflowRepository(pool)
        result = await repo.list_by_status("completed")
        assert result == []

    async def test_returns_multiple_records(self):
        records = [
            _make_record(workflow_id=f"WF-{i}", status="completed")
            for i in range(3)
        ]
        pool = _mock_pool()
        pool.fetch = AsyncMock(return_value=[_make_db_row(r) for r in records])
        repo = WorkflowRepository(pool)

        result = await repo.list_by_status("completed")
        assert len(result) == 3
        assert all(r.status == "completed" for r in result)

    async def test_passes_status_and_limit_to_query(self):
        pool = _mock_pool()
        repo = WorkflowRepository(pool)
        await repo.list_by_status("escalated", limit=10)
        call_args = pool.fetch.call_args[0]
        assert "escalated" in call_args
        assert 10 in call_args

    async def test_returns_empty_list_on_db_error(self):
        pool = _mock_pool()
        pool.fetch = AsyncMock(side_effect=ConnectionError("DB down"))
        repo = WorkflowRepository(pool)
        result = await repo.list_by_status("running")
        assert result == []

    async def test_returns_empty_list_when_pool_is_none(self):
        repo = WorkflowRepository(None)
        result = await repo.list_by_status("completed")
        assert result == []


# ── graph integration ─────────────────────────────────────────────────────────

class TestGraphIntegration:
    """
    Verify that complete_ticket_node and escalate_ticket_node call repo.save()
    and that they still work when repo=None (DB disabled).
    """

    async def test_complete_node_calls_repo_save(self):
        from apps.orchestrator.graph import complete_ticket_node
        from langchain_core.runnables import RunnableConfig
        from apps.orchestrator.graph import WorkflowContext
        from streaming.producer import KafkaEventProducer
        from apps.orchestrator.result_bus import ResultBus

        mock_repo = MagicMock()
        mock_repo.save = AsyncMock()

        ctx = WorkflowContext(
            producer=MagicMock(),
            result_bus=MagicMock(),
            repo=mock_repo,
        )
        state = {
            "workflow_id": "WF-TEST",
            "ticket_id": "TKT-TEST",
            "response_text": "Your refund has been processed.",
            "recommended_action": "refund_shipping_fee",
            "refund_amount": 24.99,
            "triage_category": "delivery_delay",
        }
        config = RunnableConfig(configurable={"ctx": ctx})

        result = await complete_ticket_node(state, config)

        mock_repo.save.assert_called_once()
        saved: WorkflowRecord = mock_repo.save.call_args[0][0]
        assert saved.status == "completed"
        assert saved.workflow_id == "WF-TEST"
        assert saved.recommended_action == "refund_shipping_fee"
        assert saved.refund_amount == pytest.approx(24.99)
        assert result["completed"] is True

    async def test_escalate_node_calls_repo_save(self):
        from apps.orchestrator.graph import escalate_ticket_node
        from langchain_core.runnables import RunnableConfig
        from apps.orchestrator.graph import WorkflowContext

        mock_repo = MagicMock()
        mock_repo.save = AsyncMock()

        ctx = WorkflowContext(
            producer=MagicMock(),
            result_bus=MagicMock(),
            repo=mock_repo,
        )
        state = {
            "workflow_id": "WF-ESC",
            "ticket_id": "TKT-ESC",
            "recommended_action": "follow_up",
            "resolution_confidence": 0.4,
        }
        config = RunnableConfig(configurable={"ctx": ctx})

        result = await escalate_ticket_node(state, config)

        mock_repo.save.assert_called_once()
        saved: WorkflowRecord = mock_repo.save.call_args[0][0]
        assert saved.status == "escalated"
        assert saved.escalation_reason is not None
        assert result["final_action"] == "escalated"

    async def test_complete_node_works_without_repo(self):
        """repo=None must not raise — DB is optional."""
        from apps.orchestrator.graph import complete_ticket_node
        from langchain_core.runnables import RunnableConfig
        from apps.orchestrator.graph import WorkflowContext

        ctx = WorkflowContext(
            producer=MagicMock(),
            result_bus=MagicMock(),
            repo=None,
        )
        state = {
            "workflow_id": "WF-NODB",
            "ticket_id": "TKT-NODB",
            "response_text": "Thank you.",
            "recommended_action": "follow_up",
            "refund_amount": 0.0,
        }
        config = RunnableConfig(configurable={"ctx": ctx})
        result = await complete_ticket_node(state, config)
        assert result["completed"] is True
