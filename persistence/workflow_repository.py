"""
WorkflowRepository — persist and query LangGraph workflow state in PostgreSQL.

Each workflow produces one row in workflow_states that is upserted at three
points in the graph:

  1. triage_node          → status='running'   (initial record)
  2. complete_ticket_node → status='completed' (final happy-path outcome)
  3. escalate_ticket_node → status='escalated' (quality-check failure)

All methods gracefully degrade: if the pool is None or the DB is unreachable,
a warning is logged and the graph continues without failing the workflow.
This is consistent with the MCP / Qdrant / Redis fallback pattern used across
the rest of the platform.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class WorkflowRecord:
    workflow_id: str
    ticket_id: str
    customer_id: str | None = None
    order_id: str | None = None
    status: str = "running"             # running | completed | escalated | failed
    category: str | None = None
    recommended_action: str | None = None
    refund_amount: float = 0.0
    final_response: str | None = None
    escalation_reason: str | None = None
    state_json: dict = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class WorkflowRepository:
    """
    asyncpg-based repository for workflow_states table.

    Args:
        pool: asyncpg.Pool — pass None to run in no-op mode (tests / DB down).
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    # ── factory ───────────────────────────────────────────────────────────────

    @classmethod
    async def from_settings(cls) -> "WorkflowRepository":
        """Build a real pool from settings. Returns no-op repo if DB unavailable."""
        try:
            import asyncpg
            from config.settings import settings

            dsn = settings.postgres.url.replace("postgresql+asyncpg://", "postgresql://")
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5, command_timeout=5)
            logger.info("WorkflowRepository: PostgreSQL pool created")
            return cls(pool)
        except Exception as exc:
            logger.warning("WorkflowRepository: DB unavailable (%s) — running in no-op mode", exc)
            return cls(None)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    # ── write ─────────────────────────────────────────────────────────────────

    async def save(self, record: WorkflowRecord) -> None:
        """
        Upsert a workflow record.

        Creates the row on first call (status='running'), then updates it on
        subsequent calls (complete / escalate).  ON CONFLICT ensures idempotency.
        """
        if self._pool is None:
            return

        try:
            state_bytes = json.dumps(record.state_json)
            await self._pool.execute(
                """
                INSERT INTO workflow_states (
                    workflow_id, ticket_id, customer_id, order_id, status,
                    category, recommended_action, refund_amount,
                    final_response, escalation_reason, state_json,
                    created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,NOW(),NOW())
                ON CONFLICT (workflow_id) DO UPDATE SET
                    status             = EXCLUDED.status,
                    category           = COALESCE(EXCLUDED.category,           workflow_states.category),
                    recommended_action = COALESCE(EXCLUDED.recommended_action, workflow_states.recommended_action),
                    refund_amount      = EXCLUDED.refund_amount,
                    final_response     = COALESCE(EXCLUDED.final_response,     workflow_states.final_response),
                    escalation_reason  = COALESCE(EXCLUDED.escalation_reason,  workflow_states.escalation_reason),
                    state_json         = EXCLUDED.state_json,
                    updated_at         = NOW()
                """,
                record.workflow_id,
                record.ticket_id,
                record.customer_id,
                record.order_id,
                record.status,
                record.category,
                record.recommended_action,
                record.refund_amount,
                record.final_response,
                record.escalation_reason,
                state_bytes,
            )
            logger.debug(
                "WorkflowRepository.save  wf=%s  status=%s",
                record.workflow_id, record.status,
            )
        except Exception as exc:
            logger.warning(
                "WorkflowRepository.save failed wf=%s: %s", record.workflow_id, exc
            )

    # ── read ──────────────────────────────────────────────────────────────────

    async def get(self, workflow_id: str) -> WorkflowRecord | None:
        """Return a single workflow record, or None if not found / DB down."""
        if self._pool is None:
            return None
        try:
            row = await self._pool.fetchrow(
                "SELECT * FROM workflow_states WHERE workflow_id = $1",
                workflow_id,
            )
            return _row_to_record(row) if row else None
        except Exception as exc:
            logger.warning("WorkflowRepository.get failed wf=%s: %s", workflow_id, exc)
            return None

    async def get_by_ticket_id(self, ticket_id: str) -> WorkflowRecord | None:
        """Return the most recent workflow for a given ticket_id, or None."""
        if self._pool is None:
            return None
        try:
            row = await self._pool.fetchrow(
                """
                SELECT * FROM workflow_states
                WHERE  ticket_id = $1
                ORDER  BY created_at DESC
                LIMIT  1
                """,
                ticket_id,
            )
            return _row_to_record(row) if row else None
        except Exception as exc:
            logger.warning(
                "WorkflowRepository.get_by_ticket_id failed ticket=%s: %s", ticket_id, exc
            )
            return None

    async def list_by_status(
        self, status: str, limit: int = 50
    ) -> list[WorkflowRecord]:
        """Return up to `limit` workflows with the given status, newest first."""
        if self._pool is None:
            return []
        try:
            rows = await self._pool.fetch(
                """
                SELECT * FROM workflow_states
                WHERE  status = $1
                ORDER  BY created_at DESC
                LIMIT  $2
                """,
                status,
                limit,
            )
            return [_row_to_record(r) for r in rows]
        except Exception as exc:
            logger.warning(
                "WorkflowRepository.list_by_status failed status=%s: %s", status, exc
            )
            return []


# ── helpers ───────────────────────────────────────────────────────────────────

def _row_to_record(row: Any) -> WorkflowRecord:
    state = row["state_json"]
    if isinstance(state, str):
        state = json.loads(state)
    return WorkflowRecord(
        workflow_id=row["workflow_id"],
        ticket_id=row["ticket_id"],
        customer_id=row["customer_id"],
        order_id=row["order_id"],
        status=row["status"],
        category=row["category"],
        recommended_action=row["recommended_action"],
        refund_amount=float(row["refund_amount"] or 0),
        final_response=row["final_response"],
        escalation_reason=row["escalation_reason"],
        state_json=dict(state) if state else {},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
