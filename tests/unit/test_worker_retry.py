"""
Unit tests for Phase 11 reliability features in WorkerService:

  1. Retry on transient error then succeeds
  2. DLQ after all retries exhausted
  3. Non-retryable error: FAILED result published, exception re-raised
  4. Idempotency: duplicate task_id is silently skipped
  5. Idempotency: task_id marked in Redis after success
  6. Graceful fallback when Redis is unavailable (idempotency disabled)

None of these tests require Kafka, Redis, or any external service — all
infrastructure is replaced with AsyncMocks injected before WorkerService.start().
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
from tenacity import wait_none

from apps.worker.service import WorkerService, _RETRYABLE, _IDEMPOTENCY_TTL
from apps.worker.registry import HandlerRegistry
from streaming.schemas import AgentCommand, AgentResult, TaskStatus, TaskType, new_id


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_command(task_type: TaskType = TaskType.TRIAGE_TICKET, payload: dict | None = None) -> AgentCommand:
    return AgentCommand(
        workflow_id=new_id(),
        ticket_id="TKT-RETRY-001",
        task_type=task_type,
        payload=payload or {"message": "test"},
        correlation_id=new_id(),
    )


def _make_record(command: AgentCommand) -> SimpleNamespace:
    """Minimal ConsumerRecord-like object with serialised command."""
    raw = command.model_dump_json().encode()
    return SimpleNamespace(value=raw, partition=0, offset=0)


async def _make_service(registry: HandlerRegistry | None = None) -> WorkerService:
    """
    Build a WorkerService with all infrastructure mocked out.

    Returns the service in a state as if start() completed except Kafka/Redis
    setup — those are injected directly so tests don't need running services.
    """
    reg = registry or HandlerRegistry()
    svc = WorkerService(
        worker_id="test-worker",
        registry=reg,
        _retry_wait=wait_none(),  # no real sleep between retries
    )
    # Inject a mock producer
    svc._producer = AsyncMock()
    svc._producer.send = AsyncMock()
    svc._producer.send_raw = AsyncMock()
    return svc


# ── 1. Retry on transient error then succeeds ─────────────────────────────────

class TestRetryOnTransientError:

    async def test_retries_connection_error_then_succeeds(self):
        """
        Handler raises ConnectionError twice, succeeds on third attempt.
        No DLQ published, COMPLETED result published.
        """
        registry = HandlerRegistry()
        call_count = 0

        @registry.register(TaskType.TRIAGE_TICKET)
        async def flaky_handler(cmd: AgentCommand) -> dict:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("DB connection refused")
            return {"category": "delivery_delay", "priority": "high"}

        svc = await _make_service(registry)
        record = _make_record(_make_command(TaskType.TRIAGE_TICKET))
        await svc._handle_record(record)

        assert call_count == 3
        svc._producer.send_raw.assert_not_called()  # no DLQ
        svc._producer.send.assert_called_once()
        result_arg: AgentResult = svc._producer.send.call_args[0][1]
        assert result_arg.status == TaskStatus.COMPLETED
        assert result_arg.result["category"] == "delivery_delay"

    async def test_retries_timeout_error(self):
        """asyncio.TimeoutError is retryable."""
        registry = HandlerRegistry()
        attempts = []

        @registry.register(TaskType.TRIAGE_TICKET)
        async def slow_handler(cmd: AgentCommand) -> dict:
            attempts.append(1)
            if len(attempts) < 2:
                raise asyncio.TimeoutError()
            return {"ok": True}

        svc = await _make_service(registry)
        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        assert len(attempts) == 2
        result_arg: AgentResult = svc._producer.send.call_args[0][1]
        assert result_arg.status == TaskStatus.COMPLETED

    async def test_retry_count_is_max_retries(self):
        """Number of attempts is capped at settings.worker.max_retries."""
        from config.settings import settings

        registry = HandlerRegistry()
        attempts = []

        @registry.register(TaskType.TRIAGE_TICKET)
        async def always_fails(cmd: AgentCommand) -> dict:
            attempts.append(1)
            raise ConnectionError("always")

        svc = await _make_service(registry)
        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        assert len(attempts) == settings.worker.max_retries


# ── 2. DLQ after all retries exhausted ───────────────────────────────────────

class TestDeadLetterQueue:

    async def test_dlq_published_after_max_retries(self):
        """After all retries fail, command is sent to DLQ and offset committed."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def always_fails(cmd: AgentCommand) -> dict:
            raise TimeoutError("network timeout")

        svc = await _make_service(registry)
        command = _make_command(TaskType.TRIAGE_TICKET)
        await svc._handle_record(_make_record(command))

        # DLQ was called once
        svc._producer.send_raw.assert_called_once()
        dlq_call = svc._producer.send_raw.call_args
        raw_bytes: bytes = dlq_call[0][1]
        dlq_payload = json.loads(raw_bytes)

        assert dlq_payload["task_id"] == command.task_id
        # task_type stored as string (use_enum_values=True)
        assert dlq_payload["task_type"] == command.task_type
        assert "network timeout" in dlq_payload["error"]
        assert dlq_payload["worker_id"] == "test-worker"
        assert "failed_at" in dlq_payload

    async def test_success_result_not_published_after_dlq(self):
        """When DLQ path is taken, no COMPLETED/FAILED result is sent."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def always_fails(cmd: AgentCommand) -> dict:
            raise ConnectionError("gone")

        svc = await _make_service(registry)
        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        svc._producer.send.assert_not_called()

    async def test_dlq_topic_key_is_workflow_id(self):
        """DLQ message key is the workflow_id for routing/compaction."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def always_fails(cmd: AgentCommand) -> dict:
            raise ConnectionError("x")

        svc = await _make_service(registry)
        command = _make_command(TaskType.TRIAGE_TICKET)
        await svc._handle_record(_make_record(command))

        dlq_call = svc._producer.send_raw.call_args
        assert dlq_call[1].get("key") == command.workflow_id or dlq_call[0][2] == command.workflow_id


# ── 3. Non-retryable errors ───────────────────────────────────────────────────

class TestNonRetryableErrors:

    async def test_value_error_not_retried(self):
        """ValueError is not in _RETRYABLE — handler must be called exactly once."""
        registry = HandlerRegistry()
        call_count = 0

        @registry.register(TaskType.TRIAGE_TICKET)
        async def bad_handler(cmd: AgentCommand) -> dict:
            nonlocal call_count
            call_count += 1
            raise ValueError("bad payload")

        svc = await _make_service(registry)
        with pytest.raises(ValueError, match="bad payload"):
            await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        assert call_count == 1  # no retries

    async def test_failed_result_published_for_non_retryable(self):
        """A FAILED AgentResult is published before re-raising non-transient errors."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def bad_handler(cmd: AgentCommand) -> dict:
            raise KeyError("missing_field")

        svc = await _make_service(registry)
        with pytest.raises(KeyError):
            await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        svc._producer.send.assert_called_once()
        result_arg: AgentResult = svc._producer.send.call_args[0][1]
        assert result_arg.status == TaskStatus.FAILED
        assert "missing_field" in result_arg.error

    async def test_no_dlq_for_non_retryable_error(self):
        """Non-transient errors must NOT be sent to DLQ — caller should investigate."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def bad_handler(cmd: AgentCommand) -> dict:
            raise RuntimeError("logic error")

        svc = await _make_service(registry)
        with pytest.raises(RuntimeError):
            await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        svc._producer.send_raw.assert_not_called()


# ── 4. Idempotency — duplicate skip ──────────────────────────────────────────

class TestIdempotencyDuplicateSkip:

    async def test_duplicate_task_id_not_dispatched(self):
        """When Redis says task_id is already processed, handler is never called."""
        registry = HandlerRegistry()
        handler_called = False

        @registry.register(TaskType.TRIAGE_TICKET)
        async def should_not_run(cmd: AgentCommand) -> dict:
            nonlocal handler_called
            handler_called = True
            return {}

        svc = await _make_service(registry)
        # Simulate Redis already having this key
        svc._redis = AsyncMock()
        svc._redis.exists = AsyncMock(return_value=1)

        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        assert handler_called is False
        svc._producer.send.assert_not_called()
        svc._producer.send_raw.assert_not_called()

    async def test_duplicate_check_uses_correct_key(self):
        """Redis key must be novasupport:dedup:<task_id>."""
        svc = await _make_service()
        svc._redis = AsyncMock()
        svc._redis.exists = AsyncMock(return_value=0)

        task_id = new_id()
        assert await svc._is_duplicate(task_id) is False
        svc._redis.exists.assert_called_once_with(f"novasupport:dedup:{task_id}")


# ── 5. Idempotency — mark processed ──────────────────────────────────────────

class TestIdempotencyMarkProcessed:

    async def test_task_id_written_to_redis_after_success(self):
        """After a successful dispatch, task_id is written to Redis with 24h TTL."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def ok_handler(cmd: AgentCommand) -> dict:
            return {"category": "delivery_delay"}

        svc = await _make_service(registry)
        svc._redis = AsyncMock()
        svc._redis.exists = AsyncMock(return_value=0)
        svc._redis.set = AsyncMock()

        command = _make_command(TaskType.TRIAGE_TICKET)
        await svc._handle_record(_make_record(command))

        svc._redis.set.assert_called_once_with(
            f"novasupport:dedup:{command.task_id}", "1", ex=_IDEMPOTENCY_TTL
        )

    async def test_task_id_not_written_after_dlq(self):
        """When a command is dead-lettered, it is NOT marked processed (allow manual retry)."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def always_fails(cmd: AgentCommand) -> dict:
            raise ConnectionError("x")

        svc = await _make_service(registry)
        svc._redis = AsyncMock()
        svc._redis.exists = AsyncMock(return_value=0)
        svc._redis.set = AsyncMock()

        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        svc._redis.set.assert_not_called()


# ── 6. Redis unavailable — graceful degradation ───────────────────────────────

class TestIdempotencyRedisUnavailable:

    async def test_processing_continues_without_redis(self):
        """When Redis is None (unavailable), tasks still execute normally."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def ok_handler(cmd: AgentCommand) -> dict:
            return {"category": "delivery_delay"}

        svc = await _make_service(registry)
        svc._redis = None  # Redis not available

        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        svc._producer.send.assert_called_once()
        result_arg: AgentResult = svc._producer.send.call_args[0][1]
        assert result_arg.status == TaskStatus.COMPLETED

    async def test_is_duplicate_returns_false_without_redis(self):
        """_is_duplicate() returns False (not a dup) when Redis is unavailable."""
        svc = await _make_service()
        svc._redis = None

        assert await svc._is_duplicate("any-task-id") is False

    async def test_mark_processed_silently_skips_without_redis(self):
        """_mark_processed() is a no-op when Redis is unavailable."""
        svc = await _make_service()
        svc._redis = None
        # Should not raise
        await svc._mark_processed("any-task-id")

    async def test_redis_exception_does_not_block_processing(self):
        """If Redis.exists() raises mid-flight, processing still completes."""
        registry = HandlerRegistry()

        @registry.register(TaskType.TRIAGE_TICKET)
        async def ok_handler(cmd: AgentCommand) -> dict:
            return {"done": True}

        svc = await _make_service(registry)
        svc._redis = AsyncMock()
        svc._redis.exists = AsyncMock(side_effect=ConnectionError("redis down"))
        svc._redis.set = AsyncMock(side_effect=ConnectionError("redis down"))

        await svc._handle_record(_make_record(_make_command(TaskType.TRIAGE_TICKET)))

        result_arg: AgentResult = svc._producer.send.call_args[0][1]
        assert result_arg.status == TaskStatus.COMPLETED
