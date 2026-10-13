"""
NovaSupport Worker Service — Phase 11: Reliability.

Flow per message:
  1. Deserialize AgentCommand (poison-pill protection)
  2. Idempotency check via Redis — skip if task_id already processed
  3. Dispatch with exponential-backoff retry (transient errors only)
     • All retries exhausted → publish to DLQ, commit offset
     • Non-transient error  → publish FAILED result, re-raise (no commit)
  4. Mark task_id processed in Redis (TTL = 24 h)
  5. Publish AgentResult(COMPLETED), consumer commits offset

Retry policy:
  Retries ConnectionError / TimeoutError up to MAX_RETRIES attempts.
  Non-transient errors (ValueError, KeyError, …) fail immediately — no retry.

DLQ:
  After all retries fail, the command is published to agent.commands.dlq
  as a JSON blob with the original payload + error + metadata.
  The offset IS committed so the poison message doesn't re-deliver forever.

Idempotency:
  Redis key  novasupport:dedup:<task_id>  with 24-hour TTL prevents
  double-processing when Kafka re-delivers an already-completed message.
  Silently disabled when Redis is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from aiokafka import ConsumerRecord
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity.wait import wait_base

from config.settings import settings
from observability.metrics import inc_dlq, inc_worker_retries, record_task_duration
from streaming.consumer import KafkaEventConsumer
from streaming.producer import KafkaEventProducer
from streaming.schemas import (
    AgentCommand,
    AgentResult,
    TaskStatus,
    deserialize_command,
)
from apps.worker.registry import HandlerRegistry

logger = logging.getLogger(__name__)

# Transient infrastructure errors that warrant a retry
_RETRYABLE = (ConnectionError, TimeoutError, asyncio.TimeoutError)

_IDEMPOTENCY_TTL = 86_400  # 24 hours in seconds


class WorkerService:
    """
    A single worker instance: consumes commands, dispatches, publishes results.

    Args:
        worker_id:    Human-readable label shown in logs (e.g. "worker-1")
        registry:     HandlerRegistry with registered task handlers.
        group_id:     Kafka consumer group.
        _retry_wait:  tenacity wait strategy — inject wait_none() in unit tests
                      to avoid sleeping between retry attempts.
    """

    def __init__(
        self,
        worker_id: str,
        registry: HandlerRegistry,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        auto_offset_reset: str = "earliest",
        _retry_wait: wait_base | None = None,
    ) -> None:
        self.worker_id = worker_id
        self._registry = registry
        self._group_id = group_id or settings.kafka.consumer_group
        self._bootstrap = bootstrap_servers or settings.kafka.bootstrap_servers
        self._auto_offset_reset = auto_offset_reset
        self._retry_wait = _retry_wait or wait_exponential(multiplier=0.5, min=0.5, max=8)
        self._consumer: KafkaEventConsumer | None = None
        self._producer: KafkaEventProducer | None = None
        self._redis: Any = None  # redis.asyncio.Redis, optional

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._producer = KafkaEventProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        self._consumer = KafkaEventConsumer(
            topic=settings.kafka.command_topic,
            handler=self._handle_record,
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap,
            auto_offset_reset=self._auto_offset_reset,
        )
        await self._consumer.start()

        # Optional Redis for idempotency — silently disabled if unavailable
        try:
            import redis.asyncio as aioredis
            self._redis = await aioredis.from_url(
                settings.redis.url, socket_connect_timeout=2, socket_timeout=2,
            )
            await self._redis.ping()
            logger.info("[%s] Redis connected — idempotency enabled", self.worker_id)
        except Exception as exc:
            logger.warning(
                "[%s] Redis unavailable (%s) — idempotency disabled", self.worker_id, exc
            )
            self._redis = None

        logger.info(
            "[%s] Worker started  group=%s  topic=%s",
            self.worker_id, self._group_id, settings.kafka.command_topic,
        )

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()
        if self._redis:
            await self._redis.aclose()
            self._redis = None
        logger.info("[%s] Worker stopped", self.worker_id)

    async def run(self) -> None:
        """Block until cancelled."""
        if self._consumer is None:
            raise RuntimeError("Call start() before run()")
        await self._consumer.run()

    async def __aenter__(self) -> "WorkerService":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    # ── message handler ───────────────────────────────────────────────────────

    async def _handle_record(self, record: ConsumerRecord) -> None:
        """
        Main per-message pipeline: idempotency → retry dispatch → DLQ or result.
        """
        t0 = time.monotonic()

        # 1. Deserialize ──────────────────────────────────────────────────────
        try:
            command: AgentCommand = deserialize_command(record.value)
        except Exception as exc:
            logger.error(
                "[%s] Cannot deserialize  partition=%d  offset=%d: %s",
                self.worker_id, record.partition, record.offset, exc,
            )
            return  # poison pill — commit offset to avoid infinite loop

        logger.info(
            "[%s] Received  task_type=%-25s  task_id=%s  wf=%s  attempt=%d",
            self.worker_id, command.task_type, command.task_id,
            command.workflow_id, command.attempt,
        )

        # 2. Idempotency check ────────────────────────────────────────────────
        if await self._is_duplicate(command.task_id):
            logger.info(
                "[%s] Duplicate skip  task_id=%s", self.worker_id, command.task_id,
            )
            return  # commit offset — already processed

        # 3. Dispatch with retry ──────────────────────────────────────────────
        try:
            result_data = await self._dispatch_with_retry(command)
            elapsed = time.monotonic() - t0
            logger.info(
                "[%s] Completed task_type=%-25s  task_id=%s  elapsed=%.3fs",
                self.worker_id, command.task_type, command.task_id, elapsed,
            )

        except _RETRYABLE as exc:
            # All retry attempts exhausted for a transient error → Dead Letter Queue
            elapsed = time.monotonic() - t0
            logger.error(
                "[%s] DLQ      task_type=%-25s  task_id=%s  elapsed=%.3fs  error=%s",
                self.worker_id, command.task_type, command.task_id, elapsed, exc,
            )
            inc_dlq(task_type=str(command.task_type))
            await self._send_to_dlq(command, str(exc))
            return  # commit offset — message dead-lettered, don't re-deliver

        except Exception as exc:
            # Non-transient failure — notify orchestrator then re-raise (no commit)
            elapsed = time.monotonic() - t0
            logger.exception(
                "[%s] Failed   task_type=%-25s  task_id=%s  elapsed=%.3fs  error=%s",
                self.worker_id, command.task_type, command.task_id, elapsed, exc,
            )
            failed = AgentResult(
                workflow_id=command.workflow_id,
                ticket_id=command.ticket_id,
                task_id=command.task_id,
                task_type=command.task_type,
                status=TaskStatus.FAILED,
                result={},
                error=str(exc),
                correlation_id=command.correlation_id,
                causation_id=command.event_id,
            )
            await self._publish_result(failed, command.workflow_id)
            raise  # keeps offset uncommitted → Kafka redelivers

        # 4. Mark processed (idempotency) + record duration metric ───────────
        await self._mark_processed(command.task_id)
        record_task_duration(task_type=str(command.task_type), seconds=time.monotonic() - t0)

        # 5. Publish success result ────────────────────────────────────────────
        result = AgentResult(
            workflow_id=command.workflow_id,
            ticket_id=command.ticket_id,
            task_id=command.task_id,
            task_type=command.task_type,
            status=TaskStatus.COMPLETED,
            result=result_data,
            correlation_id=command.correlation_id,
            causation_id=command.event_id,
        )
        await self._publish_result(result, command.workflow_id)

    # ── retry ─────────────────────────────────────────────────────────────────

    async def _dispatch_with_retry(self, command: AgentCommand) -> dict:
        """
        Call registry.dispatch() with exponential-backoff retry on transient errors.

        Only retries _RETRYABLE exceptions; non-retryable exceptions propagate
        immediately. After all attempts, re-raises the last transient exception.
        """
        max_retries = settings.worker.max_retries

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type(_RETRYABLE),
            stop=stop_after_attempt(max_retries),
            wait=self._retry_wait,
            reraise=True,
        ):
            with attempt:
                n = attempt.retry_state.attempt_number
                if n > 1:
                    logger.warning(
                        "[%s] Retry %d/%d  task_type=%s  task_id=%s",
                        self.worker_id, n, max_retries,
                        command.task_type, command.task_id,
                    )
                    inc_worker_retries(task_type=str(command.task_type))
                return await self._registry.dispatch(command)

    # ── idempotency ───────────────────────────────────────────────────────────

    async def _is_duplicate(self, task_id: str) -> bool:
        if self._redis is None:
            return False
        try:
            return bool(await self._redis.exists(f"novasupport:dedup:{task_id}"))
        except Exception as exc:
            logger.warning("[%s] Redis exists() failed: %s", self.worker_id, exc)
            return False

    async def _mark_processed(self, task_id: str) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.set(
                f"novasupport:dedup:{task_id}", "1", ex=_IDEMPOTENCY_TTL
            )
        except Exception as exc:
            logger.warning("[%s] Redis set() failed: %s", self.worker_id, exc)

    # ── DLQ ───────────────────────────────────────────────────────────────────

    async def _send_to_dlq(self, command: AgentCommand, error: str) -> None:
        payload = json.dumps({
            "task_id": command.task_id,
            "task_type": command.task_type,
            "workflow_id": command.workflow_id,
            "ticket_id": command.ticket_id,
            "payload": command.payload,
            "error": error,
            "worker_id": self.worker_id,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "attempts": settings.worker.max_retries,
        }).encode()
        try:
            if self._producer:
                await self._producer.send_raw(
                    settings.kafka.dlq_topic, payload, key=command.workflow_id,
                )
        except Exception as exc:
            logger.error("[%s] Could not publish to DLQ: %s", self.worker_id, exc)

    # ── result publisher ──────────────────────────────────────────────────────

    async def _publish_result(self, result: AgentResult, workflow_id: str) -> None:
        if self._producer is None:
            logger.warning("[%s] Producer not available — result dropped", self.worker_id)
            return
        await self._producer.send(settings.kafka.result_topic, result, key=workflow_id)
        logger.debug(
            "[%s] Published  task_id=%s  status=%s",
            self.worker_id, result.task_id, result.status,
        )
