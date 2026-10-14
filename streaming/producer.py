"""
Kafka producer wrapper for NovaSupport.

Wraps aiokafka with:
  - automatic serialization of BaseEvent models
  - partition key strategy (workflow_id keeps related events on same partition)
  - async context manager for clean startup/shutdown
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaProducer

from config.settings import settings
from streaming.schemas import BaseEvent, serialize

if TYPE_CHECKING:
    pass


class KafkaEventProducer:
    """
    Async Kafka producer.

    Usage:
        async with KafkaEventProducer() as producer:
            await producer.send(topic, event, key=workflow_id)
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._bootstrap = bootstrap_servers or settings.kafka.bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            # Serialize event → UTF-8 JSON bytes
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
            # Partition key as bytes
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Wait for leader acknowledgment (safe default for demo)
            acks="all",
            # Retry on transient failures
            retry_backoff_ms=200,
            request_timeout_ms=15_000,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def send(
        self,
        topic: str,
        event: BaseEvent,
        key: str | None = None,
    ) -> None:
        """
        Serialize and publish an event.

        key should be the workflow_id so all events for one workflow
        land on the same partition (preserving order within a workflow).
        """
        if self._producer is None:
            raise RuntimeError("Producer not started — use 'async with' or call start()")

        raw = serialize(event)
        await self._producer.send_and_wait(topic, value=raw, key=key)

    async def send_raw(self, topic: str, value: bytes, key: str | None = None) -> None:
        """Send pre-serialized bytes directly (useful for testing)."""
        if self._producer is None:
            raise RuntimeError("Producer not started")
        await self._producer.send_and_wait(topic, value=value, key=key)

    # ── async context manager ──────────────────────────────────────────────

    async def __aenter__(self) -> "KafkaEventProducer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop()


# ── convenience function for one-shot sends ───────────────────────────────────

async def publish_event(topic: str, event: BaseEvent, key: str | None = None) -> None:
    """Send a single event without keeping the producer alive."""
    async with KafkaEventProducer() as producer:
        await producer.send(topic, event, key=key)
