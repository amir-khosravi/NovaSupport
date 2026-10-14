"""
Kafka consumer wrapper for NovaSupport.

Wraps aiokafka with:
  - manual offset commit (offset committed only after successful processing)
  - typed message delivery via callback
  - graceful shutdown on SIGINT / cancellation
  - async context manager
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from types import TracebackType

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from config.settings import settings

logger = logging.getLogger(__name__)

# Type alias: handler receives raw bytes, returns nothing (processes in-place)
MessageHandler = Callable[[ConsumerRecord], Awaitable[None]]


class KafkaEventConsumer:
    """
    Async Kafka consumer that calls a handler for every message.

    Manual offset commit ensures a message is only marked as "done"
    AFTER the handler completes successfully.  If the process crashes
    mid-handler, the message will be re-delivered on restart.

    Usage:
        async def handle(record: ConsumerRecord) -> None:
            event = deserialize_command(record.value)
            ...

        async with KafkaEventConsumer("agent.commands", handle) as consumer:
            await consumer.run()
    """

    def __init__(
        self,
        topic: str,
        handler: MessageHandler,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._topic = topic
        self._handler = handler
        self._group_id = group_id or settings.kafka.consumer_group
        self._bootstrap = bootstrap_servers or settings.kafka.bootstrap_servers
        self._auto_offset_reset = auto_offset_reset
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            # Manual commit: we commit only after successful processing
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
            request_timeout_ms=15_000,
            # Receive raw bytes; handler decides how to deserialize
            value_deserializer=None,
        )
        await self._consumer.start()
        self._running = True
        logger.info(
            "Consumer started",
            extra={"topic": self._topic, "group": self._group_id},
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def run(self) -> None:
        """
        Poll for messages in a loop until stop() is called or cancelled.

        Commit order per message:
          1. Receive message
          2. Call handler  ← if this crashes, offset is NOT committed
          3. Commit offset ← only reaches here on success
        """
        if self._consumer is None:
            raise RuntimeError("Consumer not started")

        try:
            async for record in self._consumer:
                if not self._running:
                    break
                try:
                    await self._handler(record)
                    # Manual commit — only after handler succeeds
                    await self._consumer.commit()
                except Exception:
                    logger.exception(
                        "Handler failed — offset not committed, message will be redelivered",
                        extra={
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                        },
                    )
                    # Do NOT commit. On restart, message will be reprocessed.
        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down cleanly")
        finally:
            await self.stop()

    # ── async context manager ──────────────────────────────────────────────

    async def __aenter__(self) -> "KafkaEventConsumer":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop()
