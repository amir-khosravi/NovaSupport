"""
ResultBus — bridges Kafka AgentResult messages to waiting graph nodes.

Problem: LangGraph nodes are async functions that send a Kafka command
and need to WAIT for the corresponding result. But Kafka is fire-and-forget;
there's no native request/reply.

Solution — correlation_id pattern:
  1. Node generates a unique correlation_id before sending the command.
  2. Node calls result_bus.subscribe(correlation_id) → gets an asyncio.Future.
  3. Node sends the command (with that correlation_id) to Kafka.
  4. Node does: result = await asyncio.wait_for(future, timeout=30)
  5. Meanwhile, ResultBus background task consumes from agent.results.
  6. When a result arrives, ResultBus resolves the matching Future.

Thread safety: all asyncio, single event loop, no locks needed.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from aiokafka import ConsumerRecord

from streaming.consumer import KafkaEventConsumer
from streaming.schemas import AgentResult, deserialize_result
from config.settings import settings

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ResultBus:
    """
    Asynchronous result router for in-flight Kafka commands.

    Usage in a graph node:
        future = ctx.result_bus.subscribe(correlation_id)
        await producer.send(topic, command, key=workflow_id)
        result = await asyncio.wait_for(future, timeout=30.0)
    """

    def __init__(self) -> None:
        # correlation_id → Future that resolves when the result arrives
        self._waiters: dict[str, asyncio.Future[AgentResult]] = {}
        self._consumer: KafkaEventConsumer | None = None
        self._consumer_task: asyncio.Task | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(
        self,
        group_id: str = "novasupport-orchestrator-results",
        bootstrap_servers: str | None = None,
    ) -> None:
        self._consumer = KafkaEventConsumer(
            topic=settings.kafka.result_topic,
            handler=self._on_result,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers or settings.kafka.bootstrap_servers,
            auto_offset_reset="latest",  # only process results for in-flight commands
        )
        await self._consumer.start()
        self._consumer_task = asyncio.create_task(
            self._consumer.run(), name="result-bus-consumer"
        )
        logger.info("ResultBus started — consuming from %s", settings.kafka.result_topic)

    async def stop(self) -> None:
        if self._consumer_task:
            self._consumer_task.cancel()
            await asyncio.gather(self._consumer_task, return_exceptions=True)
        if self._consumer:
            await self._consumer.stop()
        # Fail any still-waiting futures so callers don't hang
        for corr_id, fut in list(self._waiters.items()):
            if not fut.done():
                fut.cancel()
        self._waiters.clear()
        logger.info("ResultBus stopped")

    async def __aenter__(self) -> "ResultBus":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    # ── subscription API ──────────────────────────────────────────────────────

    def subscribe(self, correlation_id: str) -> asyncio.Future[AgentResult]:
        """
        Register interest in a result with the given correlation_id.

        Call this BEFORE sending the command so no result is missed.
        Returns a Future that resolves when the result arrives.
        """
        loop = asyncio.get_event_loop()
        future: asyncio.Future[AgentResult] = loop.create_future()
        self._waiters[correlation_id] = future
        logger.debug("Subscribed corr_id=%s  pending=%d", correlation_id, len(self._waiters))
        return future

    def unsubscribe(self, correlation_id: str) -> None:
        """Cancel and remove a subscription (e.g. on timeout)."""
        fut = self._waiters.pop(correlation_id, None)
        if fut and not fut.done():
            fut.cancel()

    # ── internal handler ──────────────────────────────────────────────────────

    async def deliver(self, result: AgentResult) -> None:
        """Resolve the Future registered for result.correlation_id (public for testing)."""
        future = self._waiters.pop(result.correlation_id, None)
        if future is None:
            logger.debug("No waiter for corr_id=%s", result.correlation_id)
            return
        if not future.done():
            future.set_result(result)

    async def _on_result(self, record: ConsumerRecord) -> None:
        """
        Called by KafkaEventConsumer for every message on agent.results.

        Resolves the matching Future; logs a warning for unmatched results
        (can happen if a worker produced a result for a timed-out command).
        Never raises — KafkaEventConsumer only commits offset after success.
        """
        try:
            result = deserialize_result(record.value)
        except Exception as exc:
            logger.error("Cannot deserialize result at offset=%d: %s", record.offset, exc)
            return  # commit offset — bad message, nothing we can do

        await self.deliver(result)
        logger.debug(
            "Delivered result corr_id=%s status=%s  pending=%d",
            result.correlation_id, result.status, len(self._waiters),
        )
