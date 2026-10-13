"""
OrchestratorRunner — listens for TicketCreatedEvents and runs the LangGraph workflow.

Startup sequence:
  1. Start KafkaEventProducer
  2. Start ResultBus (background Kafka consumer on agent.results)
  3. Start ticket consumer (listens to ticket.created)
  4. For each ticket: ainvoke the compiled LangGraph workflow

Shutdown: cancels all in-flight workflow tasks, stops consumers/producer cleanly.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import ConsumerRecord

from apps.orchestrator.graph import WorkflowContext, workflow
from apps.orchestrator.result_bus import ResultBus
from config.settings import settings
from streaming.consumer import KafkaEventConsumer
from streaming.producer import KafkaEventProducer
from streaming.schemas import TicketCreatedEvent, deserialize_ticket_created

logger = logging.getLogger(__name__)


class OrchestratorRunner:
    """
    Orchestrates the full NovaSupport ticket resolution workflow.

    Each incoming TicketCreatedEvent spawns an independent asyncio.Task
    that runs the LangGraph workflow to completion.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        result_group_id: str = "novasupport-orchestrator-results",
        ticket_group_id: str = "novasupport-orchestrator-tickets",
    ) -> None:
        self._bootstrap = bootstrap_servers or settings.kafka.bootstrap_servers
        self._result_group_id = result_group_id
        self._ticket_group_id = ticket_group_id

        self._producer: KafkaEventProducer | None = None
        self._result_bus: ResultBus | None = None
        self._ticket_consumer: KafkaEventConsumer | None = None
        self._consumer_task: asyncio.Task | None = None
        self._workflow_tasks: set[asyncio.Task] = set()
        self._redis = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            import redis.asyncio as aioredis
            from config.settings import settings
            self._redis = aioredis.from_url(
                settings.redis.url, socket_connect_timeout=2, socket_timeout=2,
            )
            await self._redis.ping()
            logger.info("OrchestratorRunner: Redis connected for step events")
        except Exception as exc:
            logger.warning("OrchestratorRunner: Redis unavailable (%s) — step events disabled", exc)
            self._redis = None

        self._producer = KafkaEventProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        self._result_bus = ResultBus()
        await self._result_bus.start(
            group_id=self._result_group_id,
            bootstrap_servers=self._bootstrap,
        )

        self._ticket_consumer = KafkaEventConsumer(
            topic=settings.kafka.ticket_topic,
            handler=self._on_ticket,
            group_id=self._ticket_group_id,
            bootstrap_servers=self._bootstrap,
        )
        await self._ticket_consumer.start()

        logger.info(
            "OrchestratorRunner started  ticket_topic=%s  result_topic=%s",
            settings.kafka.ticket_topic,
            settings.kafka.result_topic,
        )

    async def stop(self) -> None:
        # Cancel in-flight workflows
        for task in list(self._workflow_tasks):
            task.cancel()
        if self._workflow_tasks:
            await asyncio.gather(*self._workflow_tasks, return_exceptions=True)

        if self._ticket_consumer:
            await self._ticket_consumer.stop()
        if self._result_bus:
            await self._result_bus.stop()
        if self._producer:
            await self._producer.stop()
        if self._redis:
            await self._redis.aclose()

        logger.info("OrchestratorRunner stopped")

    async def run(self) -> None:
        """Block until cancelled."""
        if self._ticket_consumer is None:
            raise RuntimeError("Call start() before run()")
        await self._ticket_consumer.run()

    async def __aenter__(self) -> "OrchestratorRunner":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    # ── ticket handler ────────────────────────────────────────────────────────

    async def _on_ticket(self, record: ConsumerRecord) -> None:
        """
        Called for every message on ticket.created.
        Deserializes the event and spawns a workflow task.
        """
        try:
            ticket = deserialize_ticket_created(record.value)
        except Exception as exc:
            logger.error(
                "Cannot deserialize TicketCreatedEvent at offset=%d: %s",
                record.offset, exc,
            )
            return  # commit offset — poison pill

        logger.info(
            "New ticket  ticket_id=%s  customer=%s  order=%s",
            ticket.ticket_id, ticket.customer_id, ticket.order_id,
        )

        task = asyncio.create_task(
            self._run_workflow(ticket),
            name=f"workflow-{ticket.workflow_id}",
        )
        self._workflow_tasks.add(task)
        task.add_done_callback(self._workflow_tasks.discard)

    # ── workflow execution ────────────────────────────────────────────────────

    async def _run_workflow(self, ticket: TicketCreatedEvent) -> None:
        """
        Run the LangGraph workflow for one ticket.
        Any unhandled exception is logged; it does NOT crash the runner.
        """
        assert self._producer is not None
        assert self._result_bus is not None

        initial_state: dict = {
            "workflow_id": ticket.workflow_id,
            "ticket_id": ticket.ticket_id,
            "customer_id": ticket.customer_id,
            "order_id": ticket.order_id,
            "message": ticket.message,
            "language": ticket.language,
        }

        ctx = WorkflowContext(
            producer=self._producer,
            result_bus=self._result_bus,
            redis=self._redis,
        )

        config = {"configurable": {"ctx": ctx}}

        try:
            logger.info(
                "Workflow START  wf=%s  ticket=%s",
                ticket.workflow_id, ticket.ticket_id,
            )
            final_state = await workflow.ainvoke(initial_state, config=config)

            logger.info(
                "Workflow END    wf=%s  ticket=%s  action=%s  refund=%.2f CAD  quality=%s",
                ticket.workflow_id,
                ticket.ticket_id,
                final_state.get("final_action", "unknown"),
                final_state.get("refund_amount", 0.0),
                final_state.get("quality_status", "unknown"),
            )

        except asyncio.CancelledError:
            logger.info("Workflow cancelled  wf=%s", ticket.workflow_id)
        except Exception as exc:
            logger.exception(
                "Workflow FAILED  wf=%s  ticket=%s  error=%s",
                ticket.workflow_id, ticket.ticket_id, exc,
            )
