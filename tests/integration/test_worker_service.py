"""
Integration test: WorkerService end-to-end.

Sends a real AgentCommand to agent.commands,
starts a WorkerService, and verifies an AgentResult
appears on agent.results with the correct correlation_id.

Requires Redpanda running: docker compose up -d redpanda
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka import AIOKafkaConsumer

from apps.worker.handlers import registry
from apps.worker.service import WorkerService
from config.settings import settings
from streaming.producer import KafkaEventProducer
from streaming.schemas import (
    AgentCommand,
    AgentResult,
    TaskStatus,
    TaskType,
    deserialize_result,
    new_id,
    serialize,
)

BOOTSTRAP = settings.kafka.bootstrap_servers


async def _send_command(cmd: AgentCommand) -> None:
    async with KafkaEventProducer(bootstrap_servers=BOOTSTRAP) as producer:
        await producer.send(
            settings.kafka.command_topic,
            cmd,
            key=cmd.workflow_id,
        )


async def _receive_result(
    correlation_id: str,
    timeout_s: float = 10.0,
) -> AgentResult:
    """
    Start a fresh consumer on agent.results and return the first message
    whose correlation_id matches.
    """
    group = f"test-result-consumer-{new_id()}"
    consumer = AIOKafkaConsumer(
        settings.kafka.result_topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=int(timeout_s * 1000),
    )
    await consumer.start()
    await asyncio.sleep(0.4)  # wait for partition assignment

    try:
        async for record in consumer:
            result = deserialize_result(record.value)
            if result.correlation_id == correlation_id:
                return result
    finally:
        await consumer.stop()

    raise AssertionError(
        f"No result with correlation_id={correlation_id!r} received within {timeout_s}s"
    )


# ─────────────────────────────────────────────────────────────────────────────

class TestWorkerServiceEndToEnd:

    async def test_triage_command_produces_completed_result(self):
        """
        Worker receives a TRIAGE_TICKET command and publishes a COMPLETED result.
        """
        corr_id = new_id()
        wf_id = new_id()
        cmd = AgentCommand(
            workflow_id=wf_id,
            ticket_id="TKT-INT-001",
            task_type=TaskType.TRIAGE_TICKET,
            payload={"message": "My express delivery is 5 days late.", "language": "en"},
            correlation_id=corr_id,
        )

        # Start worker with a unique group so it doesn't steal other tests' messages
        worker_group = f"test-worker-{new_id()}"
        worker = WorkerService(
            worker_id="test-worker",
            registry=registry,
            group_id=worker_group,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
        )
        await worker.start()

        # Start result consumer BEFORE sending command (latest offset)
        result_task = asyncio.create_task(_receive_result(corr_id, timeout_s=12.0))
        await asyncio.sleep(0.4)

        # Send the command
        await _send_command(cmd)

        # Run worker until result is received (then cancel)
        worker_task = asyncio.create_task(worker.run())

        try:
            result = await asyncio.wait_for(result_task, timeout=15.0)
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
            await worker.stop()

        # Verify result
        assert result.correlation_id == corr_id
        assert result.workflow_id == wf_id
        assert result.ticket_id == "TKT-INT-001"
        assert result.task_type == TaskType.TRIAGE_TICKET.value
        assert result.status == TaskStatus.COMPLETED
        assert result.is_success()
        assert result.result["category"] == "delivery_delay"
        assert result.result["priority"] == "high"

    async def test_resolve_ticket_produces_refund_action(self):
        """
        Worker receives RESOLVE_TICKET for a 5-day-delayed express order
        and returns refund_shipping_fee action.
        """
        corr_id = new_id()
        wf_id = new_id()
        cmd = AgentCommand(
            workflow_id=wf_id,
            ticket_id="TKT-INT-002",
            task_type=TaskType.RESOLVE_TICKET,
            payload={
                "category": "delivery_delay",
                "days_delayed": 5,
                "shipping_fee": 24.99,
                "order_id": "ORD-0001",
            },
            correlation_id=corr_id,
        )

        worker_group = f"test-worker-{new_id()}"
        worker = WorkerService(
            worker_id="test-worker-2",
            registry=registry,
            group_id=worker_group,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
        )
        await worker.start()

        result_task = asyncio.create_task(_receive_result(corr_id, timeout_s=12.0))
        await asyncio.sleep(0.4)

        await _send_command(cmd)

        worker_task = asyncio.create_task(worker.run())
        try:
            result = await asyncio.wait_for(result_task, timeout=15.0)
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
            await worker.stop()

        assert result.is_success()
        assert result.result["recommended_action"] == "refund_shipping_fee"
        assert abs(result.result["refund_amount"] - 24.99) < 0.01

    async def test_causation_id_chain_preserved(self):
        """
        AgentResult.causation_id must equal the original AgentCommand.event_id.
        """
        corr_id = new_id()
        wf_id = new_id()
        cmd = AgentCommand(
            workflow_id=wf_id,
            ticket_id="TKT-INT-003",
            task_type=TaskType.QUALITY_CHECK,
            payload={"recommended_action": "refund_shipping_fee", "confidence": 0.95},
            correlation_id=corr_id,
        )
        original_event_id = cmd.event_id

        worker_group = f"test-worker-{new_id()}"
        worker = WorkerService(
            worker_id="test-worker-3",
            registry=registry,
            group_id=worker_group,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
        )
        await worker.start()

        result_task = asyncio.create_task(_receive_result(corr_id, timeout_s=12.0))
        await asyncio.sleep(0.4)

        await _send_command(cmd)

        worker_task = asyncio.create_task(worker.run())
        try:
            result = await asyncio.wait_for(result_task, timeout=15.0)
        finally:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
            await worker.stop()

        # causation_id links result back to the command that caused it
        assert result.causation_id == original_event_id
        assert result.correlation_id == corr_id
