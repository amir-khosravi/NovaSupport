"""
Integration test: Orchestrator + Worker end-to-end.

Sends a real TicketCreatedEvent, runs the orchestrator and a worker pool,
and verifies the final workflow state is correct.

Requires Redpanda running: docker compose up -d redpanda

This is the demo scenario:
  TKT-0001 → express delivery delayed 5 days → refund_shipping_fee
"""

from __future__ import annotations

import asyncio

import pytest

from apps.orchestrator.graph import WorkflowContext, build_graph
from apps.orchestrator.result_bus import ResultBus
from apps.worker.handlers import registry
from apps.worker.service import WorkerService
from config.settings import settings
from streaming.producer import KafkaEventProducer
from streaming.schemas import TicketCreatedEvent, new_id

BOOTSTRAP = settings.kafka.bootstrap_servers


async def _run_workers(group_id: str, count: int = 2) -> list[asyncio.Task]:
    """Start `count` WorkerService instances using 'latest' offset (clean test runs)."""
    tasks = []
    for i in range(count):
        worker = WorkerService(
            worker_id=f"e2e-worker-{i + 1}",
            registry=registry,
            group_id=group_id,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",  # skip old test messages
        )
        await worker.start()
        task = asyncio.create_task(worker.run(), name=f"worker-{i + 1}")
        tasks.append((worker, task))
    return tasks


class TestOrchestratorEndToEnd:

    async def test_express_delay_full_pipeline(self):
        """
        Full pipeline: TicketCreatedEvent → Orchestrator → Workers → final state.

        Verifies:
          - workflow completes without errors
          - triage classifies as delivery_delay
          - 5-day delay triggers refund_shipping_fee
          - quality check approves
          - final_action == refund_shipping_fee
        """
        # Unique group IDs so this test's workers don't interfere with others
        worker_group = f"e2e-workers-{new_id()[:8]}"
        result_group = f"e2e-orchestrator-results-{new_id()[:8]}"

        # Start workers
        worker_pairs = await _run_workers(worker_group, count=2)
        worker_tasks = [t for _, t in worker_pairs]
        workers = [w for w, _ in worker_pairs]

        # Start orchestrator components
        producer = KafkaEventProducer(bootstrap_servers=BOOTSTRAP)
        await producer.start()

        result_bus = ResultBus()
        await result_bus.start(group_id=result_group, bootstrap_servers=BOOTSTRAP)

        # Give consumers time to get partition assignments
        await asyncio.sleep(1.0)

        ctx = WorkflowContext(producer=producer, result_bus=result_bus)
        config = {"configurable": {"ctx": ctx}}

        # Build a TicketCreatedEvent (simulating what the intake API would produce)
        wf_id = new_id()
        ticket = TicketCreatedEvent(
            ticket_id="TKT-E2E-001",
            customer_id="C-0001",
            order_id="ORD-0001",
            message="My express delivery is 5 days late. I want a refund please.",
        )

        initial_state = {
            "workflow_id": ticket.workflow_id,
            "ticket_id": ticket.ticket_id,
            "customer_id": ticket.customer_id,
            "order_id": ticket.order_id,
            "message": ticket.message,
            "language": ticket.language,
        }

        graph = build_graph()

        try:
            # Run workflow — this blocks until all 7 tasks complete
            final = await asyncio.wait_for(
                graph.ainvoke(initial_state, config=config),
                timeout=60.0,
            )
        finally:
            # Shutdown workers and infrastructure
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            for w in workers:
                await w.stop()
            await result_bus.stop()
            await producer.stop()

        # ── Assertions ──────────────────────────────────────────────────────
        assert final["triage_category"] == "delivery_delay"
        assert final["triage_priority"] == "high"
        assert final["order_shipping_method"] == "express"
        assert final["shipment_status"] == "delayed"
        assert final["days_delayed"] == 5
        assert len(final["policy_sources"]) >= 1
        assert final["recommended_action"] == "refund_shipping_fee"
        assert abs(final["refund_amount"] - 24.99) < 0.01
        assert final["quality_status"] == "approved"
        assert final["validation_passed"] is True
        assert final["final_action"] == "refund_shipping_fee"
        assert final["completed"] is True
        assert "refund" in final["final_response"].lower()

    async def test_correlation_ids_not_crossed(self):
        """
        Two concurrent workflows must not mix up results.

        Sends two tickets simultaneously; each should get its own results.
        """
        worker_group = f"e2e-concurrent-{new_id()[:8]}"
        result_group = f"e2e-concurrent-results-{new_id()[:8]}"

        worker_pairs = await _run_workers(worker_group, count=3)
        worker_tasks = [t for _, t in worker_pairs]
        workers = [w for w, _ in worker_pairs]

        producer = KafkaEventProducer(bootstrap_servers=BOOTSTRAP)
        await producer.start()

        result_bus = ResultBus()
        await result_bus.start(group_id=result_group, bootstrap_servers=BOOTSTRAP)

        await asyncio.sleep(1.0)

        ctx = WorkflowContext(producer=producer, result_bus=result_bus)
        config = {"configurable": {"ctx": ctx}}
        graph = build_graph()

        states = [
            {
                "workflow_id": new_id(),
                "ticket_id": f"TKT-CONC-{i}",
                "customer_id": "C-0001",
                "order_id": "ORD-0001",
                "message": "My express delivery is 5 days late.",
                "language": "en",
            }
            for i in range(2)
        ]

        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    graph.ainvoke(states[0], config=config),
                    graph.ainvoke(states[1], config=config),
                ),
                timeout=90.0,
            )
        finally:
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            for w in workers:
                await w.stop()
            await result_bus.stop()
            await producer.stop()

        # Both workflows should complete successfully
        for i, final in enumerate(results):
            assert final["completed"] is True, f"Workflow {i} did not complete"
            assert final["recommended_action"] == "refund_shipping_fee", (
                f"Workflow {i} wrong action: {final.get('recommended_action')}"
            )
