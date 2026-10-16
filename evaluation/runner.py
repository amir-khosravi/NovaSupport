"""
Evaluation runner.

Runs a batch of EvalCases through the full LangGraph workflow
(with real Kafka + Workers) and returns EvalResults.

Usage:
    import asyncio
    from evaluation.runner import EvalRunner
    from evaluation.dataset import EVAL_DATASET_NO_DB

    async def main():
        runner = EvalRunner()
        results = await runner.run(EVAL_DATASET_NO_DB)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from apps.orchestrator.graph import WorkflowContext, build_graph
from apps.orchestrator.result_bus import ResultBus
from apps.worker.handlers import registry
from apps.worker.service import WorkerService
from config.settings import settings
from evaluation.metrics import EvalResult
from streaming.producer import KafkaEventProducer
from streaming.schemas import new_id

if TYPE_CHECKING:
    from evaluation.dataset import EvalCase

logger = logging.getLogger(__name__)

# How long to wait for a single case to complete before marking it as a timeout
CASE_TIMEOUT_SECONDS = 90.0
# Worker count started for the evaluation run
WORKER_COUNT = 3


class EvalRunner:
    """
    Manages the Kafka + Worker infrastructure for a batch evaluation run.

    Lifecycle:
        async with EvalRunner() as runner:
            results = await runner.run(cases)
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        worker_count: int = WORKER_COUNT,
        case_timeout: float = CASE_TIMEOUT_SECONDS,
    ) -> None:
        self._bootstrap = bootstrap_servers or settings.kafka.bootstrap_servers
        self._worker_count = worker_count
        self._case_timeout = case_timeout

        self._producer: KafkaEventProducer | None = None
        self._result_bus: ResultBus | None = None
        self._workers: list[WorkerService] = []
        self._worker_tasks: list[asyncio.Task] = []

    async def __aenter__(self) -> "EvalRunner":
        await self._start()
        return self

    async def __aexit__(self, *_) -> None:
        await self._stop()

    async def _start(self) -> None:
        group_id = f"eval-workers-{new_id()[:8]}"
        result_group = f"eval-results-{new_id()[:8]}"

        self._producer = KafkaEventProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        self._result_bus = ResultBus()
        await self._result_bus.start(
            group_id=result_group,
            bootstrap_servers=self._bootstrap,
        )

        for i in range(self._worker_count):
            worker = WorkerService(
                worker_id=f"eval-worker-{i + 1}",
                registry=registry,
                group_id=group_id,
                bootstrap_servers=self._bootstrap,
                auto_offset_reset="latest",
            )
            await worker.start()
            task = asyncio.create_task(worker.run(), name=f"eval-worker-{i + 1}")
            self._workers.append(worker)
            self._worker_tasks.append(task)

        # Let consumers settle their partition assignments
        await asyncio.sleep(1.5)
        logger.info(
            "EvalRunner started: %d workers, bootstrap=%s",
            self._worker_count, self._bootstrap,
        )

    async def _stop(self) -> None:
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        for worker in self._workers:
            await worker.stop()
        if self._result_bus:
            await self._result_bus.stop()
        if self._producer:
            await self._producer.stop()
        logger.info("EvalRunner stopped.")

    async def _run_case(self, case: "EvalCase") -> EvalResult:
        """Run a single EvalCase through the graph and return an EvalResult."""
        workflow_id = new_id()
        initial_state = {
            "workflow_id": workflow_id,
            "ticket_id": f"EVAL-TKT-{case.case_id}",
            "customer_id": case.customer_id,
            "order_id": case.order_id,
            "message": case.message,
            "language": case.language,
        }

        ctx = WorkflowContext(
            producer=self._producer,
            result_bus=self._result_bus,
        )
        config = {"configurable": {"ctx": ctx}}
        graph = build_graph()

        t0 = time.monotonic()
        try:
            final = await asyncio.wait_for(
                graph.ainvoke(initial_state, config=config),
                timeout=self._case_timeout,
            )
            elapsed = time.monotonic() - t0
            logger.info(
                "Case %-10s  done  %.1fs  category=%s  action=%s",
                case.case_id, elapsed,
                final.get("triage_category", "?"),
                final.get("final_action", "?"),
            )
            return EvalResult(
                case_id=case.case_id,
                elapsed_seconds=elapsed,
                final_state=dict(final),
            )
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - t0
            logger.warning("Case %s TIMED OUT after %.1fs", case.case_id, elapsed)
            return EvalResult(
                case_id=case.case_id,
                elapsed_seconds=elapsed,
                final_state={},
                error=f"Timeout after {self._case_timeout}s",
            )
        except Exception as exc:
            elapsed = time.monotonic() - t0
            logger.error("Case %s FAILED: %s", case.case_id, exc, exc_info=True)
            return EvalResult(
                case_id=case.case_id,
                elapsed_seconds=elapsed,
                final_state={},
                error=str(exc),
            )

    async def run(
        self,
        cases: list["EvalCase"],
        *,
        concurrency: int = 1,
    ) -> list[EvalResult]:
        """
        Run all cases and return results in the same order.

        concurrency=1 : sequential (stable, default)
        concurrency>1 : N cases run simultaneously (faster, requires more workers)
        """
        if concurrency == 1:
            results = []
            for case in cases:
                logger.info("Running case %s: %s", case.case_id, case.description or case.message[:50])
                result = await self._run_case(case)
                results.append(result)
            return results

        # Semaphore-bounded concurrency
        sem = asyncio.Semaphore(concurrency)

        async def _guarded(case: "EvalCase") -> EvalResult:
            async with sem:
                return await self._run_case(case)

        return list(await asyncio.gather(*[_guarded(c) for c in cases]))
