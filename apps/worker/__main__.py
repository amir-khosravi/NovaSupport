"""
NovaSupport Worker — entry point.

Run a single worker instance:
    python -m apps.worker --id worker-1

Run multiple workers in the same process (for demo):
    python -m apps.worker --id demo --count 3
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add project root to path when running as __main__
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from apps.worker.handlers import registry  # noqa: E402 — registers all handlers
from apps.worker.service import WorkerService  # noqa: E402
from config.settings import settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def run_worker(worker_id: str) -> None:
    async with WorkerService(worker_id=worker_id, registry=registry) as worker:
        logger.info("[%s] Running — waiting for commands on %s", worker_id, settings.kafka.command_topic)
        await worker.run()


async def run_pool(count: int, base_id: str) -> None:
    """Run `count` workers concurrently in the same process."""
    worker_ids = [f"{base_id}-{i + 1}" for i in range(count)]

    logger.info(
        "Starting worker pool: %d workers in group=%s",
        count, settings.kafka.consumer_group,
    )

    tasks = [asyncio.create_task(run_worker(wid)) for wid in worker_ids]

    # Graceful shutdown on Ctrl+C
    loop = asyncio.get_running_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Received %s — cancelling workers ...", sig.name)
        for t in tasks:
            t.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("All workers stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NovaSupport Worker")
    parser.add_argument("--id", default="worker-1", help="Worker ID label (shown in logs)")
    parser.add_argument("--count", type=int, default=1, help="Number of concurrent workers (demo mode)")
    args = parser.parse_args()

    logger.info(
        "NovaSupport Worker starting  id=%s  count=%d  topic=%s",
        args.id, args.count, settings.kafka.command_topic,
    )
    logger.info("Registered task handlers: %s", registry.registered_types)

    try:
        asyncio.run(run_pool(count=args.count, base_id=args.id))
    except KeyboardInterrupt:
        logger.info("Interrupted.")


if __name__ == "__main__":
    main()
