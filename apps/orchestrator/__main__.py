"""
NovaSupport Orchestrator — entry point.

Run:
    python -m apps.orchestrator

The orchestrator listens on ticket.created and drives the full
LangGraph resolution workflow for each incoming ticket.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from apps.orchestrator.runner import OrchestratorRunner  # noqa: E402
from config.settings import settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("NovaSupport Orchestrator starting")
    logger.info("  ticket topic : %s", settings.kafka.ticket_topic)
    logger.info("  command topic: %s", settings.kafka.command_topic)
    logger.info("  result topic : %s", settings.kafka.result_topic)

    runner = OrchestratorRunner()
    await runner.start()

    loop = asyncio.get_running_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Received %s — shutting down ...", sig.name)
        loop.create_task(runner.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig)
        except NotImplementedError:
            pass  # Windows

    try:
        await runner.run()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.stop()
        logger.info("Orchestrator stopped.")


if __name__ == "__main__":
    asyncio.run(main())
