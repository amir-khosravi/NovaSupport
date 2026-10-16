"""Quick benchmark: measure full workflow latency with parallel fetch_context."""
import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from apps.orchestrator.graph import WorkflowContext, build_graph
from apps.orchestrator.result_bus import ResultBus
from apps.worker.handlers import registry
from apps.worker.service import WorkerService
from config.settings import settings
from streaming.producer import KafkaEventProducer
from streaming.schemas import new_id

BOOTSTRAP = settings.kafka.bootstrap_servers


async def bench() -> None:
    wg = f"bench-{new_id()[:6]}"
    rg = f"bench-r-{new_id()[:6]}"

    workers = []
    tasks = []
    for i in range(3):
        w = WorkerService(
            f"bw-{i}", registry,
            group_id=wg, bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
        )
        await w.start()
        tasks.append(asyncio.create_task(w.run()))
        workers.append(w)

    prod = KafkaEventProducer(bootstrap_servers=BOOTSTRAP)
    await prod.start()
    bus = ResultBus()
    await bus.start(group_id=rg, bootstrap_servers=BOOTSTRAP)
    await asyncio.sleep(1.0)

    ctx = WorkflowContext(producer=prod, result_bus=bus)
    config = {"configurable": {"ctx": ctx}}
    graph = build_graph()
    state = {
        "workflow_id": new_id(),
        "ticket_id": "BENCH-001",
        "customer_id": "C-0001",
        "order_id": "ORD-0001",
        "message": "My express delivery is 5 days late.",
        "language": "en",
    }

    t0 = time.monotonic()
    final = await asyncio.wait_for(graph.ainvoke(state, config=config), timeout=60.0)
    elapsed = time.monotonic() - t0

    print(f"\nBenchmark result")
    print(f"  Total workflow time : {elapsed:.3f}s")
    print(f"  Final action        : {final['final_action']}")
    print(f"  Refund amount       : {final.get('refund_amount', 0):.2f} CAD")
    print(f"  Customer tier       : {final.get('customer_tier', '?')}")
    print(f"  Quality             : {final.get('quality_status', '?')}")
    print(f"\n  fetch_context (parallel GET_ORDER+GET_SHIPMENT+GET_CUSTOMER)")
    print(f"  handler sleeps: 0.20 + 0.20 + 0.10 = 0.50s sequential")
    print(f"  parallel max:   max(0.20, 0.20, 0.10) = 0.20s")

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for w in workers:
        await w.stop()
    await bus.stop()
    await prod.stop()


if __name__ == "__main__":
    asyncio.run(bench())
