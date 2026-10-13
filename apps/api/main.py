"""
NovaSupport FastAPI application — Phase 13 intake service.

Startup sequence (lifespan):
  1. Start Kafka producer (graceful degrade if unavailable)
  2. Connect WorkflowRepository to PostgreSQL (graceful degrade if unavailable)

Shutdown sequence:
  1. Stop Kafka producer
  2. Close DB pool

Endpoints:
  GET  /health                         — liveness probe
  POST /api/v1/tickets                 — submit support ticket → publishes to Kafka
  GET  /api/v1/tickets/{id}/status     — poll workflow outcome from PostgreSQL
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from apps.api.deps import get_producer, get_repo
from apps.api.models import HealthResponse
from apps.api.routers.stream import router as stream_router
from apps.api.routers.tickets import router as tickets_router
from config.settings import settings
from observability.logging import configure_logging
from persistence.workflow_repository import WorkflowRepository
from streaming.producer import KafkaEventProducer

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ───────────────────────────────────────────────────────────────
    configure_logging(env=settings.app.env, level=settings.app.log_level)

    producer = KafkaEventProducer(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        await producer.start()
        logger.info("Kafka producer started")
    except Exception as exc:
        logger.warning("Kafka unavailable at startup (%s) — producer not ready", exc)

    repo = await WorkflowRepository.from_settings()

    # Optional Redis for rate limiting
    redis_client = None
    try:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(
            settings.redis.url, socket_connect_timeout=2, socket_timeout=2,
        )
        await redis_client.ping()
        logger.info("Redis connected for rate limiting")
    except Exception as exc:
        logger.warning("Redis unavailable for rate limiting (%s) — disabled", exc)
        redis_client = None

    app.state.producer = producer
    app.state.repo = repo
    app.state.redis = redis_client

    yield

    # ── shutdown ──────────────────────────────────────────────────────────────
    try:
        await producer.stop()
    except Exception:
        pass
    await repo.close()
    if redis_client is not None:
        await redis_client.aclose()
    logger.info("NovaSupport API shutdown complete")


app = FastAPI(
    title="NovaSupport API",
    description="Event-driven multi-agent e-commerce support platform",
    version=settings.app.version,
    lifespan=lifespan,
)

app.include_router(tickets_router)
app.include_router(stream_router)

# Serve the web UI at /
_STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/", include_in_schema=False)
async def ui() -> FileResponse:
    return FileResponse(str(_STATIC / "index.html"))


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health() -> HealthResponse:
    return HealthResponse(version=settings.app.version)


@app.get("/health/ready", tags=["ops"], summary="Readiness probe — checks DB connectivity")
async def health_ready(
    repo: WorkflowRepository = Depends(get_repo),
    producer: KafkaEventProducer = Depends(get_producer),
) -> Response:
    """
    Returns 200 if all critical dependencies are reachable, 503 otherwise.
    Checked by Kubernetes readinessProbe / load-balancer health checks.
    """
    from fastapi.responses import JSONResponse

    checks: dict[str, str] = {}
    healthy = True

    # DB check
    if repo._pool is not None:
        try:
            await repo._pool.fetchrow("SELECT 1")
            checks["postgres"] = "ok"
        except Exception as exc:
            checks["postgres"] = f"error: {exc}"
            healthy = False
    else:
        checks["postgres"] = "unavailable"
        healthy = False

    # Kafka check
    checks["kafka"] = "ok" if producer is not None else "unavailable"

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ready" if healthy else "not_ready", "checks": checks},
    )


@app.get("/metrics", tags=["ops"], summary="Prometheus metrics scrape endpoint")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
