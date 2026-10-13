"""
SSE streaming endpoint — delivers real-time workflow step events to the browser.

GET /api/v1/workflows/{workflow_id}/stream

The client opens this URL with EventSource. Events are published by the
orchestrator via Redis and replayed here to late-connecting clients.
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from apps.api.deps import get_redis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["stream"])

_CHANNEL    = "novasupport:wf:{wf}:steps"
_LIST       = "novasupport:wf:{wf}:events"
_TIMEOUT    = 300    # max seconds to keep SSE connection open
_HEARTBEAT  = 15     # send a comment every N seconds to prevent proxy timeouts


@router.get(
    "/workflows/{workflow_id}/stream",
    summary="Server-Sent Events stream for real-time workflow steps",
    response_class=StreamingResponse,
)
async def stream_workflow(
    workflow_id: str,
    redis=Depends(get_redis),
) -> StreamingResponse:
    async def event_generator():
        # ── No Redis: yield error then close ──────────────────────────────────
        if redis is None:
            yield _sse({"error": "Redis not available — step events disabled", "done": True})
            return

        channel = _CHANNEL.format(wf=workflow_id)
        lst     = _LIST.format(wf=workflow_id)

        # ── Replay already-stored events (handles late connections) ────────────
        try:
            stored = await redis.lrange(lst, 0, -1)
        except Exception as exc:
            logger.warning("SSE lrange failed wf=%s: %s", workflow_id, exc)
            stored = []

        for raw in stored:
            text = raw.decode() if isinstance(raw, bytes) else raw
            yield _sse(text, raw=True)
            try:
                if json.loads(text).get("done"):
                    return   # workflow already finished — nothing more to stream
            except Exception:
                pass

        # ── Subscribe for live events ──────────────────────────────────────────
        pubsub = redis.pubsub()
        try:
            await pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning("SSE subscribe failed wf=%s: %s", workflow_id, exc)
            yield _sse({"error": f"Subscribe failed: {exc}", "done": True})
            return

        loop     = asyncio.get_running_loop()
        deadline = loop.time() + _TIMEOUT
        last_hb  = loop.time()

        try:
            while loop.time() < deadline:
                # Non-blocking get with short timeout — lets us send heartbeats
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    message = None
                except Exception as exc:
                    logger.error("SSE pubsub error wf=%s: %s", workflow_id, exc)
                    yield _sse({"error": str(exc), "done": True})
                    break

                if message is None:
                    # No message yet — send heartbeat if needed to keep connection alive
                    if loop.time() - last_hb >= _HEARTBEAT:
                        yield ": heartbeat\n\n"
                        last_hb = loop.time()
                    continue

                if message["type"] != "message":
                    continue

                raw  = message["data"]
                text = raw.decode() if isinstance(raw, bytes) else raw
                yield _sse(text, raw=True)

                try:
                    if json.loads(text).get("done"):
                        break
                except Exception:
                    pass

                last_hb = loop.time()  # reset heartbeat after a real message

        finally:
            try:
                await pubsub.unsubscribe(channel)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _sse(data, *, raw: bool = False) -> str:
    payload = data if raw else json.dumps(data)
    return f"data: {payload}\n\n"
