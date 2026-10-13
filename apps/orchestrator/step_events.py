"""
Step event emitter — publishes workflow progress to Redis so the
API's SSE endpoint can stream them live to the browser.

Each event is:
  1. Published to Redis channel  novasupport:wf:{workflow_id}:steps  (live)
  2. Appended to Redis list      novasupport:wf:{workflow_id}:events (replay for late joiners)

The list has a 1-hour TTL so clients that connect after the workflow
finishes can still see all the steps.
"""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)

_CHANNEL = "novasupport:wf:{wf}:steps"
_LIST    = "novasupport:wf:{wf}:events"
_TTL     = 3600  # 1 hour


async def emit_step(
    redis,
    workflow_id: str,
    step: str,
    label: str,
    data: dict,
    *,
    done: bool = False,
) -> None:
    """
    Publish one workflow step event.

    redis      — asyncio Redis client (may be None → no-op)
    workflow_id — identifies the workflow
    step        — machine name: "triage", "fetch_context", …
    label       — human label shown in the UI
    data        — arbitrary dict with step-specific fields
    done        — True only on the final event
    """
    if redis is None:
        return
    try:
        payload = json.dumps({
            "step":  step,
            "label": label,
            "data":  data,
            "ts":    round(time.time(), 3),
            "done":  done,
        })
        channel = _CHANNEL.format(wf=workflow_id)
        lst     = _LIST.format(wf=workflow_id)
        async with redis.pipeline(transaction=False) as pipe:
            pipe.publish(channel, payload)
            pipe.rpush(lst, payload)
            pipe.expire(lst, _TTL)
            await pipe.execute()
    except Exception as exc:
        logger.debug("emit_step failed (non-critical): %s", exc)
