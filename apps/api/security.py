"""
Phase 15 — API security layer.

Two concerns:

1. API key authentication (require_api_key)
   - Reads X-API-Key header
   - Empty API_KEYS setting → open mode (no auth, for local dev)
   - Missing header → 401 Unauthorized
   - Wrong key      → 403 Forbidden

2. Rate limiting (check_rate_limit)
   - Sliding-window algorithm via Redis sorted sets
   - Limit: RATE_LIMIT_REQUESTS per RATE_LIMIT_WINDOW_SECONDS per API key
   - Redis unavailable → allow through (graceful degradation)
   - Exceeded         → 429 Too Many Requests + Retry-After header
"""

from __future__ import annotations

import time
import logging
from typing import Any

from fastapi import Depends, Header, HTTPException, Request

from apps.api.deps import get_redis
from config.settings import settings

logger = logging.getLogger(__name__)


# ── API key authentication ────────────────────────────────────────────────────

async def require_api_key(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> str:
    """
    Validate X-API-Key header.
    Returns the key on success; raises HTTPException on failure.
    When API_KEYS is empty the service runs in open mode and returns 'anonymous'.
    """
    configured = settings.security.api_keys
    if not configured:
        return "anonymous"
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="X-API-Key header is required.")
    if x_api_key not in configured:
        raise HTTPException(status_code=403, detail="Invalid API key.")
    return x_api_key


# ── Rate limiting ─────────────────────────────────────────────────────────────

async def check_rate_limit(
    api_key: str = Depends(require_api_key),
    redis: Any = Depends(get_redis),
) -> None:
    """
    Sliding-window rate limiter.

    Uses a Redis sorted set keyed by API key.  Each request adds the current
    timestamp as both score and member.  Before counting, entries older than
    the window are pruned.  If the count exceeds the limit a 429 is raised.

    Silently allows through if Redis is None (unavailable).
    """
    if redis is None:
        return

    limit = settings.security.rate_limit_requests
    window = settings.security.rate_limit_window_seconds
    key = f"novasupport:ratelimit:{api_key}"
    now = time.time()
    window_start = now - window

    try:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zadd(key, {str(now): now})
            pipe.zcard(key)
            pipe.expire(key, window)
            results = await pipe.execute()
        count: int = results[2]
        if count > limit:
            logger.warning("Rate limit exceeded  key=%s  count=%d  limit=%d", api_key, count, limit)
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded. Please retry later.",
                headers={"Retry-After": str(window)},
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Rate limiter Redis error (%s) — allowing request", exc)
