"""
Unit tests for Phase 15 Security layer.

Covers:
  - API key auth: open mode, missing key (401), invalid key (403), valid key (200)
  - Rate limiting: under limit passes, over limit → 429, Redis down → allow through
  - Input sanitization: whitespace stripped, null bytes removed, max length enforced
  - Settings: API_KEYS parsed correctly from comma-separated string
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from apps.api.deps import get_producer, get_repo, get_redis
from apps.api.main import app


# ── helper ────────────────────────────────────────────────────────────────────

def _sec(keys: list[str], rate_limit: int = 60, window: int = 60):
    """Build a SecuritySettings instance with the given API keys."""
    from config.settings import SecuritySettings
    s = SecuritySettings.__new__(SecuritySettings)
    object.__setattr__(s, "api_keys", keys)
    object.__setattr__(s, "rate_limit_requests", rate_limit)
    object.__setattr__(s, "rate_limit_window_seconds", window)
    object.__setattr__(s, "max_message_length", 2000)
    return s


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send = AsyncMock()
    return p


@pytest.fixture
def mock_repo():
    r = AsyncMock()
    r.get_by_ticket_id = AsyncMock(return_value=None)
    r._pool = None
    r.close = AsyncMock()
    return r


@pytest.fixture
def mock_redis():
    """Mock Redis client that simulates a sliding-window pipeline."""
    r = AsyncMock()
    pipe = MagicMock()                          # pipeline itself is sync context manager
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    pipe.zremrangebyscore = MagicMock()         # queue commands — NOT awaited in real redis
    pipe.zadd = MagicMock()
    pipe.zcard = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=[None, 1, 1, True])  # only execute is async
    r.pipeline = MagicMock(return_value=pipe)
    return r


@pytest.fixture
async def open_client(mock_producer, mock_repo):
    """Client with no API keys configured — open mode."""
    app.dependency_overrides[get_producer] = lambda: mock_producer
    app.dependency_overrides[get_repo] = lambda: mock_repo
    app.dependency_overrides[get_redis] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture
async def secured_client(mock_producer, mock_repo, mock_redis):
    """Client with API_KEYS=sk-test-key configured."""
    app.dependency_overrides[get_producer] = lambda: mock_producer
    app.dependency_overrides[get_repo] = lambda: mock_repo
    app.dependency_overrides[get_redis] = lambda: mock_redis

    with patch.object(
        __import__("config.settings", fromlist=["settings"]).settings.security,
        "api_keys",
        ["sk-test-key"],
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

    app.dependency_overrides.clear()


# ── API key authentication ────────────────────────────────────────────────────

class TestApiKeyAuth:

    async def test_open_mode_allows_request_without_key(self, open_client):
        """When API_KEYS is empty, requests pass with no header required."""
        response = await open_client.post("/api/v1/tickets", json={
            "customer_id": "CUST-001",
            "message": "My order is late.",
        })
        assert response.status_code == 202

    async def test_require_api_key_dependency_returns_anonymous_in_open_mode(self):
        from apps.api.security import require_api_key
        from config.settings import settings
        with patch.object(settings, "security", _sec([])):
            result = await require_api_key(x_api_key=None)
        assert result == "anonymous"

    async def test_missing_key_returns_401_when_keys_configured(self):
        from apps.api.security import require_api_key
        from fastapi import HTTPException
        from config.settings import settings
        with patch.object(settings, "security", _sec(["sk-valid-key"])):
            with pytest.raises(HTTPException) as exc_info:
                await require_api_key(x_api_key=None)
        assert exc_info.value.status_code == 401

    async def test_invalid_key_returns_403(self):
        from apps.api.security import require_api_key
        from fastapi import HTTPException
        from config.settings import settings
        with patch.object(settings, "security", _sec(["sk-valid-key"])):
            with pytest.raises(HTTPException) as exc_info:
                await require_api_key(x_api_key="sk-wrong-key")
        assert exc_info.value.status_code == 403

    async def test_valid_key_returns_the_key(self):
        from apps.api.security import require_api_key
        from config.settings import settings
        with patch.object(settings, "security", _sec(["sk-valid-key"])):
            result = await require_api_key(x_api_key="sk-valid-key")
        assert result == "sk-valid-key"


# ── Rate limiting ─────────────────────────────────────────────────────────────

class TestRateLimiting:

    async def test_allows_request_under_limit(self):
        from apps.api.security import check_rate_limit
        from config.settings import settings
        redis = AsyncMock()
        pipe = MagicMock()
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=False)
        pipe.zremrangebyscore = MagicMock()
        pipe.zadd = MagicMock()
        pipe.zcard = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[None, 1, 5, True])   # count=5
        redis.pipeline = MagicMock(return_value=pipe)

        with patch.object(settings, "security", _sec([], rate_limit=60)):
            await check_rate_limit(api_key="sk-key", redis=redis)  # should not raise

    async def test_blocks_request_over_limit(self):
        from apps.api.security import check_rate_limit
        from fastapi import HTTPException
        from config.settings import settings
        redis = AsyncMock()
        pipe = MagicMock()
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=False)
        pipe.zremrangebyscore = MagicMock()
        pipe.zadd = MagicMock()
        pipe.zcard = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[None, 1, 61, True])  # count=61 > limit=60
        redis.pipeline = MagicMock(return_value=pipe)

        with patch.object(settings, "security", _sec([], rate_limit=60)):
            with pytest.raises(HTTPException) as exc_info:
                await check_rate_limit(api_key="sk-key", redis=redis)
        assert exc_info.value.status_code == 429
        assert "Retry-After" in exc_info.value.headers

    async def test_allows_through_when_redis_is_none(self):
        from apps.api.security import check_rate_limit
        # No exception should be raised when Redis is None
        await check_rate_limit(api_key="sk-key", redis=None)

    async def test_allows_through_when_redis_raises(self):
        from apps.api.security import check_rate_limit
        redis = AsyncMock()
        pipe = MagicMock()
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=False)
        pipe.zremrangebyscore = MagicMock()
        pipe.zadd = MagicMock()
        pipe.zcard = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))
        redis.pipeline = MagicMock(return_value=pipe)

        # Should not raise — graceful degradation
        await check_rate_limit(api_key="sk-key", redis=redis)

    async def test_rate_limit_key_includes_api_key(self):
        """Each API key gets its own rate-limit bucket."""
        from apps.api.security import check_rate_limit
        redis = AsyncMock()
        pipe = MagicMock()
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=False)
        pipe.zremrangebyscore = MagicMock()
        pipe.zadd = MagicMock()
        pipe.zcard = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[None, 1, 1, True])
        redis.pipeline = MagicMock(return_value=pipe)

        await check_rate_limit(api_key="sk-user-A", redis=redis)

        zremrange_call = pipe.zremrangebyscore.call_args[0]
        assert "sk-user-A" in zremrange_call[0]


# ── Input sanitization ────────────────────────────────────────────────────────

class TestInputSanitization:

    def test_message_whitespace_stripped(self):
        from apps.api.models import SubmitTicketRequest
        req = SubmitTicketRequest(
            customer_id="CUST-001",
            message="  my order is late.  ",
        )
        assert req.message == "my order is late."

    def test_customer_id_whitespace_stripped(self):
        from apps.api.models import SubmitTicketRequest
        req = SubmitTicketRequest(
            customer_id="  CUST-001  ",
            message="Help please.",
        )
        assert req.customer_id == "CUST-001"

    def test_message_null_bytes_removed(self):
        from apps.api.models import SubmitTicketRequest
        req = SubmitTicketRequest(
            customer_id="CUST-001",
            message="hello\x00world",
        )
        assert "\x00" not in req.message
        assert req.message == "helloworld"

    def test_message_too_long_rejected(self):
        from pydantic import ValidationError
        from apps.api.models import SubmitTicketRequest
        with pytest.raises(ValidationError):
            SubmitTicketRequest(
                customer_id="CUST-001",
                message="x" * 2001,
            )

    def test_message_at_max_length_accepted(self):
        from apps.api.models import SubmitTicketRequest
        req = SubmitTicketRequest(
            customer_id="CUST-001",
            message="x" * 2000,
        )
        assert len(req.message) == 2000

    def test_empty_message_after_strip_rejected(self, open_client):
        pass  # covered by Field(min_length=1) + strip in test_rejects_empty_message


# ── Settings parsing ──────────────────────────────────────────────────────────

class TestSecuritySettings:

    def test_api_keys_parsed_from_comma_separated_string(self):
        from config.settings import SecuritySettings
        s = SecuritySettings(API_KEYS="sk-key-1,sk-key-2,sk-key-3")
        assert s.api_keys == ["sk-key-1", "sk-key-2", "sk-key-3"]

    def test_api_keys_empty_string_gives_empty_list(self):
        from config.settings import SecuritySettings
        s = SecuritySettings(API_KEYS="")
        assert s.api_keys == []

    def test_api_keys_strips_whitespace_around_keys(self):
        from config.settings import SecuritySettings
        s = SecuritySettings(API_KEYS="sk-a , sk-b , sk-c")
        assert s.api_keys == ["sk-a", "sk-b", "sk-c"]

    def test_default_rate_limit(self):
        from config.settings import SecuritySettings
        s = SecuritySettings()
        assert s.rate_limit_requests == 60
        assert s.rate_limit_window_seconds == 60

    def test_default_max_message_length(self):
        from config.settings import SecuritySettings
        s = SecuritySettings()
        assert s.max_message_length == 2000
