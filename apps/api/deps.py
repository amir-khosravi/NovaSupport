"""
FastAPI dependency functions.

Using Depends() keeps routers testable: tests override these with mocks via
app.dependency_overrides[get_producer] = lambda: mock_producer
"""

from __future__ import annotations

from typing import Any

from fastapi import Request

from persistence.workflow_repository import WorkflowRepository
from streaming.producer import KafkaEventProducer


def get_producer(request: Request) -> KafkaEventProducer:
    return request.app.state.producer


def get_repo(request: Request) -> WorkflowRepository:
    return request.app.state.repo


def get_redis(request: Request) -> Any:
    """Returns the redis.asyncio client, or None if Redis is unavailable."""
    return getattr(request.app.state, "redis", None)
