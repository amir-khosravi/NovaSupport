"""
Handler registry for the NovaSupport worker.

Maps TaskType values to async handler functions.
Each handler receives an AgentCommand and returns a result dict.

Usage:
    registry = HandlerRegistry()

    @registry.register(TaskType.TRIAGE_TICKET)
    async def handle_triage(cmd: AgentCommand) -> dict:
        return {"category": "delivery_delay"}

    result = await registry.dispatch(command)
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from streaming.schemas import AgentCommand, TaskType

logger = logging.getLogger(__name__)

# Type alias for handler functions
HandlerFn = Callable[[AgentCommand], Awaitable[dict]]


class HandlerRegistry:
    """
    Registry that maps TaskType → async handler function.

    Raises KeyError on dispatch if no handler is registered for the task type.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFn] = {}

    def register(self, task_type: TaskType) -> Callable[[HandlerFn], HandlerFn]:
        """Decorator: register a handler for a given TaskType."""
        def decorator(fn: HandlerFn) -> HandlerFn:
            key = task_type.value
            if key in self._handlers:
                raise ValueError(f"Handler already registered for task_type={key!r}")
            self._handlers[key] = fn
            logger.debug("Registered handler for %s → %s", key, fn.__name__)
            return fn
        return decorator

    async def dispatch(self, command: AgentCommand) -> dict:
        """
        Route command to the appropriate handler and return its result dict.

        Raises:
            KeyError: if no handler is registered for command.task_type
        """
        key = command.task_type
        handler = self._handlers.get(key)
        if handler is None:
            raise KeyError(
                f"No handler registered for task_type={key!r}. "
                f"Registered: {list(self._handlers)}"
            )
        logger.debug(
            "Dispatching task_type=%s task_id=%s", key, command.task_id
        )
        return await handler(command)

    @property
    def registered_types(self) -> list[str]:
        return sorted(self._handlers)
