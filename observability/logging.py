"""
Structured logging configuration for NovaSupport.

Development: colourful console output (human-readable)
Production:  JSON lines (machine-readable, ingestible by ELK / Loki / CloudWatch)

Usage:
    from observability.logging import configure_logging
    configure_logging(env="production")   # call once at process startup

    import structlog
    logger = structlog.get_logger(__name__)
    logger.info("ticket_accepted", ticket_id="TKT-001", workflow_id="WF-XYZ")

Existing stdlib `logging.getLogger()` calls are automatically picked up
because structlog wraps the stdlib root logger.
"""

from __future__ import annotations

import logging
import sys


def configure_logging(env: str = "development", level: str = "INFO") -> None:
    """
    Configure structlog + stdlib logging for the given environment.

    Args:
        env:   "production" → JSON renderer; anything else → ConsoleRenderer.
        level: stdlib log level string (INFO, DEBUG, WARNING, …).
    """
    import structlog

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
    ]

    if env == "production":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
