"""Structured logging configuration.

JSON-formatted logs with task_id, worker_id, attempt, fence on every
task-scoped line. Tracing this system without those four fields is impossible.
"""

from __future__ import annotations

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Configure structured JSON logging with standard fields."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}.get(
                level.upper(), 20
            )
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
