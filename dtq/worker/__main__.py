"""Worker CLI entry point.

Usage:
    python -m dtq.worker --queues default,high --concurrency 8 --metrics-port 7206
"""

from __future__ import annotations

import asyncio
import sys

import click
import structlog

from dtq.core.config import Settings


def configure_logging() -> None:
    """Set up structured JSON logging."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


@click.command()
@click.option("--queues", default="default", help="Comma-separated queue names")
@click.option("--concurrency", default=8, type=int, help="Max concurrent tasks")
@click.option("--metrics-port", default=7206, type=int, help="Prometheus metrics port")
@click.option("--worker-id", default=None, help="Worker ID (auto-generated if not set)")
@click.option("--broker", default="redis", help="Broker backend: redis, kafka, rabbitmq")
def main(
    queues: str,
    concurrency: int,
    metrics_port: int,
    worker_id: str | None,
    broker: str,
) -> None:
    """Start a DTQ worker process."""
    configure_logging()

    import uuid

    settings = Settings(
        worker_queues=queues.split(","),
        worker_concurrency=concurrency,
        metrics_port=metrics_port,
        worker_id=worker_id or f"worker-{uuid.uuid4().hex[:8]}",
        broker_backend=broker,
    )

    from dtq.worker.loop import WorkerLoop

    worker = WorkerLoop(settings)

    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
