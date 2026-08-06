"""Broker factory — instantiate the configured backend."""

from __future__ import annotations

from dtq.broker.base import Broker
from dtq.core.config import Settings


def create_broker(settings: Settings | None = None) -> Broker:
    """Create the appropriate broker instance based on configuration."""
    if settings is None:
        from dtq.core.config import settings as default_settings

        settings = default_settings

    backend = settings.broker_backend.lower()

    if backend == "redis":
        from dtq.broker.redis_broker import RedisBroker

        return RedisBroker(url=settings.redis_url)  # type: ignore[return-value]
    elif backend == "kafka":
        from dtq.broker.kafka_broker import KafkaBroker

        return KafkaBroker(bootstrap_servers=settings.kafka_bootstrap_servers)  # type: ignore[return-value]
    elif backend == "rabbitmq":
        from dtq.broker.rabbitmq_broker import RabbitMQBroker

        return RabbitMQBroker(url=settings.rabbitmq_url)  # type: ignore[return-value]
    else:
        raise ValueError(f"Unknown broker backend: {backend!r}. Use 'redis', 'kafka', or 'rabbitmq'.")
