"""Application configuration via pydantic-settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration — env vars override defaults."""

    # Redis
    redis_url: str = "redis://localhost:7202/0"

    # Postgres
    database_url: str = "postgresql+asyncpg://dtq:dtq_dev@localhost:7203/dtq"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:7216"

    # RabbitMQ
    rabbitmq_url: str = "amqp://dtq:dtq_dev@localhost:7217/"

    # Worker
    worker_id: str = "worker-1"
    worker_queues: list[str] = ["default"]
    worker_concurrency: int = 8
    lease_ms: int = 30_000
    heartbeat_interval_s: float = 10.0
    reclaim_interval_s: float = 5.0
    reclaim_min_idle_ms: int = 30_000

    # Control plane
    control_host: str = "0.0.0.0"
    control_port: int = 7201
    enable_chaos: bool = False

    # gRPC
    grpc_port: int = 7219

    # Broker backend: "redis" | "kafka" | "rabbitmq"
    broker_backend: str = "redis"

    # Observability
    otel_enabled: bool = False
    otel_endpoint: str = "http://localhost:7221"
    metrics_port: int = 7206

    model_config = {"env_prefix": "DTQ_", "env_file": ".env", "extra": "ignore"}


settings = Settings()
