"""Integration test fixtures — real Redis + Postgres via docker-compose.

Tests are parameterized over broker backends. A Redis-only assumption
leaking into the engine must surface as a test failure, not a silent difference.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dtq.broker.base import Broker
from dtq.broker.redis_broker import RedisBroker
from dtq.core.config import Settings
from dtq.store.tables import Base


# Parameterize integration tests over broker backends
BROKER_BACKENDS = ["redis"]  # Add "kafka", "rabbitmq" when compose --profile brokers is up


def pytest_generate_tests(metafunc):
    """Parameterize tests that use the `broker` fixture over all backends."""
    if "broker_backend" in metafunc.fixturenames:
        metafunc.parametrize("broker_backend", BROKER_BACKENDS, indirect=True)


@pytest.fixture
def broker_backend(request) -> str:
    return request.param


@pytest.fixture
def integration_settings(broker_backend: str) -> Settings:
    return Settings(
        redis_url="redis://localhost:7202/0",
        database_url="postgresql+asyncpg://dtq:dtq_dev@localhost:7203/dtq",
        kafka_bootstrap_servers="localhost:7216",
        rabbitmq_url="amqp://dtq:dtq_dev@localhost:7217/",
        worker_id=f"test-worker-{uuid.uuid4().hex[:8]}",
        broker_backend=broker_backend,
        otel_enabled=False,
        lease_ms=5000,  # Short lease for faster tests
        heartbeat_interval_s=1.0,
        reclaim_interval_s=2.0,
        reclaim_min_idle_ms=5000,
    )


@pytest_asyncio.fixture
async def integration_db(integration_settings: Settings):
    """Create test tables in Postgres."""
    engine = create_async_engine(integration_settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def integration_session(integration_db) -> AsyncGenerator[AsyncSession, None]:
    session_factory = async_sessionmaker(integration_db, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def redis_broker(integration_settings: Settings) -> AsyncGenerator[RedisBroker, None]:
    """Real Redis broker for integration tests."""
    broker = RedisBroker(url=integration_settings.redis_url)
    await broker.connect()
    yield broker
    # Clean up test keys
    client = broker.client
    async for key in client.scan_iter(match="q:test-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="sched:test-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="dlq:test-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="lease:*"):
        await client.delete(key)
    async for key in client.scan_iter(match="fence:*"):
        await client.delete(key)
    async for key in client.scan_iter(match="worker:test-*"):
        await client.delete(key)
    await broker.close()
