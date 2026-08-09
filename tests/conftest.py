"""Shared test fixtures.

Integration and chaos tests run against real Redis + Postgres via docker-compose.
No fakeredis — it hides exactly the bugs this project is about.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dtq.core.config import Settings
from dtq.store.tables import Base


@pytest.fixture(scope="session")
def event_loop():
    """Create a session-scoped event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def settings() -> Settings:
    """Test settings — uses the project's compose ports."""
    return Settings(
        redis_url="redis://localhost:7202/0",
        database_url="postgresql+asyncpg://dtq:dtq_dev@localhost:7203/dtq",
        kafka_bootstrap_servers="localhost:7216",
        rabbitmq_url="amqp://dtq:dtq_dev@localhost:7217/",
        worker_id=f"test-worker-{uuid.uuid4().hex[:8]}",
        broker_backend="redis",
        otel_enabled=False,
    )


@pytest_asyncio.fixture
async def db_engine(settings: Settings):
    """Create a test database engine and tables."""
    engine = create_async_engine(settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional session that rolls back after each test."""
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def redis_client(settings: Settings):
    """Real Redis client for integration tests."""
    import redis.asyncio as aioredis

    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    yield client
    # Flush test data
    await client.flushdb()
    await client.aclose()
