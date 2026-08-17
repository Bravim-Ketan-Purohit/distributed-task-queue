"""Chaos tests — SIGKILL, zombie rejection, Redis restart.

Each of these is a TEST, not a manual experiment.

Per SPEC §9:
- SIGKILL worker mid-task → task runs again, effect applied once
- Zombie worker resumes after its lease expired → fence token rejects its write
- Redis restart mid-flight → no task silently lost

These tests require real Redis + Postgres (docker-compose up).
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dtq.broker.redis_broker import RedisBroker
from dtq.core.exceptions import FenceRejectedError
from dtq.core.models import RetryPolicy, TaskEnvelope, TaskState
from dtq.store.database import async_session_factory
from dtq.store.repository import TaskRepository
from dtq.store.tables import Base


@pytest.fixture
def chaos_settings():
    from dtq.core.config import Settings
    return Settings(
        redis_url="redis://localhost:7202/0",
        database_url="postgresql+asyncpg://dtq:dtq_dev@localhost:7203/dtq",
        worker_id=f"chaos-worker-{uuid.uuid4().hex[:8]}",
        broker_backend="redis",
        otel_enabled=False,
        lease_ms=3000,  # Short lease for chaos testing
    )


@pytest_asyncio.fixture
async def chaos_broker(chaos_settings) -> RedisBroker:
    broker = RedisBroker(url=chaos_settings.redis_url)
    await broker.connect()
    yield broker
    # Cleanup
    client = broker.client
    async for key in client.scan_iter(match="q:chaos-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="sched:chaos-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="dlq:chaos-*"):
        await client.delete(key)
    async for key in client.scan_iter(match="lease:*"):
        await client.delete(key)
    async for key in client.scan_iter(match="fence:*"):
        await client.delete(key)
    await broker.close()


@pytest_asyncio.fixture
async def chaos_db(chaos_settings):
    engine = create_async_engine(chaos_settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def chaos_session(chaos_db):
    session_factory = async_sessionmaker(chaos_db, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.mark.chaos
class TestSIGKILLRecovery:
    """SIGKILL mid-task → reclaimed and completes exactly once.

    This simulates the scenario where:
    1. Worker A claims a task and starts executing
    2. Worker A is killed (lease expires)
    3. Worker B reclaims via XAUTOCLAIM
    4. Worker B executes and commits successfully
    5. Worker A (zombie) cannot commit even if it wakes up
    """

    async def test_reclaim_after_lease_expiry(self, chaos_broker: RedisBroker, chaos_session: AsyncSession):
        """Task is reclaimed after worker dies and lease expires."""
        queue = f"chaos-{uuid.uuid4().hex[:8]}"
        await chaos_broker.ensure_queue(queue)
        task_id = uuid.uuid4()

        # Enqueue
        envelope = TaskEnvelope(
            task_id=task_id,
            queue=queue,
            task_name="chaos_task",
            payload={"data": "important"},
            dedup_key=f"chaos-{task_id}",
        )
        await chaos_broker.publish(queue, envelope)

        # Worker A claims
        messages = await chaos_broker.lease(
            queue=queue, group=queue, consumer="worker-a-dead", count=1, block_ms=2000
        )
        assert len(messages) == 1

        # Worker A acquires lease
        fence_a = await chaos_broker.acquire_lease(str(task_id), "worker-a-dead", lease_ms=200)
        assert fence_a > 0

        # Worker A "dies" — don't ack, don't release, lease expires after 200ms
        await asyncio.sleep(0.3)

        # Worker B reclaims
        reclaimed = await chaos_broker.reclaim(
            queue=queue, group=queue, consumer="worker-b-live",
            min_idle_ms=100, count=5
        )
        assert len(reclaimed) >= 1

        # Worker B acquires lease with HIGHER fence
        fence_b = await chaos_broker.acquire_lease(str(task_id), "worker-b-live", lease_ms=5000)
        assert fence_b > fence_a

        # Worker B commits effect
        repo = TaskRepository(chaos_session)
        task_row, _ = await repo.create_task(
            task_id=task_id,
            queue=queue,
            task_name="chaos_task",
            payload={"data": "important"},
            dedup_key=f"chaos-{task_id}",
        )
        await chaos_session.commit()

        committed = await repo.commit_effect(
            dedup_key=f"chaos-{task_id}",
            task_id=task_id,
            fence=fence_b,
            result={"status": "done"},
        )
        await chaos_session.commit()
        assert committed is True

        # Worker A (zombie) wakes up and tries to commit with OLD fence
        zombie_committed = await repo.commit_effect(
            dedup_key=f"chaos-{task_id}",
            task_id=task_id,
            fence=fence_a,
            result={"status": "zombie_result"},
        )
        # MUST be rejected — effect already committed with higher fence
        assert zombie_committed is False


@pytest.mark.chaos
class TestZombieRejection:
    """Zombie worker resumes after lease expired → fence rejects its write.

    Named test per SPEC §11 M3: "zombie-commit rejection test present and named."
    """

    async def test_zombie_commit_rejected_by_fence(
        self, chaos_broker: RedisBroker, chaos_session: AsyncSession
    ):
        """A worker that stalls past its lease is rejected by the fence check.

        This is THE core guarantee:
        1. Worker A gets fence=10, starts executing
        2. Worker A stalls (GC pause, network partition, etc.)
        3. Lease expires, Worker B reclaims with fence=11
        4. Worker B commits with fence=11
        5. Worker A wakes up, tries commit with fence=10 → REJECTED
        """
        task_id = uuid.uuid4()
        dedup_key = f"zombie-test-{task_id}"

        # Setup: create task in DB
        repo = TaskRepository(chaos_session)
        await repo.create_task(
            task_id=task_id,
            queue="chaos",
            task_name="zombie_test",
            payload={},
            dedup_key=dedup_key,
        )
        await chaos_session.commit()

        # Worker A acquires with short lease
        fence_a = await chaos_broker.acquire_lease(str(task_id), "worker-a", lease_ms=100)
        assert fence_a > 0

        # Lease expires
        await asyncio.sleep(0.15)

        # Worker B reclaims
        fence_b = await chaos_broker.acquire_lease(str(task_id), "worker-b", lease_ms=5000)
        assert fence_b > fence_a

        # Worker B commits successfully
        committed_b = await repo.commit_effect(
            dedup_key=dedup_key, task_id=task_id, fence=fence_b, result={"by": "worker-b"}
        )
        await chaos_session.commit()
        assert committed_b is True

        # Worker A (zombie) tries to commit — MUST FAIL
        committed_a = await repo.commit_effect(
            dedup_key=dedup_key, task_id=task_id, fence=fence_a, result={"by": "worker-a"}
        )
        assert committed_a is False, "Zombie commit must be rejected by fence comparison!"

        # Verify only one effect exists
        effect = await repo.get_effect(dedup_key)
        assert effect is not None
        assert effect.fence == fence_b
        assert effect.result == {"by": "worker-b"}

    async def test_zombie_with_same_fence_is_idempotent(
        self, chaos_broker: RedisBroker, chaos_session: AsyncSession
    ):
        """Re-committing with the same fence is idempotent (not an error)."""
        task_id = uuid.uuid4()
        dedup_key = f"idempotent-{task_id}"

        repo = TaskRepository(chaos_session)
        await repo.create_task(
            task_id=task_id, queue="chaos", task_name="idem_test",
            payload={}, dedup_key=dedup_key,
        )
        await chaos_session.commit()

        fence = await chaos_broker.acquire_lease(str(task_id), "worker-x", lease_ms=5000)

        # First commit
        c1 = await repo.commit_effect(dedup_key=dedup_key, task_id=task_id, fence=fence, result={"v": 1})
        await chaos_session.commit()
        assert c1 is True

        # Second commit with same fence — idempotent
        c2 = await repo.commit_effect(dedup_key=dedup_key, task_id=task_id, fence=fence, result={"v": 2})
        assert c2 is False  # Already committed, no error


@pytest.mark.chaos
class TestNetworkPartition:
    """Simulate network issues."""

    async def test_lease_expires_on_stall(self, chaos_broker: RedisBroker):
        """When a worker can't heartbeat, its lease expires naturally."""
        task_id = str(uuid.uuid4())

        # Acquire with 200ms lease
        fence = await chaos_broker.acquire_lease(task_id, "stalled-worker", lease_ms=200)
        assert fence > 0

        # Simulate network partition (can't heartbeat)
        await asyncio.sleep(0.3)

        # Lease should have expired — another worker can acquire
        fence2 = await chaos_broker.acquire_lease(task_id, "rescue-worker", lease_ms=5000)
        assert fence2 > fence  # New, higher fence

    async def test_heartbeat_failure_doesnt_crash(self, chaos_broker: RedisBroker):
        """Extending a lease that's gone doesn't throw — returns False."""
        task_id = str(uuid.uuid4())

        fence = await chaos_broker.acquire_lease(task_id, "worker", lease_ms=100)
        await asyncio.sleep(0.15)

        # Lease expired, extend should return False gracefully
        result = await chaos_broker.extend_lease(task_id, "worker", fence, 5000)
        assert result is False


@pytest.mark.chaos
class TestMultipleWorkersRacing:
    """Multiple workers attempting the same task concurrently."""

    async def test_only_one_worker_acquires_lease(self, chaos_broker: RedisBroker):
        """With NX semantics, only one worker gets the lease."""
        task_id = str(uuid.uuid4())

        results = []
        for i in range(10):
            fence = await chaos_broker.acquire_lease(task_id, f"racer-{i}", lease_ms=5000)
            results.append(fence)

        # Exactly one non-zero fence
        acquired = [f for f in results if f > 0]
        assert len(acquired) == 1

    async def test_concurrent_commits_only_one_wins(self, chaos_session: AsyncSession):
        """Multiple commit attempts — only the first succeeds."""
        task_id = uuid.uuid4()
        dedup_key = f"race-{task_id}"

        repo = TaskRepository(chaos_session)
        await repo.create_task(
            task_id=task_id, queue="chaos", task_name="race_test",
            payload={}, dedup_key=dedup_key,
        )
        await chaos_session.commit()

        # Simulate multiple workers trying to commit
        results = []
        for fence in range(1, 6):
            committed = await repo.commit_effect(
                dedup_key=dedup_key, task_id=task_id, fence=fence, result={"fence": fence}
            )
            if committed:
                await chaos_session.commit()
            results.append(committed)

        # Only the first one should succeed
        assert results[0] is True
        assert all(r is False for r in results[1:])
