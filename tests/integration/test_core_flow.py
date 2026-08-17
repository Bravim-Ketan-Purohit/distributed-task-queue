"""Integration tests — enqueue through execute, real Redis + Postgres.

Test matrix per SPEC §9:
- enqueue → execute → succeed
- dedup enqueue
- delayed run_at
- DLQ after max attempts
- requeue from DLQ
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
import pytest_asyncio

from dtq.broker.redis_broker import RedisBroker
from dtq.core.models import RetryPolicy, TaskEnvelope, TaskState
from dtq.store.repository import TaskRepository


@pytest.mark.integration
class TestEnqueueExecute:
    """End-to-end: enqueue → claim → ack."""

    async def test_publish_and_consume(self, redis_broker: RedisBroker):
        """A published task can be consumed from the stream."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        envelope = TaskEnvelope(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="test_task",
            payload={"key": "value"},
        )
        await redis_broker.publish(queue, envelope)

        # Consume
        messages = await redis_broker.lease(
            queue=queue, group=queue, consumer="test-consumer", count=1, block_ms=2000
        )
        assert len(messages) == 1
        assert messages[0].envelope.task_name == "test_task"
        assert messages[0].envelope.payload == {"key": "value"}

        # Ack
        await redis_broker.ack(queue, queue, [messages[0].broker_msg_id])

        # Nothing left
        messages2 = await redis_broker.lease(
            queue=queue, group=queue, consumer="test-consumer", count=1, block_ms=500
        )
        assert len(messages2) == 0

    async def test_multiple_messages_ordered(self, redis_broker: RedisBroker):
        """Messages come out in FIFO order."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        ids = []
        for i in range(5):
            tid = uuid.uuid4()
            ids.append(tid)
            envelope = TaskEnvelope(
                task_id=tid,
                queue=queue,
                task_name="order_test",
                payload={"seq": i},
            )
            await redis_broker.publish(queue, envelope)

        messages = await redis_broker.lease(
            queue=queue, group=queue, consumer="test-consumer", count=5, block_ms=2000
        )
        assert len(messages) == 5
        for i, msg in enumerate(messages):
            assert msg.envelope.payload["seq"] == i


@pytest.mark.integration
class TestDedup:
    """Dedup key prevents duplicate enqueue."""

    async def test_dedup_enqueue(self, redis_broker: RedisBroker, integration_session):
        """Second enqueue with same dedup_key returns deduplicated=True."""
        repo = TaskRepository(integration_session)
        queue = f"test-{uuid.uuid4().hex[:8]}"

        task_id = uuid.uuid4()
        task_row, deduped = await repo.create_task(
            task_id=task_id,
            queue=queue,
            task_name="dedup_test",
            payload={"x": 1},
            dedup_key="my-dedup-key",
        )
        await integration_session.commit()
        assert not deduped

        # Second create with same key
        task_row2, deduped2 = await repo.create_task(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="dedup_test",
            payload={"x": 2},
            dedup_key="my-dedup-key",
        )
        assert deduped2
        assert task_row2.id == task_id  # Returns original


@pytest.mark.integration
class TestScheduledDelivery:
    """Delayed tasks via ZSET scheduling."""

    async def test_scheduled_task_promoted(self, redis_broker: RedisBroker):
        """A task scheduled for the past is promoted on next tick."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        envelope = TaskEnvelope(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="delayed_task",
            payload={"delay": True},
        )
        # Schedule 1 second in the past
        await redis_broker.schedule(queue, envelope, time.time() - 1)

        # Promote
        promoted = await redis_broker.promote_scheduled(queue)
        assert promoted == 1

        # Should now be in the stream
        messages = await redis_broker.lease(
            queue=queue, group=queue, consumer="test-consumer", count=1, block_ms=2000
        )
        assert len(messages) == 1
        assert messages[0].envelope.task_name == "delayed_task"

    async def test_future_task_not_promoted(self, redis_broker: RedisBroker):
        """A task scheduled for the future is NOT promoted."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        envelope = TaskEnvelope(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="future_task",
            payload={},
        )
        await redis_broker.schedule(queue, envelope, time.time() + 3600)

        promoted = await redis_broker.promote_scheduled(queue)
        assert promoted == 0


@pytest.mark.integration
class TestDLQ:
    """Dead letter queue operations."""

    async def test_dead_letter_and_retrieve(self, redis_broker: RedisBroker):
        """Tasks can be dead-lettered and retrieved."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        envelope = TaskEnvelope(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="failing_task",
            payload={"will": "fail"},
            attempt=5,
        )
        await redis_broker.dead_letter(queue, envelope, reason="max_attempts_exhausted")

        # Retrieve from DLQ
        dlq_msgs = await redis_broker.get_dlq_messages(queue)
        assert len(dlq_msgs) == 1
        assert dlq_msgs[0].envelope.task_name == "failing_task"


@pytest.mark.integration
class TestLeaseAndFencing:
    """Lease acquisition with fencing tokens."""

    async def test_acquire_and_release_lease(self, redis_broker: RedisBroker):
        """Lease acquire returns fence > 0, release succeeds."""
        task_id = str(uuid.uuid4())
        worker_id = "test-worker-1"

        fence = await redis_broker.acquire_lease(task_id, worker_id, lease_ms=5000)
        assert fence > 0

        released = await redis_broker.release_lease(task_id, worker_id, fence)
        assert released is True

    async def test_double_acquire_fails(self, redis_broker: RedisBroker):
        """Second acquire of same task returns 0 (NX semantics)."""
        task_id = str(uuid.uuid4())

        fence1 = await redis_broker.acquire_lease(task_id, "worker-1", lease_ms=5000)
        assert fence1 > 0

        fence2 = await redis_broker.acquire_lease(task_id, "worker-2", lease_ms=5000)
        assert fence2 == 0  # Already held

    async def test_fence_monotonically_increases(self, redis_broker: RedisBroker):
        """Each new lease gets a higher fence token."""
        fences = []
        for i in range(5):
            task_id = str(uuid.uuid4())
            fence = await redis_broker.acquire_lease(task_id, f"worker-{i}", lease_ms=5000)
            fences.append(fence)
            await redis_broker.release_lease(task_id, f"worker-{i}", fence)

        # Strictly monotonically increasing
        for i in range(1, len(fences)):
            assert fences[i] > fences[i - 1]

    async def test_heartbeat_extends_lease(self, redis_broker: RedisBroker):
        """Heartbeat extends the lease TTL."""
        task_id = str(uuid.uuid4())
        worker_id = "test-worker-hb"

        fence = await redis_broker.acquire_lease(task_id, worker_id, lease_ms=2000)
        assert fence > 0

        # Extend
        extended = await redis_broker.extend_lease(task_id, worker_id, fence, lease_ms=5000)
        assert extended is True

    async def test_extend_after_release_fails(self, redis_broker: RedisBroker):
        """Cannot extend a lease we no longer own."""
        task_id = str(uuid.uuid4())
        worker_id = "test-worker-gone"

        fence = await redis_broker.acquire_lease(task_id, worker_id, lease_ms=2000)
        await redis_broker.release_lease(task_id, worker_id, fence)

        extended = await redis_broker.extend_lease(task_id, worker_id, fence, lease_ms=5000)
        assert extended is False

    async def test_release_wrong_owner_fails(self, redis_broker: RedisBroker):
        """Cannot release a lease owned by someone else."""
        task_id = str(uuid.uuid4())

        fence = await redis_broker.acquire_lease(task_id, "worker-a", lease_ms=5000)
        # Worker B tries to release
        released = await redis_broker.release_lease(task_id, "worker-b", fence)
        assert released is False


@pytest.mark.integration
class TestReclaim:
    """XAUTOCLAIM — crash recovery of orphaned tasks."""

    async def test_reclaim_idle_message(self, redis_broker: RedisBroker):
        """Messages idle too long are reclaimed by XAUTOCLAIM."""
        queue = f"test-{uuid.uuid4().hex[:8]}"
        await redis_broker.ensure_queue(queue)

        # Publish and claim (but don't ack)
        envelope = TaskEnvelope(
            task_id=uuid.uuid4(),
            queue=queue,
            task_name="orphan_task",
            payload={"orphaned": True},
        )
        await redis_broker.publish(queue, envelope)

        messages = await redis_broker.lease(
            queue=queue, group=queue, consumer="dead-worker", count=1, block_ms=2000
        )
        assert len(messages) == 1
        # Don't ack — simulate crash

        # Wait for min_idle to pass
        await asyncio.sleep(0.1)

        # Reclaim with very low min_idle for testing
        reclaimed = await redis_broker.reclaim(
            queue=queue, group=queue, consumer="live-worker",
            min_idle_ms=50, count=5
        )
        assert len(reclaimed) == 1
        assert reclaimed[0].envelope.task_name == "orphan_task"
