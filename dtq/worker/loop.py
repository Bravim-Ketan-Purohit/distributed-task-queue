"""Worker main loop — polls broker, dispatches to executor, manages lifecycle.

The worker is an asyncio application that:
1. Polls the broker for new tasks via XREADGROUP
2. Dispatches tasks to the executor (with concurrency limit)
3. Runs the reclaimer to adopt orphaned tasks
4. Runs the scheduler to promote due delayed tasks
5. Reports worker heartbeat for fleet visibility
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import structlog

from dtq.broker.base import Leased
from dtq.broker.redis_broker import RedisBroker
from dtq.core.backoff import compute_delay, is_retryable
from dtq.core.config import Settings
from dtq.core.models import RetryPolicy, TaskEnvelope, TaskState
from dtq.store.database import async_session_factory
from dtq.store.repository import TaskRepository
from dtq.worker.executor import TaskExecutor

logger = structlog.get_logger()


class WorkerLoop:
    """The main worker process.

    Orchestrates polling, execution, retry scheduling, reclamation,
    and heartbeats.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._broker = RedisBroker(url=settings.redis_url)
        self._worker_id = settings.worker_id
        self._queues = settings.worker_queues
        self._concurrency = settings.worker_concurrency
        self._lease_ms = settings.lease_ms
        self._heartbeat_interval = settings.heartbeat_interval_s
        self._reclaim_interval = settings.reclaim_interval_s
        self._reclaim_min_idle_ms = settings.reclaim_min_idle_ms

        self._thread_pool = ThreadPoolExecutor(max_workers=self._concurrency)
        self._executor = TaskExecutor(
            broker=self._broker,
            worker_id=self._worker_id,
            lease_ms=self._lease_ms,
            thread_pool=self._thread_pool,
        )

        self._running = False
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._shutting_down = False
        self._drain_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker loop."""
        log = logger.bind(worker_id=self._worker_id, queues=self._queues)
        log.info("worker_starting", concurrency=self._concurrency)

        await self._broker.connect()

        # Ensure queues exist
        for queue in self._queues:
            await self._broker.ensure_queue(queue)

        self._running = True

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._signal_shutdown)

        # Run all loops concurrently
        await asyncio.gather(
            self._poll_loop(),
            self._scheduler_loop(),
            self._reclaim_loop(),
            self._heartbeat_loop(),
            return_exceptions=True,
        )

    async def stop(self) -> None:
        """Graceful shutdown — finish in-flight tasks, stop polling."""
        logger.info("worker_stopping", worker_id=self._worker_id, in_flight=len(self._in_flight))
        self._running = False

        # Wait for in-flight tasks to complete
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)

        await self._broker.close()
        self._thread_pool.shutdown(wait=False)
        logger.info("worker_stopped", worker_id=self._worker_id)

    def _signal_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        if not self._shutting_down:
            self._shutting_down = True
            self._running = False
            logger.info("shutdown_signal_received", worker_id=self._worker_id)

    async def drain(self) -> None:
        """Stop accepting new tasks but finish in-flight work."""
        self._running = False
        self._drain_event.set()
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)

    # --- Main poll loop ---

    async def _poll_loop(self) -> None:
        """Continuously poll broker for new tasks."""
        while self._running:
            for queue in self._queues:
                # Respect concurrency limit
                if self._semaphore._value == 0:
                    await asyncio.sleep(0.1)
                    continue

                batch_size = min(self._semaphore._value, 5)
                try:
                    messages = await self._broker.lease(
                        queue=queue,
                        group=queue,
                        consumer=self._worker_id,
                        count=batch_size,
                        block_ms=1000,
                    )
                except Exception as e:
                    logger.error("poll_error", queue=queue, error=str(e))
                    await asyncio.sleep(1)
                    continue

                for msg in messages:
                    await self._semaphore.acquire()
                    task = asyncio.create_task(
                        self._process_message(queue, msg),
                        name=f"task-{msg.envelope.task_id}",
                    )
                    self._in_flight.add(task)
                    task.add_done_callback(self._in_flight.discard)

            # Small sleep to avoid tight loop when no messages
            if not self._running:
                break
            await asyncio.sleep(0.05)

    async def _process_message(self, queue: str, msg: Leased) -> None:
        """Process a single message from the broker."""
        envelope = msg.envelope
        try:
            success, error_repr = await self._executor.execute(envelope)

            if success:
                # Ack the broker message
                await self._broker.ack(queue, queue, [msg.broker_msg_id])
            else:
                # Determine if retryable
                error_type = ""
                if error_repr:
                    error_type = error_repr.split(":")[0] if ":" in error_repr else error_repr

                retry_policy = envelope.retry_policy
                if is_retryable(retry_policy, error_type, envelope.attempt + 1):
                    # Schedule retry with backoff
                    delay = compute_delay(retry_policy, envelope.attempt + 1)
                    run_at = time.time() + delay
                    retry_envelope = envelope.model_copy(
                        update={"attempt": envelope.attempt + 1}
                    )
                    await self._broker.schedule(queue, retry_envelope, run_at)
                    logger.info(
                        "task_scheduled_retry",
                        task_id=str(envelope.task_id),
                        attempt=envelope.attempt + 1,
                        delay_s=delay,
                    )
                else:
                    # Dead letter
                    await self._broker.dead_letter(
                        queue, envelope, reason=error_repr or "max_attempts_exhausted"
                    )
                    # Update state in Postgres
                    async with async_session_factory() as session:
                        repo = TaskRepository(session)
                        await repo.update_task_state(envelope.task_id, TaskState.DEAD)
                        await session.commit()
                    logger.warning(
                        "task_dead_lettered",
                        task_id=str(envelope.task_id),
                        reason=error_repr,
                    )

                # Ack from broker (we've handled it — retry is via schedule)
                await self._broker.ack(queue, queue, [msg.broker_msg_id])

        except Exception as e:
            logger.error(
                "process_message_error",
                task_id=str(envelope.task_id),
                error=str(e),
            )
        finally:
            self._semaphore.release()

    # --- Scheduler tick ---

    async def _scheduler_loop(self) -> None:
        """Promote due scheduled tasks into the ready queue."""
        while self._running:
            for queue in self._queues:
                try:
                    promoted = await self._broker.promote_scheduled(queue)
                    if promoted > 0:
                        logger.debug("scheduler_promoted", queue=queue, count=promoted)
                except Exception as e:
                    logger.error("scheduler_error", queue=queue, error=str(e))
            await asyncio.sleep(1.0)

    # --- Reclaimer ---

    async def _reclaim_loop(self) -> None:
        """Reclaim orphaned tasks whose lease expired.

        Uses XAUTOCLAIM to adopt entries that have been idle too long.
        On adoption: attempt++, prior attempt closed as lease_lost, new fence issued.
        """
        # Jitter the initial delay to avoid thundering herd
        import random

        await asyncio.sleep(random.uniform(0, self._reclaim_interval))

        while self._running:
            for queue in self._queues:
                try:
                    reclaimed = await self._broker.reclaim(
                        queue=queue,
                        group=queue,
                        consumer=self._worker_id,
                        min_idle_ms=self._reclaim_min_idle_ms,
                        count=5,
                    )
                    for msg in reclaimed:
                        # Close prior attempt as lease_lost
                        async with async_session_factory() as session:
                            repo = TaskRepository(session)
                            try:
                                await repo.close_attempt(
                                    msg.envelope.task_id,
                                    msg.envelope.attempt,
                                    "lease_lost",
                                )
                                await session.commit()
                            except Exception:
                                await session.rollback()

                        # Re-process with incremented attempt
                        new_envelope = msg.envelope.model_copy(
                            update={"attempt": msg.envelope.attempt + 1}
                        )
                        new_msg = Leased(
                            envelope=new_envelope,
                            broker_msg_id=msg.broker_msg_id,
                            idle_ms=msg.idle_ms,
                        )

                        await self._semaphore.acquire()
                        task = asyncio.create_task(
                            self._process_message(queue, new_msg),
                            name=f"reclaim-{msg.envelope.task_id}",
                        )
                        self._in_flight.add(task)
                        task.add_done_callback(self._in_flight.discard)

                        logger.info(
                            "task_reclaimed",
                            task_id=str(msg.envelope.task_id),
                            idle_ms=msg.idle_ms,
                            new_attempt=new_envelope.attempt,
                        )

                except Exception as e:
                    logger.error("reclaim_error", queue=queue, error=str(e))

            await asyncio.sleep(self._reclaim_interval)

    # --- Worker heartbeat ---

    async def _heartbeat_loop(self) -> None:
        """Write per-worker presence key every heartbeat_interval.

        Key: worker:<id> with PX = 3 * heartbeat_interval
        Value: {queues, in_flight, started_at, version}
        """
        started_at = time.time()
        ttl_ms = int(self._heartbeat_interval * 3 * 1000)

        while self._running:
            try:
                data = {
                    "queues": self._queues,
                    "in_flight": len(self._in_flight),
                    "concurrency": self._concurrency,
                    "started_at": started_at,
                    "version": "0.1.0",
                    "timestamp": time.time(),
                }
                await self._broker.set_worker_heartbeat(
                    self._worker_id, data, ttl_ms
                )
            except Exception as e:
                logger.error("heartbeat_error", error=str(e))

            await asyncio.sleep(self._heartbeat_interval)
