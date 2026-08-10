"""Task executor — runs the handler and commits the effect atomically.

The executor boundary is explicit: CPU-bound task bodies go to a thread or
process pool. The event loop is never blocked by task execution.
"""

from __future__ import annotations

import asyncio
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import structlog

from dtq.broker.redis_broker import RedisBroker
from dtq.core.exceptions import FenceRejectedError, LeaseExpiredError
from dtq.core.models import TaskEnvelope
from dtq.store.database import async_session_factory
from dtq.store.repository import TaskRepository
from dtq.worker.registry import get_handler

logger = structlog.get_logger()


class TaskExecutor:
    """Executes a single task with the full lease/fence/commit protocol.

    Flow:
    1. Acquire lease (fence token)
    2. Record attempt in Postgres
    3. Start heartbeat (renews lease every lease_ms/3)
    4. Execute handler
    5. Commit effect atomically (fence check rejects zombies)
    6. Ack broker message
    7. Release lease
    """

    def __init__(
        self,
        broker: RedisBroker,
        worker_id: str,
        lease_ms: int = 30_000,
        thread_pool: ThreadPoolExecutor | None = None,
    ) -> None:
        self._broker = broker
        self._worker_id = worker_id
        self._lease_ms = lease_ms
        self._thread_pool = thread_pool or ThreadPoolExecutor(max_workers=4)
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}

    async def execute(self, envelope: TaskEnvelope) -> tuple[bool, str | None]:
        """Execute a task envelope. Returns (success, error_repr).

        On success: effect committed, broker message acked.
        On failure: attempt closed, task eligible for retry/DLQ.
        On fence rejection: zombie detected, no commit.
        """
        task_id = str(envelope.task_id)
        attempt_no = envelope.attempt + 1
        dedup_key = envelope.dedup_key or task_id

        log = logger.bind(
            task_id=task_id,
            worker_id=self._worker_id,
            attempt=attempt_no,
            task_name=envelope.task_name,
        )

        # Step 1: Acquire lease
        fence = await self._broker.acquire_lease(task_id, self._worker_id, self._lease_ms)
        if fence == 0:
            log.warning("lease_acquire_failed", reason="already_held")
            return False, "LeaseAcquireError: already held"

        log = log.bind(fence=fence)
        log.info("lease_acquired")

        # Step 2: Record attempt
        async with async_session_factory() as session:
            repo = TaskRepository(session)
            try:
                await repo.create_attempt(
                    task_id=envelope.task_id,
                    attempt_no=attempt_no,
                    worker_id=self._worker_id,
                    fence=fence,
                )
                from dtq.core.models import TaskState

                await repo.update_task_state(envelope.task_id, TaskState.LEASED, attempt=attempt_no)
                await session.commit()
            except Exception as e:
                log.error("attempt_record_failed", error=str(e))
                await session.rollback()
                await self._broker.release_lease(task_id, self._worker_id, fence)
                return False, f"StoreError: {e}"

        # Step 3: Start heartbeat
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(task_id, fence), name=f"heartbeat-{task_id}"
        )
        self._heartbeat_tasks[task_id] = heartbeat_task

        # Step 4: Execute handler
        result: dict[str, Any] | None = None
        error_repr: str | None = None
        error_type: str | None = None
        success = False

        try:
            handler = get_handler(envelope.task_name)
            if handler is None:
                raise RuntimeError(f"No handler registered for task: {envelope.task_name}")

            # Run handler — use thread pool for CPU-bound work
            result = await asyncio.wait_for(
                handler(envelope.payload),
                timeout=self._lease_ms / 1000 * 0.8,  # 80% of lease time
            )
            success = True
            log.info("task_executed", result_keys=list(result.keys()) if result else None)

        except asyncio.TimeoutError:
            error_type = "TimeoutError"
            error_repr = f"Task exceeded execution timeout ({self._lease_ms * 0.8}ms)"
            log.warning("task_timeout")

        except FenceRejectedError as e:
            error_type = "FenceRejectedError"
            error_repr = str(e)
            log.error("fence_rejected_during_execution", error=str(e))

        except Exception as e:
            error_type = type(e).__name__
            error_repr = traceback.format_exc()
            log.error("task_failed", error_type=error_type, error=str(e))

        finally:
            # Stop heartbeat
            heartbeat_task.cancel()
            self._heartbeat_tasks.pop(task_id, None)
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        # Step 5: Commit or record failure
        async with async_session_factory() as session:
            repo = TaskRepository(session)
            try:
                if success:
                    # Commit effect atomically with fence check
                    committed = await repo.commit_effect(
                        dedup_key=dedup_key,
                        task_id=envelope.task_id,
                        fence=fence,
                        result=result,
                    )
                    if committed:
                        from dtq.core.models import TaskState

                        await repo.update_task_state(envelope.task_id, TaskState.SUCCEEDED)
                        await repo.close_attempt(
                            envelope.task_id, attempt_no, "succeeded"
                        )
                        log.info("effect_committed")
                    else:
                        # Already committed by another worker (idempotent)
                        log.info("effect_already_committed")
                        await repo.close_attempt(
                            envelope.task_id, attempt_no, "succeeded"
                        )
                else:
                    await repo.close_attempt(
                        envelope.task_id,
                        attempt_no,
                        "failed" if error_type != "FenceRejectedError" else "lease_lost",
                        error_type=error_type,
                        error_repr=error_repr,
                    )
                await session.commit()

            except FenceRejectedError as e:
                log.error("zombie_commit_rejected", error=str(e))
                await session.rollback()
                success = False
                error_repr = str(e)

            except Exception as e:
                log.error("commit_failed", error=str(e))
                await session.rollback()
                success = False
                error_repr = f"CommitError: {e}"

        # Step 6: Release lease
        released = await self._broker.release_lease(task_id, self._worker_id, fence)
        if not released:
            log.warning("lease_release_failed", reason="no_longer_owner")

        return success, error_repr

    async def _heartbeat_loop(self, task_id: str, fence: int) -> None:
        """Renew lease every lease_ms / 3 while executing.

        If renewal fails, the lease is lost and the worker should stop.
        """
        interval = self._lease_ms / 3 / 1000  # Convert to seconds
        while True:
            await asyncio.sleep(interval)
            extended = await self._broker.extend_lease(
                task_id, self._worker_id, fence, self._lease_ms
            )
            if not extended:
                logger.warning(
                    "heartbeat_lease_lost",
                    task_id=task_id,
                    worker_id=self._worker_id,
                    fence=fence,
                )
                break
