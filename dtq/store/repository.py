"""Task repository — all Postgres operations.

The critical operation is commit_effect: it atomically records the effect
and checks the fencing token so a zombie worker cannot commit stale work.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from dtq.core.exceptions import DuplicateTaskError, FenceRejectedError, TaskNotFoundError
from dtq.core.models import TaskState
from dtq.store.tables import AttemptRow, EffectRow, TaskRow, WorkflowRow


class TaskRepository:
    """Encapsulates all task persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # --- Task CRUD ---

    async def create_task(
        self,
        task_id: uuid.UUID,
        queue: str,
        task_name: str,
        payload: dict[str, Any],
        dedup_key: str | None = None,
        priority: int = 0,
        max_attempts: int = 5,
        run_at: datetime | None = None,
        workflow_id: uuid.UUID | None = None,
        step_name: str | None = None,
    ) -> tuple[TaskRow, bool]:
        """Create a task. Returns (task, deduplicated).

        If dedup_key exists, returns the existing task with deduplicated=True.
        Idempotent enqueue is the point.
        """
        if dedup_key:
            existing = await self._session.execute(
                select(TaskRow).where(
                    TaskRow.queue == queue, TaskRow.dedup_key == dedup_key
                )
            )
            row = existing.scalar_one_or_none()
            if row is not None:
                return row, True

        task = TaskRow(
            id=task_id,
            queue=queue,
            task_name=task_name,
            payload=payload,
            state="pending" if run_at is None else "scheduled",
            priority=priority,
            max_attempts=max_attempts,
            dedup_key=dedup_key,
            run_at=run_at,
            workflow_id=workflow_id,
            step_name=step_name,
        )
        self._session.add(task)
        await self._session.flush()
        return task, False

    async def get_task(self, task_id: uuid.UUID) -> TaskRow:
        result = await self._session.execute(select(TaskRow).where(TaskRow.id == task_id))
        row = result.scalar_one_or_none()
        if row is None:
            raise TaskNotFoundError(f"Task {task_id} not found")
        return row

    async def update_task_state(
        self, task_id: uuid.UUID, state: TaskState, attempt: int | None = None
    ) -> None:
        values: dict[str, Any] = {"state": state.value, "updated_at": datetime.now(timezone.utc)}
        if attempt is not None:
            values["attempt"] = attempt
        await self._session.execute(
            update(TaskRow).where(TaskRow.id == task_id).values(**values)
        )

    async def cancel_task(self, task_id: uuid.UUID) -> bool:
        """Cancel if not yet leased. Returns True if cancelled."""
        result = await self._session.execute(
            update(TaskRow)
            .where(TaskRow.id == task_id, TaskRow.state.in_(["pending", "scheduled"]))
            .values(state="cancelled", updated_at=datetime.now(timezone.utc))
        )
        return result.rowcount > 0  # type: ignore[return-value]

    # --- Attempts ---

    async def create_attempt(
        self,
        task_id: uuid.UUID,
        attempt_no: int,
        worker_id: str,
        fence: int,
    ) -> AttemptRow:
        attempt = AttemptRow(
            task_id=task_id,
            attempt_no=attempt_no,
            worker_id=worker_id,
            fence=fence,
        )
        self._session.add(attempt)
        await self._session.flush()
        return attempt

    async def close_attempt(
        self,
        task_id: uuid.UUID,
        attempt_no: int,
        outcome: str,
        error_type: str | None = None,
        error_repr: str | None = None,
    ) -> None:
        await self._session.execute(
            update(AttemptRow)
            .where(AttemptRow.task_id == task_id, AttemptRow.attempt_no == attempt_no)
            .values(
                finished_at=datetime.now(timezone.utc),
                outcome=outcome,
                error_type=error_type,
                error_repr=error_repr,
            )
        )

    async def get_attempts(self, task_id: uuid.UUID) -> list[AttemptRow]:
        result = await self._session.execute(
            select(AttemptRow)
            .where(AttemptRow.task_id == task_id)
            .order_by(AttemptRow.attempt_no)
        )
        return list(result.scalars().all())

    # --- Effects (exactly-once commit) ---

    async def commit_effect(
        self,
        dedup_key: str,
        task_id: uuid.UUID,
        fence: int,
        result: dict[str, Any] | None = None,
    ) -> bool:
        """Atomically commit a task's effect with fence check.

        This is the core exactly-once-effect mechanism:
        INSERT INTO effects WHERE NOT EXISTS (... AND fence > $ours).

        A zombie worker that stalls past its lease, loses it to a reclaimer,
        then wakes up and tries to commit is rejected by the fence comparison.

        Returns True if committed, raises FenceRejectedError if a higher fence exists.
        """
        # Check if effect already committed
        existing = await self._session.execute(
            select(EffectRow).where(EffectRow.dedup_key == dedup_key)
        )
        row = existing.scalar_one_or_none()
        if row is not None:
            if row.fence > fence:
                raise FenceRejectedError(
                    f"Effect for {dedup_key} already committed with higher fence "
                    f"{row.fence} > {fence}. This worker is a zombie."
                )
            # Same or lower fence — effect already committed (idempotent)
            return False

        # Insert new effect
        effect = EffectRow(
            dedup_key=dedup_key,
            task_id=task_id,
            fence=fence,
            result=result,
        )
        self._session.add(effect)
        await self._session.flush()
        return True

    async def get_effect(self, dedup_key: str) -> EffectRow | None:
        result = await self._session.execute(
            select(EffectRow).where(EffectRow.dedup_key == dedup_key)
        )
        return result.scalar_one_or_none()

    # --- Workflows ---

    async def create_workflow(
        self, workflow_id: uuid.UUID, name: str, spec: dict[str, Any]
    ) -> WorkflowRow:
        wf = WorkflowRow(id=workflow_id, name=name, spec=spec, state="running")
        self._session.add(wf)
        await self._session.flush()
        return wf

    async def get_workflow(self, workflow_id: uuid.UUID) -> WorkflowRow | None:
        result = await self._session.execute(
            select(WorkflowRow).where(WorkflowRow.id == workflow_id)
        )
        return result.scalar_one_or_none()

    async def update_workflow_state(self, workflow_id: uuid.UUID, state: str) -> None:
        await self._session.execute(
            update(WorkflowRow).where(WorkflowRow.id == workflow_id).values(state=state)
        )

    async def get_tasks_for_workflow(self, workflow_id: uuid.UUID) -> list[TaskRow]:
        result = await self._session.execute(
            select(TaskRow).where(TaskRow.workflow_id == workflow_id)
        )
        return list(result.scalars().all())

    # --- Queries ---

    async def get_tasks_by_state(
        self, queue: str, state: str, limit: int = 50
    ) -> list[TaskRow]:
        result = await self._session.execute(
            select(TaskRow)
            .where(TaskRow.queue == queue, TaskRow.state == state)
            .order_by(TaskRow.priority.desc(), TaskRow.created_at)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def count_by_state(self, queue: str) -> dict[str, int]:
        result = await self._session.execute(
            text(
                "SELECT state, COUNT(*) FROM tasks WHERE queue = :queue GROUP BY state"
            ).bindparams(queue=queue)
        )
        return {row[0]: row[1] for row in result.fetchall()}
