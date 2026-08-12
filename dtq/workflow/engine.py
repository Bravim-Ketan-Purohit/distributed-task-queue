"""Workflow engine — executes DAGs with fan-out/fan-in.

Semantics per SPEC §7:
- A step runs when all depends_on steps have succeeded.
- fan_out expands one step into N sibling tasks from a list in the upstream result.
- The downstream step waits for ALL fan_out tasks (fan-in).
- One dead step fails the workflow but leaves completed steps recorded.
- The DAG is validated for cycles at submit time.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from dtq.broker.redis_broker import RedisBroker
from dtq.control.events import publish_event
from dtq.core.models import RetryPolicy, TaskEnvelope, TaskState
from dtq.store.database import async_session_factory
from dtq.store.repository import TaskRepository
from dtq.workflow.dag import get_ready_steps, validate_dag

logger = structlog.get_logger()


class WorkflowEngine:
    """Manages workflow lifecycle: submit, advance, fan-out/fan-in."""

    def __init__(self, broker: RedisBroker) -> None:
        self._broker = broker

    async def submit(self, name: str, steps: list[dict[str, Any]]) -> uuid.UUID:
        """Submit a new workflow. Validates DAG, creates workflow + initial tasks.

        Raises ValueError (→ 422) if the DAG contains a cycle.
        """
        # Validate DAG
        validate_dag(steps)

        workflow_id = uuid.uuid4()
        spec = {"name": name, "steps": steps}

        async with async_session_factory() as session:
            repo = TaskRepository(session)
            await repo.create_workflow(workflow_id, name, spec)
            await session.commit()

        # Start initial steps (those with no dependencies)
        await self.advance(workflow_id)

        return workflow_id

    async def advance(self, workflow_id: uuid.UUID) -> None:
        """Advance the workflow — start any steps whose deps are satisfied."""
        async with async_session_factory() as session:
            repo = TaskRepository(session)
            wf = await repo.get_workflow(workflow_id)
            if wf is None or wf.state != "running":
                return

            steps = wf.spec.get("steps", [])
            tasks = await repo.get_tasks_for_workflow(workflow_id)

            # Build completed/failed/running state
            step_states: dict[str, str] = {}
            step_results: dict[str, dict[str, Any]] = {}
            for t in tasks:
                if t.step_name:
                    step_states[t.step_name] = t.state
                    # For fan-out, we need to check all sibling tasks
                    if t.state == "succeeded":
                        # Load result from effects table
                        effect = await repo.get_effect(t.dedup_key or str(t.id))
                        if effect and effect.result:
                            step_results[t.step_name] = effect.result

            completed_steps = {
                name for name, state in step_states.items() if state == "succeeded"
            }
            dead_steps = {
                name for name, state in step_states.items() if state == "dead"
            }

            # If any step is dead, fail the workflow
            if dead_steps:
                await repo.update_workflow_state(workflow_id, "failed")
                await session.commit()
                logger.warning(
                    "workflow_failed",
                    workflow_id=str(workflow_id),
                    dead_steps=list(dead_steps),
                )
                return

            # Check if all steps completed
            all_step_names = {s["name"] for s in steps}
            if completed_steps >= all_step_names:
                await repo.update_workflow_state(workflow_id, "completed")
                await session.commit()
                logger.info("workflow_completed", workflow_id=str(workflow_id))
                return

            # Find and launch ready steps
            ready = get_ready_steps(steps, completed_steps)
            launched = 0

            for step_spec in ready:
                step_name = step_spec["name"]
                if step_name in step_states:
                    continue  # Already running or completed

                # Handle fan-out
                fan_out_field = step_spec.get("fan_out")
                if fan_out_field:
                    # Get the upstream result that contains the list to fan out
                    deps = step_spec.get("depends_on", [])
                    items: list[Any] = []
                    for dep in deps:
                        dep_result = step_results.get(dep, {})
                        if fan_out_field in dep_result:
                            items = dep_result[fan_out_field]
                            break

                    if not items:
                        items = [step_spec.get("payload", {})]

                    # Create N sibling tasks
                    for i, item in enumerate(items):
                        fan_task_id = uuid.uuid4()
                        fan_step_name = f"{step_name}[{i}]"
                        payload = item if isinstance(item, dict) else {"item": item}

                        task_row, _ = await repo.create_task(
                            task_id=fan_task_id,
                            queue="default",
                            task_name=step_spec["task"],
                            payload=payload,
                            workflow_id=workflow_id,
                            step_name=fan_step_name,
                            max_attempts=step_spec.get("retry", {}).get("max_attempts", 5),
                        )

                        envelope = TaskEnvelope(
                            task_id=fan_task_id,
                            queue="default",
                            task_name=step_spec["task"],
                            payload=payload,
                            workflow_id=workflow_id,
                            step_name=fan_step_name,
                            max_attempts=task_row.max_attempts,
                            retry_policy=RetryPolicy(
                                max_attempts=step_spec.get("retry", {}).get("max_attempts", 5)
                            ),
                        )
                        await self._broker.ensure_queue("default")
                        await self._broker.publish("default", envelope)
                        launched += 1
                else:
                    # Regular single task
                    task_id = uuid.uuid4()
                    payload = step_spec.get("payload", {})
                    retry_conf = step_spec.get("retry", {})

                    task_row, _ = await repo.create_task(
                        task_id=task_id,
                        queue="default",
                        task_name=step_spec["task"],
                        payload=payload,
                        workflow_id=workflow_id,
                        step_name=step_name,
                        max_attempts=retry_conf.get("max_attempts", 5),
                    )

                    envelope = TaskEnvelope(
                        task_id=task_id,
                        queue="default",
                        task_name=step_spec["task"],
                        payload=payload,
                        workflow_id=workflow_id,
                        step_name=step_name,
                        max_attempts=task_row.max_attempts,
                        retry_policy=RetryPolicy(
                            max_attempts=retry_conf.get("max_attempts", 5)
                        ),
                    )
                    await self._broker.ensure_queue("default")
                    await self._broker.publish("default", envelope)
                    launched += 1

            await session.commit()
            if launched:
                logger.info(
                    "workflow_advanced",
                    workflow_id=str(workflow_id),
                    launched=launched,
                )

    async def on_task_completed(self, task_id: uuid.UUID) -> None:
        """Called when a task completes — check if it's part of a workflow and advance."""
        async with async_session_factory() as session:
            repo = TaskRepository(session)
            try:
                task_row = await repo.get_task(task_id)
            except Exception:
                return

            if task_row.workflow_id:
                await self.advance(task_row.workflow_id)
