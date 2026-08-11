"""FastAPI control plane application.

Endpoints per SPEC §7:
  POST   /v1/tasks              → enqueue
  GET    /v1/tasks/{id}         → task + attempts
  DELETE /v1/tasks/{id}         → cancel
  POST   /v1/workflows          → create workflow
  GET    /v1/workflows/{id}     → DAG + per-step state
  GET    /v1/queues             → depth, in-flight, throughput
  GET    /v1/workers            → fleet with heartbeat ages
  GET    /v1/dlq/{queue}        → dead tasks
  POST   /v1/dlq/{queue}/requeue → re-enqueue
  GET    /metrics               → Prometheus
  GET    /v1/events             → SSE: state transitions
"""

from __future__ import annotations

import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from dtq.broker.factory import create_broker
from dtq.broker.redis_broker import RedisBroker
from dtq.control.events import publish_event, subscribe
from dtq.control.schemas import (
    ChaosInjectFailureRequest,
    ChaosKillWorkerRequest,
    ChaosPauseQueueRequest,
    CreateTaskRequest,
    CreateTaskResponse,
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    DLQTaskResponse,
    QueueInfoResponse,
    RequeueRequest,
    TaskDetailResponse,
    AttemptInfo,
    WorkerInfoResponse,
    WorkflowDetailResponse,
    WorkflowStepState,
)
from dtq.core.config import settings
from dtq.core.models import RetryPolicy, TaskEnvelope, TaskState
from dtq.store.database import async_session_factory, engine
from dtq.store.repository import TaskRepository

logger = structlog.get_logger()

# Module-level broker instance
_broker: RedisBroker | None = None


def get_broker() -> RedisBroker:
    if _broker is None:
        raise RuntimeError("Broker not initialized")
    return _broker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup/shutdown lifecycle."""
    global _broker
    _broker = create_broker(settings)  # type: ignore[assignment]
    await _broker.connect()
    logger.info("control_plane_started", port=settings.control_port)
    yield
    await _broker.close()
    await engine.dispose()
    logger.info("control_plane_stopped")


app = FastAPI(
    title="DTQ Control Plane",
    version="0.1.0",
    description="Distributed Task Queue & Workflow Engine",
    lifespan=lifespan,
)

# CORS for the web console
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:7200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Tasks
# =============================================================================


@app.post("/v1/tasks", response_model=CreateTaskResponse, status_code=201)
async def create_task(req: CreateTaskRequest) -> CreateTaskResponse:
    """Enqueue a task. Idempotent if dedup_key is provided."""
    task_id = uuid.uuid4()
    broker = get_broker()

    async with async_session_factory() as session:
        repo = TaskRepository(session)
        task_row, deduplicated = await repo.create_task(
            task_id=task_id,
            queue=req.queue,
            task_name=req.task_name,
            payload=req.payload,
            dedup_key=req.dedup_key,
            priority=req.priority,
            max_attempts=req.max_attempts,
            run_at=req.run_at,
        )
        await session.commit()

        if deduplicated:
            return CreateTaskResponse(task_id=task_row.id, deduplicated=True)

    # Publish to broker
    envelope = TaskEnvelope(
        task_id=task_row.id,
        queue=req.queue,
        task_name=req.task_name,
        payload=req.payload,
        max_attempts=req.max_attempts,
        priority=req.priority,
        dedup_key=req.dedup_key,
        retry_policy=RetryPolicy(max_attempts=req.max_attempts),
    )

    if req.run_at and req.run_at > datetime.now(timezone.utc):
        await broker.schedule(req.queue, envelope, req.run_at.timestamp())
    else:
        await broker.ensure_queue(req.queue)
        await broker.publish(req.queue, envelope)

    publish_event("enqueue", task_row.id, req.queue, "pending")
    return CreateTaskResponse(task_id=task_row.id, deduplicated=False)


@app.get("/v1/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(task_id: uuid.UUID) -> TaskDetailResponse:
    """Get task details with attempt history."""
    async with async_session_factory() as session:
        repo = TaskRepository(session)
        try:
            task_row = await repo.get_task(task_id)
        except Exception:
            raise HTTPException(status_code=404, detail="Task not found")
        attempts = await repo.get_attempts(task_id)

        return TaskDetailResponse(
            id=task_row.id,
            queue=task_row.queue,
            task_name=task_row.task_name,
            payload=task_row.payload,
            state=task_row.state,
            priority=task_row.priority,
            attempt=task_row.attempt,
            max_attempts=task_row.max_attempts,
            dedup_key=task_row.dedup_key,
            run_at=task_row.run_at,
            workflow_id=task_row.workflow_id,
            step_name=task_row.step_name,
            created_at=task_row.created_at,
            updated_at=task_row.updated_at,
            attempts=[
                AttemptInfo(
                    attempt_no=a.attempt_no,
                    worker_id=a.worker_id,
                    fence=a.fence,
                    started_at=a.started_at,
                    finished_at=a.finished_at,
                    outcome=a.outcome,
                    error_type=a.error_type,
                    error_repr=a.error_repr,
                )
                for a in attempts
            ],
        )


@app.delete("/v1/tasks/{task_id}", status_code=200)
async def cancel_task(task_id: uuid.UUID) -> dict[str, Any]:
    """Cancel a task if not yet leased."""
    async with async_session_factory() as session:
        repo = TaskRepository(session)
        cancelled = await repo.cancel_task(task_id)
        await session.commit()

    if not cancelled:
        raise HTTPException(status_code=409, detail="Task cannot be cancelled (already leased or completed)")

    publish_event("state_change", task_id, "", "cancelled")
    return {"task_id": str(task_id), "cancelled": True}


# =============================================================================
# Workflows
# =============================================================================


@app.post("/v1/workflows", response_model=CreateWorkflowResponse, status_code=201)
async def create_workflow(req: CreateWorkflowRequest) -> CreateWorkflowResponse:
    """Create and start a workflow DAG."""
    from dtq.workflow.engine import WorkflowEngine

    engine_inst = WorkflowEngine(get_broker())
    try:
        workflow_id = await engine_inst.submit(req.name, [s.model_dump() for s in req.steps])
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return CreateWorkflowResponse(workflow_id=workflow_id)


@app.get("/v1/workflows/{workflow_id}", response_model=WorkflowDetailResponse)
async def get_workflow(workflow_id: uuid.UUID) -> WorkflowDetailResponse:
    """Get workflow DAG state."""
    async with async_session_factory() as session:
        repo = TaskRepository(session)
        wf = await repo.get_workflow(workflow_id)
        if wf is None:
            raise HTTPException(status_code=404, detail="Workflow not found")

        tasks = await repo.get_tasks_for_workflow(workflow_id)
        step_map = {t.step_name: t for t in tasks if t.step_name}

        steps = []
        for step_spec in wf.spec.get("steps", []):
            task_row = step_map.get(step_spec["name"])
            steps.append(
                WorkflowStepState(
                    name=step_spec["name"],
                    task_name=step_spec["task"],
                    state=task_row.state if task_row else "pending",
                    task_id=task_row.id if task_row else None,
                )
            )

        return WorkflowDetailResponse(
            id=wf.id,
            name=wf.name,
            state=wf.state,
            created_at=wf.created_at,
            steps=steps,
        )


# =============================================================================
# Queues
# =============================================================================


@app.get("/v1/queues", response_model=list[QueueInfoResponse])
async def list_queues() -> list[QueueInfoResponse]:
    """Get depth and stats for all known queues."""
    broker = get_broker()
    # Discover queues from settings (could also scan Redis keys)
    queues = settings.worker_queues
    result = []
    for q in queues:
        depth = await broker.queue_depth(q)
        in_flight = await broker.in_flight_count(q, q)
        result.append(
            QueueInfoResponse(
                queue=q,
                depth=depth,
                in_flight=in_flight,
            )
        )
    return result


# =============================================================================
# Workers
# =============================================================================


@app.get("/v1/workers", response_model=list[WorkerInfoResponse])
async def list_workers() -> list[WorkerInfoResponse]:
    """Get fleet info with heartbeat ages."""
    broker = get_broker()
    client = broker.client
    # Scan for worker:* keys
    workers: list[WorkerInfoResponse] = []
    async for key in client.scan_iter(match="worker:*"):
        worker_id = key.replace("worker:", "")
        raw = await client.get(key)
        if raw:
            data = json.loads(raw)
            age = time.time() - data.get("timestamp", 0)
            workers.append(
                WorkerInfoResponse(
                    worker_id=worker_id,
                    queues=data.get("queues", []),
                    in_flight=data.get("in_flight", 0),
                    concurrency=data.get("concurrency", 0),
                    heartbeat_age_s=round(age, 1),
                    version=data.get("version", ""),
                    alive=age < settings.heartbeat_interval_s * 3,
                )
            )
    return workers


# =============================================================================
# DLQ
# =============================================================================


@app.get("/v1/dlq/{queue}", response_model=list[DLQTaskResponse])
async def get_dlq(queue: str) -> list[DLQTaskResponse]:
    """List dead-lettered tasks."""
    broker = get_broker()
    messages = await broker.get_dlq_messages(queue, count=50)
    return [
        DLQTaskResponse(
            task_id=m.envelope.task_id,
            queue=m.envelope.queue,
            task_name=m.envelope.task_name,
            payload=m.envelope.payload,
            attempt=m.envelope.attempt,
        )
        for m in messages
    ]


@app.post("/v1/dlq/{queue}/requeue", status_code=200)
async def requeue_dlq(queue: str, req: RequeueRequest) -> dict[str, Any]:
    """Re-enqueue dead-lettered tasks with attempt reset."""
    broker = get_broker()
    requeued = 0

    async with async_session_factory() as session:
        repo = TaskRepository(session)
        for task_id in req.task_ids:
            try:
                task_row = await repo.get_task(task_id)
                # Reset state and attempt
                await repo.update_task_state(task_id, TaskState.PENDING, attempt=0)

                envelope = TaskEnvelope(
                    task_id=task_id,
                    queue=queue,
                    task_name=task_row.task_name,
                    payload=task_row.payload,
                    max_attempts=task_row.max_attempts,
                    priority=task_row.priority,
                    dedup_key=None,  # Clear dedup on requeue
                    retry_policy=RetryPolicy(max_attempts=task_row.max_attempts),
                )
                await broker.publish(queue, envelope)
                requeued += 1
                publish_event("requeue", task_id, queue, "pending")
            except Exception as e:
                logger.error("requeue_failed", task_id=str(task_id), error=str(e))
        await session.commit()

    return {"requeued": requeued, "requested": len(req.task_ids)}


# =============================================================================
# SSE Events
# =============================================================================


@app.get("/v1/events")
async def event_stream(request: Request) -> EventSourceResponse:
    """Server-Sent Events stream for real-time state transitions."""

    async def generate():
        async for event in subscribe():
            if await request.is_disconnected():
                break
            yield {
                "event": event.event_type,
                "data": event.model_dump_json(),
            }

    return EventSourceResponse(generate())


# =============================================================================
# Metrics (Prometheus)
# =============================================================================


@app.get("/metrics")
async def prometheus_metrics() -> str:
    """Export Prometheus metrics."""
    from prometheus_client import generate_latest

    return generate_latest().decode("utf-8")


# =============================================================================
# Chaos endpoints (only with DTQ_ENABLE_CHAOS=1)
# =============================================================================

if settings.enable_chaos:

    @app.post("/v1/chaos/kill-worker")
    async def chaos_kill_worker(req: ChaosKillWorkerRequest) -> dict[str, str]:
        """Simulate killing a worker (removes its heartbeat key)."""
        broker = get_broker()
        await broker.client.delete(f"worker:{req.worker_id}")
        return {"status": "killed", "worker_id": req.worker_id}

    @app.post("/v1/chaos/pause-queue")
    async def chaos_pause_queue(req: ChaosPauseQueueRequest) -> dict[str, str]:
        """Pause a queue (consumers stop reading for duration)."""
        # In practice this would set a flag workers check
        await get_broker().client.set(
            f"pause:{req.queue}", "1", ex=int(req.duration_s)
        )
        return {"status": "paused", "queue": req.queue, "duration_s": req.duration_s}

    @app.post("/v1/chaos/inject-failure")
    async def chaos_inject_failure(req: ChaosInjectFailureRequest) -> dict[str, Any]:
        """Inject a task that will always fail."""
        task_id = uuid.uuid4()
        envelope = TaskEnvelope(
            task_id=task_id,
            queue=req.queue,
            task_name=req.task_name,
            payload={**req.payload, "_inject_error": req.error_type},
            max_attempts=1,
            retry_policy=RetryPolicy(max_attempts=1),
        )
        broker = get_broker()
        await broker.ensure_queue(req.queue)
        await broker.publish(req.queue, envelope)
        return {"task_id": str(task_id), "will_fail_with": req.error_type}


# =============================================================================
# Entry point
# =============================================================================


def run() -> None:
    """Run the control plane server."""
    uvicorn.run(
        "dtq.control.app:app",
        host=settings.control_host,
        port=settings.control_port,
        reload=True,
    )


if __name__ == "__main__":
    run()
