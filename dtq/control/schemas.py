"""Request/response schemas for the control plane API — Pydantic v2."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Task schemas ---


class CreateTaskRequest(BaseModel):
    queue: str
    task_name: str
    payload: dict[str, Any]
    dedup_key: str | None = None
    priority: int = 0
    run_at: datetime | None = None
    max_attempts: int = 5


class CreateTaskResponse(BaseModel):
    task_id: uuid.UUID
    deduplicated: bool


class AttemptInfo(BaseModel):
    attempt_no: int
    worker_id: str
    fence: int
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    error_type: str | None
    error_repr: str | None

    model_config = {"from_attributes": True}


class TaskDetailResponse(BaseModel):
    id: uuid.UUID
    queue: str
    task_name: str
    payload: dict[str, Any]
    state: str
    priority: int
    attempt: int
    max_attempts: int
    dedup_key: str | None
    run_at: datetime | None
    workflow_id: uuid.UUID | None
    step_name: str | None
    created_at: datetime
    updated_at: datetime
    attempts: list[AttemptInfo] = []

    model_config = {"from_attributes": True}


# --- Workflow schemas ---


class WorkflowStep(BaseModel):
    name: str
    task: str
    payload: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    fan_out: str | None = None
    retry: dict[str, Any] | None = None


class CreateWorkflowRequest(BaseModel):
    name: str
    steps: list[WorkflowStep]


class CreateWorkflowResponse(BaseModel):
    workflow_id: uuid.UUID


class WorkflowStepState(BaseModel):
    name: str
    task_name: str
    state: str
    task_id: uuid.UUID | None = None


class WorkflowDetailResponse(BaseModel):
    id: uuid.UUID
    name: str
    state: str
    created_at: datetime
    steps: list[WorkflowStepState] = []

    model_config = {"from_attributes": True}


# --- Queue schemas ---


class QueueInfoResponse(BaseModel):
    queue: str
    depth: int
    in_flight: int
    throughput: float = 0.0
    oldest_pending_age_s: float = 0.0
    error_rate: float = 0.0


# --- Worker schemas ---


class WorkerInfoResponse(BaseModel):
    worker_id: str
    queues: list[str]
    in_flight: int
    concurrency: int = 0
    heartbeat_age_s: float
    version: str = ""
    alive: bool = True


# --- DLQ schemas ---


class DLQTaskResponse(BaseModel):
    task_id: uuid.UUID
    queue: str
    task_name: str
    payload: dict[str, Any]
    attempt: int
    reason: str = ""


class RequeueRequest(BaseModel):
    task_ids: list[uuid.UUID]


# --- Chaos schemas (only with DTQ_ENABLE_CHAOS=1) ---


class ChaosKillWorkerRequest(BaseModel):
    worker_id: str


class ChaosPauseQueueRequest(BaseModel):
    queue: str
    duration_s: float = 30.0


class ChaosInjectFailureRequest(BaseModel):
    queue: str
    task_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    error_type: str = "InjectedError"


# --- SSE Event ---


class TaskEvent(BaseModel):
    event_type: str  # state_change, enqueue, complete, dead_letter, reclaim
    task_id: uuid.UUID
    queue: str
    state: str
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
