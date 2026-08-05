"""Core domain models — Pydantic v2 throughout."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class TaskState(str, enum.Enum):
    """Lifecycle states for a task."""

    PENDING = "pending"
    SCHEDULED = "scheduled"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"


class AttemptOutcome(str, enum.Enum):
    """How an attempt concluded."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"
    TIMEOUT = "timeout"


class RetryPolicy(BaseModel):
    """Jittered exponential backoff configuration."""

    base_delay_s: float = 1.0
    cap_s: float = 300.0
    max_attempts: int = 5
    retryable_errors: list[str] = Field(default_factory=list)
    terminal_errors: list[str] = Field(default_factory=list)


class Task(BaseModel):
    """A task record as persisted in Postgres."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    queue: str
    task_name: str
    payload: dict[str, Any]
    state: TaskState = TaskState.PENDING
    priority: int = 0
    attempt: int = 0
    max_attempts: int = 5
    dedup_key: str | None = None
    run_at: datetime | None = None
    workflow_id: uuid.UUID | None = None
    step_name: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"from_attributes": True}


class Attempt(BaseModel):
    """A single execution attempt of a task."""

    id: int | None = None
    task_id: uuid.UUID
    attempt_no: int
    worker_id: str
    fence: int
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    outcome: AttemptOutcome | None = None
    error_type: str | None = None
    error_repr: str | None = None

    model_config = {"from_attributes": True}


class TaskEnvelope(BaseModel):
    """Wire-format envelope carried through the broker.

    Contains everything a worker needs to execute without a Postgres round-trip.
    Trace context (W3C traceparent/tracestate) propagates inside this envelope.
    """

    task_id: uuid.UUID
    queue: str
    task_name: str
    payload: dict[str, Any]
    attempt: int = 0
    max_attempts: int = 5
    priority: int = 0
    dedup_key: str | None = None
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    workflow_id: uuid.UUID | None = None
    step_name: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    # OTel W3C trace propagation
    traceparent: str | None = None
    tracestate: str | None = None
