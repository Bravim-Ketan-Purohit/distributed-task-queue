"""Prometheus metrics for the task queue.

Metrics exposed:
- Queue depth (gauge)
- Lease age (histogram)
- Reclaim count (counter)
- Retry count by attempt (counter)
- DLQ rate (counter)
- Worker heartbeat age (gauge)
- Task execution duration (histogram)
- Tasks in-flight (gauge)
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Info

# --- Counters ---

TASKS_ENQUEUED = Counter(
    "dtq_tasks_enqueued_total",
    "Total tasks enqueued",
    ["queue", "task_name"],
)

TASKS_COMPLETED = Counter(
    "dtq_tasks_completed_total",
    "Total tasks completed",
    ["queue", "task_name", "outcome"],
)

TASKS_RETRIED = Counter(
    "dtq_tasks_retried_total",
    "Total retry attempts",
    ["queue", "task_name", "attempt"],
)

TASKS_DEAD_LETTERED = Counter(
    "dtq_tasks_dead_lettered_total",
    "Total tasks sent to DLQ",
    ["queue", "task_name"],
)

TASKS_RECLAIMED = Counter(
    "dtq_tasks_reclaimed_total",
    "Total tasks reclaimed from dead workers",
    ["queue"],
)

LEASES_ACQUIRED = Counter(
    "dtq_leases_acquired_total",
    "Total lease acquisitions",
    ["worker_id"],
)

LEASES_REJECTED = Counter(
    "dtq_leases_rejected_total",
    "Total lease acquisition failures (zombie rejections)",
    ["worker_id", "reason"],
)

# --- Gauges ---

QUEUE_DEPTH = Gauge(
    "dtq_queue_depth",
    "Current queue depth",
    ["queue"],
)

TASKS_IN_FLIGHT = Gauge(
    "dtq_tasks_in_flight",
    "Tasks currently being processed",
    ["worker_id"],
)

WORKER_HEARTBEAT_AGE = Gauge(
    "dtq_worker_heartbeat_age_seconds",
    "Seconds since last heartbeat",
    ["worker_id"],
)

SCHEDULED_TASKS = Gauge(
    "dtq_scheduled_tasks",
    "Tasks waiting in the schedule ZSET",
    ["queue"],
)

# --- Histograms ---

TASK_DURATION = Histogram(
    "dtq_task_duration_seconds",
    "Task execution duration",
    ["queue", "task_name"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120],
)

LEASE_AGE = Histogram(
    "dtq_lease_age_seconds",
    "Age of leases at release time",
    ["queue"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)

TASK_LATENCY = Histogram(
    "dtq_task_end_to_end_latency_seconds",
    "End-to-end latency from enqueue to completion",
    ["queue", "task_name"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

# --- Info ---

WORKER_INFO = Info(
    "dtq_worker",
    "Worker metadata",
)


def init_metrics() -> None:
    """Initialize metrics (register custom collectors if needed)."""
    pass


def record_task_enqueued(queue: str, task_name: str) -> None:
    TASKS_ENQUEUED.labels(queue=queue, task_name=task_name).inc()


def record_task_completed(queue: str, task_name: str, outcome: str, duration_s: float) -> None:
    TASKS_COMPLETED.labels(queue=queue, task_name=task_name, outcome=outcome).inc()
    TASK_DURATION.labels(queue=queue, task_name=task_name).observe(duration_s)


def record_task_retried(queue: str, task_name: str, attempt: int) -> None:
    TASKS_RETRIED.labels(queue=queue, task_name=task_name, attempt=str(attempt)).inc()


def record_task_dead_lettered(queue: str, task_name: str) -> None:
    TASKS_DEAD_LETTERED.labels(queue=queue, task_name=task_name).inc()


def record_task_reclaimed(queue: str) -> None:
    TASKS_RECLAIMED.labels(queue=queue).inc()


def record_lease_acquired(worker_id: str) -> None:
    LEASES_ACQUIRED.labels(worker_id=worker_id).inc()


def record_lease_rejected(worker_id: str, reason: str) -> None:
    LEASES_REJECTED.labels(worker_id=worker_id, reason=reason).inc()


def set_queue_depth(queue: str, depth: int) -> None:
    QUEUE_DEPTH.labels(queue=queue).set(depth)


def set_in_flight(worker_id: str, count: int) -> None:
    TASKS_IN_FLIGHT.labels(worker_id=worker_id).set(count)
