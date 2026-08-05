"""Exception hierarchy for DTQ.

Exception classes declare retryability. A ValueError from bad payload is terminal
on attempt 1; a socket timeout retries. Retrying non-retryable errors five times
is a bug, not caution.
"""

from __future__ import annotations


class DTQError(Exception):
    """Base exception for all DTQ errors."""

    retryable: bool = True


class TaskNotFoundError(DTQError):
    """Task does not exist in the store."""

    retryable = False


class DuplicateTaskError(DTQError):
    """Task with same dedup_key already exists."""

    retryable = False


class FenceRejectedError(DTQError):
    """A higher fence token exists — this worker is a zombie.

    This is the core mechanism: a worker that stalls past its lease, loses it
    to a reclaimer, then wakes up and tries to commit is rejected here.
    """

    retryable = False


class LeaseAcquireError(DTQError):
    """Could not acquire the lease for a task."""

    retryable = True


class LeaseExpiredError(DTQError):
    """Lease expired before the worker could commit."""

    retryable = True


class BrokerError(DTQError):
    """Error communicating with the message broker."""

    retryable = True


class StoreError(DTQError):
    """Error communicating with the persistence store."""

    retryable = True


class WorkflowCycleError(DTQError):
    """Workflow DAG contains a cycle."""

    retryable = False


class TaskTerminalError(DTQError):
    """Task failed with a non-retryable error (e.g. bad payload)."""

    retryable = False
