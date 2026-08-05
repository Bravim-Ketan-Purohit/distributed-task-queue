"""Jittered exponential backoff — full jitter per SPEC §6."""

from __future__ import annotations

import random

from dtq.core.models import RetryPolicy


def compute_delay(policy: RetryPolicy, attempt: int) -> float:
    """Compute retry delay with full jitter.

    delay = min(base * 2^(attempt-1), cap)
    jittered = random(0, delay)

    This is "full jitter" — the entire range [0, delay] is valid.
    """
    exponential = policy.base_delay_s * (2 ** (attempt - 1))
    capped = min(exponential, policy.cap_s)
    return random.uniform(0, capped)


def is_retryable(
    policy: RetryPolicy, error_type: str, attempt: int
) -> bool:
    """Determine if an error should be retried.

    Rules:
    - If attempt >= max_attempts, never retry (goes to DLQ).
    - If error_type is in terminal_errors, never retry.
    - If retryable_errors is non-empty, only those are retried.
    - If retryable_errors is empty, everything not terminal is retried.
    """
    if attempt >= policy.max_attempts:
        return False
    if error_type in policy.terminal_errors:
        return False
    if policy.retryable_errors:
        return error_type in policy.retryable_errors
    return True
