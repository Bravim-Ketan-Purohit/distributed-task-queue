"""Unit tests for jittered exponential backoff.

Tests the maths and jitter bounds — no IO required.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st

from dtq.core.backoff import compute_delay, is_retryable
from dtq.core.models import RetryPolicy


class TestComputeDelay:
    """Backoff formula: delay = min(base * 2^(attempt-1), cap), then full jitter [0, delay]."""

    def test_first_attempt_bounded_by_base(self):
        policy = RetryPolicy(base_delay_s=1.0, cap_s=300.0)
        for _ in range(100):
            d = compute_delay(policy, attempt=1)
            assert 0 <= d <= 1.0

    def test_second_attempt_bounded_by_2x_base(self):
        policy = RetryPolicy(base_delay_s=1.0, cap_s=300.0)
        for _ in range(100):
            d = compute_delay(policy, attempt=2)
            assert 0 <= d <= 2.0

    def test_high_attempt_bounded_by_cap(self):
        policy = RetryPolicy(base_delay_s=1.0, cap_s=300.0)
        for _ in range(100):
            d = compute_delay(policy, attempt=100)
            assert 0 <= d <= 300.0

    def test_cap_respected_exactly(self):
        policy = RetryPolicy(base_delay_s=2.0, cap_s=10.0)
        # 2 * 2^9 = 1024, capped at 10
        for _ in range(100):
            d = compute_delay(policy, attempt=10)
            assert 0 <= d <= 10.0

    @given(
        base=st.floats(min_value=0.01, max_value=10.0),
        cap=st.floats(min_value=1.0, max_value=600.0),
        attempt=st.integers(min_value=1, max_value=20),
    )
    @hyp_settings(max_examples=200)
    def test_delay_always_non_negative_and_within_bounds(self, base: float, cap: float, attempt: int):
        """Property: delay is always in [0, min(base * 2^(attempt-1), cap)]."""
        policy = RetryPolicy(base_delay_s=base, cap_s=cap)
        d = compute_delay(policy, attempt=attempt)
        upper_bound = min(base * (2 ** (attempt - 1)), cap)
        assert 0 <= d <= upper_bound + 1e-9  # float tolerance

    def test_jitter_produces_variety(self):
        """Full jitter should produce different values across calls."""
        policy = RetryPolicy(base_delay_s=5.0, cap_s=300.0)
        delays = [compute_delay(policy, attempt=3) for _ in range(50)]
        # Not all the same
        assert len(set(delays)) > 1
        # All within bounds: [0, min(5*4, 300)] = [0, 20]
        for d in delays:
            assert 0 <= d <= 20.0


class TestIsRetryable:
    """Retryability rules per SPEC §6."""

    def test_max_attempts_reached(self):
        policy = RetryPolicy(max_attempts=5)
        assert not is_retryable(policy, "TimeoutError", attempt=5)
        assert not is_retryable(policy, "TimeoutError", attempt=6)

    def test_under_max_attempts(self):
        policy = RetryPolicy(max_attempts=5)
        assert is_retryable(policy, "TimeoutError", attempt=1)
        assert is_retryable(policy, "TimeoutError", attempt=4)

    def test_terminal_error_never_retried(self):
        policy = RetryPolicy(max_attempts=5, terminal_errors=["ValueError", "InvalidPayload"])
        assert not is_retryable(policy, "ValueError", attempt=1)
        assert not is_retryable(policy, "InvalidPayload", attempt=1)

    def test_retryable_whitelist(self):
        """If retryable_errors is non-empty, only those are retried."""
        policy = RetryPolicy(
            max_attempts=5,
            retryable_errors=["TimeoutError", "ConnectionError"],
        )
        assert is_retryable(policy, "TimeoutError", attempt=1)
        assert is_retryable(policy, "ConnectionError", attempt=1)
        assert not is_retryable(policy, "RuntimeError", attempt=1)

    def test_empty_retryable_list_retries_everything_not_terminal(self):
        policy = RetryPolicy(
            max_attempts=5,
            retryable_errors=[],
            terminal_errors=["ValueError"],
        )
        assert is_retryable(policy, "TimeoutError", attempt=1)
        assert is_retryable(policy, "RuntimeError", attempt=1)
        assert not is_retryable(policy, "ValueError", attempt=1)
