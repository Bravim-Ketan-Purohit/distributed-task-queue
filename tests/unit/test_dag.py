"""Unit tests for DAG validation and cycle detection."""

from __future__ import annotations

import pytest

from dtq.core.exceptions import WorkflowCycleError
from dtq.workflow.dag import get_ready_steps, validate_dag


class TestValidateDAG:
    """DAG cycle detection — Kahn's algorithm."""

    def test_simple_linear_dag(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
            {"name": "c", "task": "t3", "depends_on": ["b"]},
        ]
        order = validate_dag(steps)
        assert order.index("a") < order.index("b") < order.index("c")

    def test_diamond_dag(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
            {"name": "c", "task": "t3", "depends_on": ["a"]},
            {"name": "d", "task": "t4", "depends_on": ["b", "c"]},
        ]
        order = validate_dag(steps)
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_simple_cycle_detected(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": ["b"]},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
        ]
        with pytest.raises(WorkflowCycleError):
            validate_dag(steps)

    def test_self_cycle(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": ["a"]},
        ]
        with pytest.raises(WorkflowCycleError):
            validate_dag(steps)

    def test_complex_cycle(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
            {"name": "c", "task": "t3", "depends_on": ["b"]},
            {"name": "d", "task": "t4", "depends_on": ["c", "b"]},
            {"name": "e", "task": "t5", "depends_on": ["d"]},
            {"name": "f", "task": "t6", "depends_on": ["e", "a"]},
            # This creates a cycle: b -> c -> d -> e -> ... if we add e -> b
        ]
        # No cycle in this one
        validate_dag(steps)

        # Add cycle
        steps_with_cycle = steps + [
            {"name": "g", "task": "t7", "depends_on": ["f"]},
        ]
        # Still no cycle
        validate_dag(steps_with_cycle)

        # Real cycle
        steps_cyclic = [
            {"name": "a", "task": "t1", "depends_on": ["c"]},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
            {"name": "c", "task": "t3", "depends_on": ["b"]},
        ]
        with pytest.raises(WorkflowCycleError):
            validate_dag(steps_cyclic)

    def test_no_deps_valid(self):
        steps = [
            {"name": "a", "task": "t1"},
            {"name": "b", "task": "t2"},
        ]
        order = validate_dag(steps)
        assert set(order) == {"a", "b"}

    def test_unknown_dependency_raises(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": ["nonexistent"]},
        ]
        with pytest.raises(WorkflowCycleError, match="unknown step"):
            validate_dag(steps)


class TestGetReadySteps:
    """Find steps whose dependencies are all satisfied."""

    def test_initial_steps_ready(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
        ]
        ready = get_ready_steps(steps, completed_steps=set())
        assert len(ready) == 1
        assert ready[0]["name"] == "a"

    def test_dependent_step_ready_after_deps(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": ["a"]},
            {"name": "c", "task": "t3", "depends_on": ["a", "b"]},
        ]
        ready = get_ready_steps(steps, completed_steps={"a"})
        assert len(ready) == 1
        assert ready[0]["name"] == "b"

    def test_multiple_ready(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": []},
            {"name": "c", "task": "t3", "depends_on": ["a", "b"]},
        ]
        ready = get_ready_steps(steps, completed_steps=set())
        names = {s["name"] for s in ready}
        assert names == {"a", "b"}

    def test_fan_in_waits_for_all(self):
        steps = [
            {"name": "a", "task": "t1", "depends_on": []},
            {"name": "b", "task": "t2", "depends_on": []},
            {"name": "c", "task": "t3", "depends_on": ["a", "b"]},
        ]
        # Only a completed
        ready = get_ready_steps(steps, completed_steps={"a"})
        names = {s["name"] for s in ready}
        assert "c" not in names
        assert "b" in names

        # Both completed
        ready = get_ready_steps(steps, completed_steps={"a", "b"})
        names = {s["name"] for s in ready}
        assert "c" in names
