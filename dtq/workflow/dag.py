"""DAG validation and topological operations.

The DAG is validated for cycles at submit time and rejected with 422.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from dtq.core.exceptions import WorkflowCycleError


def validate_dag(steps: list[dict[str, Any]]) -> list[str]:
    """Validate a workflow DAG for cycles using Kahn's algorithm (topological sort).

    Returns topological order if valid, raises WorkflowCycleError if cyclic.
    """
    # Build adjacency list
    graph: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {}
    all_names: set[str] = set()

    for step in steps:
        name = step["name"]
        all_names.add(name)
        in_degree.setdefault(name, 0)
        for dep in step.get("depends_on", []):
            graph[dep].append(name)
            in_degree[name] = in_degree.get(name, 0) + 1
            in_degree.setdefault(dep, 0)
            all_names.add(dep)

    # Check all dependencies reference known steps
    for step in steps:
        for dep in step.get("depends_on", []):
            step_names = {s["name"] for s in steps}
            if dep not in step_names:
                raise WorkflowCycleError(f"Step '{step['name']}' depends on unknown step '{dep}'")

    # Kahn's algorithm
    queue: deque[str] = deque()
    for name in all_names:
        if in_degree.get(name, 0) == 0:
            queue.append(name)

    topo_order: list[str] = []
    while queue:
        node = queue.popleft()
        topo_order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(topo_order) != len(all_names):
        # Cycle detected
        remaining = all_names - set(topo_order)
        raise WorkflowCycleError(
            f"Workflow DAG contains a cycle involving steps: {remaining}"
        )

    return topo_order


def get_ready_steps(
    steps: list[dict[str, Any]], completed_steps: set[str]
) -> list[dict[str, Any]]:
    """Find steps whose dependencies are all satisfied."""
    ready = []
    for step in steps:
        name = step["name"]
        if name in completed_steps:
            continue
        deps = step.get("depends_on", [])
        if all(d in completed_steps for d in deps):
            ready.append(step)
    return ready
