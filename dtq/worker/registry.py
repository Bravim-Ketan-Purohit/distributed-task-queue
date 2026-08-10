"""Task handler registry.

Task handlers are async callables registered by name. The worker looks up
the handler by task_name from the envelope and invokes it.
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Coroutine

# Type for a task handler: async (payload: dict) -> result: dict | None
TaskHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any] | None]]

_registry: dict[str, TaskHandler] = {}


def task(
    name: str | None = None,
    retryable_errors: list[str] | None = None,
    terminal_errors: list[str] | None = None,
    max_attempts: int | None = None,
):
    """Decorator to register a task handler.

    Usage:
        @task(name="send_email")
        async def send_email(payload: dict) -> dict | None:
            ...
    """

    def decorator(fn: TaskHandler) -> TaskHandler:
        task_name = name or fn.__name__
        _registry[task_name] = fn
        # Attach metadata
        fn._task_meta = {  # type: ignore[attr-defined]
            "name": task_name,
            "retryable_errors": retryable_errors or [],
            "terminal_errors": terminal_errors or [],
            "max_attempts": max_attempts,
        }
        return fn

    return decorator


def get_handler(task_name: str) -> TaskHandler | None:
    """Look up a registered task handler by name."""
    return _registry.get(task_name)


def list_handlers() -> dict[str, TaskHandler]:
    """Return all registered handlers."""
    return dict(_registry)


def clear_registry() -> None:
    """Clear all registered handlers (for testing)."""
    _registry.clear()
