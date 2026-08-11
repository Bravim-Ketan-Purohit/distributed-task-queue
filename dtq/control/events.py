"""SSE event bus for real-time state transitions.

The console connects to GET /v1/events and receives state_change events
in real-time. This is backed by an asyncio queue that the control plane
publishes to on every state transition.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from dtq.control.schemas import TaskEvent

# In-memory event bus — subscribers get an asyncio.Queue
_subscribers: set[asyncio.Queue[TaskEvent]] = set()


def publish_event(
    event_type: str,
    task_id: uuid.UUID,
    queue: str,
    state: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Publish an event to all connected SSE subscribers."""
    event = TaskEvent(
        event_type=event_type,
        task_id=task_id,
        queue=queue,
        state=state,
        timestamp=datetime.now(timezone.utc),
        metadata=metadata or {},
    )
    dead: set[asyncio.Queue[TaskEvent]] = set()
    for q in _subscribers:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.add(q)
    _subscribers -= dead


async def subscribe() -> AsyncGenerator[TaskEvent, None]:
    """Subscribe to the event stream. Yields TaskEvent instances."""
    q: asyncio.Queue[TaskEvent] = asyncio.Queue(maxsize=1000)
    _subscribers.add(q)
    try:
        while True:
            event = await q.get()
            yield event
    finally:
        _subscribers.discard(q)
