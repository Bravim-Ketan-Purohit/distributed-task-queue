"""Broker protocol — the interface all adapters must implement.

Writing three implementations against this interface forces the interesting
question: each broker gives a different crash-recovery primitive, and the
engine has to work with all three.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from dtq.core.models import TaskEnvelope


@dataclass(frozen=True, slots=True)
class Leased:
    """A task envelope that has been leased from the broker.

    Attributes:
        envelope: the full task envelope
        broker_msg_id: broker-specific message ID (stream entry ID, offset, delivery tag)
        idle_ms: how long the message was idle (for reclaimed messages)
    """

    envelope: TaskEnvelope
    broker_msg_id: str
    idle_ms: int = 0


@runtime_checkable
class Broker(Protocol):
    """Abstract broker interface.

    Each adapter maps these operations onto its native primitives:
    - Redis Streams: XADD/XREADGROUP/XACK/XAUTOCLAIM + ZSET scheduler
    - Kafka: produce/consume with consumer-group rebalance
    - RabbitMQ: basic_publish/basic_consume with ack/nack + TTL DLX
    """

    async def connect(self) -> None:
        """Establish connection to the broker."""
        ...

    async def close(self) -> None:
        """Close all connections."""
        ...

    async def ensure_queue(self, queue: str) -> None:
        """Create queue/topic/stream if it doesn't exist."""
        ...

    async def publish(self, queue: str, envelope: TaskEnvelope) -> None:
        """Publish a task to the queue."""
        ...

    async def lease(
        self, queue: str, group: str, consumer: str, count: int, block_ms: int = 2000
    ) -> list[Leased]:
        """Read and claim messages from the queue.

        Returns up to `count` messages. The broker considers them in-flight
        until ack() is called.
        """
        ...

    async def ack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Acknowledge successfully processed messages."""
        ...

    async def nack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Negative-acknowledge — return messages for redelivery."""
        ...

    async def reclaim(
        self, queue: str, group: str, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[Leased]:
        """Reclaim messages that have been idle too long (crash recovery).

        This is the mid-task crash recovery primitive:
        - Redis: XAUTOCLAIM
        - Kafka: consumer-group rebalance (handled externally)
        - RabbitMQ: messages redelivered on channel close
        """
        ...

    async def schedule(self, queue: str, envelope: TaskEnvelope, run_at: float) -> None:
        """Schedule a task for future delivery (backoff retries, delayed tasks)."""
        ...

    async def promote_scheduled(self, queue: str) -> int:
        """Move due scheduled tasks into the ready queue.

        Returns the number of tasks promoted. Called by the scheduler tick.
        """
        ...

    async def dead_letter(self, queue: str, envelope: TaskEnvelope, reason: str) -> None:
        """Move a task to the dead-letter queue after exhausting retries."""
        ...

    async def get_dlq_messages(self, queue: str, count: int = 50) -> list[Leased]:
        """Read messages from the dead-letter queue (admin/inspection)."""
        ...

    async def queue_depth(self, queue: str) -> int:
        """Return the number of pending messages in the queue."""
        ...

    async def in_flight_count(self, queue: str, group: str) -> int:
        """Return number of messages currently leased/in-flight."""
        ...
