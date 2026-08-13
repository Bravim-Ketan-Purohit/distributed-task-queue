"""RabbitMQ broker adapter.

In-flight recovery: unacked messages redelivered on channel close.
Ordering: per-queue.
Delayed delivery: TTL + dead-letter exchange, or a delay plugin.

Exactly-once note: this adapter relies on the ENGINE's at-least-once delivery
plus idempotent commit with fencing tokens = exactly-once EFFECT.
RabbitMQ provides at-least-once via publisher confirms + manual ack.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from dtq.broker.base import Broker, Leased
from dtq.core.models import TaskEnvelope


class RabbitMQBroker:
    """RabbitMQ implementation of the Broker protocol.

    Uses quorum queues, publisher confirms, TTL + DLX for backoff.
    """

    def __init__(self, url: str = "amqp://dtq:dtq_dev@localhost:7217/") -> None:
        self._url = url
        self._connection: Any = None
        self._channel: Any = None
        self._prefetch_buffers: dict[str, asyncio.Queue[Leased]] = {}
        self._consumer_tags: dict[str, str] = {}

    async def connect(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()
            self._channel = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        self._prefetch_buffers.clear()
        self._consumer_tags.clear()

    async def ensure_queue(self, queue: str) -> None:
        """Declare quorum queue with DLX for dead letters."""
        import aio_pika

        # Main queue
        queue_name = f"dtq.{queue}"
        dlq_name = f"dtq.{queue}.dlq"
        delay_name = f"dtq.{queue}.delay"

        # Declare exchanges
        exchange = await self._channel.declare_exchange(
            f"dtq.{queue}.exchange", aio_pika.ExchangeType.DIRECT, durable=True
        )
        dlx = await self._channel.declare_exchange(
            f"dtq.{queue}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
        )
        delay_exchange = await self._channel.declare_exchange(
            f"dtq.{queue}.delay-exchange", aio_pika.ExchangeType.DIRECT, durable=True
        )

        # Main queue — dead letters go to DLX
        main_q = await self._channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": f"dtq.{queue}.dlx",
                "x-dead-letter-routing-key": dlq_name,
            },
        )
        await main_q.bind(exchange, routing_key=queue_name)

        # DLQ
        dead_q = await self._channel.declare_queue(dlq_name, durable=True, arguments={"x-queue-type": "quorum"})
        await dead_q.bind(dlx, routing_key=dlq_name)

        # Delay queue — messages with TTL, DLX back to main exchange
        delay_q = await self._channel.declare_queue(
            delay_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": f"dtq.{queue}.exchange",
                "x-dead-letter-routing-key": queue_name,
            },
        )
        await delay_q.bind(delay_exchange, routing_key=delay_name)

    async def publish(self, queue: str, envelope: TaskEnvelope) -> None:
        import aio_pika

        exchange_name = f"dtq.{queue}.exchange"
        queue_name = f"dtq.{queue}"
        exchange = await self._channel.get_exchange(exchange_name)
        data = envelope.model_dump_json()
        message = aio_pika.Message(
            body=data.encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(envelope.task_id),
        )
        await exchange.publish(message, routing_key=queue_name)

    async def lease(
        self, queue: str, group: str, consumer: str, count: int, block_ms: int = 2000
    ) -> list[Leased]:
        """Consume from RabbitMQ with manual ack."""
        queue_name = f"dtq.{queue}"

        # Set up consumer buffer if not exists
        if queue_name not in self._prefetch_buffers:
            self._prefetch_buffers[queue_name] = asyncio.Queue()
            rmq_queue = await self._channel.get_queue(queue_name)

            async def _on_message(message: Any) -> None:
                try:
                    envelope = TaskEnvelope.model_validate_json(message.body.decode("utf-8"))
                    leased = Leased(
                        envelope=envelope,
                        broker_msg_id=str(message.delivery_tag),
                    )
                    await self._prefetch_buffers[queue_name].put(leased)
                except Exception:
                    await message.nack(requeue=True)

            tag = await rmq_queue.consume(_on_message, consumer_tag=consumer)
            self._consumer_tags[queue_name] = tag

        # Drain up to count messages with timeout
        result: list[Leased] = []
        deadline = time.time() + block_ms / 1000
        while len(result) < count and time.time() < deadline:
            try:
                remaining = max(0.01, deadline - time.time())
                msg = await asyncio.wait_for(
                    self._prefetch_buffers[queue_name].get(), timeout=remaining
                )
                result.append(msg)
            except asyncio.TimeoutError:
                break
        return result

    async def ack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Acknowledge messages by delivery tag."""
        for tag_str in msg_ids:
            try:
                tag = int(tag_str)
                await self._channel.default_exchange.channel.basic_ack(tag)
            except Exception:
                pass

    async def nack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Negative ack — requeue messages."""
        for tag_str in msg_ids:
            try:
                tag = int(tag_str)
                await self._channel.default_exchange.channel.basic_nack(tag, requeue=True)
            except Exception:
                pass

    async def reclaim(
        self, queue: str, group: str, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[Leased]:
        """RabbitMQ handles redelivery automatically on channel close.

        When a consumer dies, unacked messages are redelivered to other
        consumers in the group. No explicit reclaim needed.
        """
        return []

    async def schedule(self, queue: str, envelope: TaskEnvelope, run_at: float) -> None:
        """Publish to delay queue with TTL (message expires and goes to main queue via DLX)."""
        import aio_pika

        delay_exchange_name = f"dtq.{queue}.delay-exchange"
        delay_queue_name = f"dtq.{queue}.delay"

        delay_ms = max(0, int((run_at - time.time()) * 1000))
        exchange = await self._channel.get_exchange(delay_exchange_name)
        data = envelope.model_dump_json()
        message = aio_pika.Message(
            body=data.encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=str(delay_ms),
            message_id=str(envelope.task_id),
        )
        await exchange.publish(message, routing_key=delay_queue_name)

    async def promote_scheduled(self, queue: str) -> int:
        """RabbitMQ handles promotion via TTL + DLX automatically.

        Messages in the delay queue expire based on their TTL and get
        routed to the main queue via the dead-letter exchange.
        """
        return 0

    async def dead_letter(self, queue: str, envelope: TaskEnvelope, reason: str) -> None:
        """Explicitly publish to DLQ."""
        import aio_pika

        dlx_name = f"dtq.{queue}.dlx"
        dlq_name = f"dtq.{queue}.dlq"

        dlx = await self._channel.get_exchange(dlx_name)
        data = json.dumps({"envelope": envelope.model_dump_json(), "reason": reason})
        message = aio_pika.Message(
            body=data.encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await dlx.publish(message, routing_key=dlq_name)

    async def get_dlq_messages(self, queue: str, count: int = 50) -> list[Leased]:
        """Read from the DLQ for inspection."""
        dlq_name = f"dtq.{queue}.dlq"
        dlq = await self._channel.get_queue(dlq_name)
        messages: list[Leased] = []

        for _ in range(count):
            msg = await dlq.get(fail=False)
            if msg is None:
                break
            try:
                body = msg.body.decode("utf-8")
                # Could be raw envelope or wrapped
                try:
                    record = json.loads(body)
                    if "envelope" in record:
                        envelope = TaskEnvelope.model_validate_json(record["envelope"])
                    else:
                        envelope = TaskEnvelope.model_validate_json(body)
                except (json.JSONDecodeError, KeyError):
                    envelope = TaskEnvelope.model_validate_json(body)
                messages.append(Leased(envelope=envelope, broker_msg_id=str(msg.delivery_tag)))
            except Exception:
                continue
            finally:
                # Don't ack — just peek
                await msg.nack(requeue=True)
        return messages

    async def queue_depth(self, queue: str) -> int:
        queue_name = f"dtq.{queue}"
        try:
            q = await self._channel.get_queue(queue_name)
            decl = await q.declare(passive=True)
            return decl.message_count
        except Exception:
            return 0

    async def in_flight_count(self, queue: str, group: str) -> int:
        queue_name = f"dtq.{queue}"
        try:
            q = await self._channel.get_queue(queue_name)
            decl = await q.declare(passive=True)
            return decl.consumer_count
        except Exception:
            return 0
