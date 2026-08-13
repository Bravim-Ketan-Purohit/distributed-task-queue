"""Kafka broker adapter — KRaft mode, single broker.

In-flight recovery: consumer-group rebalance + committed offsets.
Ordering: per-partition.
Delayed delivery: not native — uses a delay topic + scheduler.

Exactly-once note: this adapter relies on the ENGINE's at-least-once delivery
plus idempotent commit with fencing tokens. It does NOT use Kafka's own EOS
(idempotent producer + transactions) — that is a different mechanism with a
different scope. This distinction matters in interviews.
"""

from __future__ import annotations

import json
import time
from typing import Any

from dtq.broker.base import Broker, Leased
from dtq.core.models import TaskEnvelope


class KafkaBroker:
    """Kafka implementation of the Broker protocol.

    Topics per queue, partitions as the parallelism unit,
    consumer groups for workers. Message values are JSON-encoded TaskEnvelopes.
    """

    def __init__(self, bootstrap_servers: str = "localhost:7216") -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: Any = None
        self._consumer: Any = None
        self._admin: Any = None
        self._subscribed_topics: set[str] = set()
        # Delayed tasks stored in a companion topic
        self._delay_consumers: dict[str, Any] = {}

    async def connect(self) -> None:
        from aiokafka import AIOKafkaProducer
        from aiokafka.admin import AIOKafkaAdminClient

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )
        await self._producer.start()

        self._admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await self._admin.start()

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        if self._admin:
            await self._admin.close()
            self._admin = None
        for c in self._delay_consumers.values():
            await c.stop()
        self._delay_consumers.clear()

    async def ensure_queue(self, queue: str) -> None:
        """Create Kafka topic for the queue if it doesn't exist."""
        from aiokafka.admin import NewTopic

        topic_name = f"dtq-{queue}"
        try:
            await self._admin.create_topics(
                [NewTopic(name=topic_name, num_partitions=4, replication_factor=1)]
            )
        except Exception:
            # Topic may already exist
            pass

        # Also create delay and DLQ topics
        for suffix in ("-delay", "-dlq"):
            try:
                await self._admin.create_topics(
                    [NewTopic(name=f"{topic_name}{suffix}", num_partitions=1, replication_factor=1)]
                )
            except Exception:
                pass

    async def _ensure_consumer(self, queue: str, group: str, consumer: str) -> Any:
        """Lazily create a consumer for the given queue."""
        from aiokafka import AIOKafkaConsumer

        topic_name = f"dtq-{queue}"
        if self._consumer is None or topic_name not in self._subscribed_topics:
            if self._consumer:
                await self._consumer.stop()
            self._consumer = AIOKafkaConsumer(
                topic_name,
                bootstrap_servers=self._bootstrap_servers,
                group_id=group,
                client_id=consumer,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v.decode("utf-8") if v else None,
            )
            await self._consumer.start()
            self._subscribed_topics = {topic_name}
        return self._consumer

    async def publish(self, queue: str, envelope: TaskEnvelope) -> None:
        topic_name = f"dtq-{queue}"
        data = envelope.model_dump_json()
        await self._producer.send_and_wait(
            topic_name,
            value=data.encode("utf-8"),
            key=str(envelope.task_id).encode("utf-8"),
        )

    async def lease(
        self, queue: str, group: str, consumer: str, count: int, block_ms: int = 2000
    ) -> list[Leased]:
        """Consume messages from Kafka — acts as lease via consumer group."""
        kafka_consumer = await self._ensure_consumer(queue, group, consumer)
        result = await kafka_consumer.getmany(timeout_ms=block_ms, max_records=count)

        leased: list[Leased] = []
        for _tp, messages in result.items():
            for msg in messages:
                try:
                    envelope = TaskEnvelope.model_validate_json(msg.value)
                    msg_id = f"{msg.partition}:{msg.offset}"
                    leased.append(Leased(envelope=envelope, broker_msg_id=msg_id))
                except Exception:
                    continue
        return leased

    async def ack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Commit offsets for processed messages."""
        if self._consumer:
            await self._consumer.commit()

    async def nack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """Kafka doesn't have nack — message will be redelivered on rebalance."""
        pass

    async def reclaim(
        self, queue: str, group: str, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[Leased]:
        """Kafka handles redelivery via consumer-group rebalance.

        Stale partitions are reassigned automatically when a consumer dies.
        This method is a no-op for Kafka — recovery is handled externally.
        """
        return []

    async def schedule(self, queue: str, envelope: TaskEnvelope, run_at: float) -> None:
        """Publish to the delay topic with run_at in headers."""
        topic_name = f"dtq-{queue}-delay"
        data = json.dumps({"envelope": envelope.model_dump_json(), "run_at": run_at})
        await self._producer.send_and_wait(
            topic_name,
            value=data.encode("utf-8"),
            key=str(envelope.task_id).encode("utf-8"),
        )

    async def promote_scheduled(self, queue: str) -> int:
        """Poll delay topic and re-publish due tasks to the main topic."""
        from aiokafka import AIOKafkaConsumer

        topic_name = f"dtq-{queue}-delay"
        if queue not in self._delay_consumers:
            consumer = AIOKafkaConsumer(
                topic_name,
                bootstrap_servers=self._bootstrap_servers,
                group_id=f"dtq-scheduler-{queue}",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v.decode("utf-8") if v else None,
            )
            await consumer.start()
            self._delay_consumers[queue] = consumer

        consumer = self._delay_consumers[queue]
        now = time.time()
        result = await consumer.getmany(timeout_ms=500, max_records=100)
        promoted = 0

        for _tp, messages in result.items():
            for msg in messages:
                try:
                    record = json.loads(msg.value)
                    run_at = record["run_at"]
                    if run_at <= now:
                        envelope = TaskEnvelope.model_validate_json(record["envelope"])
                        await self.publish(queue, envelope)
                        promoted += 1
                    else:
                        # Not yet due — in a real implementation we'd seek back
                        # For now, republish to delay topic
                        await self._producer.send_and_wait(
                            topic_name,
                            value=msg.value.encode("utf-8") if isinstance(msg.value, str) else msg.value,
                            key=msg.key,
                        )
                except Exception:
                    continue

        await consumer.commit()
        return promoted

    async def dead_letter(self, queue: str, envelope: TaskEnvelope, reason: str) -> None:
        topic_name = f"dtq-{queue}-dlq"
        data = json.dumps({"envelope": envelope.model_dump_json(), "reason": reason})
        await self._producer.send_and_wait(
            topic_name,
            value=data.encode("utf-8"),
            key=str(envelope.task_id).encode("utf-8"),
        )

    async def get_dlq_messages(self, queue: str, count: int = 50) -> list[Leased]:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        topic_name = f"dtq-{queue}-dlq"
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=self._bootstrap_servers,
            group_id=f"dtq-dlq-reader-{queue}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: v.decode("utf-8") if v else None,
        )
        await consumer.start()
        try:
            result = await consumer.getmany(timeout_ms=1000, max_records=count)
            messages: list[Leased] = []
            for _tp, msgs in result.items():
                for msg in msgs:
                    try:
                        record = json.loads(msg.value)
                        envelope = TaskEnvelope.model_validate_json(record["envelope"])
                        msg_id = f"{msg.partition}:{msg.offset}"
                        messages.append(Leased(envelope=envelope, broker_msg_id=msg_id))
                    except Exception:
                        continue
            return messages
        finally:
            await consumer.stop()

    async def queue_depth(self, queue: str) -> int:
        """Approximate queue depth via end offsets - committed offsets."""
        # This is a rough estimate for Kafka
        return 0  # Would need admin client to calculate properly

    async def in_flight_count(self, queue: str, group: str) -> int:
        """Not directly available in Kafka without admin queries."""
        return 0
