"""Redis Streams broker adapter.

In-flight recovery: Pending Entries List + XAUTOCLAIM.
Ordering: per-stream.
Delayed delivery: ZSET + scheduler tick (built by us).

Every multi-step Redis mutation is a Lua script — read-modify-write across
round trips is a bug here.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from dtq.broker.base import Broker, Leased
from dtq.core.models import TaskEnvelope

_SCRIPTS_DIR = Path(__file__).parent / "scripts"


def _load_lua(name: str) -> str:
    return (_SCRIPTS_DIR / name).read_text()


class RedisBroker:
    """Redis Streams implementation of the Broker protocol."""

    def __init__(self, url: str = "redis://localhost:7202/0") -> None:
        self._url = url
        self._client: aioredis.Redis | None = None
        # Lua script SHA cache
        self._scripts: dict[str, Any] = {}

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("RedisBroker not connected — call connect() first")
        return self._client

    async def connect(self) -> None:
        self._client = aioredis.from_url(
            self._url,
            decode_responses=True,
            max_connections=50,
        )
        await self._client.ping()
        # Register Lua scripts
        for script_name in ("acquire_lease", "release_lease", "extend_lease", "promote_scheduled"):
            lua_src = _load_lua(f"{script_name}.lua")
            self._scripts[script_name] = self._client.register_script(lua_src)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def ensure_queue(self, queue: str) -> None:
        """Create stream and consumer group if they don't exist."""
        stream_key = f"q:{queue}"
        try:
            await self.client.xgroup_create(stream_key, queue, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def publish(self, queue: str, envelope: TaskEnvelope) -> None:
        stream_key = f"q:{queue}"
        data = envelope.model_dump_json()
        await self.client.xadd(stream_key, {"data": data})

    async def lease(
        self, queue: str, group: str, consumer: str, count: int, block_ms: int = 2000
    ) -> list[Leased]:
        stream_key = f"q:{queue}"
        try:
            results = await self.client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=count,
                block=block_ms,
            )
        except aioredis.ResponseError:
            return []

        if not results:
            return []

        leased: list[Leased] = []
        for _stream, messages in results:
            for msg_id, fields in messages:
                envelope = TaskEnvelope.model_validate_json(fields["data"])
                leased.append(Leased(envelope=envelope, broker_msg_id=msg_id))
        return leased

    async def ack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        if not msg_ids:
            return
        stream_key = f"q:{queue}"
        await self.client.xack(stream_key, group, *msg_ids)

    async def nack(self, queue: str, group: str, msg_ids: list[str]) -> None:
        """For Redis Streams, nack is implicit — just don't ack.

        The message stays in PEL and will be reclaimed by XAUTOCLAIM.
        """
        pass

    async def reclaim(
        self, queue: str, group: str, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[Leased]:
        """XAUTOCLAIM — adopt entries whose lease expired."""
        stream_key = f"q:{queue}"
        try:
            # XAUTOCLAIM returns: [start_id, [[msg_id, fields], ...], [deleted_ids]]
            result = await self.client.xautoclaim(
                name=stream_key,
                groupname=group,
                consumername=consumer,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count,
            )
        except (aioredis.ResponseError, TypeError):
            return []

        if not result or len(result) < 2:
            return []

        messages = result[1]
        leased: list[Leased] = []
        for msg_id, fields in messages:
            if not fields:
                continue
            envelope = TaskEnvelope.model_validate_json(fields["data"])
            leased.append(Leased(envelope=envelope, broker_msg_id=msg_id, idle_ms=min_idle_ms))
        return leased

    async def schedule(self, queue: str, envelope: TaskEnvelope, run_at: float) -> None:
        """Add to ZSET sched:<queue> scored by run_at (unix timestamp)."""
        zset_key = f"sched:{queue}"
        data = envelope.model_dump_json()
        await self.client.zadd(zset_key, {data: run_at})

    async def promote_scheduled(self, queue: str) -> int:
        """Lua: atomically move due entries from ZSET to STREAM."""
        zset_key = f"sched:{queue}"
        stream_key = f"q:{queue}"
        now = time.time()
        result = await self._scripts["promote_scheduled"](
            keys=[zset_key, stream_key],
            args=[str(now), "100"],
        )
        return int(result)

    async def dead_letter(self, queue: str, envelope: TaskEnvelope, reason: str) -> None:
        dlq_key = f"dlq:{queue}"
        data = envelope.model_dump_json()
        await self.client.xadd(dlq_key, {"data": data, "reason": reason})

    async def get_dlq_messages(self, queue: str, count: int = 50) -> list[Leased]:
        dlq_key = f"dlq:{queue}"
        messages = await self.client.xrange(dlq_key, count=count)
        result: list[Leased] = []
        for msg_id, fields in messages:
            envelope = TaskEnvelope.model_validate_json(fields["data"])
            result.append(Leased(envelope=envelope, broker_msg_id=msg_id))
        return result

    async def queue_depth(self, queue: str) -> int:
        stream_key = f"q:{queue}"
        try:
            info = await self.client.xinfo_stream(stream_key)
            return int(info.get("length", 0))
        except aioredis.ResponseError:
            return 0

    async def in_flight_count(self, queue: str, group: str) -> int:
        stream_key = f"q:{queue}"
        try:
            groups = await self.client.xinfo_groups(stream_key)
            for g in groups:
                if g.get("name") == group:
                    return int(g.get("pending", 0))
        except aioredis.ResponseError:
            pass
        return 0

    # --- Lease management (not part of Broker protocol, but used by the worker) ---

    async def acquire_lease(self, task_id: str, worker_id: str, lease_ms: int) -> int:
        """Atomically acquire a lease with a fencing token.

        Returns fence > 0 on success, 0 if already leased.
        """
        result = await self._scripts["acquire_lease"](
            keys=["fence:seq", f"lease:{task_id}"],
            args=[worker_id, str(lease_ms)],
        )
        return int(result)

    async def release_lease(self, task_id: str, worker_id: str, fence: int) -> bool:
        """Compare-and-delete: release only if we still own the lease."""
        expected = f"{worker_id}:{fence}"
        result = await self._scripts["release_lease"](
            keys=[f"lease:{task_id}"],
            args=[expected],
        )
        return bool(result)

    async def extend_lease(self, task_id: str, worker_id: str, fence: int, lease_ms: int) -> bool:
        """Heartbeat: extend TTL only if we still own it."""
        expected = f"{worker_id}:{fence}"
        result = await self._scripts["extend_lease"](
            keys=[f"lease:{task_id}"],
            args=[expected, str(lease_ms)],
        )
        return bool(result)

    async def set_worker_heartbeat(self, worker_id: str, data: dict[str, Any], ttl_ms: int) -> None:
        """Write per-worker presence key."""
        import json

        key = f"worker:{worker_id}"
        await self.client.set(key, json.dumps(data), px=ttl_ms)

    async def get_worker_heartbeat(self, worker_id: str) -> dict[str, Any] | None:
        """Read per-worker presence."""
        import json

        key = f"worker:{worker_id}"
        raw = await self.client.get(key)
        if raw:
            return json.loads(raw)
        return None
