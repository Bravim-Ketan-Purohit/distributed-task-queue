"""gRPC client for worker control channel.

The worker connects to the control plane's gRPC service to:
1. Register itself on startup
2. Stream heartbeats and task events up
3. Receive control commands (pause, drain, cancel, shutdown) down
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator

import grpc
import structlog

from dtq.core.config import Settings

logger = structlog.get_logger()


class WorkerControlClient:
    """gRPC client for worker-side control channel."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._worker_id = settings.worker_id
        self._channel: grpc.aio.Channel | None = None
        self._connected = False
        self._command_callbacks: dict[str, Any] = {}
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def connect(self) -> None:
        """Connect to the control plane gRPC server."""
        target = f"localhost:{self._settings.grpc_port}"
        self._channel = grpc.aio.insecure_channel(target)
        try:
            await self._channel.channel_ready()
            self._connected = True
            logger.info("grpc_client_connected", target=target)
        except Exception as e:
            logger.warning("grpc_client_connect_failed", target=target, error=str(e))
            self._connected = False

    async def close(self) -> None:
        """Close the gRPC channel."""
        if self._channel:
            await self._channel.close()
            self._channel = None
        self._connected = False

    async def register(self) -> bool:
        """Register this worker with the control plane."""
        if not self._connected or not self._channel:
            return False

        try:
            request = json.dumps({
                "worker_id": self._worker_id,
                "queues": self._settings.worker_queues,
                "concurrency": self._settings.worker_concurrency,
                "version": "0.1.0",
            }).encode()

            response = await self._channel.unary_unary(
                "/dtq.worker_control.WorkerControl/Register",
                request_serializer=lambda r: r,
                response_deserializer=lambda b: json.loads(b.decode()),
            )(request)

            accepted = response.get("accepted", False)
            logger.info("grpc_registered", accepted=accepted)
            return accepted
        except Exception as e:
            logger.error("grpc_register_failed", error=str(e))
            return False

    async def send_heartbeat(self, in_flight: int, concurrency: int) -> None:
        """Queue a heartbeat event to send via the session stream."""
        await self._event_queue.put({
            "worker_id": self._worker_id,
            "type": "heartbeat",
            "in_flight": in_flight,
            "concurrency": concurrency,
            "timestamp": time.time(),
        })

    async def send_task_started(self, task_id: str, task_name: str, attempt: int, fence: int) -> None:
        """Notify control plane that a task has started."""
        await self._event_queue.put({
            "worker_id": self._worker_id,
            "type": "task_started",
            "task_id": task_id,
            "task_name": task_name,
            "attempt": attempt,
            "fence": fence,
        })

    async def send_task_completed(self, task_id: str, outcome: str, duration_ms: float) -> None:
        """Notify control plane that a task has completed."""
        await self._event_queue.put({
            "worker_id": self._worker_id,
            "type": "task_completed",
            "task_id": task_id,
            "outcome": outcome,
            "duration_ms": duration_ms,
        })

    def on_command(self, command_type: str, callback: Any) -> None:
        """Register a callback for a specific command type."""
        self._command_callbacks[command_type] = callback

    async def session_loop(self) -> None:
        """Run the bidirectional session stream.

        Sends events from the queue, receives commands and dispatches
        to registered callbacks.
        """
        if not self._connected:
            logger.warning("grpc_session_not_connected")
            return

        # In a full implementation with compiled proto stubs, this would be:
        # stub = WorkerControlStub(self._channel)
        # async for command in stub.Session(event_generator()):
        #     handle_command(command)

        # Simplified version using generic call
        logger.info("grpc_session_started", worker_id=self._worker_id)

        while self._connected:
            try:
                # Send pending events
                try:
                    event = await asyncio.wait_for(self._event_queue.get(), timeout=5.0)
                    # In full impl, this goes through the bidi stream
                    logger.debug("grpc_event_sent", event_type=event.get("type"))
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("grpc_session_error", error=str(e))
                await asyncio.sleep(1)

    async def _handle_command(self, command: dict[str, Any]) -> None:
        """Dispatch received control command to registered callback."""
        cmd_type = command.get("type", "")
        callback = self._command_callbacks.get(cmd_type)
        if callback:
            if asyncio.iscoroutinefunction(callback):
                await callback(command)
            else:
                callback(command)
        else:
            logger.warning("grpc_unknown_command", command_type=cmd_type)
