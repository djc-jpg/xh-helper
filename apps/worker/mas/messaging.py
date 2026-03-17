from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from typing import Any, Protocol

try:
    from prometheus_client import Gauge, REGISTRY
except ImportError:  # pragma: no cover - fallback for minimal test environments
    class _NoopGauge:
        def labels(self, **_kwargs):
            return self

        def set(self, _value: float) -> None:
            return None

    class _NoopRegistry:
        _names_to_collectors: dict[str, Any] = {}

    class Gauge(_NoopGauge):  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs) -> None:
            _ = (args, kwargs)

    REGISTRY = _NoopRegistry()  # type: ignore[assignment]


def _get_or_create_gauge(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Gauge:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Gauge):
        return existing
    try:
        return Gauge(name, documentation, labelnames=labelnames)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Gauge):
            return existing
        raise


message_queue_backlog = _get_or_create_gauge(
    "mas_message_queue_backlog",
    "MAS message backlog by receiver",
    labelnames=("receiver", "backend"),
)


def _observe_queue_backlog(*, receiver: str, backend: str, size: int) -> None:
    message_queue_backlog.labels(receiver=receiver, backend=backend).set(max(0.0, float(size)))


@dataclass(slots=True)
class AgentMessage:
    message_id: str
    topic: str
    sender: str
    receiver: str
    task_id: str | None
    run_id: str | None
    correlation_id: str
    payload: dict[str, Any]
    timestamp: float
    priority: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "AgentMessage":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = dict(json.loads(raw))
        if "timestamp" not in data:
            # Backward compatibility: historical payloads may only have `created_at`.
            created_at = data.pop("created_at", None)
            data["timestamp"] = float(created_at if created_at is not None else time.time())
        if "run_id" not in data:
            data["run_id"] = None
        allowed = {f.name for f in fields(cls)}
        normalized = {k: v for k, v in data.items() if k in allowed}
        return cls(**normalized)


class MessageQueue(Protocol):
    async def send_message(self, receiver: str, message: AgentMessage) -> None: ...

    async def receive_message(self, receiver: str, timeout_s: float = 0.0) -> AgentMessage | None: ...


class InMemoryMessageQueue:
    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[AgentMessage]] = defaultdict(asyncio.Queue)

    async def send_message(self, receiver: str, message: AgentMessage) -> None:
        queue = self._queues[receiver]
        queue.put_nowait(message)
        _observe_queue_backlog(receiver=receiver, backend="memory", size=queue.qsize())

    async def receive_message(self, receiver: str, timeout_s: float = 0.0) -> AgentMessage | None:
        queue = self._queues[receiver]
        if timeout_s <= 0:
            try:
                message = queue.get_nowait()
                _observe_queue_backlog(receiver=receiver, backend="memory", size=queue.qsize())
                return message
            except asyncio.QueueEmpty:
                _observe_queue_backlog(receiver=receiver, backend="memory", size=0)
                return None
        try:
            message = await asyncio.wait_for(queue.get(), timeout=timeout_s)
            _observe_queue_backlog(receiver=receiver, backend="memory", size=queue.qsize())
            return message
        except TimeoutError:
            _observe_queue_backlog(receiver=receiver, backend="memory", size=queue.qsize())
            return None


class RedisMessageQueue:
    """Redis LIST based queue for cross-process agent messaging."""

    def __init__(self, redis_client: Any, *, key_prefix: str = "mas:queue") -> None:
        self._redis = redis_client
        self._key_prefix = key_prefix.rstrip(":")

    def _queue_key(self, receiver: str) -> str:
        return f"{self._key_prefix}:{receiver}"

    async def _observe_backlog(self, receiver: str) -> None:
        try:
            size = await self._redis.llen(self._queue_key(receiver))
        except Exception:
            return
        _observe_queue_backlog(receiver=receiver, backend="redis", size=int(size))

    async def send_message(self, receiver: str, message: AgentMessage) -> None:
        await self._redis.rpush(self._queue_key(receiver), message.to_json())
        await self._observe_backlog(receiver)

    async def receive_message(self, receiver: str, timeout_s: float = 0.0) -> AgentMessage | None:
        queue_key = self._queue_key(receiver)
        if timeout_s > 0:
            result = await self._redis.blpop(queue_key, timeout=int(timeout_s))
            if not result:
                await self._observe_backlog(receiver)
                return None
            _key, payload = result
            await self._observe_backlog(receiver)
            return AgentMessage.from_json(payload)
        payload = await self._redis.lpop(queue_key)
        if payload is None:
            await self._observe_backlog(receiver)
            return None
        await self._observe_backlog(receiver)
        return AgentMessage.from_json(payload)


class EventBus:
    def __init__(self, queue_backend: MessageQueue, *, default_timeout_s: float = 0.1) -> None:
        self._queue_backend = queue_backend
        self._default_timeout_s = max(0.0, float(default_timeout_s))

    @staticmethod
    def _resolve_run_id(payload: dict[str, Any], run_id: str | None) -> str | None:
        if run_id:
            return str(run_id)
        direct = payload.get("run_id")
        if direct:
            return str(direct)
        nested_task = payload.get("task")
        if isinstance(nested_task, dict) and nested_task.get("run_id"):
            return str(nested_task["run_id"])
        return None

    async def send_message(
        self,
        *,
        sender: str,
        receiver: str,
        topic: str,
        payload: dict[str, Any],
        task_id: str | None = None,
        run_id: str | None = None,
        correlation_id: str | None = None,
        priority: int = 0,
    ) -> AgentMessage:
        if not topic or not str(topic).strip():
            raise ValueError("topic is required")
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        now = time.time()
        message = AgentMessage(
            message_id=str(uuid.uuid4()),
            topic=topic,
            sender=sender,
            receiver=receiver,
            task_id=task_id,
            run_id=self._resolve_run_id(payload, run_id),
            correlation_id=correlation_id or str(uuid.uuid4()),
            payload=payload,
            timestamp=now,
            priority=priority,
        )
        await self._queue_backend.send_message(receiver, message)
        return message

    async def receive_message(self, receiver: str, *, timeout_s: float | None = None) -> AgentMessage | None:
        wait_s = self._default_timeout_s if timeout_s is None else max(0.0, float(timeout_s))
        return await self._queue_backend.receive_message(receiver, timeout_s=wait_s)
