from __future__ import annotations

import json
import time
from typing import Any


def create_redis_client(redis_url: str, *, decode_responses: bool = True) -> Any:
    try:
        import redis.asyncio as redis  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on runtime installation
        raise RuntimeError("redis package is not installed; add redis to requirements") from exc
    return redis.from_url(redis_url, decode_responses=decode_responses)


class InMemoryCache:
    def __init__(self) -> None:
        self._entries: dict[str, tuple[float, dict[str, Any]]] = {}

    async def set_json(self, key: str, value: dict[str, Any], ttl_s: int) -> None:
        expire_at = time.time() + max(1, int(ttl_s))
        self._entries[key] = (expire_at, value)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        item = self._entries.get(key)
        if not item:
            return None
        expire_at, value = item
        if expire_at < time.time():
            self._entries.pop(key, None)
            return None
        return value


class RedisCache:
    def __init__(self, redis_client: Any, *, key_prefix: str = "mas:cache") -> None:
        self._redis = redis_client
        self._key_prefix = key_prefix.rstrip(":")

    def _key(self, key: str) -> str:
        return f"{self._key_prefix}:{key}"

    async def set_json(self, key: str, value: dict[str, Any], ttl_s: int) -> None:
        await self._redis.set(self._key(key), json.dumps(value, ensure_ascii=True), ex=max(1, int(ttl_s)))

    async def get_json(self, key: str) -> dict[str, Any] | None:
        raw = await self._redis.get(self._key(key))
        if raw is None:
            return None
        return json.loads(raw)


class TaskCache:
    def __init__(self, backend: Any, *, ttl_s: int = 120) -> None:
        self._backend = backend
        self._ttl_s = max(1, int(ttl_s))

    async def set_task_state(self, task_id: str, state: dict[str, Any]) -> None:
        await self._backend.set_json(f"task:{task_id}", state, self._ttl_s)

    async def get_task_state(self, task_id: str) -> dict[str, Any] | None:
        return await self._backend.get_json(f"task:{task_id}")


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._counters: dict[str, tuple[int, int]] = {}

    async def allow(self, *, agent_id: str, scope: str, limit: int, window_s: int) -> tuple[bool, int]:
        now_bucket = int(time.time()) // max(1, int(window_s))
        key = f"{agent_id}:{scope}:{now_bucket}"
        count, _bucket = self._counters.get(key, (0, now_bucket))
        count += 1
        self._counters[key] = (count, now_bucket)
        allowed = count <= limit
        remaining = max(int(limit) - count, 0)
        return allowed, remaining


class RedisRateLimiter:
    """Fixed-window distributed limiter based on INCR + EXPIRE."""

    def __init__(self, redis_client: Any, *, key_prefix: str = "mas:ratelimit") -> None:
        self._redis = redis_client
        self._key_prefix = key_prefix.rstrip(":")

    def _counter_key(self, *, agent_id: str, scope: str, window_s: int) -> str:
        bucket = int(time.time()) // max(1, int(window_s))
        return f"{self._key_prefix}:{agent_id}:{scope}:{bucket}"

    async def allow(self, *, agent_id: str, scope: str, limit: int, window_s: int) -> tuple[bool, int]:
        key = self._counter_key(agent_id=agent_id, scope=scope, window_s=window_s)
        current = int(await self._redis.incr(key))
        if current == 1:
            await self._redis.expire(key, max(1, int(window_s)))
        allowed = current <= int(limit)
        remaining = max(int(limit) - current, 0)
        return allowed, remaining
