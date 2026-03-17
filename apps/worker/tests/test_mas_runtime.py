import unittest
from unittest.mock import patch

from apps.worker.mas.messaging import InMemoryMessageQueue
from apps.worker.mas.runtime import build_mas_runtime
from apps.worker.mas.redis_support import InMemoryRateLimiter


class _Settings:
    mas_message_backend = "redis"
    redis_url = "redis://redis:6379/0"
    mas_cache_ttl_s = 120
    mas_rate_limit_requests = 30
    mas_rate_limit_window_s = 60
    mas_retry_max_attempts = 3
    mas_retry_base_delay_s = 1.0
    mas_retry_max_delay_s = 10.0


class _BrokenRedisClient:
    async def ping(self) -> bool:
        raise RuntimeError("redis unavailable")


async def _task_handler(task: dict) -> dict:
    return {"ok": True, "task_id": task.get("task_id")}


class MasRuntimeFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_redis_unavailable_falls_back_to_memory_backend(self) -> None:
        with patch("apps.worker.mas.runtime.create_redis_client", return_value=_BrokenRedisClient()):
            coordinator = await build_mas_runtime(settings=_Settings(), task_handler=_task_handler)

        # Accessing protected member is intentional in tests to verify backend selection.
        self.assertIsInstance(coordinator.event_bus._queue_backend, InMemoryMessageQueue)  # type: ignore[attr-defined]
        self.assertIsInstance(coordinator.execution_agent.rate_limiter, InMemoryRateLimiter)


if __name__ == "__main__":
    unittest.main()
