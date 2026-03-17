from __future__ import annotations

import logging
from typing import Any

from .adaptive import RecoveryPolicy
from .agents import ApprovalAgent, TaskExecutionAgent
from .messaging import EventBus, InMemoryMessageQueue, RedisMessageQueue
from .observability import AgentTelemetry
from .orchestration import MultiAgentCoordinator, TaskScheduler
from .redis_support import (
    InMemoryCache,
    InMemoryRateLimiter,
    RedisCache,
    RedisRateLimiter,
    TaskCache,
    create_redis_client,
)

logger = logging.getLogger(__name__)


async def build_mas_runtime(
    *,
    settings: Any,
    task_handler: Any,
) -> MultiAgentCoordinator:
    backend = str(getattr(settings, "mas_message_backend", "memory") or "memory").lower()
    redis_url = str(getattr(settings, "redis_url", "redis://localhost:6379/0"))
    cache_ttl_s = int(getattr(settings, "mas_cache_ttl_s", 120))
    req_per_window = int(getattr(settings, "mas_rate_limit_requests", 30))
    window_seconds = int(getattr(settings, "mas_rate_limit_window_s", 60))
    max_attempts = int(getattr(settings, "mas_retry_max_attempts", 3))
    base_delay_s = float(getattr(settings, "mas_retry_base_delay_s", 1.0))
    max_delay_s = float(getattr(settings, "mas_retry_max_delay_s", 10.0))

    if backend == "redis":
        try:
            redis_client = create_redis_client(redis_url)
            await redis_client.ping()
            queue = RedisMessageQueue(redis_client)
            cache_backend = RedisCache(redis_client)
            limiter = RedisRateLimiter(redis_client)
        except Exception as exc:
            logger.warning("mas_redis_backend_unavailable fallback=memory error=%s", exc)
            queue = InMemoryMessageQueue()
            cache_backend = InMemoryCache()
            limiter = InMemoryRateLimiter()
    else:
        queue = InMemoryMessageQueue()
        cache_backend = InMemoryCache()
        limiter = InMemoryRateLimiter()

    bus = EventBus(queue)
    telemetry = AgentTelemetry()
    task_cache = TaskCache(cache_backend, ttl_s=cache_ttl_s)
    recovery = RecoveryPolicy(max_attempts=max_attempts, base_delay_s=base_delay_s, max_delay_s=max_delay_s)

    approval = ApprovalAgent(
        agent_id="approval_agent",
        event_bus=bus,
        execution_agent_id="execution_agent",
        telemetry=telemetry,
        cache=task_cache,
    )
    execution = TaskExecutionAgent(
        agent_id="execution_agent",
        event_bus=bus,
        task_handler=task_handler,
        recovery_policy=recovery,
        telemetry=telemetry,
        cache=task_cache,
        rate_limiter=limiter,
        requests_per_window=req_per_window,
        window_seconds=window_seconds,
    )
    scheduler = TaskScheduler()
    return MultiAgentCoordinator(
        scheduler=scheduler,
        event_bus=bus,
        approval_agent=approval,
        execution_agent=execution,
    )
