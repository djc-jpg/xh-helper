from __future__ import annotations

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from .adaptive import RecoveryPolicy, classify_failure_type
from .messaging import EventBus
from .observability import AgentTelemetry

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Base class for all agents: perceive -> decide -> execute -> communicate."""

    def __init__(
        self,
        *,
        agent_id: str,
        event_bus: EventBus,
        telemetry: AgentTelemetry | None = None,
        cache: Any | None = None,
        rate_limiter: Any | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.event_bus = event_bus
        self.telemetry = telemetry or AgentTelemetry()
        self.cache = cache
        self.rate_limiter = rate_limiter

    @abstractmethod
    async def perceive(self, context: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def decide(self, perception: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def execute(self, decision: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def communicate(
        self,
        *,
        receiver: str,
        topic: str,
        payload: dict[str, Any],
        task_id: str | None = None,
        run_id: str | None = None,
        correlation_id: str | None = None,
        priority: int = 0,
    ) -> Any:
        return await self.event_bus.send_message(
            sender=self.agent_id,
            receiver=receiver,
            topic=topic,
            payload=payload,
            task_id=task_id,
            run_id=run_id,
            correlation_id=correlation_id,
            priority=priority,
        )

    async def run_once(self, context: dict[str, Any]) -> dict[str, Any]:
        self.telemetry.inc_inflight(agent_id=self.agent_id)
        try:
            with self.telemetry.span(agent_id=self.agent_id, step="perceive"):
                perception = await self.perceive(context)
            with self.telemetry.span(agent_id=self.agent_id, step="decide"):
                decision = await self.decide(perception)
            with self.telemetry.span(agent_id=self.agent_id, step="execute"):
                result = await self.execute(decision)
            return {"perception": perception, "decision": decision, "result": result}
        finally:
            self.telemetry.dec_inflight(agent_id=self.agent_id)


class ApprovalAgent(BaseAgent):
    def __init__(
        self,
        *,
        agent_id: str,
        event_bus: EventBus,
        execution_agent_id: str = "execution_agent",
        telemetry: AgentTelemetry | None = None,
        cache: Any | None = None,
        rate_limiter: Any | None = None,
    ) -> None:
        super().__init__(
            agent_id=agent_id,
            event_bus=event_bus,
            telemetry=telemetry,
            cache=cache,
            rate_limiter=rate_limiter,
        )
        self.execution_agent_id = execution_agent_id

    async def perceive(self, context: dict[str, Any]) -> dict[str, Any]:
        task = dict(context.get("task") or {})
        task_id = str(task.get("task_id") or "")
        if not task and self.cache and task_id:
            cached = await self.cache.get_task_state(task_id)
            if cached:
                task = dict(cached)
        if not task:
            raise ValueError("approval agent requires task context")
        return {"task": task, "task_id": str(task.get("task_id") or task_id)}

    async def decide(self, perception: dict[str, Any]) -> dict[str, Any]:
        task = perception["task"]
        budget = float(task.get("budget", 0.0))
        estimated_cost = float(task.get("estimated_cost", task.get("expected_cost", 0.0)))
        estimated_minutes = int(task.get("estimated_minutes", task.get("eta_minutes", 0)))
        deadline_minutes = int(task.get("deadline_minutes", 0))

        budget_ok = estimated_cost <= budget
        time_ok = True if deadline_minutes <= 0 else estimated_minutes <= deadline_minutes
        decision = "APPROVED" if budget_ok and time_ok else "REJECTED"
        reason = "constraints_passed" if decision == "APPROVED" else "budget_or_time_not_met"
        self.telemetry.record_decision(agent_id=self.agent_id, decision=decision)

        return {
            "decision": decision,
            "reason": reason,
            "budget_ok": budget_ok,
            "time_ok": time_ok,
            "task": task,
            "task_id": str(task.get("task_id") or perception.get("task_id") or ""),
            "correlation_id": str(task.get("run_id") or task.get("task_id") or ""),
        }

    async def execute(self, decision: dict[str, Any]) -> dict[str, Any]:
        task_id = decision["task_id"]
        run_id = str((decision.get("task") or {}).get("run_id") or "") or None
        decision_value = decision["decision"]
        payload = {
            "task_id": task_id,
            "decision": decision_value,
            "reason": decision["reason"],
            "budget_ok": decision["budget_ok"],
            "time_ok": decision["time_ok"],
        }

        if decision_value == "APPROVED":
            await self.communicate(
                receiver=self.execution_agent_id,
                topic="approval.granted",
                payload={**payload, "task": decision["task"]},
                task_id=task_id,
                run_id=run_id,
                correlation_id=decision["correlation_id"],
                priority=8,
            )
            self.telemetry.record_execution(agent_id=self.agent_id, outcome="approved")
            return {"status": "APPROVED", "task_id": task_id}

        await self.communicate(
            receiver="scheduler_agent",
            topic="approval.denied",
            payload=payload,
            task_id=task_id,
            run_id=run_id,
            correlation_id=decision["correlation_id"],
            priority=9,
        )
        self.telemetry.record_execution(agent_id=self.agent_id, outcome="rejected")
        return {"status": "REJECTED", "task_id": task_id}


TaskHandler = Callable[[dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]]


class TaskExecutionAgent(BaseAgent):
    def __init__(
        self,
        *,
        agent_id: str,
        event_bus: EventBus,
        task_handler: TaskHandler,
        recovery_policy: RecoveryPolicy | None = None,
        telemetry: AgentTelemetry | None = None,
        cache: Any | None = None,
        rate_limiter: Any | None = None,
        requests_per_window: int = 30,
        window_seconds: int = 60,
        sleep_fn: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(
            agent_id=agent_id,
            event_bus=event_bus,
            telemetry=telemetry,
            cache=cache,
            rate_limiter=rate_limiter,
        )
        self.task_handler = task_handler
        self.recovery_policy = recovery_policy or RecoveryPolicy(max_attempts=3, base_delay_s=1.0, max_delay_s=8.0)
        self.requests_per_window = max(1, int(requests_per_window))
        self.window_seconds = max(1, int(window_seconds))
        self.sleep_fn = sleep_fn or asyncio.sleep

    async def perceive(self, context: dict[str, Any]) -> dict[str, Any]:
        task = dict(context.get("task") or {})
        if not task:
            raise ValueError("execution agent requires task context")
        task_id = str(task.get("task_id") or "")
        rate_allowed, remaining = True, self.requests_per_window
        if self.rate_limiter is not None:
            scope = str(task.get("task_type") or "default")
            rate_allowed, remaining = await self.rate_limiter.allow(
                agent_id=self.agent_id,
                scope=scope,
                limit=self.requests_per_window,
                window_s=self.window_seconds,
            )
        return {
            "task": task,
            "task_id": task_id,
            "rate_allowed": rate_allowed,
            "remaining_quota": remaining,
            "correlation_id": str(task.get("run_id") or task_id),
        }

    async def decide(self, perception: dict[str, Any]) -> dict[str, Any]:
        task = perception["task"]
        if not perception["rate_allowed"]:
            decision = {
                "action": "THROTTLE",
                "reason": "rate_limit_exceeded",
                "task": task,
                "task_id": perception["task_id"],
                "correlation_id": perception["correlation_id"],
            }
            self.telemetry.record_decision(agent_id=self.agent_id, decision="THROTTLE")
            return decision

        status = str(task.get("status") or "APPROVED")
        if status in {"APPROVED", "READY", "QUEUED", "FAILED_RETRYABLE"}:
            decision = {
                "action": "EXECUTE",
                "task": task,
                "task_id": perception["task_id"],
                "correlation_id": perception["correlation_id"],
            }
            self.telemetry.record_decision(agent_id=self.agent_id, decision="EXECUTE")
            return decision

        decision = {
            "action": "SKIP",
            "reason": f"state_not_executable:{status}",
            "task": task,
            "task_id": perception["task_id"],
            "correlation_id": perception["correlation_id"],
        }
        self.telemetry.record_decision(agent_id=self.agent_id, decision="SKIP")
        return decision

    async def execute(self, decision: dict[str, Any]) -> dict[str, Any]:
        action = decision["action"]
        task = decision["task"]
        task_id = decision["task_id"]
        run_id = str(task.get("run_id") or "") or None
        correlation_id = decision["correlation_id"]

        if action == "THROTTLE":
            await self.communicate(
                receiver="scheduler_agent",
                topic="execution.throttled",
                payload={"task_id": task_id, "reason": decision["reason"]},
                task_id=task_id,
                run_id=run_id,
                correlation_id=correlation_id,
                priority=10,
            )
            self.telemetry.record_execution(agent_id=self.agent_id, outcome="throttled")
            return {"status": "THROTTLED", "task_id": task_id}

        if action == "SKIP":
            self.telemetry.record_execution(agent_id=self.agent_id, outcome="skipped")
            return {"status": "SKIPPED", "task_id": task_id, "reason": decision["reason"]}

        max_attempts = self.recovery_policy.max_attempts
        for attempt in range(1, max_attempts + 1):
            try:
                result = self.task_handler(task)
                if inspect.isawaitable(result):
                    result = await result
                if self.cache is not None:
                    await self.cache.set_task_state(task_id, {**task, "status": "SUCCEEDED"})
                await self.communicate(
                    receiver="scheduler_agent",
                    topic="execution.succeeded",
                    payload={"task_id": task_id, "attempt": attempt, "result": result},
                    task_id=task_id,
                    run_id=run_id,
                    correlation_id=correlation_id,
                    priority=8,
                )
                self.telemetry.record_execution(agent_id=self.agent_id, outcome="succeeded")
                return {
                    "status": "SUCCEEDED",
                    "task_id": task_id,
                    "attempt": attempt,
                    "result": result,
                    "next_action": "PROCESS_NEXT_TASK",
                }
            except Exception as exc:
                failure_type = classify_failure_type(exc)
                recovery = self.recovery_policy.decide(failure_type=failure_type, attempt=attempt)
                self.telemetry.record_retry(agent_id=self.agent_id, failure_type=failure_type.value)
                logger.warning(
                    "execution_failed agent=%s task_id=%s attempt=%d type=%s error=%s",
                    self.agent_id,
                    task_id,
                    attempt,
                    failure_type.value,
                    exc,
                )
                if recovery.request_collaboration and recovery.collaborator:
                    await self.communicate(
                        receiver=recovery.collaborator,
                        topic="execution.assistance_requested",
                        payload={
                            "task_id": task_id,
                            "attempt": attempt,
                            "failure_type": failure_type.value,
                            "reason": recovery.reason,
                        },
                        task_id=task_id,
                        run_id=run_id,
                        correlation_id=correlation_id,
                        priority=10,
                    )
                if recovery.should_retry:
                    await self.sleep_fn(recovery.delay_s)
                    continue

                await self.communicate(
                    receiver="approval_agent",
                    topic="execution.failed",
                    payload={
                        "task_id": task_id,
                        "attempt": attempt,
                        "failure_type": failure_type.value,
                        "error": str(exc),
                        "reason": recovery.reason,
                        "request_reapproval": True,
                    },
                    task_id=task_id,
                    run_id=run_id,
                    correlation_id=correlation_id,
                    priority=10,
                )
                self.telemetry.record_execution(agent_id=self.agent_id, outcome="failed")
                return {
                    "status": "FAILED",
                    "task_id": task_id,
                    "attempt": attempt,
                    "failure_type": failure_type.value,
                    "recovery_reason": recovery.reason,
                }

        self.telemetry.record_execution(agent_id=self.agent_id, outcome="failed")
        return {"status": "FAILED", "task_id": task_id, "recovery_reason": "attempts_exhausted"}
