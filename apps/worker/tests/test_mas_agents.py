import unittest

from apps.worker.mas.adaptive import RecoveryPolicy
from apps.worker.mas.agents import ApprovalAgent, TaskExecutionAgent
from apps.worker.mas.messaging import EventBus, InMemoryMessageQueue
from apps.worker.mas.orchestration import MultiAgentCoordinator, TaskScheduler
from apps.worker.mas.redis_support import InMemoryCache, InMemoryRateLimiter, TaskCache


class ApprovalAgentDecisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_approval_agent_approves_when_budget_and_time_constraints_pass(self) -> None:
        bus = EventBus(InMemoryMessageQueue())
        agent = ApprovalAgent(agent_id="approval_agent", event_bus=bus, execution_agent_id="execution_agent")

        result = await agent.run_once(
            {
                "task": {
                    "task_id": "t-1",
                    "run_id": "r-1",
                    "budget": 100.0,
                    "estimated_cost": 65.0,
                    "estimated_minutes": 20,
                    "deadline_minutes": 30,
                    "status": "QUEUED",
                }
            }
        )

        self.assertEqual("APPROVED", result["result"]["status"])
        message = await bus.receive_message("execution_agent", timeout_s=0.0)
        self.assertIsNotNone(message)
        self.assertEqual("approval.granted", message.topic)

    async def test_approval_agent_rejects_when_constraints_fail(self) -> None:
        bus = EventBus(InMemoryMessageQueue())
        agent = ApprovalAgent(agent_id="approval_agent", event_bus=bus, execution_agent_id="execution_agent")

        result = await agent.run_once(
            {
                "task": {
                    "task_id": "t-2",
                    "run_id": "r-2",
                    "budget": 50.0,
                    "estimated_cost": 80.0,
                    "estimated_minutes": 45,
                    "deadline_minutes": 30,
                    "status": "QUEUED",
                }
            }
        )

        self.assertEqual("REJECTED", result["result"]["status"])
        message = await bus.receive_message("scheduler_agent", timeout_s=0.0)
        self.assertIsNotNone(message)
        self.assertEqual("approval.denied", message.topic)


class TaskExecutionAgentBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def test_execution_agent_retries_network_failure_then_succeeds(self) -> None:
        bus = EventBus(InMemoryMessageQueue())
        cache = TaskCache(InMemoryCache(), ttl_s=60)
        limiter = InMemoryRateLimiter()
        attempts = {"count": 0}
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        async def task_handler(task: dict) -> dict:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("network connection reset by peer")
            return {"ok": True, "task_id": task["task_id"]}

        agent = TaskExecutionAgent(
            agent_id="execution_agent",
            event_bus=bus,
            task_handler=task_handler,
            recovery_policy=RecoveryPolicy(max_attempts=3, base_delay_s=0.1, max_delay_s=1.0),
            cache=cache,
            rate_limiter=limiter,
            sleep_fn=fake_sleep,
        )

        result = await agent.run_once({"task": {"task_id": "t-3", "run_id": "r-3", "status": "APPROVED"}})

        self.assertEqual("SUCCEEDED", result["result"]["status"])
        self.assertEqual(2, result["result"]["attempt"])
        self.assertTrue(sleep_calls)

    async def test_execution_agent_requests_collaboration_on_non_retryable_failure(self) -> None:
        bus = EventBus(InMemoryMessageQueue())

        async def task_handler(_task: dict) -> dict:
            raise ValueError("validation failed for payload schema")

        agent = TaskExecutionAgent(
            agent_id="execution_agent",
            event_bus=bus,
            task_handler=task_handler,
            recovery_policy=RecoveryPolicy(max_attempts=2, base_delay_s=0.1, max_delay_s=1.0),
            rate_limiter=InMemoryRateLimiter(),
            cache=TaskCache(InMemoryCache(), ttl_s=60),
            sleep_fn=lambda _d: _noop_sleep(),
        )

        result = await agent.run_once({"task": {"task_id": "t-4", "run_id": "r-4", "status": "APPROVED"}})

        self.assertEqual("FAILED", result["result"]["status"])
        self.assertEqual("validation", result["result"]["failure_type"])
        first = await bus.receive_message("approval_agent", timeout_s=0.0)
        second = await bus.receive_message("approval_agent", timeout_s=0.0)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual("execution.assistance_requested", first.topic)
        self.assertEqual("execution.failed", second.topic)


class MultiAgentCollaborationTests(unittest.IsolatedAsyncioTestCase):
    async def test_coordinator_drives_approval_to_execution_success(self) -> None:
        bus = EventBus(InMemoryMessageQueue())
        scheduler = TaskScheduler()
        cache = TaskCache(InMemoryCache(), ttl_s=120)
        limiter = InMemoryRateLimiter()

        async def task_handler(task: dict) -> dict:
            return {"ok": True, "task_id": task["task_id"]}

        approval_agent = ApprovalAgent(
            agent_id="approval_agent",
            event_bus=bus,
            execution_agent_id="execution_agent",
            cache=cache,
        )
        execution_agent = TaskExecutionAgent(
            agent_id="execution_agent",
            event_bus=bus,
            task_handler=task_handler,
            cache=cache,
            rate_limiter=limiter,
        )
        coordinator = MultiAgentCoordinator(
            scheduler=scheduler,
            event_bus=bus,
            approval_agent=approval_agent,
            execution_agent=execution_agent,
        )

        coordinator.submit_task(
            {
                "task_id": "t-5",
                "run_id": "r-5",
                "status": "QUEUED",
                "priority": 9,
                "budget": 200.0,
                "estimated_cost": 80.0,
                "estimated_minutes": 10,
                "deadline_minutes": 30,
            }
        )
        summary = await coordinator.run_until_idle(max_cycles=10)

        self.assertEqual("SUCCEEDED", summary["tasks"]["t-5"]["status"])
        self.assertEqual(0, summary["pending"])


async def _noop_sleep() -> None:
    return None


if __name__ == "__main__":
    unittest.main()
