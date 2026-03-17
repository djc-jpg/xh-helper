from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any

from .agents import ApprovalAgent, TaskExecutionAgent
from .messaging import EventBus

STATUS_WEIGHT = {
    "FAILED_RETRYABLE": 0,
    "PENDING_APPROVAL": 1,
    "WAITING_HUMAN": 1,
    "QUEUED": 2,
    "APPROVED": 3,
    "RUNNING": 4,
    "SUCCEEDED": 99,
    "REJECTED": 99,
    "FAILED_FINAL": 99,
    "CANCELLED": 99,
}


@dataclass(order=True)
class _ScheduledItem:
    sort_key: tuple[int, int, int] = field(init=False, repr=False)
    status_weight: int
    neg_priority: int
    seq: int
    task: dict[str, Any] = field(compare=False)

    def __post_init__(self) -> None:
        self.sort_key = (self.status_weight, self.neg_priority, self.seq)


class TaskScheduler:
    def __init__(self) -> None:
        self._heap: list[_ScheduledItem] = []
        self._seq = 0

    def enqueue(self, task: dict[str, Any]) -> None:
        self._seq += 1
        status = str(task.get("status") or "QUEUED")
        weight = STATUS_WEIGHT.get(status, 50)
        priority = int(task.get("priority", 0))
        heapq.heappush(
            self._heap,
            _ScheduledItem(status_weight=weight, neg_priority=-priority, seq=self._seq, task=task),
        )

    def next_task(self) -> dict[str, Any] | None:
        if not self._heap:
            return None
        return heapq.heappop(self._heap).task

    def __len__(self) -> int:
        return len(self._heap)


class MultiAgentCoordinator:
    """Coordinates approval + execution agents for end-to-end task progression."""

    def __init__(
        self,
        *,
        scheduler: TaskScheduler,
        event_bus: EventBus,
        approval_agent: ApprovalAgent,
        execution_agent: TaskExecutionAgent,
    ) -> None:
        self.scheduler = scheduler
        self.event_bus = event_bus
        self.approval_agent = approval_agent
        self.execution_agent = execution_agent
        self.task_state: dict[str, dict[str, Any]] = {}

    def submit_task(self, task: dict[str, Any]) -> None:
        task_id = str(task.get("task_id") or "")
        if not task_id:
            raise ValueError("task_id is required")
        self.task_state[task_id] = dict(task)
        self.scheduler.enqueue(dict(task))

    async def process_next(self) -> dict[str, Any] | None:
        task = self.scheduler.next_task()
        if task is None:
            return None
        task_id = str(task["task_id"])
        status = str(task.get("status") or "QUEUED")

        if status in {"QUEUED", "PENDING_APPROVAL", "WAITING_HUMAN"}:
            result = await self.approval_agent.run_once({"task": task})
            final = result["result"]["status"]
            if final == "APPROVED":
                task["status"] = "APPROVED"
                self.scheduler.enqueue(task)
            else:
                task["status"] = "REJECTED"
            self.task_state[task_id] = task
            return {"agent": self.approval_agent.agent_id, "task": task, "result": result}

        if status in {"APPROVED", "FAILED_RETRYABLE"}:
            result = await self.execution_agent.run_once({"task": task})
            final = result["result"]["status"]
            if final == "SUCCEEDED":
                task["status"] = "SUCCEEDED"
            elif final == "FAILED":
                task["status"] = "PENDING_APPROVAL"
                task["failure_type"] = result["result"].get("failure_type")
                self.scheduler.enqueue(task)
            elif final == "THROTTLED":
                task["status"] = "FAILED_RETRYABLE"
                self.scheduler.enqueue(task)
            self.task_state[task_id] = task
            return {"agent": self.execution_agent.agent_id, "task": task, "result": result}

        self.task_state[task_id] = task
        return {"agent": "coordinator", "task": task, "result": {"status": "SKIPPED"}}

    async def pump_scheduler_messages(self, *, max_items: int = 100) -> int:
        processed = 0
        while processed < max_items:
            message = await self.event_bus.receive_message("scheduler_agent", timeout_s=0.0)
            if not message:
                break
            processed += 1
            task_id = str(message.payload.get("task_id") or message.task_id or "")
            if not task_id:
                continue
            task = self.task_state.get(task_id, {"task_id": task_id, "status": "QUEUED"})
            if message.topic == "approval.denied":
                task["status"] = "REJECTED"
            elif message.topic == "execution.succeeded":
                task["status"] = "SUCCEEDED"
            elif message.topic == "execution.throttled":
                task["status"] = "FAILED_RETRYABLE"
                self.scheduler.enqueue(task)
            self.task_state[task_id] = task
        return processed

    async def run_until_idle(self, *, max_cycles: int = 50) -> dict[str, Any]:
        cycles = 0
        while cycles < max_cycles:
            cycles += 1
            await self.pump_scheduler_messages(max_items=50)
            result = await self.process_next()
            if result is None:
                break
        return {"cycles": cycles, "pending": len(self.scheduler), "tasks": self.task_state}
