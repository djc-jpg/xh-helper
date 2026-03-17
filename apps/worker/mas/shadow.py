from __future__ import annotations

import asyncio
from typing import Any

from .adaptive import RecoveryPolicy
from .agents import ApprovalAgent, TaskExecutionAgent
from .messaging import EventBus, InMemoryMessageQueue


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _has_force_flag(input_payload: dict[str, Any], marker: str) -> bool:
    if bool(input_payload.get(marker)):
        return True
    for key in ("query", "question", "action"):
        text = str(input_payload.get(key) or "").lower()
        if marker in text:
            return True
    return False


async def _noop_sleep(_delay: float) -> None:
    return None


def _build_shadow_task(task_payload: dict[str, Any]) -> dict[str, Any]:
    inp = dict(task_payload.get("input") or {})
    budget = _to_float(task_payload.get("budget"), 0.0)
    return {
        "task_id": str(task_payload.get("task_id") or ""),
        "run_id": str(task_payload.get("run_id") or ""),
        "task_type": str(task_payload.get("task_type") or ""),
        "status": "QUEUED",
        "budget": budget,
        "estimated_cost": _to_float(inp.get("estimated_cost"), budget),
        "estimated_minutes": _to_int(inp.get("estimated_minutes"), 0),
        "deadline_minutes": _to_int(inp.get("deadline_minutes"), 0),
        "_input": inp,
    }


def _shadow_task_handler(task: dict[str, Any]) -> dict[str, Any]:
    inp = dict(task.get("_input") or {})
    if _has_force_flag(inp, "force_500"):
        raise RuntimeError("shadow_force_500 timeout")
    if _has_force_flag(inp, "force_400"):
        raise ValueError("shadow_force_400 validation")
    return {"shadow_result": "ok"}


def _map_execution_outcome(status: str) -> str:
    if status == "SUCCEEDED":
        return "SUCCEEDED"
    if status in {"FAILED", "THROTTLED"}:
        return "FAILED_RETRYABLE"
    return "FAILED_FINAL"


async def run_shadow_simulation(task_payload: dict[str, Any]) -> dict[str, Any]:
    task = _build_shadow_task(task_payload)
    bus = EventBus(InMemoryMessageQueue(), default_timeout_s=0.0)
    approval = ApprovalAgent(agent_id="approval_agent", event_bus=bus, execution_agent_id="execution_agent")
    execution = TaskExecutionAgent(
        agent_id="execution_agent",
        event_bus=bus,
        task_handler=_shadow_task_handler,
        recovery_policy=RecoveryPolicy(max_attempts=3, base_delay_s=1.0, max_delay_s=10.0),
        sleep_fn=_noop_sleep,
    )

    path: list[str] = []
    approval_result = await approval.run_once({"task": task})
    approval_status = str(approval_result["result"]["status"])
    path.append(f"approval:{approval_status}")
    if approval_status != "APPROVED":
        return {
            "predicted_status": "FAILED_FINAL",
            "path": path,
            "approval_status": approval_status,
            "execution_status": None,
        }

    task["status"] = "APPROVED"
    execution_result = await execution.run_once({"task": task})
    execution_status = str(execution_result["result"]["status"])
    path.append(f"execution:{execution_status}")
    return {
        "predicted_status": _map_execution_outcome(execution_status),
        "path": path,
        "approval_status": approval_status,
        "execution_status": execution_status,
    }


async def run_shadow_comparison(*, task_payload: dict[str, Any], actual_status: str) -> dict[str, Any]:
    predicted = await run_shadow_simulation(task_payload)
    actual = str(actual_status or "")
    comparable = actual in {"SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL"}
    consistent = bool(comparable and predicted["predicted_status"] == actual)
    return {
        "predicted_status": predicted["predicted_status"],
        "actual_status": actual,
        "comparable": comparable,
        "consistent": consistent,
        "path": predicted["path"],
        "approval_status": predicted["approval_status"],
        "execution_status": predicted["execution_status"],
    }

