from __future__ import annotations

from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _optional_dict(value: Any) -> dict[str, Any] | None:
    parsed = _as_dict(value)
    return parsed or None


def build_agent_run(turn: dict[str, Any], task_card: dict[str, Any] | None = None) -> dict[str, Any]:
    runtime = _as_dict(turn.get("runtime_state"))
    status = str((task_card or {}).get("status") or turn.get("status") or "RUNNING")
    current_phase = str(runtime.get("current_phase") or turn.get("current_phase") or "understand")
    final_output = _as_dict(runtime.get("final_output"))

    if task_card and not final_output and task_card.get("result_preview"):
        final_output = {"preview": task_card.get("result_preview")}

    return {
        "turn_id": str(turn.get("turn_id") or ""),
        "route": str(turn.get("route") or "direct_answer"),
        "status": status,
        "current_phase": current_phase,
        "task_id": str(turn.get("task_id") or "") or None,
        "trace_id": str(turn.get("trace_id") or ""),
        "planner": _as_dict(runtime.get("planner")),
        "retrieval_hits": _as_list(runtime.get("retrieval_hits")),
        "memory": _as_dict(runtime.get("memory")),
        "goal_ref": _as_dict(runtime.get("goal_ref")),
        "goal": _optional_dict(runtime.get("goal")),
        "unified_task": _optional_dict(runtime.get("unified_task")),
        "task_state": _optional_dict(runtime.get("task_state")),
        "current_action": _optional_dict(runtime.get("current_action")),
        "policy": _optional_dict(runtime.get("policy")),
        "episodes": _as_list(runtime.get("episodes")),
        "observations": _as_list(runtime.get("observations")),
        "decision": runtime.get("decision"),
        "reflection": _optional_dict(runtime.get("reflection")),
        "steps": _as_list(runtime.get("steps")),
        "final_output": final_output,
    }


def build_turn_summary(turn: dict[str, Any], task_card: dict[str, Any] | None = None) -> dict[str, Any]:
    runtime = _as_dict(turn.get("runtime_state"))
    return {
        "turn_id": str(turn.get("turn_id") or ""),
        "route": str(turn.get("route") or "direct_answer"),
        "status": str((task_card or {}).get("status") or turn.get("status") or "RUNNING"),
        "current_phase": str(runtime.get("current_phase") or turn.get("current_phase") or "understand"),
        "response_type": str(turn.get("response_type") or "direct_answer"),
        "user_message": str(turn.get("user_message") or ""),
        "assistant_message": str(turn.get("assistant_message") or "") or None,
        "task_id": str(turn.get("task_id") or "") or None,
        "trace_id": str(turn.get("trace_id") or ""),
        "created_at": turn.get("created_at"),
        "updated_at": turn.get("updated_at"),
        "agent_run": build_agent_run(turn, task_card=task_card),
    }
