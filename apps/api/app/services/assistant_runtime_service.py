from __future__ import annotations

import re
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


def _step_key_from_title(title: str, index: int) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", title.strip().lower()).strip("_")
    return normalized or f"runtime_step_{index + 1}"


def _normalize_runtime_steps(value: Any) -> list[dict[str, Any]]:
    normalized_steps: list[dict[str, Any]] = []
    for index, raw_step in enumerate(_as_list(value)):
        step = _as_dict(raw_step)
        if not step:
            continue
        title = str(step.get("title") or f"Runtime Step {index + 1}")
        summary = str(step.get("summary") or title)
        phase = str(step.get("phase") or "runtime")
        status = str(step.get("status") or "completed")
        normalized_steps.append(
            {
                "key": str(step.get("key") or _step_key_from_title(title, index)),
                "phase": phase,
                "title": title,
                "status": status,
                "summary": summary,
                "created_at": step.get("created_at"),
                "observation": _optional_dict(step.get("observation")),
                "decision": _optional_dict(step.get("decision")),
                "reflection": _optional_dict(step.get("reflection")),
                "state_before": _as_dict(step.get("state_before")),
                "state_after": _as_dict(step.get("state_after")),
            }
        )
    return normalized_steps


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
        "steps": _normalize_runtime_steps(runtime.get("steps")),
        "final_output": final_output,
    }


def build_turn_summary(turn: dict[str, Any], task_card: dict[str, Any] | None = None) -> dict[str, Any]:
    runtime = _as_dict(turn.get("runtime_state"))
    assistant_message = str(turn.get("assistant_message") or "") or None
    return {
        "turn_id": str(turn.get("turn_id") or ""),
        "route": str(turn.get("route") or "direct_answer"),
        "status": str((task_card or {}).get("status") or turn.get("status") or "RUNNING"),
        "current_phase": str(runtime.get("current_phase") or turn.get("current_phase") or "understand"),
        "display_state": str((task_card or {}).get("chat_state") or ""),
        "display_summary": str((task_card or {}).get("assistant_summary") or assistant_message or ""),
        "response_type": str(turn.get("response_type") or "direct_answer"),
        "user_message": str(turn.get("user_message") or ""),
        "assistant_message": assistant_message,
        "task_id": str(turn.get("task_id") or "") or None,
        "trace_id": str(turn.get("trace_id") or ""),
        "created_at": turn.get("created_at"),
        "updated_at": turn.get("updated_at"),
        "agent_run": build_agent_run(turn, task_card=task_card),
    }
