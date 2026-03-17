from __future__ import annotations

from typing import Any

from ..masking import summarize_payload
from ..state_machine import FINAL_STATES

STATUS_LABELS: dict[str, str] = {
    "RECEIVED": "已接收",
    "QUEUED": "排队中",
    "VALIDATING": "校验中",
    "PLANNING": "规划中",
    "RUNNING": "执行中",
    "WAITING_TOOL": "等待工具",
    "WAITING_HUMAN": "等待审批",
    "REVIEWING": "复核中",
    "SUCCEEDED": "已完成",
    "FAILED_RETRYABLE": "执行失败（可重试）",
    "FAILED_FINAL": "执行失败",
    "CANCELLED": "已取消",
    "TIMED_OUT": "已超时",
    "APPROVED": "已通过",
    "REJECTED": "已拒绝",
    "EDITED": "已编辑并通过",
}

STEP_LABELS: dict[str, str] = {
    "task_create": "已创建任务",
    "task_rerun": "已重新发起任务",
    "task_cancel": "任务已取消",
    "workflow_start": "启动工作流",
    "assistant_tool_run": "调用工具中",
    "assistant_tool_done": "工具调用完成",
}


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status or "未知状态")


def _step_label(step_key: str) -> str:
    if not step_key:
        return "处理中"
    if step_key in STEP_LABELS:
        return STEP_LABELS[step_key]
    return step_key.replace("_", " ").strip()


def _extract_last_messages(history: list[dict[str, Any]]) -> tuple[str | None, str | None, str | None]:
    last_user = None
    last_assistant = None
    last_route = None
    for item in reversed(history):
        role = str(item.get("role") or "")
        if role == "assistant" and last_assistant is None:
            last_assistant = str(item.get("message") or "")
            last_route = str(item.get("route") or "") or None
        if role == "user" and last_user is None:
            last_user = str(item.get("message") or "")
        if last_user is not None and last_assistant is not None:
            break
    return last_user, last_assistant, last_route


def build_conversation_summary(row: dict[str, Any]) -> dict[str, Any]:
    history = list(row.get("message_history") or [])
    last_user, last_assistant, last_route = _extract_last_messages(history)
    return {
        "conversation_id": str(row.get("conversation_id") or ""),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "last_user_message": last_user,
        "last_assistant_message": last_assistant,
        "last_route": last_route,
        "task_count": int(row.get("task_count") or 0),
        "running_task_count": int(row.get("running_task_count") or 0),
        "waiting_approval_count": int(row.get("waiting_approval_count") or 0),
    }


def build_memory_snapshot(conversation: dict[str, Any]) -> dict[str, Any]:
    return {
        "last_task_result": _as_dict(conversation.get("last_task_result")),
        "last_tool_result": _as_dict(conversation.get("last_tool_result")),
        "user_preferences": _as_dict(conversation.get("user_preferences")),
    }


def _route_from_task(task: dict[str, Any]) -> str:
    task_type = str(task.get("task_type") or "")
    planner = _as_dict(_as_dict(task.get("input_masked")).get("planner"))
    planner_action = str(planner.get("action") or "")
    if task_type == "tool_flow" or planner_action in {"use_tool", "need_approval"}:
        return "tool_task"
    if planner_action in {"answer_only", "use_retrieval"}:
        return "direct_answer"
    return "workflow_task"


def _task_kind_label(route: str) -> str:
    if route == "tool_task":
        return "工具任务"
    if route == "direct_answer":
        return "直接回答"
    return "长任务"


def _progress_message(
    *,
    status: str,
    latest_step_key: str,
    waiting_approval_count: int,
    tool_call_count: int,
) -> tuple[str, str | None, str | None]:
    if waiting_approval_count > 0 or status == "WAITING_HUMAN":
        return ("任务等待人工审批", "等待审批", "审批通过后系统会自动继续")
    if status == "WAITING_TOOL":
        return ("任务正在等待工具结果", "等待工具执行", "工具返回后系统会继续")
    if status == "SUCCEEDED":
        return ("任务已完成", None, "可查看回放和最终结果")
    if status in {"FAILED_RETRYABLE", "FAILED_FINAL"}:
        return ("任务执行失败", None, "可查看失败原因并考虑重试")
    if status == "CANCELLED":
        return ("任务已取消", None, None)
    if status == "TIMED_OUT":
        return ("任务执行超时", None, "可重试或调整任务规模")
    current_step = _step_label(latest_step_key)
    if tool_call_count > 0:
        return (f"系统正在执行，当前步骤：{current_step}", None, "可继续观察执行状态")
    return (f"系统正在处理，当前步骤：{current_step}", None, "可继续观察执行状态")


def _failure_reason(task: dict[str, Any]) -> str | None:
    status = str(task.get("status") or "")
    if status not in {"FAILED_RETRYABLE", "FAILED_FINAL", "TIMED_OUT"}:
        return None
    error_code = str(task.get("error_code") or "").strip()
    error_message = str(task.get("error_message") or "").strip()
    if error_code and error_message:
        return f"{error_code}: {error_message}"
    if error_code:
        return error_code
    if error_message:
        return error_message
    return "unknown_error"


def _result_preview(task: dict[str, Any]) -> str | None:
    output = _as_dict(task.get("output_masked"))
    if output:
        return str(summarize_payload(output, max_len=220).get("summary") or "")
    if str(task.get("status") or "") == "SUCCEEDED":
        return "任务已成功完成，可展开查看详情。"
    return None


def build_task_card(task: dict[str, Any]) -> dict[str, Any]:
    status = str(task.get("status") or "")
    latest_step_key = str(task.get("latest_step_key") or "")
    waiting_approval_count = int(task.get("waiting_approval_count") or 0)
    tool_call_count = int(task.get("tool_call_count") or 0)
    route = _route_from_task(task)
    progress_message, waiting_for, next_action = _progress_message(
        status=status,
        latest_step_key=latest_step_key,
        waiting_approval_count=waiting_approval_count,
        tool_call_count=tool_call_count,
    )
    return {
        "task_id": str(task.get("id") or task.get("task_id") or ""),
        "task_type": str(task.get("task_type") or ""),
        "task_kind": _task_kind_label(route),
        "route": route,
        "status": status,
        "status_label": _status_label(status),
        "progress_message": progress_message,
        "current_step": _step_label(latest_step_key) if latest_step_key else None,
        "waiting_for": waiting_for,
        "next_action": next_action,
        "tool_call_count": tool_call_count,
        "waiting_approval_count": waiting_approval_count,
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "trace_id": str(task.get("trace_id") or ""),
        "result_preview": _result_preview(task),
        "failure_reason": _failure_reason(task),
    }


def build_trace_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    ordered = sorted(steps, key=lambda item: (item.get("created_at") or "", item.get("id") or 0))
    for step in ordered:
        payload = _as_dict(step.get("payload_masked"))
        out.append(
            {
                "step_key": str(step.get("step_key") or ""),
                "title": _step_label(str(step.get("step_key") or "")),
                "status": str(step.get("status") or ""),
                "status_label": _status_label(str(step.get("status") or "")),
                "created_at": step.get("created_at"),
                "detail": str(summarize_payload(payload, max_len=200).get("summary") or "") if payload else None,
            }
        )
    return out


def build_trace_tool_calls(tool_calls: list[dict[str, Any]], planner: dict[str, Any]) -> list[dict[str, Any]]:
    selected_tool = str(planner.get("selected_tool") or "")
    candidate_tools = list(planner.get("tool_candidates") or [])
    out: list[dict[str, Any]] = []
    ordered = sorted(tool_calls, key=lambda item: (item.get("created_at") or "", str(item.get("tool_call_id") or "")))
    for call in ordered:
        tool_name = str(call.get("tool_id") or "")
        why = None
        if selected_tool and selected_tool == tool_name:
            why = "规划器将该工具作为首选候选。"
        elif tool_name and tool_name in [str(x) for x in candidate_tools]:
            why = "该工具命中规划器候选集合。"
        out.append(
            {
                "tool_call_id": str(call.get("tool_call_id") or ""),
                "tool_name": tool_name,
                "status": str(call.get("status") or ""),
                "status_label": _status_label(str(call.get("status") or "")),
                "reason_code": str(call.get("reason_code") or "") or None,
                "duration_ms": int(call.get("duration_ms") or 0),
                "why_this_tool": why,
                "request_summary": str(
                    summarize_payload(_as_dict(call.get("request_masked")), max_len=180).get("summary") or ""
                )
                if _as_dict(call.get("request_masked"))
                else None,
                "response_summary": str(
                    summarize_payload(_as_dict(call.get("response_masked")), max_len=180).get("summary") or ""
                )
                if _as_dict(call.get("response_masked"))
                else None,
                "created_at": call.get("created_at"),
            }
        )
    return out


def build_trace_approvals(approvals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    ordered = sorted(approvals, key=lambda item: (item.get("created_at") or "", str(item.get("id") or "")))
    for item in ordered:
        status = str(item.get("status") or "")
        action_hint = None
        if status == "WAITING_HUMAN":
            action_hint = "等待人工审批，审批通过后任务继续。"
        elif status in {"APPROVED", "EDITED"}:
            action_hint = "审批已通过，任务可继续执行。"
        elif status == "REJECTED":
            action_hint = "审批被拒绝，任务可能进入失败或终止状态。"
        out.append(
            {
                "approval_id": str(item.get("id") or ""),
                "status": status,
                "status_label": _status_label(status),
                "reason": str(item.get("reason") or "") or None,
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "action_hint": action_hint,
            }
        )
    return out


def _extract_agent_runtime(task: dict[str, Any], steps: list[dict[str, Any]]) -> dict[str, Any]:
    task_runtime = _as_dict(task.get("runtime_state"))
    if task_runtime:
        return task_runtime
    ordered = sorted(steps, key=lambda item: (item.get("created_at") or "", item.get("id") or 0), reverse=True)
    for step in ordered:
        payload = _as_dict(step.get("payload_masked"))
        runtime = _as_dict(payload.get("agent_runtime"))
        if runtime:
            return runtime
    return {}


def _optional_runtime_dict(value: Any) -> dict[str, Any] | None:
    parsed = _as_dict(value)
    return parsed or None


def _normalize_runtime_steps(runtime_steps: Any) -> list[dict[str, Any]]:
    if not isinstance(runtime_steps, list):
        return []
    out: list[dict[str, Any]] = []
    for item in runtime_steps:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "key": str(item.get("key") or ""),
                "phase": str(item.get("phase") or ""),
                "title": str(item.get("title") or ""),
                "status": str(item.get("status") or ""),
                "summary": str(item.get("summary") or ""),
                "created_at": item.get("created_at"),
                "observation": _as_dict(item.get("observation")) or None,
                "decision": _as_dict(item.get("decision")) or None,
                "reflection": _as_dict(item.get("reflection")) or None,
                "state_before": _as_dict(item.get("state_before")),
                "state_after": _as_dict(item.get("state_after")),
            }
        )
    return out


def _runtime_snapshot(runtime: dict[str, Any]) -> dict[str, Any]:
    goal = _as_dict(runtime.get("goal"))
    task_state = _as_dict(runtime.get("task_state"))
    current_action = _as_dict(runtime.get("current_action"))
    policy = _as_dict(runtime.get("policy"))
    agenda = _as_dict(runtime.get("agenda"))
    wake_condition = _as_dict(runtime.get("wake_condition"))
    return {
        "status": str(runtime.get("status") or ""),
        "current_phase": str(runtime.get("current_phase") or task_state.get("current_phase") or ""),
        "goal": {
            "goal_id": str(goal.get("goal_id") or "") or None,
            "normalized_goal": str(goal.get("normalized_goal") or ""),
            "risk_level": str(goal.get("risk_level") or "") or None,
            "unknowns": list(goal.get("unknowns") or task_state.get("unknowns") or []),
        },
        "blockers": list(task_state.get("blockers") or []),
        "pending_approvals": list(task_state.get("pending_approvals") or []),
        "latest_result": _as_dict(task_state.get("latest_result")),
        "current_action": {
            "action_type": str(current_action.get("action_type") or "") or None,
            "target": str(current_action.get("target") or "") or None,
            "expected_result": str(current_action.get("expected_result") or "") or None,
            "fallback": str(current_action.get("fallback") or "") or None,
        },
        "policy": {
            "selected_action": str(policy.get("selected_action") or "") or None,
            "fallback_action": str(policy.get("fallback_action") or "") or None,
            "policy_version_id": str(policy.get("policy_version_id") or "") or None,
        },
        "agenda": agenda or None,
        "wake_condition": wake_condition or None,
    }


def _build_runtime_debugger(runtime: dict[str, Any], runtime_steps: list[dict[str, Any]]) -> dict[str, Any]:
    decision = _as_dict(runtime.get("decision"))
    reflection = _as_dict(runtime.get("reflection"))
    observations = runtime.get("observations")
    observation_rows = observations if isinstance(observations, list) else []
    latest_observation = _as_dict(observation_rows[-1]) if observation_rows else {}
    first_before = next((step.get("state_before") for step in runtime_steps if _as_dict(step.get("state_before"))), {})
    last_after = next((step.get("state_after") for step in reversed(runtime_steps) if _as_dict(step.get("state_after"))), {})
    action = _as_dict(runtime.get("current_action"))
    return {
        "state_before": first_before or None,
        "latest_observation": latest_observation or None,
        "decision": decision or None,
        "why_not": dict(decision.get("why_not") or {}),
        "candidate_actions": list(decision.get("candidate_actions") or []),
        "reflection": reflection or None,
        "state_after": last_after or _runtime_snapshot(runtime),
        "action_contract": {
            "action_type": str(action.get("action_type") or "") or None,
            "expected_result": str(action.get("expected_result") or "") or None,
            "success_conditions": list(action.get("success_conditions") or []),
            "fallback": str(action.get("fallback") or "") or None,
            "stop_conditions": list(action.get("stop_conditions") or []),
        },
    }


def build_task_trace_view(
    *,
    task: dict[str, Any],
    runs: list[dict[str, Any]],
    steps: list[dict[str, Any]],
    tool_calls: list[dict[str, Any]],
    approvals: list[dict[str, Any]],
) -> dict[str, Any]:
    latest_step = steps[-1] if steps else {}
    task_row = dict(task)
    task_row["latest_step_key"] = str(latest_step.get("step_key") or "")
    task_row["tool_call_count"] = len(tool_calls)
    task_row["waiting_approval_count"] = len([x for x in approvals if str(x.get("status") or "") == "WAITING_HUMAN"])
    task_card = build_task_card(task_row)

    input_masked = _as_dict(task.get("input_masked"))
    runtime = _extract_agent_runtime(task_row, steps)
    planner = _as_dict(runtime.get("planner") or runtime.get("plan") or input_masked.get("planner"))
    retrieval_hits = runtime.get("retrieval_hits")
    if not isinstance(retrieval_hits, list):
        retrieval_hits = input_masked.get("retrieval_hits") if isinstance(input_masked.get("retrieval_hits"), list) else []
    trace_steps = build_trace_steps(steps)
    trace_tool_calls = build_trace_tool_calls(tool_calls, planner)
    trace_approvals = build_trace_approvals(approvals)
    final_output = _as_dict(runtime.get("final_output")) or _as_dict(task.get("output_masked"))
    goal = _optional_runtime_dict(runtime.get("goal"))
    unified_task = _optional_runtime_dict(runtime.get("unified_task"))
    task_state = _optional_runtime_dict(runtime.get("task_state"))
    current_action = _optional_runtime_dict(runtime.get("current_action"))
    policy = _optional_runtime_dict(runtime.get("policy"))
    episodes = runtime.get("episodes")
    if not isinstance(episodes, list):
        episodes = []
    reflection = _optional_runtime_dict(runtime.get("reflection"))
    runtime_steps = _normalize_runtime_steps(runtime.get("steps"))
    runtime_debugger = _build_runtime_debugger(runtime, runtime_steps)

    summary_parts = [
        f"任务类型：{task_card.get('task_kind')}",
        f"当前状态：{task_card.get('status_label')}",
    ]
    if task_card.get("waiting_for"):
        summary_parts.append(f"阻塞原因：{task_card.get('waiting_for')}")
    if retrieval_hits:
        summary_parts.append(f"本次规划引用了 {len(retrieval_hits)} 条检索片段")
    if task_card.get("failure_reason"):
        summary_parts.append(f"失败原因：{task_card.get('failure_reason')}")
    task_summary = "；".join(summary_parts)

    run_history = [
        {
            "run_id": str(run.get("id") or ""),
            "run_no": int(run.get("run_no") or 0),
            "status": str(run.get("status") or ""),
            "status_label": _status_label(str(run.get("status") or "")),
            "started_at": run.get("started_at"),
            "ended_at": run.get("ended_at"),
        }
        for run in runs
    ]

    return {
        "task": task_card,
        "task_summary": task_summary,
        "planner": planner,
        "retrieval_hits": retrieval_hits,
        "goal": goal,
        "unified_task": unified_task,
        "task_state": task_state,
        "current_action": current_action,
        "policy": policy,
        "episodes": episodes,
        "reflection": reflection,
        "trace_steps": trace_steps,
        "runtime_steps": runtime_steps,
        "runtime_debugger": runtime_debugger,
        "tool_calls": trace_tool_calls,
        "approvals": trace_approvals,
        "run_history": run_history,
        "final_output": final_output,
        "failure_reason": task_card.get("failure_reason"),
        "is_final": str(task.get("status") or "") in set(FINAL_STATES),
    }
