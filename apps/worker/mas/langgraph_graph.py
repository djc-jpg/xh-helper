from __future__ import annotations

import time
from functools import lru_cache
from typing import Any

from langgraph.graph import END, StateGraph
from runtime_backbone import apply_runtime_event, merge_runtime_state

from .state import MASState, validate_protocol_message

FINAL_FAILURE_TYPES = {
    "POLICY_VIOLATION",
    "CONSTRAINT_VIOLATION",
    "NEED_INFO",
    "INVALID_TASK_SPEC",
    "RETRY_BUDGET_EXHAUSTED",
    "FAIL_FINAL",
}
RETRYABLE_FAILURE_TYPES = {
    "UPSTREAM_TIMEOUT",
    "UPSTREAM_UNAVAILABLE",
    "TOOL_TRANSIENT",
    "LATENCY_BUDGET_EXCEEDED",
    "QUALITY_GAP",
    "PROTOCOL_ERROR",
}


def _runtime_seed_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    nested = payload.get("input") if isinstance(payload.get("input"), dict) else {}
    runtime = nested.get("runtime_state") if isinstance(nested.get("runtime_state"), dict) else payload.get("runtime_state")
    if isinstance(runtime, dict):
        return dict(runtime)
    return {}


def _runtime_phase(state: MASState) -> str:
    phase = str(state.get("phase") or "").upper()
    status = str(state.get("status") or "").upper()
    if phase == "PERCEIVE":
        return "understand"
    if phase == "PLAN":
        if str(state.get("plan_state") or "").upper() == "REPLANNING":
            return "replan"
        if str(state.get("next_route") or "").strip() == "approval":
            return "approval_request"
        return "plan"
    if phase == "KNOWLEDGE":
        return "observe"
    if phase == "EXECUTE":
        return "act"
    if phase == "EVALUATE":
        return "reflect"
    if phase == "ASK_USER":
        return "ask_user"
    if phase == "TERMINATE":
        if status == "SUCCEEDED":
            return "respond"
        if str(state.get("failure_semantic") or "") == "FAIL_RETRYABLE":
            return "replan"
        return "respond"
    return "observe"


def _runtime_goal(state: MASState) -> dict[str, Any]:
    task_spec = dict(state.get("task_spec") or {})
    input_payload = dict(state.get("input_payload") or {})
    goal_text = str(
        task_spec.get("goal")
        or input_payload.get("goal")
        or input_payload.get("query")
        or input_payload.get("question")
        or input_payload.get("content")
        or state.get("task_type")
        or ""
    )
    return {
        "normalized_goal": goal_text,
        "risk_level": str(task_spec.get("risk_level") or state.get("risk_level") or "medium"),
        "success_criteria": list(task_spec.get("success_criteria") or []),
        "constraints": list(task_spec.get("constraints") or []),
        "unknowns": list(task_spec.get("inputs_needed") or []),
        "user_intent": str(task_spec.get("intent") or state.get("task_type") or ""),
    }


def _runtime_available_actions(state: MASState) -> list[str]:
    actions = ["workflow_call", "replan", "respond"]
    if list((_runtime_goal(state)).get("unknowns") or []):
        actions.extend(["ask_user", "wait"])
    if str(state.get("next_route") or "").strip() == "approval":
        actions.append("approval_request")
    seen: set[str] = set()
    ordered: list[str] = []
    for item in actions:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _runtime_observations(state: MASState) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    task_spec = dict(state.get("task_spec") or {})
    if task_spec:
        observations.append(
            {
                "kind": "task_spec",
                "summary": f"MAS task spec captured goal `{str(task_spec.get('goal') or '')[:120]}`.",
                "source": "mas.perceptor",
            }
        )
    for item in list(state.get("evidence") or [])[:6]:
        if not isinstance(item, dict):
            continue
        observations.append(
            {
                "kind": "evidence",
                "summary": f"{str(item.get('kind') or 'evidence')}: {str(item.get('value') or item)[:160]}",
                "source": str(item.get("source") or "mas.knowledge"),
            }
        )
    latest_message = list(state.get("msgs") or [])[-1:]  # keep runtime projection compact
    for message in latest_message:
        if not isinstance(message, dict):
            continue
        observations.append(
            {
                "kind": "protocol",
                "summary": str(message.get("rationale_brief") or "")[:180],
                "source": str(message.get("agent") or "mas"),
            }
        )
    return observations


def _runtime_steps(state: MASState) -> list[dict[str, Any]]:
    phase_map = {
        "perceptor_agent": "understand",
        "planner_agent": "plan",
        "scheduler_agent": "plan",
        "approval_agent": "approval_request",
        "knowledge_resolution_agent": "observe",
        "researcher_agent": "act",
        "weather_agent": "act",
        "writer_agent": "act",
        "execution_agent": "act",
        "critic_agent": "reflect",
    }
    steps: list[dict[str, Any]] = []
    for index, message in enumerate(list(state.get("msgs") or [])[-12:], start=1):
        if not isinstance(message, dict):
            continue
        agent = str(message.get("agent") or "mas")
        steps.append(
            {
                "key": f"mas_{index}_{agent}",
                "phase": phase_map.get(agent, "observe"),
                "title": str(message.get("type") or "protocol"),
                "status": "completed" if str(message.get("status") or "").upper() not in {"ERROR", "FAIL"} else "failed",
                "summary": str(message.get("rationale_brief") or "")[:200],
            }
        )
    return steps


def _runtime_latest_result(state: MASState) -> dict[str, Any]:
    status = str(state.get("status") or "IN_PROGRESS")
    if status == "SUCCEEDED":
        execution_result = dict(state.get("execution_result") or {})
        return {
            "status": "SUCCEEDED",
            "summary": str(
                execution_result.get("final_output")
                or execution_result.get("summary")
                or state.get("stop_reason")
                or "MAS runtime completed successfully."
            )[:240],
        }
    if status in {"FAILED_FINAL", "FAILED_RETRYABLE", "RETRYING", "NEED_INFO"}:
        return {
            "status": status,
            "failure_type": str(state.get("failure_type") or ""),
            "reason": str(state.get("stop_reason") or ""),
        }
    return {"status": status, "route": str(state.get("next_route") or "")}


def _sync_agent_runtime(state: MASState, *, event_type: str) -> MASState:
    goal = _runtime_goal(state)
    current_phase = _runtime_phase(state)
    observations = _runtime_observations(state)
    steps = _runtime_steps(state)
    latest_result = _runtime_latest_result(state)
    pending_approvals = [str(state.get("task_id") or "")] if str(state.get("next_route") or "").strip() == "approval" else []
    existing_runtime = _runtime_seed_from_payload({"runtime_state": state.get("agent_runtime"), "input": dict(state.get("input_payload") or {})})
    unified_task = {
        "goal": goal.get("normalized_goal"),
        "task_type": str(state.get("task_type") or ""),
        "available_actions": _runtime_available_actions(state),
        "beliefs": [f"mas_turn:{int(state.get('turn') or 1)}", f"mas_plan_state:{str(state.get('plan_state') or '').lower()}"],
        "planner_signal": {"plan_state": str(state.get("plan_state") or ""), "next_route": str(state.get("next_route") or "")},
        "episode_context": [],
    }
    base_runtime = merge_runtime_state(
        existing_runtime,
        {
            "goal": goal,
            "unified_task": unified_task,
            "task_state": {
                "current_goal": goal,
                "current_subgoals": list(goal.get("success_criteria") or [])
                or [
                    str(item.get("action") or item.get("step_id") or "")
                    for item in list(state.get("plan") or [])
                    if isinstance(item, dict) and str(item.get("action") or item.get("step_id") or "")
                ][:6],
                "observations": observations,
                "beliefs": list(unified_task.get("beliefs") or []),
                "known_facts": [str(item.get("source") or "") for item in list(state.get("evidence") or [])[:3] if isinstance(item, dict)],
                "blockers": list(goal.get("unknowns") or []),
                "pending_approvals": pending_approvals,
                "fallback_state": str(state.get("plan_state") or "idle"),
                "latest_result": latest_result,
                "available_actions": list(unified_task.get("available_actions") or []),
                "unknowns": list(goal.get("unknowns") or []),
            },
            "episodes": list(existing_runtime.get("episodes") or []),
        },
    )
    runtime = apply_runtime_event(
        base_runtime,
        event_type=event_type,
        status=str(state.get("status") or "IN_PROGRESS"),
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        final_output={"message": str((dict(state.get("execution_result") or {})).get("final_output") or "")}
        if str(state.get("status") or "") == "SUCCEEDED"
        else None,
        decision={
            "action": str(state.get("next_route") or ""),
            "route": "workflow_task",
            "selected_tool": str(state.get("next_route") or "") or None,
            "need_confirmation": bool(pending_approvals),
            "summary": str(state.get("stop_reason") or "MAS runtime decision updated."),
        },
        route="workflow_task",
        observations=observations,
        steps=steps,
        summary=str(state.get("stop_reason") or ""),
        target=str(state.get("next_route") or "") or None,
    )
    state["agent_runtime"] = runtime
    return state


def _coerce_int(value: Any, default: int, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, parsed)


def _coerce_list_of_strings(value: Any, *, default: list[str] | None = None) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return list(default or [])


def _has_marker(input_payload: dict[str, Any], marker: str) -> bool:
    if bool(input_payload.get(marker)):
        return True
    marker_lower = marker.lower()
    for key in ("query", "question", "action", "content"):
        text = str(input_payload.get(key) or "").lower()
        if marker_lower in text:
            return True
    return False


def _extract_token_from_criterion(criterion: str) -> str:
    if not criterion.startswith("must_include:"):
        return ""
    return criterion.split(":", 1)[1].strip()


def _extract_fix_tokens(fix_instructions: list[str]) -> list[str]:
    tokens: list[str] = []
    for item in fix_instructions:
        normalized = str(item).strip()
        if "token=" in normalized:
            token = normalized.split("token=", 1)[1].strip()
            if token:
                tokens.append(token)
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def _semantic_for_failure(failure_type: str, fallback: str = "FAIL_RETRYABLE") -> str:
    if failure_type in FINAL_FAILURE_TYPES:
        return "FAIL_FINAL"
    if failure_type in RETRYABLE_FAILURE_TYPES:
        return "FAIL_RETRYABLE"
    return fallback


def _msg(
    *,
    message_type: str,
    agent: str,
    task_id: str,
    turn: int,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    status: str,
    rationale_brief: str,
    next_agent: str,
) -> dict[str, Any]:
    return {
        "type": message_type,
        "agent": agent,
        "task_id": task_id,
        "turn": turn,
        "inputs": inputs,
        "outputs": outputs,
        "status": status,
        "rationale_brief": rationale_brief,
        "next": next_agent,
    }


def _ensure_metrics(state: MASState) -> dict[str, Any]:
    metrics = dict(state.get("metrics") or {})
    metrics.setdefault("graph_engine", "langgraph")
    metrics.setdefault("graph_node_calls", {})
    metrics.setdefault("agent_message_counts", {})
    metrics.setdefault("protocol_error_count", 0)
    metrics.setdefault("retry_count", 0)
    metrics.setdefault("loop_oscillation_count", 0)
    state["metrics"] = metrics
    return metrics


def _record_node_call(state: MASState, node_name: str) -> None:
    metrics = _ensure_metrics(state)
    node_calls = dict(metrics.get("graph_node_calls") or {})
    node_calls[node_name] = int(node_calls.get(node_name) or 0) + 1
    metrics["graph_node_calls"] = node_calls
    state["metrics"] = metrics


def _append_protocol_message(
    state: MASState,
    *,
    node_name: str,
    output_slot: str,
    message: dict[str, Any],
) -> None:
    validate_protocol_message(message)
    msgs = list(state.get("msgs") or [])
    msgs.append(message)
    state["msgs"] = msgs
    state[output_slot] = dict(message.get("outputs") or {})

    metrics = _ensure_metrics(state)
    agent = str(message.get("agent") or "")
    counts = dict(metrics.get("agent_message_counts") or {})
    counts[agent] = int(counts.get(agent) or 0) + 1
    metrics["agent_message_counts"] = counts
    metrics["last_node"] = node_name
    metrics["last_agent"] = agent
    metrics["message_total"] = len(msgs)
    state["metrics"] = metrics


def _set_terminal_failure(
    state: MASState,
    *,
    failure_type: str,
    semantic: str,
    stop_reason: str,
) -> None:
    final_semantic = _semantic_for_failure(failure_type, semantic)
    state["failure_type"] = failure_type
    state["failure_semantic"] = final_semantic
    state["stop_reason"] = stop_reason
    state["verdict"] = "FAIL"
    if final_semantic == "FAIL_FINAL":
        state["status"] = "FAILED_FINAL"
        state["task_state"] = "FAILED_FINAL"
    else:
        state["status"] = "FAILED_RETRYABLE"
        state["task_state"] = "FAILED_RETRYABLE"


def _set_protocol_error(
    state: MASState,
    *,
    node_name: str,
    exc: Exception,
) -> MASState:
    metrics = _ensure_metrics(state)
    metrics["protocol_error_count"] = int(metrics.get("protocol_error_count") or 0) + 1
    state["metrics"] = metrics
    _set_terminal_failure(
        state,
        failure_type="PROTOCOL_ERROR",
        semantic="FAIL_RETRYABLE",
        stop_reason=f"{node_name} protocol_error: {exc}",
    )
    state["next_route"] = "terminate_fail"
    return state


def _update_latency_budget(state: MASState) -> bool:
    start_time_ms = int(state.get("start_time_ms") or int(time.monotonic() * 1000))
    latency_budget = dict(state.get("latency_budget") or {})
    max_ms = _coerce_int(latency_budget.get("max_ms"), 20000, minimum=1000)
    elapsed_ms = max(0, int(time.monotonic() * 1000) - start_time_ms)
    remaining_ms = max(0, max_ms - elapsed_ms)
    latency_budget["max_ms"] = max_ms
    latency_budget["remaining_ms"] = remaining_ms
    state["latency_budget"] = latency_budget
    state["start_time_ms"] = start_time_ms
    metrics = _ensure_metrics(state)
    metrics["elapsed_ms"] = elapsed_ms
    state["metrics"] = metrics
    if remaining_ms <= 0:
        _set_terminal_failure(
            state,
            failure_type="LATENCY_BUDGET_EXCEEDED",
            semantic="FAIL_RETRYABLE",
            stop_reason=f"elapsed_ms={elapsed_ms} exceeds latency_budget={max_ms}",
        )
        state["next_route"] = "terminate_fail"
        return False
    return True


def _classify_execution_exception(exc: Exception) -> str:
    text = str(exc).lower()
    if "timeout" in text:
        return "UPSTREAM_TIMEOUT"
    if "unavailable" in text or "connection" in text:
        return "UPSTREAM_UNAVAILABLE"
    return "TOOL_TRANSIENT"


def _initialize_state(
    payload: dict[str, Any],
    *,
    default_retry_budget: int,
    default_latency_budget_ms: int,
) -> MASState:
    task_id = str(payload.get("task_id") or "")
    run_id = str(payload.get("run_id") or "")
    task_type = str(payload.get("task_type") or "")
    input_payload = dict(payload.get("input") or {})
    retry_remaining = _coerce_int(
        input_payload.get("retry_budget", payload.get("retry_budget", default_retry_budget)),
        default_retry_budget,
        minimum=0,
    )
    latency_max_ms = _coerce_int(
        input_payload.get("latency_budget_ms", payload.get("latency_budget_ms", default_latency_budget_ms)),
        default_latency_budget_ms,
        minimum=1000,
    )
    now_ms = int(time.monotonic() * 1000)
    state: MASState = {
        "task_id": task_id,
        "run_id": run_id,
        "trace_id": str(payload.get("trace_id") or run_id or task_id),
        "task_type": task_type,
        "input_payload": input_payload,
        "budget": float(payload.get("budget") or 1.0),
        "turn": 1,
        "phase": "PERCEIVE",
        "status": "IN_PROGRESS",
        "verdict": "PENDING",
        "stop_reason": "",
        "task_state": "RECEIVED",
        "plan_state": "UNPLANNED",
        "evidence": [],
        "risk_level": str(input_payload.get("risk_level") or "medium"),
        "retry_budget": {"remaining": retry_remaining, "max": retry_remaining},
        "latency_budget": {"remaining_ms": latency_max_ms, "max_ms": latency_max_ms},
        "msgs": [],
        "metrics": {"graph_engine": "langgraph"},
        "start_time_ms": now_ms,
        "task_spec": {},
        "plan": [],
        "plan_meta": {},
        "approval_decision": "",
        "knowledge_pack": {},
        "research_pack": {},
        "weather_data": {},
        "draft_response": "",
        "execution_result": {},
        "fix_instructions": [],
        "next_route": "planner",
        "agent_runtime": _runtime_seed_from_payload(payload),
    }
    _ensure_metrics(state)
    return _sync_agent_runtime(state, event_type="mas.initialize")


def _perceive(
    *,
    task_id: str,
    task_type: str,
    input_payload: dict[str, Any],
    risk_level: str,
    turn: int,
) -> dict[str, Any]:
    goal = str(
        input_payload.get("goal")
        or input_payload.get("query")
        or input_payload.get("question")
        or input_payload.get("content")
        or ""
    ).strip()
    constraints = _coerce_list_of_strings(input_payload.get("constraints"))
    success_criteria = _coerce_list_of_strings(
        input_payload.get("success_criteria"),
        default=["non_empty_output", "traceable_artifact"],
    )
    assumptions = _coerce_list_of_strings(input_payload.get("assumptions"))
    intent = str(input_payload.get("intent") or task_type or "generic").strip() or "generic"
    urgency = str(input_payload.get("urgency") or "normal").strip() or "normal"
    normalized_risk = str(input_payload.get("risk_level") or risk_level or "medium").strip() or "medium"

    inputs_needed: list[str] = []
    if not goal:
        inputs_needed.append("goal/query")
    if task_type == "ticket_email" and not str(input_payload.get("content") or "").strip():
        inputs_needed.append("content")

    task_spec = {
        "goal": goal,
        "constraints": constraints,
        "success_criteria": success_criteria,
        "inputs_needed": inputs_needed,
        "assumptions": assumptions,
        "risk_level": normalized_risk,
        "intent": intent,
        "urgency": urgency,
        "task_type": task_type,
    }
    status = "NEED_INFO" if inputs_needed else "READY"
    return _msg(
        message_type="task_spec",
        agent="perceptor_agent",
        task_id=task_id,
        turn=turn,
        inputs={"raw_input": input_payload},
        outputs={"task_spec": task_spec, "inputs_needed": inputs_needed},
        status=status,
        rationale_brief="structured user request into task_spec fields",
        next_agent="planner_agent" if status == "READY" else "coordinator",
    )


def _plan(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    fix_instructions: list[str],
    turn: int,
) -> dict[str, Any]:
    plan_steps = [
        {
            "step_id": "s1_scheduler_approval",
            "owner_agent": "scheduler_agent",
            "action": "priority route to approval",
            "inputs": ["task_spec", "retry_budget", "urgency", "risk_level"],
            "expected_output": "dispatch.approval",
            "fallback": "defer with same task state",
            "timeout": 1000,
        },
        {
            "step_id": "s2_approval",
            "owner_agent": "approval_agent",
            "action": "check budget/time/risk constraints",
            "inputs": ["task_spec", "budget", "estimated_cost", "deadline_minutes"],
            "expected_output": "approval decision",
            "fallback": "reject on invalid constraints",
            "timeout": 1200,
        },
        {
            "step_id": "s3_scheduler_execution",
            "owner_agent": "scheduler_agent",
            "action": "route approved task to knowledge/execution chain",
            "inputs": ["approval decision", "priority"],
            "expected_output": "dispatch.execution_chain",
            "fallback": "requeue with retry budget update",
            "timeout": 1000,
        },
        {
            "step_id": "s4_knowledge",
            "owner_agent": "knowledge_resolution_agent",
            "action": "collect evidence and identify gaps",
            "inputs": ["task_spec", "raw_input"],
            "expected_output": "knowledge_pack",
            "fallback": "flag knowledge gaps explicitly",
            "timeout": 1600,
        },
        {
            "step_id": "s5_researcher",
            "owner_agent": "researcher_agent",
            "action": "derive facts from evidence",
            "inputs": ["task_spec", "knowledge_pack"],
            "expected_output": "research_pack",
            "fallback": "emit limited_context finding",
            "timeout": 2200,
        },
        {
            "step_id": "s6_weather",
            "owner_agent": "weather_agent",
            "action": "attach weather context when required",
            "inputs": ["task_spec", "location"],
            "expected_output": "weather_data",
            "fallback": "explicit weather skip status",
            "timeout": 1600,
        },
        {
            "step_id": "s7_writer",
            "owner_agent": "writer_agent",
            "action": "draft customer-facing response",
            "inputs": ["task_spec", "research_pack", "weather_data", "fix_instructions"],
            "expected_output": "draft_response",
            "fallback": "return compact fallback draft",
            "timeout": 2000,
        },
        {
            "step_id": "s8_execution",
            "owner_agent": "execution_agent",
            "action": "aggregate traceable artifacts and tool logs",
            "inputs": ["task_spec", "knowledge_pack", "research_pack", "weather_data", "draft_response"],
            "expected_output": "execution_artifacts",
            "fallback": "emit retryable execution error",
            "timeout": 3000,
        },
        {
            "step_id": "s9_critic",
            "owner_agent": "critic_agent",
            "action": "evaluate pass/fail and return fix instructions",
            "inputs": ["task_spec", "execution_artifacts"],
            "expected_output": "quality_verdict",
            "fallback": "fail_final when policy/constraint violation",
            "timeout": 2000,
        },
    ]
    outputs = {
        "plan": plan_steps,
        "plan_meta": {
            "version": turn,
            "rollback_enabled": True,
            "evaluatable": True,
            "fix_instructions": list(fix_instructions),
        },
        "task_spec": task_spec,
    }
    return _msg(
        message_type="execution_plan",
        agent="planner_agent",
        task_id=task_id,
        turn=turn,
        inputs={"task_spec": task_spec, "fix_instructions": fix_instructions},
        outputs=outputs,
        status="READY",
        rationale_brief="built executable and critic-verifiable multi-step plan",
        next_agent="scheduler_agent",
    )


def _schedule(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    retry_remaining: int,
    stage: str,
    turn: int,
) -> dict[str, Any]:
    urgency = str(task_spec.get("urgency") or "normal")
    risk = str(task_spec.get("risk_level") or "medium")
    priority_score = 50
    if urgency in {"high", "urgent"}:
        priority_score += 25
    if risk in {"high", "critical"}:
        priority_score += 15
    if retry_remaining <= 0:
        priority_score -= 10

    if stage == "approval":
        next_agent = "approval_agent"
        dispatch = "dispatch.approval"
        rationale = "scheduled approval gate before execution"
    else:
        next_agent = "knowledge_resolution_agent"
        dispatch = "dispatch.execution_chain"
        rationale = "scheduled approved task into execution chain"

    return _msg(
        message_type="schedule.dispatch",
        agent="scheduler_agent",
        task_id=task_id,
        turn=turn,
        inputs={"task_spec": task_spec, "retry_budget": retry_remaining, "stage": stage},
        outputs={
            "dispatch": dispatch,
            "priority_score": priority_score,
            "queue_slot": f"{stage}:{turn}",
        },
        status="ROUTED",
        rationale_brief=rationale,
        next_agent=next_agent,
    )


def _approve(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    budget: float,
    input_payload: dict[str, Any],
    turn: int,
) -> dict[str, Any]:
    estimated_cost = float(input_payload.get("estimated_cost", min(0.5, budget)))
    estimated_minutes = _coerce_int(input_payload.get("estimated_minutes", 5), 5)
    deadline_minutes = _coerce_int(input_payload.get("deadline_minutes", 0), 0)
    force_reject = _has_marker(input_payload, "force_reject")

    budget_ok = estimated_cost <= budget
    time_ok = True if deadline_minutes <= 0 else estimated_minutes <= deadline_minutes
    if force_reject:
        budget_ok = False
    decision = "APPROVED" if budget_ok and time_ok else "REJECTED"
    reason = "constraints_passed" if decision == "APPROVED" else "budget_or_time_not_met"
    return _msg(
        message_type="approval.decision",
        agent="approval_agent",
        task_id=task_id,
        turn=turn,
        inputs={
            "task_spec": task_spec,
            "budget": budget,
            "estimated_cost": estimated_cost,
            "estimated_minutes": estimated_minutes,
            "deadline_minutes": deadline_minutes,
        },
        outputs={
            "decision": decision,
            "reason": reason,
            "budget_ok": budget_ok,
            "time_ok": time_ok,
        },
        status=decision,
        rationale_brief="evaluated budget/time policy gate for task admission",
        next_agent="scheduler_agent" if decision == "APPROVED" else "coordinator",
    )


def _resolve_knowledge(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    input_payload: dict[str, Any],
    turn: int,
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    goal = str(task_spec.get("goal") or "").strip()
    if goal:
        evidence.append(
            {
                "source": "user_input.goal",
                "value": goal,
                "confidence": 0.7,
                "timestamp": int(time.time()),
            }
        )
    content = str(input_payload.get("content") or "").strip()
    if content:
        evidence.append(
            {
                "source": "user_input.content",
                "value": content[:280],
                "confidence": 0.8,
                "timestamp": int(time.time()),
            }
        )
    citations = input_payload.get("citations")
    if isinstance(citations, list):
        for item in citations[:5]:
            evidence.append(
                {
                    "source": "provided_citation",
                    "value": str(item),
                    "confidence": 0.85,
                    "timestamp": int(time.time()),
                }
            )

    knowledge_pack = {
        "evidence": evidence,
        "knowledge_gap": len(evidence) == 0,
        "missing_topics": [] if evidence else ["no_traceable_evidence"],
    }
    return _msg(
        message_type="knowledge_pack",
        agent="knowledge_resolution_agent",
        task_id=task_id,
        turn=turn,
        inputs={"task_spec": task_spec, "raw_input": input_payload},
        outputs={"knowledge_pack": knowledge_pack},
        status="READY",
        rationale_brief="assembled available evidence and flagged gaps",
        next_agent="researcher_agent",
    )


def _research(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    knowledge_pack: dict[str, Any],
    input_payload: dict[str, Any],
    turn: int,
) -> dict[str, Any]:
    if _has_marker(input_payload, "force_500"):
        raise RuntimeError("upstream timeout force_500")
    if _has_marker(input_payload, "service_unavailable"):
        raise RuntimeError("upstream unavailable service_unavailable")

    goal = str(task_spec.get("goal") or "").strip()
    evidence = list(knowledge_pack.get("evidence") or [])
    findings = [f"goal:{goal[:120]}"] if goal else []
    findings.extend(f"evidence:{str(item.get('value') or '')[:80]}" for item in evidence[:3])
    if not findings:
        findings.append("fallback:limited_context")
    research_pack = {
        "findings": findings,
        "sources": [{"source": "knowledge_pack", "count": len(evidence), "timestamp": int(time.time())}],
        "confidence": 0.72 if evidence else 0.55,
    }
    return _msg(
        message_type="research.completed",
        agent="researcher_agent",
        task_id=task_id,
        turn=turn,
        inputs={"task_spec": task_spec, "knowledge_pack": knowledge_pack},
        outputs={"research_pack": research_pack},
        status="DONE",
        rationale_brief="assembled research facts from available evidence",
        next_agent="weather_agent",
    )


def _weather(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    input_payload: dict[str, Any],
    turn: int,
) -> dict[str, Any]:
    location = str(input_payload.get("location") or input_payload.get("city") or "").strip()
    required = _has_marker(input_payload, "need_weather") or str(task_spec.get("intent") or "") in {"travel", "weather"}
    if not required:
        weather_data = {"required": False, "status": "SKIPPED", "location": location or "unspecified"}
        status = "SKIPPED"
    else:
        weather_data = {
            "required": True,
            "status": "OK",
            "location": location or "unspecified",
            "summary": "clear",
            "temperature_c": 23,
        }
        status = "DONE"
    return _msg(
        message_type="weather.completed",
        agent="weather_agent",
        task_id=task_id,
        turn=turn,
        inputs={"location": location, "task_spec": task_spec},
        outputs={"weather_data": weather_data},
        status=status,
        rationale_brief="provided weather context or explicit skip for non-weather tasks",
        next_agent="writer_agent",
    )


def _write(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    input_payload: dict[str, Any],
    research_pack: dict[str, Any],
    weather_data: dict[str, Any],
    fix_instructions: list[str],
    turn: int,
) -> dict[str, Any]:
    goal = str(task_spec.get("goal") or "").strip()
    findings = list(research_pack.get("findings") or [])
    base = str(
        input_payload.get("draft_response")
        or input_payload.get("answer")
        or input_payload.get("content")
        or goal
        or "no_content"
    ).strip()
    if findings:
        base = f"{base} | {findings[0]}"
    if bool(weather_data.get("required")) and str(weather_data.get("status") or "") == "OK":
        base = f"{base} | weather:{weather_data.get('summary')}"
    if _has_marker(input_payload, "force_400"):
        base = "policy_violation_detected"
    for token in _extract_fix_tokens(fix_instructions):
        if token and token not in base:
            base = (base + f" {token}").strip()
    return _msg(
        message_type="writing.completed",
        agent="writer_agent",
        task_id=task_id,
        turn=turn,
        inputs={
            "task_spec": task_spec,
            "research_pack": research_pack,
            "weather_data": weather_data,
            "fix_instructions": fix_instructions,
        },
        outputs={"draft_response": base, "format": "plain_text"},
        status="DONE",
        rationale_brief="generated draft response with traceable context and fixes",
        next_agent="execution_agent",
    )


def _execute(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    knowledge_pack: dict[str, Any],
    research_pack: dict[str, Any],
    weather_data: dict[str, Any],
    draft_response: str,
    turn: int,
) -> dict[str, Any]:
    evidence = list(knowledge_pack.get("evidence") or [])
    sources: list[dict[str, Any]] = []
    if evidence:
        sources.append({"kind": "knowledge_pack", "count": len(evidence)})
    if research_pack.get("sources"):
        sources.append({"kind": "research_pack", "count": len(list(research_pack.get("sources") or []))})
    if weather_data:
        sources.append({"kind": "weather_data", "status": str(weather_data.get("status") or "UNKNOWN")})
    if not sources:
        sources.append({"kind": "user_input"})
    artifacts = [
        {
            "artifact_id": f"{task_id}-turn-{turn}",
            "artifact_type": "draft_response",
            "content": draft_response,
            "sources": sources,
            "traceable": True,
        }
    ]
    tool_logs = [
        {"tool": "mas.researcher_agent", "status": "ok", "turn": turn},
        {"tool": "mas.weather_agent", "status": "ok", "turn": turn},
        {"tool": "mas.writer_agent", "status": "ok", "turn": turn},
    ]
    outputs = {
        "artifacts": artifacts,
        "tool_logs": tool_logs,
        "summary": draft_response,
        "final_output": draft_response,
        "research_pack": research_pack,
        "weather_data": weather_data,
        "draft_response": draft_response,
    }
    return _msg(
        message_type="execution_artifacts",
        agent="execution_agent",
        task_id=task_id,
        turn=turn,
        inputs={
            "task_spec": task_spec,
            "knowledge_pack": knowledge_pack,
            "research_pack": research_pack,
            "weather_data": weather_data,
            "draft_response": draft_response,
        },
        outputs=outputs,
        status="DONE",
        rationale_brief="aggregated execution artifacts from researcher/weather/writer outputs",
        next_agent="critic_agent",
    )


def _critic(
    *,
    task_id: str,
    task_spec: dict[str, Any],
    execution_result: dict[str, Any],
    input_payload: dict[str, Any],
    turn: int,
) -> dict[str, Any]:
    artifacts = list(execution_result.get("artifacts") or [])
    combined = " ".join(str(artifact.get("content") or "") for artifact in artifacts).strip()
    failure_type = ""
    fix_instructions: list[str] = []

    if _has_marker(input_payload, "force_400"):
        failure_type = "POLICY_VIOLATION"
        fix_instructions.append("remove policy violating instruction")
    elif not combined:
        failure_type = "QUALITY_GAP"
        fix_instructions.append("provide non-empty draft_response")
    else:
        for criterion in _coerce_list_of_strings(task_spec.get("success_criteria"), default=["non_empty_output"]):
            if criterion == "non_empty_output" and not combined:
                failure_type = "QUALITY_GAP"
                fix_instructions.append("provide non-empty draft_response")
                break
            if criterion == "traceable_artifact":
                has_traceable = bool(artifacts) and all(bool(item.get("sources")) for item in artifacts)
                if not has_traceable:
                    failure_type = "QUALITY_GAP"
                    fix_instructions.append("attach at least one traceable source")
                    break
            token = _extract_token_from_criterion(criterion)
            if token and token not in combined:
                failure_type = "QUALITY_GAP"
                fix_instructions.append(f"append token={token}")
                break

    if failure_type:
        retryable = failure_type in RETRYABLE_FAILURE_TYPES
        return _msg(
            message_type="quality_verdict",
            agent="critic_agent",
            task_id=task_id,
            turn=turn,
            inputs={"task_spec": task_spec, "execution_result": execution_result},
            outputs={
                "verdict": "FAIL",
                "failure_type": failure_type,
                "retryable": retryable,
                "fix_instructions": fix_instructions,
            },
            status="FAIL",
            rationale_brief="critic found verifiable quality/policy issues",
            next_agent="planner_agent" if retryable else "coordinator",
        )

    return _msg(
        message_type="quality_verdict",
        agent="critic_agent",
        task_id=task_id,
        turn=turn,
        inputs={"task_spec": task_spec, "execution_result": execution_result},
        outputs={
            "verdict": "PASS",
            "failure_type": None,
            "retryable": False,
            "fix_instructions": [],
            "final_output": str(execution_result.get("final_output") or execution_result.get("summary") or ""),
        },
        status="PASS",
        rationale_brief="critic validated output against constraints and success criteria",
        next_agent="coordinator",
    )


def _handle_retryable_execution_error(
    state: MASState,
    *,
    stage_name: str,
    exc: Exception,
) -> MASState:
    failure_type = _classify_execution_exception(exc)
    task_spec = dict(state.get("task_spec") or {})
    knowledge_pack = dict(state.get("knowledge_pack") or {})
    fix_instructions = list(state.get("fix_instructions") or [])
    turn = int(state.get("turn") or 1)
    task_id = str(state.get("task_id") or "")
    error_msg = _msg(
        message_type="execution_artifacts",
        agent="execution_agent",
        task_id=task_id,
        turn=turn,
        inputs={
            "task_spec": task_spec,
            "knowledge_pack": knowledge_pack,
            "fix_instructions": fix_instructions,
        },
        outputs={"error": str(exc), "failure_type": failure_type, "stage": stage_name},
        status="ERROR",
        rationale_brief="execution failed with external/transient signal",
        next_agent="planner_agent",
    )
    try:
        _append_protocol_message(state, node_name=stage_name, output_slot="execution_error_output", message=error_msg)
    except Exception as protocol_exc:
        return _set_protocol_error(state, node_name=stage_name, exc=protocol_exc)

    retry_budget = dict(state.get("retry_budget") or {"remaining": 0, "max": 0})
    remaining = _coerce_int(retry_budget.get("remaining"), 0, minimum=0)
    if remaining > 0:
        retry_budget["remaining"] = remaining - 1
        state["retry_budget"] = retry_budget
        state["plan_state"] = "REPLANNING"
        state["task_state"] = "RETRYING"
        state["status"] = "RETRYING"
        state["failure_type"] = failure_type
        state["failure_semantic"] = "FAIL_RETRYABLE"
        state["stop_reason"] = f"{stage_name} raised retryable error: {failure_type}"
        state["fix_instructions"] = [f"stabilize execution path token={failure_type}"]
        state["turn"] = int(state.get("turn") or 1) + 1
        metrics = _ensure_metrics(state)
        metrics["retry_count"] = int(metrics.get("retry_count") or 0) + 1
        state["metrics"] = metrics
        state["next_route"] = "planner"
        return state

    _set_terminal_failure(
        state,
        failure_type="RETRY_BUDGET_EXHAUSTED",
        semantic="FAIL_FINAL",
        stop_reason=f"retryable failure exhausted budget; root={failure_type}",
    )
    state["next_route"] = "terminate_fail"
    return state


def _route_or_default(state: MASState, default: str) -> str:
    route = str(state.get("next_route") or "").strip()
    return route or default


def _perceptor_node(state: MASState) -> MASState:
    _record_node_call(state, "perceptor")
    state["phase"] = "PERCEIVE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _perceive(
            task_id=str(state.get("task_id") or ""),
            task_type=str(state.get("task_type") or ""),
            input_payload=dict(state.get("input_payload") or {}),
            risk_level=str(state.get("risk_level") or "medium"),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="perceptor", output_slot="perceptor_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="perceptor", exc=exc)

    task_spec = dict((message.get("outputs") or {}).get("task_spec") or {})
    state["task_spec"] = task_spec
    state["risk_level"] = str(task_spec.get("risk_level") or state.get("risk_level") or "medium")
    state["task_state"] = "SPECIFIED"

    if str(message.get("status") or "") == "NEED_INFO":
        state["task_state"] = "NEED_INFO"
        state["status"] = "NEED_INFO"
        state["verdict"] = "NEED_INFO"
        state["failure_type"] = "NEED_INFO"
        state["failure_semantic"] = "FAIL_FINAL"
        state["stop_reason"] = "missing required task inputs"
        state["next_route"] = "ask_user"
        return state

    if not task_spec.get("goal"):
        _set_terminal_failure(
            state,
            failure_type="INVALID_TASK_SPEC",
            semantic="FAIL_FINAL",
            stop_reason="goal missing after perception",
        )
        state["next_route"] = "terminate_fail"
        return state

    state["status"] = "IN_PROGRESS"
    state["next_route"] = "planner"
    return state


def _planner_node(state: MASState) -> MASState:
    _record_node_call(state, "planner")
    state["phase"] = "PLAN"
    if not _update_latency_budget(state):
        return state
    try:
        message = _plan(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            fix_instructions=list(state.get("fix_instructions") or []),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="planner", output_slot="planner_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="planner", exc=exc)

    outputs = dict(message.get("outputs") or {})
    state["plan"] = list(outputs.get("plan") or [])
    state["plan_meta"] = dict(outputs.get("plan_meta") or {})
    state["plan_state"] = "PLANNED"
    state["next_route"] = "scheduler1"
    return state


def _scheduler1_node(state: MASState) -> MASState:
    _record_node_call(state, "scheduler1")
    state["phase"] = "PLAN"
    if not _update_latency_budget(state):
        return state
    retry_remaining = _coerce_int(dict(state.get("retry_budget") or {}).get("remaining"), 0, minimum=0)
    try:
        message = _schedule(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            retry_remaining=retry_remaining,
            stage="approval",
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="scheduler1", output_slot="scheduler1_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="scheduler1", exc=exc)
    state["next_route"] = "approval"
    return state


def _approval_node(state: MASState) -> MASState:
    _record_node_call(state, "approval")
    state["phase"] = "PLAN"
    if not _update_latency_budget(state):
        return state
    try:
        message = _approve(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            budget=float(state.get("budget") or 1.0),
            input_payload=dict(state.get("input_payload") or {}),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="approval", output_slot="approval_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="approval", exc=exc)

    decision = str((message.get("outputs") or {}).get("decision") or "")
    state["approval_decision"] = decision
    if decision != "APPROVED":
        _set_terminal_failure(
            state,
            failure_type="CONSTRAINT_VIOLATION",
            semantic="FAIL_FINAL",
            stop_reason="approval agent rejected task",
        )
        state["next_route"] = "terminate_fail"
        return state

    state["next_route"] = "scheduler2"
    return state


def _scheduler2_node(state: MASState) -> MASState:
    _record_node_call(state, "scheduler2")
    state["phase"] = "PLAN"
    if not _update_latency_budget(state):
        return state
    retry_remaining = _coerce_int(dict(state.get("retry_budget") or {}).get("remaining"), 0, minimum=0)
    try:
        message = _schedule(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            retry_remaining=retry_remaining,
            stage="execution",
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="scheduler2", output_slot="scheduler2_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="scheduler2", exc=exc)
    state["next_route"] = "knowledge"
    return state


def _knowledge_node(state: MASState) -> MASState:
    _record_node_call(state, "knowledge")
    state["phase"] = "KNOWLEDGE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _resolve_knowledge(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            input_payload=dict(state.get("input_payload") or {}),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="knowledge", output_slot="knowledge_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="knowledge", exc=exc)

    knowledge_pack = dict((message.get("outputs") or {}).get("knowledge_pack") or {})
    state["knowledge_pack"] = knowledge_pack
    state["evidence"] = list(knowledge_pack.get("evidence") or [])
    state["next_route"] = "researcher"
    return state


def _researcher_node(state: MASState) -> MASState:
    _record_node_call(state, "researcher")
    state["phase"] = "EXECUTE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _research(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            knowledge_pack=dict(state.get("knowledge_pack") or {}),
            input_payload=dict(state.get("input_payload") or {}),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="researcher", output_slot="researcher_output", message=message)
    except RuntimeError as exc:
        return _handle_retryable_execution_error(state, stage_name="researcher", exc=exc)
    except Exception as exc:
        return _set_protocol_error(state, node_name="researcher", exc=exc)

    state["research_pack"] = dict((message.get("outputs") or {}).get("research_pack") or {})
    state["next_route"] = "weather"
    return state


def _weather_node(state: MASState) -> MASState:
    _record_node_call(state, "weather")
    state["phase"] = "EXECUTE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _weather(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            input_payload=dict(state.get("input_payload") or {}),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="weather", output_slot="weather_output", message=message)
    except RuntimeError as exc:
        return _handle_retryable_execution_error(state, stage_name="weather", exc=exc)
    except Exception as exc:
        return _set_protocol_error(state, node_name="weather", exc=exc)

    state["weather_data"] = dict((message.get("outputs") or {}).get("weather_data") or {})
    state["next_route"] = "writer"
    return state


def _writer_node(state: MASState) -> MASState:
    _record_node_call(state, "writer")
    state["phase"] = "EXECUTE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _write(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            input_payload=dict(state.get("input_payload") or {}),
            research_pack=dict(state.get("research_pack") or {}),
            weather_data=dict(state.get("weather_data") or {}),
            fix_instructions=list(state.get("fix_instructions") or []),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="writer", output_slot="writer_output", message=message)
    except RuntimeError as exc:
        return _handle_retryable_execution_error(state, stage_name="writer", exc=exc)
    except Exception as exc:
        return _set_protocol_error(state, node_name="writer", exc=exc)

    state["draft_response"] = str((message.get("outputs") or {}).get("draft_response") or "")
    state["next_route"] = "execution"
    return state


def _execution_node(state: MASState) -> MASState:
    _record_node_call(state, "execution")
    state["phase"] = "EXECUTE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _execute(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            knowledge_pack=dict(state.get("knowledge_pack") or {}),
            research_pack=dict(state.get("research_pack") or {}),
            weather_data=dict(state.get("weather_data") or {}),
            draft_response=str(state.get("draft_response") or ""),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="execution", output_slot="execution_output", message=message)
    except RuntimeError as exc:
        return _handle_retryable_execution_error(state, stage_name="execution", exc=exc)
    except Exception as exc:
        return _set_protocol_error(state, node_name="execution", exc=exc)

    state["execution_result"] = dict(message.get("outputs") or {})
    state["next_route"] = "critic"
    return state


def _critic_node(state: MASState) -> MASState:
    _record_node_call(state, "critic")
    state["phase"] = "EVALUATE"
    if not _update_latency_budget(state):
        return state
    try:
        message = _critic(
            task_id=str(state.get("task_id") or ""),
            task_spec=dict(state.get("task_spec") or {}),
            execution_result=dict(state.get("execution_result") or {}),
            input_payload=dict(state.get("input_payload") or {}),
            turn=int(state.get("turn") or 1),
        )
        _append_protocol_message(state, node_name="critic", output_slot="critic_output", message=message)
    except Exception as exc:
        return _set_protocol_error(state, node_name="critic", exc=exc)

    outputs = dict(message.get("outputs") or {})
    verdict = str(outputs.get("verdict") or "FAIL")
    state["verdict"] = verdict
    if verdict == "PASS":
        state["status"] = "SUCCEEDED"
        state["task_state"] = "SUCCEEDED"
        state["plan_state"] = "STABLE"
        state["stop_reason"] = "critic_pass"
        state["failure_type"] = ""
        state["failure_semantic"] = "PASS"
        state["next_route"] = "terminate_pass"
        return state

    failure_type = str(outputs.get("failure_type") or "QUALITY_GAP")
    retryable = bool(outputs.get("retryable"))
    fix_instructions = [str(item) for item in (outputs.get("fix_instructions") or []) if str(item).strip()]
    state["fix_instructions"] = fix_instructions
    if retryable:
        retry_budget = dict(state.get("retry_budget") or {"remaining": 0, "max": 0})
        remaining = _coerce_int(retry_budget.get("remaining"), 0, minimum=0)
        if remaining > 0:
            retry_budget["remaining"] = remaining - 1
            state["retry_budget"] = retry_budget
            state["plan_state"] = "REPLANNING"
            state["task_state"] = "RETRYING"
            state["status"] = "RETRYING"
            state["failure_type"] = failure_type
            state["failure_semantic"] = "FAIL_RETRYABLE"
            state["stop_reason"] = f"critic requested retry; failure_type={failure_type}"
            state["turn"] = int(state.get("turn") or 1) + 1
            metrics = _ensure_metrics(state)
            metrics["retry_count"] = int(metrics.get("retry_count") or 0) + 1
            failure_streak = int(metrics.get("failure_streak") or 0) + 1
            metrics["failure_streak"] = failure_streak
            if failure_streak >= 3:
                metrics["loop_oscillation_count"] = int(metrics.get("loop_oscillation_count") or 0) + 1
                _set_terminal_failure(
                    state,
                    failure_type="RETRY_BUDGET_EXHAUSTED",
                    semantic="FAIL_FINAL",
                    stop_reason="loop_oscillation_detected: failure streak reached 3",
                )
                state["next_route"] = "terminate_fail"
            else:
                state["next_route"] = "planner"
            state["metrics"] = metrics
            return state

        _set_terminal_failure(
            state,
            failure_type="RETRY_BUDGET_EXHAUSTED",
            semantic="FAIL_FINAL",
            stop_reason=f"critic requested retry but budget exhausted; root={failure_type}",
        )
        state["next_route"] = "terminate_fail"
        return state

    _set_terminal_failure(
        state,
        failure_type=failure_type,
        semantic="FAIL_FINAL",
        stop_reason="non-retryable critic verdict",
    )
    state["next_route"] = "terminate_fail"
    return state


def _ask_user_node(state: MASState) -> MASState:
    _record_node_call(state, "ask_user")
    state["phase"] = "ASK_USER"
    state["status"] = "FAILED_FINAL"
    state["task_state"] = "NEED_INFO"
    state["verdict"] = "NEED_INFO"
    state["failure_type"] = "NEED_INFO"
    state["failure_semantic"] = "FAIL_FINAL"
    if not str(state.get("stop_reason") or "").strip():
        state["stop_reason"] = "insufficient inputs; ask user for required fields"
    state["next_route"] = "end"
    return state


def _terminate_pass_node(state: MASState) -> MASState:
    _record_node_call(state, "terminate_pass")
    state["phase"] = "TERMINATE"
    state["status"] = "SUCCEEDED"
    state["verdict"] = "PASS"
    state["failure_type"] = ""
    state["failure_semantic"] = "PASS"
    state["next_route"] = "end"
    return state


def _terminate_fail_node(state: MASState) -> MASState:
    _record_node_call(state, "terminate_fail")
    state["phase"] = "TERMINATE"
    failure_type = str(state.get("failure_type") or "ORCHESTRATION_RUNTIME_ERROR")
    semantic = _semantic_for_failure(failure_type, str(state.get("failure_semantic") or "FAIL_RETRYABLE"))
    state["failure_type"] = failure_type
    state["failure_semantic"] = semantic
    if semantic == "FAIL_FINAL":
        state["status"] = "FAILED_FINAL"
        state["task_state"] = "FAILED_FINAL"
    else:
        state["status"] = "FAILED_RETRYABLE"
        state["task_state"] = "FAILED_RETRYABLE"
    if not str(state.get("stop_reason") or "").strip():
        state["stop_reason"] = f"terminated with failure_type={failure_type}"
    state["verdict"] = "FAIL"
    state["next_route"] = "end"
    return state


def _runtime_wrapped_node(node_name: str, handler: Any) -> Any:
    def _wrapped(state: MASState) -> MASState:
        next_state = handler(state)
        return _sync_agent_runtime(next_state, event_type=f"mas.{node_name}")

    return _wrapped


@lru_cache(maxsize=1)
def _compiled_graph() -> Any:
    graph = StateGraph(MASState)
    graph.add_node("perceptor", _runtime_wrapped_node("perceptor", _perceptor_node))
    graph.add_node("planner", _runtime_wrapped_node("planner", _planner_node))
    graph.add_node("scheduler1", _runtime_wrapped_node("scheduler1", _scheduler1_node))
    graph.add_node("approval", _runtime_wrapped_node("approval", _approval_node))
    graph.add_node("scheduler2", _runtime_wrapped_node("scheduler2", _scheduler2_node))
    graph.add_node("knowledge", _runtime_wrapped_node("knowledge", _knowledge_node))
    graph.add_node("researcher", _runtime_wrapped_node("researcher", _researcher_node))
    graph.add_node("weather", _runtime_wrapped_node("weather", _weather_node))
    graph.add_node("writer", _runtime_wrapped_node("writer", _writer_node))
    graph.add_node("execution", _runtime_wrapped_node("execution", _execution_node))
    graph.add_node("critic", _runtime_wrapped_node("critic", _critic_node))
    graph.add_node("ask_user", _runtime_wrapped_node("ask_user", _ask_user_node))
    graph.add_node("terminate_pass", _runtime_wrapped_node("terminate_pass", _terminate_pass_node))
    graph.add_node("terminate_fail", _runtime_wrapped_node("terminate_fail", _terminate_fail_node))

    graph.set_entry_point("perceptor")
    graph.add_conditional_edges(
        "perceptor",
        lambda state: _route_or_default(state, "planner"),
        {"planner": "planner", "ask_user": "ask_user", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "planner",
        lambda state: _route_or_default(state, "scheduler1"),
        {"scheduler1": "scheduler1", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "scheduler1",
        lambda state: _route_or_default(state, "approval"),
        {"approval": "approval", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "approval",
        lambda state: _route_or_default(state, "scheduler2"),
        {"scheduler2": "scheduler2", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "scheduler2",
        lambda state: _route_or_default(state, "knowledge"),
        {"knowledge": "knowledge", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "knowledge",
        lambda state: _route_or_default(state, "researcher"),
        {"researcher": "researcher", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "researcher",
        lambda state: _route_or_default(state, "weather"),
        {"weather": "weather", "planner": "planner", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "weather",
        lambda state: _route_or_default(state, "writer"),
        {"writer": "writer", "planner": "planner", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "writer",
        lambda state: _route_or_default(state, "execution"),
        {"execution": "execution", "planner": "planner", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "execution",
        lambda state: _route_or_default(state, "critic"),
        {"critic": "critic", "planner": "planner", "terminate_fail": "terminate_fail"},
    )
    graph.add_conditional_edges(
        "critic",
        lambda state: _route_or_default(state, "terminate_fail"),
        {"terminate_pass": "terminate_pass", "planner": "planner", "terminate_fail": "terminate_fail"},
    )
    graph.add_edge("ask_user", END)
    graph.add_edge("terminate_pass", END)
    graph.add_edge("terminate_fail", END)

    return graph.compile()


def _activity_state_snapshot(state: MASState) -> dict[str, Any]:
    return {
        "task_state": str(state.get("task_state") or ""),
        "plan_state": str(state.get("plan_state") or ""),
        "evidence": list(state.get("evidence") or []),
        "risk_level": str(state.get("risk_level") or ""),
        "retry_budget": dict(state.get("retry_budget") or {"remaining": 0, "max": 0}),
        "latency_budget": dict(state.get("latency_budget") or {"remaining_ms": 0, "max_ms": 0}),
        "turn": int(state.get("turn") or 1),
        "phase": str(state.get("phase") or ""),
        "status": str(state.get("status") or ""),
        "verdict": str(state.get("verdict") or ""),
        "stop_reason": str(state.get("stop_reason") or ""),
        "next_route": str(state.get("next_route") or ""),
        "trace_id": str(state.get("trace_id") or ""),
        "task_spec": dict(state.get("task_spec") or {}),
        "plan": list(state.get("plan") or []),
        "approval_decision": str(state.get("approval_decision") or ""),
        "fix_instructions": list(state.get("fix_instructions") or []),
        "metrics": dict(state.get("metrics") or {}),
        "msgs": list(state.get("msgs") or []),
        "agent_runtime": dict(state.get("agent_runtime") or {}),
    }


def _to_activity_result(state: MASState) -> dict[str, Any]:
    task_id = str(state.get("task_id") or "")
    run_id = str(state.get("run_id") or "")
    turn = int(state.get("turn") or 1)
    status = str(state.get("status") or "FAILED_RETRYABLE")
    protocol_messages = list(state.get("msgs") or [])
    snapshot = _activity_state_snapshot(state)

    if status == "SUCCEEDED":
        execution_result = dict(state.get("execution_result") or {})
        critic_output = dict(state.get("critic_output") or {})
        final_output = str(
            critic_output.get("final_output")
            or execution_result.get("final_output")
            or execution_result.get("summary")
            or ""
        )
        return {
            "status": "SUCCEEDED",
            "task_id": task_id,
            "run_id": run_id,
            "turn": turn,
            "state": snapshot,
            "protocol_messages": protocol_messages,
            "failure_type": None,
            "failure_semantic": "PASS",
            "result": {
                "output": final_output,
                "artifacts": execution_result.get("artifacts", []),
                "tool_logs": execution_result.get("tool_logs", []),
                "evidence": snapshot["evidence"],
                "task_spec": dict(state.get("task_spec") or {}),
                "turn": turn,
            },
        }

    failure_type = str(state.get("failure_type") or "ORCHESTRATION_RUNTIME_ERROR")
    failure_semantic = _semantic_for_failure(failure_type, str(state.get("failure_semantic") or "FAIL_RETRYABLE"))
    failed_status = "FAILED_FINAL" if failure_semantic == "FAIL_FINAL" else "FAILED_RETRYABLE"
    return {
        "status": failed_status,
        "task_id": task_id,
        "run_id": run_id,
        "turn": turn,
        "state": snapshot,
        "protocol_messages": protocol_messages,
        "failure_type": failure_type,
        "failure_semantic": failure_semantic,
        "reason": str(state.get("stop_reason") or ""),
        "result": None,
    }


async def run_closed_loop_graph(
    *,
    payload: dict[str, Any],
    default_retry_budget: int = 1,
    default_latency_budget_ms: int = 20000,
) -> dict[str, Any]:
    state = _initialize_state(
        payload,
        default_retry_budget=max(0, int(default_retry_budget)),
        default_latency_budget_ms=max(1000, int(default_latency_budget_ms)),
    )
    graph = _compiled_graph()
    final_state = await graph.ainvoke(state)
    return _to_activity_result(final_state)
