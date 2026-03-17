from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

ACTION_TYPES = (
    "ask_user",
    "retrieve",
    "tool_call",
    "workflow_call",
    "approval_request",
    "wait",
    "reflect",
    "replan",
    "respond",
)

COMPLEX_HINTS = {"workflow", "research", "report", "summary", "analyze", "analysis", "investigate", "prepare"}
TOOL_PREPARATION_ACTIONS = {"tool_call", "workflow_call", "approval_request"}
IN_PROGRESS_STATUSES = {"QUEUED", "VALIDATING", "PLANNING", "RUNNING", "WAITING_TOOL", "REVIEWING", "IN_PROGRESS"}
WAITING_STATUSES = {"WAITING_HUMAN", "WAITING_APPROVAL"}
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED_FINAL", "FAILED_RETRYABLE", "CANCELLED", "TIMED_OUT"}
LEGACY_ACTION_TO_RUNTIME_ACTION = {
    "answer_only": "respond",
    "use_tool": "tool_call",
    "use_retrieval": "retrieve",
    "start_workflow": "workflow_call",
    "need_approval": "approval_request",
}


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _goal_requests_durable_runtime(goal: dict[str, Any]) -> bool:
    normalized_goal = str(goal.get("normalized_goal") or "").strip().lower()
    if not normalized_goal:
        return False
    durable_markers = (
        "durable runtime",
        "continue through the durable runtime",
        "continue through durable runtime",
        "durable workflow",
        "continue in workflow",
        "continue through workflow",
        "run as workflow",
        "use workflow",
    )
    return any(marker in normalized_goal for marker in durable_markers)


def _normalized_memory_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")


def _external_source_reliability_lookup(
    reliability: dict[str, Any],
    *,
    source: str,
    event_topic: str,
) -> dict[str, Any]:
    normalized_source = _normalized_memory_key(source) or "external_signal"
    normalized_topic = _normalized_memory_key(event_topic)
    if normalized_topic:
        topic_key = f"{normalized_source}:topic:{normalized_topic}"
        topic_reliability = _as_dict(reliability.get(topic_key))
        if topic_reliability:
            return {"key": topic_key, **topic_reliability}
    source_reliability = _as_dict(reliability.get(normalized_source))
    if source_reliability:
        return {"key": normalized_source, **source_reliability}
    return {}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _build_action_contract(
    action_type: str,
    *,
    fallback_action: str | None,
    target: str | None = None,
    requires_approval: bool = False,
) -> dict[str, Any]:
    normalized_action = str(action_type or "respond")
    normalized_fallback = str(fallback_action or "respond") or "respond"
    target_label = str(target or "").strip()
    if normalized_action == "ask_user":
        expected_result = "The user clarifies the missing part of the goal."
        success_conditions = ["user_message_received", "unknowns_reduced"]
        stop_conditions = ["clarification_timeout", "goal_abandoned"]
    elif normalized_action == "retrieve":
        expected_result = "Grounding evidence is added to runtime observations."
        success_conditions = ["retrieval_hits_recorded", "grounding_observation_available"]
        stop_conditions = ["retrieval_exhausted", "budget_exhausted"]
    elif normalized_action == "tool_call":
        expected_result = (
            f"Tool `{target_label}` returns an actionable observation." if target_label else "The selected tool returns an actionable observation."
        )
        success_conditions = ["tool_call_succeeded", "observation_recorded"]
        stop_conditions = ["tool_call_failed_final", "approval_rejected" if requires_approval else "tool_budget_exhausted"]
    elif normalized_action == "workflow_call":
        expected_result = "The durable runtime advances the goal and emits a new observation or final result."
        success_conditions = ["workflow_progress_observed", "runtime_state_advanced"]
        stop_conditions = ["workflow_failed_final", "workflow_cancelled", "budget_exhausted"]
    elif normalized_action == "approval_request":
        expected_result = "A governed approval decision is recorded for the next risky action."
        success_conditions = ["approval_decision_received"]
        stop_conditions = ["approval_rejected", "approval_timeout"]
    elif normalized_action == "wait":
        expected_result = "The runtime pauses until the required event, approval, or user input arrives."
        success_conditions = ["resume_event_received", "pending_condition_resolved"]
        stop_conditions = ["wait_timeout", "goal_cancelled"]
    elif normalized_action == "reflect":
        expected_result = "The latest observation is interpreted and the runtime records what to do next."
        success_conditions = ["reflection_recorded", "next_action_inferred"]
        stop_conditions = ["reflection_budget_exhausted"]
    elif normalized_action == "replan":
        expected_result = "A safer or more promising next action is selected."
        success_conditions = ["replacement_action_selected", "policy_updated"]
        stop_conditions = ["replan_budget_exhausted", "no_safe_fallback"]
    else:
        expected_result = "A final user-facing response is produced."
        success_conditions = ["final_output_emitted", "goal_answered"]
        stop_conditions = ["response_blocked", "missing_grounding_evidence"]
    return {
        "expected_result": expected_result,
        "success_conditions": success_conditions,
        "fallback": normalized_fallback,
        "stop_conditions": stop_conditions,
    }


def _candidate_reason(
    candidate: str,
    *,
    selected_action: str,
    selected_rationale: str,
    available_actions: list[str],
    unknowns: list[str],
    has_retrieval_observation: bool,
    tool_candidates: list[dict[str, Any]],
    top_requires_approval: bool,
    confirmed: bool,
    requested_mode: str | None,
    latest_result: dict[str, Any] | None,
) -> str:
    if candidate == selected_action:
        return selected_rationale or "Selected by the runtime policy."
    if candidate not in available_actions and candidate not in {"approval_request", "wait", "reflect", "replan"}:
        return "Not selected because this action is not currently available in task_state."
    if candidate == "ask_user":
        if "ambiguous_user_reference" in unknowns:
            return "Not selected because the runtime found a stronger execution path than pausing for clarification."
        return "Not selected because the goal no longer requires user-owned clarification."
    if candidate == "retrieve":
        if has_retrieval_observation:
            return "Not selected because grounding evidence is already available."
        return "Not selected because policy judged that additional retrieval was unnecessary before continuing."
    if candidate == "tool_call":
        if not tool_candidates:
            return "Not selected because no direct tool candidate is available."
        if top_requires_approval and not confirmed:
            return "Not selected because the fastest tool path is still waiting on approval."
        return "Not selected because policy preferred a safer or more durable path."
    if candidate == "workflow_call":
        if requested_mode == "tool_task":
            return "Not selected because the request is currently constrained toward the direct tool lane."
        return "Not selected because durable execution was not the best next move for the current state."
    if candidate == "approval_request":
        if top_requires_approval and not confirmed:
            return "Not selected because another governed action already captured the approval-needed path."
        return "Not selected because the chosen action does not currently need explicit approval."
    if candidate == "wait":
        return "Not selected because the runtime is not currently blocked on an external event."
    if candidate == "reflect":
        return "Not selected because there is no fresh observation that must be interpreted first."
    if candidate == "replan":
        normalized_status = str((latest_result or {}).get("status") or "").upper()
        if normalized_status in {"FAILED_RETRYABLE", "TIMED_OUT"} or (latest_result or {}).get("status") == "retryable_tool_failure":
            return "Not selected because another action already resolved the retryable condition."
        return "Not selected because the runtime has not yet hit a blocker that requires replanning."
    return "Not selected because the current context already supports a better action."


def _build_decision_candidates(
    *,
    selected_action: str,
    selected_rationale: str,
    available_actions: list[str],
    unknowns: list[str],
    has_retrieval_observation: bool,
    tool_candidates: list[dict[str, Any]],
    planner_signal: dict[str, Any],
    top_requires_approval: bool,
    confirmed: bool,
    requested_mode: str | None,
    latest_result: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    candidate_names: list[str] = []
    for action_name in list(available_actions) + list(ACTION_TYPES):
        normalized_name = str(action_name or "").strip()
        if normalized_name and normalized_name not in candidate_names:
            candidate_names.append(normalized_name)
    candidates: list[dict[str, Any]] = []
    why_not: dict[str, str] = {}
    affinities = _as_dict(planner_signal.get("action_affinities"))
    for candidate in candidate_names:
        reason = _candidate_reason(
            candidate,
            selected_action=selected_action,
            selected_rationale=selected_rationale,
            available_actions=available_actions,
            unknowns=unknowns,
            has_retrieval_observation=has_retrieval_observation,
            tool_candidates=tool_candidates,
            top_requires_approval=top_requires_approval,
            confirmed=confirmed,
            requested_mode=requested_mode,
            latest_result=latest_result,
        )
        disposition = "selected" if candidate == selected_action else "deferred"
        score_hint = affinities.get(candidate)
        candidates.append(
            {
                "action_type": candidate,
                "disposition": disposition,
                "reason": reason,
                "score_hint": _safe_float(score_hint, 0.0) if score_hint is not None else None,
            }
        )
        if candidate != selected_action:
            why_not[candidate] = reason
    return candidates, why_not


def _goal_age_minutes(goal_row: dict[str, Any]) -> float:
    updated_at = _parse_datetime(goal_row.get("updated_at"))
    created_at = _parse_datetime(goal_row.get("created_at"))
    anchor = updated_at or created_at
    if anchor is None:
        return 0.0
    now = datetime.now(timezone.utc)
    return max(0.0, (now - anchor).total_seconds() / 60.0)


def score_goal_portfolio_entry(
    goal_row: dict[str, Any],
    *,
    active_goal_count: int = 0,
    max_active_goals: int = 4,
    soft_preempt_threshold: float = 0.88,
    portfolio_bias_override: dict[str, Any] | None = None,
    policy_memory_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    goal_state = _as_dict(goal_row.get("goal_state"))
    agenda = _as_dict(goal_state.get("agenda"))
    policy = _as_dict(goal_state.get("policy"))
    base_policy_memory = _as_dict(policy.get("policy_memory"))
    override_policy_memory = _as_dict(policy_memory_override)
    policy_memory = {
        **base_policy_memory,
        **override_policy_memory,
        "portfolio_bias": {
            **_as_dict(base_policy_memory.get("portfolio_bias")),
            **_as_dict(override_policy_memory.get("portfolio_bias")),
        },
        "portfolio_learning": {
            **_as_dict(base_policy_memory.get("portfolio_learning")),
            **_as_dict(override_policy_memory.get("portfolio_learning")),
        },
        "external_source_reliability": {
            **_as_dict(base_policy_memory.get("external_source_reliability")),
            **_as_dict(override_policy_memory.get("external_source_reliability")),
        },
    }
    portfolio_bias = _as_dict(portfolio_bias_override) or _as_dict(policy_memory.get("portfolio_bias"))
    portfolio_learning = _as_dict(policy_memory.get("portfolio_learning"))
    external_source_reliability = _as_dict(policy_memory.get("external_source_reliability"))
    wake_condition = _as_dict(goal_state.get("wake_condition"))
    active_subgoal = _as_dict(goal_state.get("active_subgoal"))

    base_priority = min(1.0, max(0.0, _safe_float(agenda.get("priority_score"), 0.0)))
    age_minutes = _goal_age_minutes(goal_row)
    continuation_count = max(0, int(goal_row.get("continuation_count") or 0))
    waiting_event_count = len(_as_list(_as_dict(goal_state.get("wake_graph")).get("waiting_events")))
    ready_count = max(0, int(agenda.get("ready_count") or len(_as_list(goal_state.get("ready_subgoals")))))
    blocked_count = max(0, int(agenda.get("blocked_count") or len(_as_list(goal_state.get("blocked_subgoals")))))

    selected_action = str(policy.get("selected_action") or agenda.get("selected_action") or "")
    wake_kind = str(wake_condition.get("kind") or "")
    active_kind = str(active_subgoal.get("kind") or agenda.get("active_subgoal_kind") or "planned")
    dynamic_boost = 0.12 if active_kind == "dynamic" else 0.0
    if selected_action == "replan":
        dynamic_boost += 0.06
    if selected_action == "workflow_call":
        dynamic_boost += 0.03

    stalled_bias = max(0.0, _safe_float(portfolio_bias.get("stalled_goal_boost")))
    dynamic_bias = max(0.0, _safe_float(portfolio_bias.get("dynamic_subgoal_boost")))
    replan_bias = max(0.0, _safe_float(portfolio_bias.get("replan_goal_boost")))
    continuation_bias = max(0.0, _safe_float(portfolio_bias.get("continuation_penalty")))
    scheduler_confidence = max(0.0, min(1.0, _safe_float(portfolio_learning.get("scheduler_confidence"))))
    preempt_regret_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("preempt_regret_rate"))))
    preempt_success_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("preempt_success_rate"))))
    starvation_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("starvation_rate"))))
    subscription_timeout_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("subscription_timeout_rate"))))
    external_wait_success_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("external_wait_success_rate"))))
    external_wait_failure_rate = max(0.0, min(1.0, _safe_float(portfolio_learning.get("external_wait_failure_rate"))))
    portfolio_throughput_score = max(0.0, min(1.0, _safe_float(portfolio_learning.get("portfolio_throughput_score"))))
    external_source = str(wake_condition.get("source") or "")
    external_topic = str(wake_condition.get("event_topic") or "")
    source_reliability = _external_source_reliability_lookup(
        external_source_reliability,
        source=external_source,
        event_topic=external_topic,
    )
    source_reliability_score = _safe_float(source_reliability.get("score"))
    source_reliability_confidence = _safe_float(source_reliability.get("confidence"))

    staleness_boost = min(0.22, (age_minutes / 60.0) * 0.06 + stalled_bias * 0.01 + starvation_rate * 0.06)
    ready_boost = min(0.14, ready_count * 0.03)
    blocked_boost = min(0.12, blocked_count * 0.02)
    waiting_penalty = min(0.18, waiting_event_count * 0.02) if wake_kind in {"user_message", "approval", "task_completion", "external_signal"} else 0.0
    continuation_penalty = min(0.2, continuation_count * 0.025 + continuation_bias * 0.01)
    learning_boost = min(
        0.1,
        scheduler_confidence * 0.04
        + preempt_success_rate * 0.02
        + portfolio_throughput_score * 0.03
        + external_wait_success_rate * 0.01,
    )
    regret_penalty = min(0.08, preempt_regret_rate * 0.08)
    external_wait_penalty = 0.0
    if wake_kind == "external_signal" and source_reliability_confidence >= 0.25 and source_reliability_score < 0.0:
        external_wait_penalty = min(0.08, abs(source_reliability_score) * 0.08)
    if wake_kind == "external_signal":
        external_wait_penalty += min(0.08, subscription_timeout_rate * 0.05 + external_wait_failure_rate * 0.04)
    external_wait_boost = 0.0
    if wake_kind == "external_signal" and source_reliability_confidence >= 0.25 and source_reliability_score > 0.0:
        external_wait_boost = min(0.04, source_reliability_score * 0.04)
    if wake_kind == "external_signal":
        external_wait_boost += min(0.05, external_wait_success_rate * 0.04)

    score = base_priority
    score += staleness_boost
    score += ready_boost
    score += blocked_boost
    score += dynamic_boost
    score += min(0.08, dynamic_bias * 0.01)
    score += min(0.08, replan_bias * 0.01) if selected_action == "replan" else 0.0
    score += learning_boost
    score += external_wait_boost
    score -= waiting_penalty
    score -= continuation_penalty
    score -= regret_penalty
    score -= external_wait_penalty
    score = max(0.0, min(1.0, round(score, 3)))

    available_slots = max(0, int(max_active_goals) - max(0, int(active_goal_count)))
    learned_soft_preempt_threshold = max(
        0.7,
        min(
            0.98,
            soft_preempt_threshold
            - min(0.06, scheduler_confidence * 0.04)
            + min(0.08, preempt_regret_rate * 0.08),
        ),
    )
    soft_preempt = (
        available_slots <= 0
        and score >= max(0.0, min(1.0, learned_soft_preempt_threshold))
        and wake_kind in {"scheduler_cooldown", "none"}
        and selected_action in {"workflow_call", "replan"}
    )
    dispatch_band = "defer"
    if score >= 0.85:
        dispatch_band = "critical"
    elif score >= 0.6:
        dispatch_band = "focus"
    elif score > 0.0:
        dispatch_band = "background"

    allocated_budget = round(min(2.5, max(0.5, 0.75 + score + (0.2 if soft_preempt else 0.0))), 2)
    rationale = [
        f"base={base_priority:.2f}",
        f"age_min={age_minutes:.1f}",
        f"wake={wake_kind or 'none'}",
        f"active_kind={active_kind}",
        f"continuations={continuation_count}",
        f"scheduler_confidence={scheduler_confidence:.2f}",
    ]
    if starvation_rate > 0.0:
        rationale.append(f"starvation_rate={starvation_rate:.2f}")
    if portfolio_throughput_score > 0.0:
        rationale.append(f"throughput={portfolio_throughput_score:.2f}")
    if external_source and source_reliability_confidence > 0.0:
        rationale.append(f"source={external_source}")
        if external_topic:
            rationale.append(f"topic={external_topic}")
        rationale.append(f"source_reliability={source_reliability_score:.2f}")
    if soft_preempt:
        rationale.append("soft_preempt")
    return {
        "portfolio_score": score,
        "base_priority": round(base_priority, 3),
        "dispatch_band": dispatch_band,
        "soft_preempt": soft_preempt,
        "allocated_budget": allocated_budget,
        "available_slots": available_slots,
        "age_minutes": round(age_minutes, 1),
        "waiting_penalty": round(waiting_penalty, 3),
        "continuation_penalty": round(continuation_penalty, 3),
        "external_wait_penalty": round(external_wait_penalty, 3),
        "rationale": rationale,
    }


def select_goal_portfolio_slice(
    goal_rows: list[dict[str, Any]],
    *,
    active_goal_count: int = 0,
    max_active_goals: int = 4,
    dispatch_limit: int = 1,
    soft_preempt_threshold: float = 0.88,
    portfolio_bias_override: dict[str, Any] | None = None,
    policy_memory_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ranked: list[dict[str, Any]] = []
    for row in goal_rows:
        portfolio = score_goal_portfolio_entry(
            row,
            active_goal_count=active_goal_count,
            max_active_goals=max_active_goals,
            soft_preempt_threshold=soft_preempt_threshold,
            portfolio_bias_override=portfolio_bias_override,
            policy_memory_override=policy_memory_override,
        )
        ranked.append(
            {
                "goal_id": str(row.get("goal_id") or ""),
                "goal_row": row,
                "portfolio": portfolio,
            }
        )
    ranked.sort(
        key=lambda item: (
            -_safe_float(_as_dict(item.get("portfolio")).get("portfolio_score"), 0.0),
            -_goal_age_minutes(_as_dict(item.get("goal_row"))),
            str(item.get("goal_id") or ""),
        )
    )

    available_slots = max(0, int(max_active_goals) - max(0, int(active_goal_count)))
    selected: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    soft_preempt_used = False

    for item in ranked:
        portfolio = _as_dict(item.get("portfolio"))
        decision = "defer"
        reason = "active_budget_exhausted"
        if len(selected) >= max(1, int(dispatch_limit)):
            reason = "dispatch_limit_reached"
        elif available_slots > 0:
            decision = "dispatch"
            reason = "within_active_budget"
            available_slots -= 1
        elif bool(portfolio.get("soft_preempt")) and not soft_preempt_used:
            decision = "soft_preempt"
            reason = "soft_preempt_urgent_goal"
            soft_preempt_used = True
        item["portfolio"] = {
            **portfolio,
            "dispatch_decision": decision,
            "dispatch_reason": reason,
        }
        if decision in {"dispatch", "soft_preempt"}:
            selected.append(item)
        else:
            deferred.append(item)
    return {
        "selected": selected,
        "deferred": deferred,
        "available_slots": max(0, available_slots),
        "active_goal_count": max(0, int(active_goal_count)),
        "max_active_goals": max(1, int(max_active_goals)),
    }


def recommend_goal_holds(
    live_goal_rows: list[dict[str, Any]],
    *,
    selected_entries: list[dict[str, Any]],
    active_goal_count: int,
    max_active_goals: int,
    hold_seconds: int = 180,
    max_holds: int = 1,
    portfolio_bias_override: dict[str, Any] | None = None,
    policy_memory_override: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if max(0, int(active_goal_count)) < max(1, int(max_active_goals)):
        return []
    urgent_selected = [entry for entry in selected_entries if bool(_as_dict(entry.get("portfolio")).get("soft_preempt"))]
    if not urgent_selected:
        return []

    selected_goal_ids = {str(entry.get("goal_id") or "") for entry in selected_entries}
    ranked_live: list[dict[str, Any]] = []
    for row in live_goal_rows:
        goal_id = str(row.get("goal_id") or "")
        if not goal_id or goal_id in selected_goal_ids:
            continue
        goal_state = _as_dict(row.get("goal_state"))
        current_action = str(_as_dict(goal_state.get("current_action")).get("action_type") or "")
        wake_kind = str(_as_dict(goal_state.get("wake_condition")).get("kind") or "")
        if current_action in {"ask_user", "approval_request"} or wake_kind in {"user_message", "approval", "task_completion"}:
            continue
        portfolio = score_goal_portfolio_entry(
            row,
            active_goal_count=active_goal_count,
            max_active_goals=max_active_goals,
            portfolio_bias_override=portfolio_bias_override,
            policy_memory_override=policy_memory_override,
        )
        ranked_live.append(
            {
                "goal_id": goal_id,
                "goal_row": row,
                "portfolio": portfolio,
            }
        )
    ranked_live.sort(
        key=lambda item: (
            _safe_float(_as_dict(item.get("portfolio")).get("portfolio_score"), 0.0),
            -_goal_age_minutes(_as_dict(item.get("goal_row"))),
            str(item.get("goal_id") or ""),
        )
    )

    held_by_goal_id = str(urgent_selected[0].get("goal_id") or "")
    hold_until = (datetime.now(timezone.utc)).timestamp() + max(30, int(hold_seconds))
    recommendations: list[dict[str, Any]] = []
    for item in ranked_live[: max(1, int(max_holds))]:
        portfolio = _as_dict(item.get("portfolio"))
        recommendations.append(
            {
                "tenant_id": str(_as_dict(item.get("goal_row")).get("tenant_id") or ""),
                "goal_id": str(item.get("goal_id") or ""),
                "hold_status": "HELD",
                "hold_reason": "soft_preempted_by_urgent_goal",
                "hold_until": datetime.fromtimestamp(hold_until, tz=timezone.utc).isoformat(),
                "held_by_goal_id": held_by_goal_id,
                "hold_score": float(portfolio.get("portfolio_score") or 0.0),
            }
        )
    return recommendations


def _normalize_lesson(text: Any) -> str:
    return str(text or "").strip().lower()


def derive_experience_profile(
    episodes: list[dict[str, Any]],
    *,
    latest_result: dict[str, Any] | None = None,
    selected_tool: str | None = None,
) -> dict[str, Any]:
    workflow_successes = 0
    tool_successes = 0
    ask_user_cases = 0
    retryable_failures = 0
    tool_retry_failures = 0
    approval_heavy = 0
    lesson_hints: set[str] = set()
    tool_name = str(selected_tool or "").strip()

    for row in episodes:
        strategy = str(row.get("chosen_strategy") or "")
        outcome = str(row.get("outcome_status") or "")
        lessons = [_normalize_lesson(item) for item in list(row.get("useful_lessons") or [])]
        tool_names = [str(item) for item in list(row.get("tool_names") or row.get("tool_usage") or [])]

        if strategy == "workflow_call" and outcome == "SUCCEEDED":
            workflow_successes += 1
        if strategy == "tool_call" and outcome == "SUCCEEDED":
            tool_successes += 1
        if strategy == "ask_user" or any("user input" in item or "clarification" in item for item in lessons):
            ask_user_cases += 1
        if outcome == "FAILED_RETRYABLE":
            retryable_failures += 1
            if tool_name and tool_name in tool_names:
                tool_retry_failures += 1
        elif any("retryable" in item or "escalate" in item for item in lessons):
            retryable_failures += 1
        if any("approval" in item or "high-risk" in item for item in lessons):
            approval_heavy += 1
        for item in lessons:
            if item:
                lesson_hints.add(item)

    latest = dict(latest_result or {})
    if latest.get("status") == "retryable_tool_failure":
        retryable_failures += 1
        if tool_name:
            tool_retry_failures += 1

    preferred_action = "respond"
    if workflow_successes >= max(1, tool_successes) and workflow_successes > 0:
        preferred_action = "workflow_call"
    elif tool_successes > workflow_successes and tool_successes > 0:
        preferred_action = "tool_call"
    if ask_user_cases > 0 and latest.get("status") == "NEED_INFO":
        preferred_action = "ask_user"

    return {
        "workflow_successes": workflow_successes,
        "tool_successes": tool_successes,
        "ask_user_cases": ask_user_cases,
        "retryable_failures": retryable_failures,
        "tool_retry_failures": tool_retry_failures,
        "approval_heavy_cases": approval_heavy,
        "preferred_action": preferred_action,
        "lesson_hints": sorted(lesson_hints)[:12],
    }


def _planner_signal_snapshot(planner: dict[str, Any]) -> dict[str, Any]:
    signals = dict(planner.get("policy_signals") or {})
    action_signal = str(signals.get("action_signal") or "").strip()
    if not action_signal:
        action_signal = LEGACY_ACTION_TO_RUNTIME_ACTION.get(str(planner.get("action") or "").strip(), "respond")
    affinities = dict(signals.get("action_affinities") or {})
    normalized_affinities: dict[str, float] = {}
    for key in ACTION_TYPES:
        raw = affinities.get(key)
        try:
            normalized_affinities[key] = round(min(1.0, max(0.0, float(raw))), 2)
        except Exception:
            normalized_affinities[key] = 0.0
    if normalized_affinities.get(action_signal, 0.0) <= 0.0:
        normalized_affinities[action_signal] = 0.7
    reasons = [str(item).strip() for item in list(signals.get("reasons") or []) if str(item).strip()]
    return {
        "action_signal": action_signal,
        "action_affinities": normalized_affinities,
        "reasons": reasons,
        "requires_approval": bool(signals.get("requires_approval")),
        "selected_tool": str(signals.get("selected_tool") or planner.get("selected_tool") or "").strip() or None,
        "signal_confidence": normalized_affinities.get(action_signal, 0.0),
    }


def choose_next_action(
    *,
    goal: dict[str, Any],
    planner: dict[str, Any],
    task_state: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    tool_candidates: list[dict[str, Any]],
    confirmed: bool,
    episodes: list[dict[str, Any]],
    has_retrieval_observation: bool,
    latest_result: dict[str, Any] | None = None,
    requested_mode: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    planner_action = str(planner.get("action") or "answer_only")
    planner_signal = _planner_signal_snapshot(planner)
    selected_tool = str(planner_signal.get("selected_tool") or planner.get("selected_tool") or "")
    top_tool = tool_candidates[0] if tool_candidates else {}
    top_requires_approval = bool(top_tool.get("requires_approval"))
    top_risk = str(top_tool.get("risk_level") or goal.get("risk_level") or "low")
    reasons: list[str] = []
    similar_episode_ids = [str(item.get("episode_id") or "") for item in episodes if item.get("episode_id")]
    strategy_hint = str(episodes[0].get("chosen_strategy") or "") if episodes else ""
    experience = derive_experience_profile(
        episodes,
        latest_result=latest_result,
        selected_tool=selected_tool or str(top_tool.get("tool_name") or ""),
    )
    fallback_action = "respond"
    unknowns = [str(item) for item in list(goal.get("unknowns") or [])]
    has_ambiguous_reference = "ambiguous_user_reference" in unknowns
    missing_grounding = "missing_grounding_evidence" in unknowns
    available_actions = list(task_state.get("available_actions") or [])
    policy_memory = dict(task_state.get("policy_memory") or {})
    action_bias = dict(policy_memory.get("action_bias") or {})
    tool_failure_counts = dict(policy_memory.get("tool_failure_counts") or {})
    tool_success_counts = dict(policy_memory.get("tool_success_counts") or {})
    tool_reliability = dict(policy_memory.get("tool_reliability") or {})
    memory_hygiene = dict(policy_memory.get("memory_hygiene") or {})
    eval_summary = dict(policy_memory.get("eval_summary") or {})
    planner_affinities = dict(planner_signal.get("action_affinities") or {})
    workflow_signal = float(planner_affinities.get("workflow_call") or 0.0)
    tool_signal = float(planner_affinities.get("tool_call") or 0.0)
    ask_user_signal = float(planner_affinities.get("ask_user") or 0.0)
    workflow_bias = int(action_bias.get("workflow_call") or 0)
    tool_bias = int(action_bias.get("tool_call") or 0)
    ask_user_bias = int(action_bias.get("ask_user") or 0)
    selected_tool_failures = int(tool_failure_counts.get(selected_tool) or 0)
    selected_tool_successes = int(tool_success_counts.get(selected_tool) or 0)
    memory_confidence = float(memory_hygiene.get("memory_confidence") or eval_summary.get("memory_confidence") or 0.0)
    selected_tool_memory = _as_dict(tool_reliability.get(selected_tool))
    selected_tool_reliability = _safe_float(selected_tool_memory.get("score"), 0.0)
    selected_tool_reliability_confidence = _safe_float(selected_tool_memory.get("confidence"), 0.0)
    explicit_durable_request = _goal_requests_durable_runtime(goal)

    if latest_result and latest_result.get("status") == "retryable_tool_failure":
        action_type = "replan"
        reasons.append("retryable tool failure requires a safer fallback path")
        fallback_action = "workflow_call"
    elif explicit_durable_request and "workflow_call" in available_actions and requested_mode != "tool_task":
        action_type = "workflow_call"
        reasons.append("the user explicitly asked the runtime to continue through a durable workflow path")
        fallback_action = "respond"
    elif planner_action == "answer_only":
        if has_ambiguous_reference:
            action_type = "ask_user"
            reasons.append("the goal uses ambiguous references that only the user can resolve")
            fallback_action = "retrieve"
        elif missing_grounding and not has_retrieval_observation:
            action_type = "retrieve"
            reasons.append("the planner prefers a direct answer, but the runtime should ground it first")
            fallback_action = "respond"
        else:
            action_type = "respond"
            reasons.append("the planner explicitly selected a direct response path")
            fallback_action = "respond"
    elif top_requires_approval and not confirmed:
        action_type = "approval_request"
        reasons.append("selected tool is governed by approval policy")
        fallback_action = "wait"
    elif top_requires_approval and confirmed:
        action_type = "workflow_call"
        reasons.append("approval was confirmed, so the runtime should continue in the durable governed path")
        fallback_action = "respond"
    elif has_ambiguous_reference:
        action_type = "ask_user"
        reasons.append("goal still has ambiguous references that require user clarification")
        fallback_action = "retrieve"
    elif planner_action in {"use_retrieval", "answer_only"} and not has_retrieval_observation:
        action_type = "retrieve"
        reasons.append("ground the goal before responding")
        fallback_action = "respond"
    elif (
        selected_tool
        and selected_tool_reliability >= 0.35
        and selected_tool_reliability_confidence >= 0.45
        and planner_action == "use_tool"
        and tool_candidates
    ):
        action_type = "tool_call"
        reasons.append("policy memory reliability scoring says this tool is consistently successful for similar goals")
        fallback_action = "workflow_call" if "workflow_call" in available_actions else "respond"
    elif (
        selected_tool
        and selected_tool_reliability <= -0.25
        and selected_tool_reliability_confidence >= 0.35
        and "workflow_call" in available_actions
    ):
        action_type = "workflow_call"
        reasons.append("policy memory reliability scoring says this tool is too unstable, so durable execution is safer")
        fallback_action = "respond"
    elif (
        selected_tool
        and selected_tool_successes > max(selected_tool_failures, 0)
        and memory_confidence >= 0.6
        and planner_action == "use_tool"
        and tool_candidates
    ):
        action_type = "tool_call"
        reasons.append("policy memory shows this tool has been reliable for similar goals, so the runtime can stay on the fast path")
        fallback_action = "workflow_call" if "workflow_call" in available_actions else "respond"
    elif selected_tool and selected_tool_failures > 0 and memory_confidence >= 0.4 and "workflow_call" in available_actions:
        action_type = "workflow_call"
        reasons.append("policy memory shows this tool has been unstable for similar goals, so durable execution is preferred")
        fallback_action = "respond"
    elif experience["tool_retry_failures"] > 0 and "workflow_call" in available_actions:
        action_type = "workflow_call"
        reasons.append("recent episode memory shows the fast tool path keeps failing for this tool, so durable execution is safer")
        fallback_action = "respond"
    elif ask_user_signal >= 0.75 and has_ambiguous_reference:
        action_type = "ask_user"
        reasons.append("planner signal strongly prefers clarifying with the user before continuing")
        fallback_action = "retrieve"
    elif (
        requested_mode != "tool_task"
        and workflow_signal >= max(0.75, tool_signal)
        and "workflow_call" in available_actions
    ):
        action_type = "workflow_call"
        reasons.append("planner signal rates durable execution as the strongest next move")
        fallback_action = "respond"
    elif (
        workflow_bias > tool_bias
        and memory_confidence >= 0.4
        and "workflow_call" in available_actions
        and requested_mode not in {"tool_task", "direct_answer"}
    ):
        action_type = "workflow_call"
        reasons.append("policy memory currently biases this class of goals toward durable execution")
        fallback_action = "respond"
    elif ask_user_bias > 0 and memory_confidence >= 0.4 and has_ambiguous_reference:
        action_type = "ask_user"
        reasons.append("policy memory shows clarification-first handling works better for ambiguous goals")
        fallback_action = "retrieve"
    elif has_ambiguous_reference and experience["ask_user_cases"] > 0:
        action_type = "ask_user"
        reasons.append("similar past episodes needed user clarification before continuing")
        fallback_action = "retrieve"
    elif requested_mode != "tool_task" and strategy_hint == "workflow_call" and "workflow_call" in available_actions:
        action_type = "workflow_call"
        reasons.append("similar successful episode suggests durable execution for this open-ended goal")
        fallback_action = "respond"
    elif (
        experience["preferred_action"] == "workflow_call"
        and "workflow_call" in available_actions
        and requested_mode not in {"tool_task", "direct_answer"}
    ):
        action_type = "workflow_call"
        reasons.append("experience profile favors durable execution for goals like this")
        fallback_action = "respond"
    elif planner_action == "use_tool" and tool_candidates:
        action_type = "tool_call"
        reasons.append("planner and registry both indicate a direct tool execution path")
        fallback_action = "workflow_call" if "workflow_call" in available_actions else "respond"
    elif planner_action in {"start_workflow", "need_approval"} or (
        requested_mode != "tool_task"
        and any(hint in str(goal.get("normalized_goal") or "").lower() for hint in COMPLEX_HINTS)
    ):
        action_type = "workflow_call"
        reasons.append("task looks open-ended or multi-step, so durable execution is safer")
        fallback_action = "respond"
    else:
        action_type = "respond"
        reasons.append("current context is sufficient for a final user-facing response")

    target = selected_tool or str(top_tool.get("tool_name") or "") or None
    action = {
        "action_type": action_type,
        "rationale": "; ".join(reasons),
        "target": target,
        "input": {
            "goal": goal.get("normalized_goal"),
            "unknowns": list(goal.get("unknowns") or []),
            "selected_tool": selected_tool or str(top_tool.get("tool_name") or ""),
        },
        "requires_approval": action_type == "approval_request"
        or (top_requires_approval and action_type in {"tool_call", "workflow_call"}),
        "status": "planned",
    }
    action.update(
        _build_action_contract(
            action_type,
            fallback_action=fallback_action,
            target=target,
            requires_approval=bool(action.get("requires_approval")),
        )
    )
    policy = {
        "selected_action": action_type,
        "reasoning": reasons,
        "fallback_action": fallback_action,
        "replan_triggers": [
            "missing_grounding_evidence",
            "retryable_tool_failure",
            "approval_not_granted",
        ],
        "approval_triggered": action_type == "approval_request",
        "ask_user_triggered": action_type == "ask_user",
        "episode_retrieval_triggered": bool(episodes),
        "similar_episode_ids": similar_episode_ids,
        "planner_action": planner_action,
        "planner_signal_action": str(planner_signal.get("action_signal") or ""),
        "risk_level": top_risk,
        "experience_profile": experience,
        "memory_confidence": round(memory_confidence, 3),
        "selected_tool_memory": {
            "failures": selected_tool_failures,
            "successes": selected_tool_successes,
            "reliability_score": round(selected_tool_reliability, 3),
            "reliability_confidence": round(selected_tool_reliability_confidence, 3),
        },
        "policy_version_id": str(policy_memory.get("version_id") or "") or None,
        "policy_memory": policy_memory,
        "planner_signal": planner_signal,
    }
    return action, policy


def select_next_runtime_step(
    *,
    goal: dict[str, Any],
    planner: dict[str, Any],
    task_state: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    tool_candidates: list[dict[str, Any]],
    confirmed: bool,
    episodes: list[dict[str, Any]],
    has_retrieval_observation: bool,
    latest_result: dict[str, Any] | None = None,
    requested_mode: str | None = None,
    selected_tool: str | None = None,
) -> dict[str, Any]:
    action, policy = choose_next_action(
        goal=goal,
        planner=planner,
        task_state=task_state,
        retrieval_hits=retrieval_hits,
        tool_candidates=tool_candidates,
        confirmed=confirmed,
        episodes=episodes,
        has_retrieval_observation=has_retrieval_observation,
        latest_result=latest_result,
        requested_mode=requested_mode,
    )
    route = route_for_action_type(str(action.get("action_type") or "respond"))
    confidence_raw = planner.get("confidence")
    confidence = float(confidence_raw) if isinstance(confidence_raw, (int, float)) else None
    reflection = reflect_and_replan(
        action=action,
        goal=goal,
        retrieval_hits=retrieval_hits,
        latest_result=latest_result,
        fallback_action=str(policy.get("fallback_action") or "respond"),
    )
    candidate_actions, why_not = _build_decision_candidates(
        selected_action=str(action.get("action_type") or ""),
        selected_rationale=str(action.get("rationale") or ""),
        available_actions=list(task_state.get("available_actions") or []),
        unknowns=list(goal.get("unknowns") or []),
        has_retrieval_observation=has_retrieval_observation,
        tool_candidates=tool_candidates,
        planner_signal=dict(policy.get("planner_signal") or {}),
        top_requires_approval=bool(tool_candidates and tool_candidates[0].get("requires_approval")),
        confirmed=confirmed,
        requested_mode=requested_mode,
        latest_result=latest_result,
    )
    decision = {
        "action": str(planner.get("action") or ""),
        "intent": str(planner.get("intent") or "") or None,
        "route": route,
        "selected_tool": selected_tool or str(action.get("target") or "") or None,
        "confidence": confidence,
        "need_confirmation": str(action.get("action_type") or "") == "approval_request",
        "summary": str(action.get("rationale") or "Selected next action."),
        "planner_signal_action": str(policy.get("planner_signal_action") or ""),
        "candidate_actions": candidate_actions,
        "why_not": why_not,
    }
    return {
        "current_action": action,
        "policy": policy,
        "reflection": reflection,
        "route": route,
        "decision": decision,
    }


def reflect_and_replan(
    *,
    action: dict[str, Any],
    goal: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    latest_result: dict[str, Any] | None,
    fallback_action: str | None,
) -> dict[str, Any]:
    action_type = str(action.get("action_type") or "")
    fallback = str(fallback_action or "")
    if action_type == "retrieve" and not retrieval_hits:
        return {
            "summary": "Retrieval did not ground the goal, so the runtime should switch strategy.",
            "requires_replan": True,
            "next_action": "ask_user" if goal.get("unknowns") else (fallback or "respond"),
        }
    if latest_result and latest_result.get("status") == "retryable_tool_failure":
        return {
            "summary": "The fast-path tool call failed in a retryable way, so escalation is safer than repeating the same move.",
            "requires_replan": True,
            "next_action": fallback or "workflow_call",
        }
    if action_type == "approval_request":
        return {
            "summary": "Execution is blocked until approval is granted.",
            "requires_replan": False,
            "next_action": "wait",
        }
    if action_type == "ask_user":
        return {
            "summary": "The runtime needs more user input before it can safely continue.",
            "requires_replan": False,
            "next_action": "wait",
        }
    if action_type == "workflow_call":
        return {
            "summary": "The runtime handed off the open-ended portion to the durable workflow loop.",
            "requires_replan": False,
            "next_action": "wait",
        }
    return {
        "summary": "The current action advanced the goal enough to continue or finalize without replanning.",
        "requires_replan": False,
        "next_action": fallback or "respond",
    }


def merge_runtime_state(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_runtime_state(dict(merged.get(key) or {}), value)
        else:
            merged[key] = value
    return merged


def derive_runtime_followup(
    base_runtime: dict[str, Any],
    *,
    event_type: str | None = None,
    status: str,
    current_phase: str,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    summary: str | None = None,
    target: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    runtime = dict(base_runtime or {})
    existing_action = dict(runtime.get("current_action") or {})
    existing_policy = dict(runtime.get("policy") or {})
    episodes = list(runtime.get("episodes") or [])
    task_state = dict(runtime.get("task_state") or {})
    goal = dict(runtime.get("goal") or task_state.get("current_goal") or {})
    risk_level = str(existing_policy.get("risk_level") or goal.get("risk_level") or "medium")
    unknowns = list(task_state.get("unknowns") or goal.get("unknowns") or [])
    latest = dict(latest_result or {})
    failure_type = str(latest.get("failure_type") or latest.get("reason_code") or "")
    normalized_status = str(status or latest.get("status") or "").upper() or "IN_PROGRESS"
    approvals = list(pending_approvals or task_state.get("pending_approvals") or [])
    detail = str(summary or latest.get("reason") or latest.get("error") or existing_action.get("rationale") or "").strip()
    selected_target = target or str(existing_action.get("target") or "") or None
    experience = derive_experience_profile(
        episodes,
        latest_result=latest_result,
        selected_tool=selected_target,
    )

    if failure_type == "NEED_INFO" or current_phase == "ask_user":
        action_type = "ask_user"
        requires_approval = False
        fallback_action = "wait"
        reflection_summary = detail or "The runtime needs more user input before it can safely continue."
        requires_replan = False
        next_action = "wait"
    elif approvals and normalized_status in WAITING_STATUSES:
        action_type = "wait"
        requires_approval = True
        fallback_action = "wait"
        reflection_summary = detail or "Execution is paused while waiting for approval."
        requires_replan = False
        next_action = "wait"
        selected_target = approvals[0]
    elif normalized_status in {"FAILED_RETRYABLE", "RETRYING", "TIMED_OUT"} or latest.get("status") == "retryable_tool_failure":
        action_type = "replan"
        requires_approval = False
        fallback_action = "workflow_call" if "workflow_call" in list(task_state.get("available_actions") or []) else "respond"
        if experience["tool_retry_failures"] > 0 and "workflow_call" in list(task_state.get("available_actions") or []):
            fallback_action = "workflow_call"
        reflection_summary = detail or "The runtime hit a retryable issue and should choose a safer next step."
        requires_replan = True
        next_action = "replan"
    elif normalized_status in IN_PROGRESS_STATUSES:
        if current_phase == "reflect":
            action_type = "reflect"
            fallback_action = "respond"
            reflection_summary = detail or "The runtime is reviewing the latest execution state."
            next_action = "respond"
        elif current_phase == "replan":
            action_type = "replan"
            fallback_action = "workflow_call"
            reflection_summary = detail or "The runtime is replanning after a new observation."
            next_action = "replan"
        else:
            action_type = "workflow_call"
            fallback_action = "respond"
            reflection_summary = detail or "The durable runtime is still advancing the goal."
            next_action = "wait"
        requires_approval = False
        requires_replan = action_type == "replan"
    elif normalized_status == "SUCCEEDED":
        action_type = "respond"
        requires_approval = False
        fallback_action = "respond"
        reflection_summary = detail or "The runtime completed successfully and can now produce the final response."
        requires_replan = False
        next_action = "respond"
    elif normalized_status in {"FAILED_FINAL", "CANCELLED"}:
        action_type = "ask_user" if failure_type == "NEED_INFO" else "respond"
        requires_approval = False
        fallback_action = "wait" if action_type == "ask_user" else "respond"
        reflection_summary = detail or "The runtime reached a final state and cannot continue automatically."
        requires_replan = False
        next_action = "wait" if action_type == "ask_user" else "respond"
    else:
        action_type = str(existing_policy.get("selected_action") or existing_action.get("action_type") or "workflow_call")
        requires_approval = bool(existing_action.get("requires_approval")) or bool(existing_policy.get("approval_triggered"))
        fallback_action = str(existing_policy.get("fallback_action") or "respond")
        reflection_summary = detail or "The runtime updated state without changing its current strategy."
        requires_replan = action_type == "replan"
        next_action = "wait" if action_type in {"workflow_call", "approval_request"} else fallback_action

    action = {
        "action_type": action_type,
        "target": selected_target,
        "input": dict(existing_action.get("input") or {}),
        "rationale": reflection_summary,
        "requires_approval": requires_approval,
        "status": "completed" if normalized_status in TERMINAL_STATUSES else "in_progress",
    }
    action.update(
        _build_action_contract(
            action_type,
            fallback_action=fallback_action,
            target=selected_target,
            requires_approval=requires_approval,
        )
    )
    policy = {
        "selected_action": action_type,
        "reasoning": [item for item in [event_type, normalized_status, current_phase, detail] if item],
        "fallback_action": fallback_action,
        "replan_triggers": list(existing_policy.get("replan_triggers") or ["retryable_tool_failure", "approval_not_granted"]),
        "approval_triggered": requires_approval,
        "ask_user_triggered": action_type == "ask_user",
        "episode_retrieval_triggered": bool(existing_policy.get("episode_retrieval_triggered")),
        "similar_episode_ids": list(existing_policy.get("similar_episode_ids") or []),
        "planner_action": str(existing_policy.get("planner_action") or event_type or ""),
        "risk_level": risk_level,
        "experience_profile": experience,
    }
    reflection = {
        "summary": reflection_summary,
        "requires_replan": requires_replan,
        "next_action": next_action,
    }
    return action, policy, reflection


def route_for_action_type(action_type: str) -> str:
    if action_type in {"respond", "ask_user", "retrieve"}:
        return "direct_answer"
    if action_type in {"tool_call", "approval_request"}:
        return "tool_task"
    return "workflow_task"


def should_prepare_tools(action_type: str) -> bool:
    return action_type in TOOL_PREPARATION_ACTIONS


def runtime_requires_approval(
    *,
    task_type: str,
    current_action: dict[str, Any] | None,
    policy: dict[str, Any] | None,
    pending_tool_plans: list[dict[str, Any]] | None = None,
) -> bool:
    action = current_action or {}
    runtime_policy = policy or {}
    return (
        task_type == "ticket_email"
        or bool(pending_tool_plans)
        or bool(action.get("requires_approval"))
        or bool(runtime_policy.get("approval_triggered"))
    )


def reduce_runtime_state(
    base_runtime: dict[str, Any],
    *,
    event_type: str | None = None,
    status: str,
    current_phase: str,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    reflection: dict[str, Any] | None = None,
    final_output: dict[str, Any] | None = None,
    current_action: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    route: str | None = None,
    observations: list[dict[str, Any]] | None = None,
    steps: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    runtime = dict(base_runtime or {})
    task_state = dict(runtime.get("task_state") or {})
    task_state["current_phase"] = current_phase
    if latest_result is not None:
        task_state["latest_result"] = latest_result
    if pending_approvals is not None:
        task_state["pending_approvals"] = list(pending_approvals)
    runtime["task_state"] = task_state
    runtime["status"] = status
    runtime["current_phase"] = current_phase
    if reflection is not None:
        runtime["reflection"] = reflection
    if final_output is not None:
        runtime["final_output"] = final_output
    if current_action is not None:
        runtime["current_action"] = dict(current_action)
    if policy is not None:
        runtime["policy"] = dict(policy)
    if decision is not None:
        runtime["decision"] = dict(decision)
    if route is not None:
        runtime["route"] = route
    if observations is not None:
        runtime["observations"] = list(observations)
    if steps is not None:
        runtime["steps"] = list(steps)
    if event_type:
        runtime["runtime_event"] = {"type": event_type}
    return runtime


def apply_runtime_event(
    base_runtime: dict[str, Any],
    *,
    event_type: str | None = None,
    status: str,
    current_phase: str,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    final_output: dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    route: str | None = None,
    observations: list[dict[str, Any]] | None = None,
    steps: list[dict[str, Any]] | None = None,
    summary: str | None = None,
    target: str | None = None,
) -> dict[str, Any]:
    action, policy, reflection = derive_runtime_followup(
        base_runtime,
        event_type=event_type,
        status=status,
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        summary=summary,
        target=target,
    )
    return reduce_runtime_state(
        base_runtime,
        event_type=event_type,
        status=status,
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        reflection=reflection,
        final_output=final_output,
        current_action=action,
        policy=policy,
        decision=decision,
        route=route,
        observations=observations,
        steps=steps,
    )
