from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Awaitable, Callable
import uuid

from ..config import settings
from ..repositories import GoalRepository, PolicyMemoryRepository, TaskRepository
from ..schemas import TaskCreateRequest
from .policy_memory_service import (
    build_runtime_policy_memory,
    record_portfolio_feedback,
    record_shadow_portfolio_probe,
)
from .task_service import create_task as service_create_task
from runtime_backbone import recommend_goal_holds, select_goal_portfolio_slice

logger = logging.getLogger(__name__)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _policy_portfolio_bias(version_row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(version_row, dict):
        return {}
    payload = _as_dict(version_row.get("memory_payload"))
    return _as_dict(payload.get("portfolio_bias"))


def _policy_memory_override(version_row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(version_row, dict):
        return {}
    return build_runtime_policy_memory(version_row)


def _selected_external_wait_sources(entries: list[dict[str, Any]]) -> list[str]:
    sources: list[str] = []
    for entry in entries:
        goal_row = _as_dict(entry.get("goal_row"))
        goal_state = _as_dict(goal_row.get("goal_state"))
        wake_condition = _as_dict(goal_state.get("wake_condition"))
        if str(wake_condition.get("kind") or "") != "external_signal":
            continue
        source = str(wake_condition.get("source") or "").strip()
        if source and source not in sources:
            sources.append(source)
    return sources


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _goal_hold_active(goal_row: dict[str, Any]) -> bool:
    goal_state = _as_dict(goal_row.get("goal_state"))
    portfolio = _as_dict(goal_state.get("portfolio"))
    if str(portfolio.get("hold_status") or "") not in {"HELD", "PREEMPTING"}:
        return False
    hold_until = _parse_datetime(portfolio.get("hold_until"))
    if hold_until is None:
        return True
    return hold_until > datetime.now(timezone.utc)


def goal_needs_continuation(goal_row: dict[str, Any]) -> bool:
    status = str(goal_row.get("status") or "")
    goal_state = _as_dict(goal_row.get("goal_state"))
    portfolio = _as_dict(goal_state.get("portfolio"))
    active_subgoal = _as_dict(goal_state.get("active_subgoal"))
    wake_condition = _as_dict(goal_state.get("wake_condition") or active_subgoal.get("wake_condition"))
    dependency_status = _as_dict(active_subgoal.get("dependency_status"))
    wake_graph = _as_dict(goal_state.get("wake_graph"))
    agenda = _as_dict(goal_state.get("agenda"))
    event_subscriptions = [item for item in _as_list(goal_state.get("event_subscriptions")) if isinstance(item, dict)]
    event_timeouts = _as_dict(goal_state.get("event_timeouts"))
    current_action = str(_as_dict(goal_state.get("current_action")).get("action_type") or "")
    next_action = str(_as_dict(goal_state.get("reflection")).get("next_action") or "")
    if status not in {"ACTIVE", "WAITING"}:
        return False
    if _goal_hold_active(goal_row):
        return False
    if int(event_timeouts.get("expired_required_count") or 0) > 0:
        return True
    if current_action in {"ask_user", "wait", "approval_request", "respond"}:
        return False
    if str(wake_condition.get("kind") or "") in {"user_message", "approval", "external_signal", "task_completion"}:
        return False
    if any(bool(item.get("required")) and str(item.get("status") or "pending") != "satisfied" for item in event_subscriptions):
        return False
    if active_subgoal and not bool(dependency_status.get("satisfied", True)):
        return False
    if active_subgoal and str(active_subgoal.get("subgoal_id") or "") not in set(_as_list(wake_graph.get("resume_candidates")) or [str(active_subgoal.get("subgoal_id") or "")]):
        return False
    if "priority_score" in agenda and float(agenda.get("priority_score") or 0.0) <= 0.0:
        return False
    if str(portfolio.get("dispatch_decision") or "") == "hold":
        return False
    if active_subgoal and str(active_subgoal.get("status") or "") not in {"ACTIVE", "WAITING"}:
        return str(active_subgoal.get("status") or "") == "PENDING" and str(wake_condition.get("kind") or "") in {"scheduler_cooldown", "none"}
    return current_action in {"workflow_call", "replan"} or next_action in {"workflow_call", "replan"}


def build_goal_continuation_request(goal_row: dict[str, Any]) -> TaskCreateRequest:
    goal_id = str(goal_row.get("goal_id") or "")
    conversation_id = str(goal_row.get("conversation_id") or "") or None
    user_id = str(goal_row.get("user_id") or "")
    goal_state = _as_dict(goal_row.get("goal_state"))
    goal = _as_dict(goal_state.get("goal"))
    planner = _as_dict(goal_state.get("planner"))
    unified_task = _as_dict(goal_state.get("unified_task"))
    task_state = _as_dict(goal_state.get("task_state"))
    current_action = _as_dict(goal_state.get("current_action"))
    policy = _as_dict(goal_state.get("policy"))
    reflection = _as_dict(goal_state.get("reflection"))
    active_subgoal = _as_dict(goal_state.get("active_subgoal"))
    wake_condition = _as_dict(goal_state.get("wake_condition") or active_subgoal.get("wake_condition"))
    dependency_status = _as_dict(active_subgoal.get("dependency_status"))
    wake_graph = _as_dict(goal_state.get("wake_graph"))
    agenda = _as_dict(goal_state.get("agenda"))
    event_memory = _as_list(goal_state.get("event_memory"))
    event_subscriptions = _as_list(goal_state.get("event_subscriptions"))
    event_timeouts = _as_dict(goal_state.get("event_timeouts"))
    portfolio = _as_dict(goal_row.get("portfolio") or goal_state.get("portfolio"))
    retrieval_hits = _as_list(goal_state.get("retrieval_hits"))
    episodes = _as_list(goal_state.get("episodes"))
    memory = _as_dict(goal_state.get("memory"))
    final_output = _as_dict(goal_state.get("final_output"))
    continuation_no = int(goal_row.get("continuation_count") or 0) + 1
    task_type = str(planner.get("task_type") or "research_summary")
    normalized_goal = str(goal.get("normalized_goal") or goal_row.get("normalized_goal") or "")
    resume_action = str(wake_condition.get("resume_action") or current_action.get("action_type") or "workflow_call")
    preemption_recovery = str(portfolio.get("resume_strategy") or "") == "replan_after_preemption"
    subscription_timeout_recovery = int(event_timeouts.get("expired_required_count") or 0) > 0
    if preemption_recovery:
        resume_action = "replan"
        latest_result = _as_dict(task_state.get("latest_result"))
        task_state["latest_result"] = {
            **latest_result,
            "status": str(latest_result.get("status") or "CANCELLED"),
            "reason_code": str(latest_result.get("reason_code") or "goal_preempted"),
            "held_by_goal_id": str(portfolio.get("last_held_by_goal_id") or portfolio.get("held_by_goal_id") or ""),
            "hold_reason": str(portfolio.get("last_hold_reason") or portfolio.get("hold_reason") or "soft_preempted_by_urgent_goal"),
        }
    elif subscription_timeout_recovery:
        resume_action = "replan"
        latest_result = _as_dict(task_state.get("latest_result"))
        task_state["latest_result"] = {
            **latest_result,
            "status": str(latest_result.get("status") or "TIMED_OUT"),
            "reason_code": str(latest_result.get("reason_code") or "subscription_timeout"),
            "expired_subscriptions": [
                {
                    "subscription_id": str(item.get("subscription_id") or ""),
                    "kind": str(item.get("kind") or ""),
                    "event_key": str(item.get("event_key") or ""),
                }
                for item in event_subscriptions
                if str(item.get("status") or "") == "expired"
            ],
        }

    runtime_state = {
        "route": "workflow_task",
        "status": "QUEUED",
        "current_phase": "replan" if (preemption_recovery or subscription_timeout_recovery) else ("plan" if resume_action == "replan" else str(goal_state.get("current_phase") or "plan")),
        "goal_ref": {
            "goal_id": goal_id,
            "lifecycle_state": str(goal_row.get("status") or "ACTIVE"),
            "continuation_count": continuation_no,
            "active_subgoal_id": str(active_subgoal.get("subgoal_id") or ""),
            "active_subgoal_index": max(0, int(active_subgoal.get("sequence_no") or 1) - 1),
        },
        "goal": {
            **goal,
            "goal_id": goal_id,
            "lifecycle_state": str(goal_row.get("status") or "ACTIVE"),
            "continuation_count": continuation_no,
        },
        "planner": planner,
        "unified_task": unified_task,
        "task_state": task_state,
        "current_action": {
            **current_action,
            "action_type": "replan" if (preemption_recovery or subscription_timeout_recovery) else ("workflow_call" if resume_action == "replan" else resume_action),
            "status": "planned",
            "target": str(active_subgoal.get("title") or current_action.get("target") or ""),
        },
        "policy": policy,
        "reflection": (
            {
                **reflection,
                "summary": "The goal is resuming after portfolio preemption and should replan before continuing.",
                "requires_replan": True,
                "next_action": "workflow_call",
            }
            if preemption_recovery
            else (
                {
                    **reflection,
                    "summary": "Required event subscriptions timed out, so the goal should replan before continuing.",
                    "requires_replan": True,
                    "next_action": "workflow_call",
                }
                if subscription_timeout_recovery
                else reflection
            )
        ),
        "retrieval_hits": retrieval_hits,
        "episodes": episodes,
        "memory": memory,
        "final_output": final_output,
        "subgoals": _as_list(goal_state.get("subgoals")),
        "active_subgoal": active_subgoal,
        "ready_subgoals": _as_list(goal_state.get("ready_subgoals")),
        "blocked_subgoals": _as_list(goal_state.get("blocked_subgoals")),
        "wake_condition": wake_condition,
        "wake_graph": wake_graph,
        "event_memory": event_memory,
        "event_subscriptions": event_subscriptions,
        "event_requirements": [
            {
                "subscription_id": str(item.get("subscription_id") or ""),
                "kind": str(item.get("kind") or ""),
                "event_key": str(item.get("event_key") or ""),
                "source": str(item.get("source") or ""),
                "event_topic": str(item.get("event_topic") or ""),
                "entity_refs": _as_list(item.get("entity_refs")),
                "expected_outcomes": _as_list(item.get("expected_outcomes")),
                "resume_action": str(item.get("resume_action") or ""),
                "required": bool(item.get("required")),
                "scope": str(item.get("scope") or "goal"),
                "subgoal_id": str(item.get("subgoal_id") or ""),
            }
            for item in event_subscriptions
            if isinstance(item, dict)
        ],
        "event_timeouts": event_timeouts,
        "agenda": agenda,
        "portfolio": portfolio,
        "scheduler": {
            "trigger": "goal_scheduler",
            "continuation_count": continuation_no,
            "active_subgoal_id": str(active_subgoal.get("subgoal_id") or ""),
            "dependency_satisfied": bool(dependency_status.get("satisfied", True)),
            "priority_score": float(agenda.get("priority_score") or 0.0),
            "portfolio_score": float(portfolio.get("portfolio_score") or agenda.get("priority_score") or 0.0),
            "dispatch_decision": str(portfolio.get("dispatch_decision") or "dispatch"),
            "dispatch_band": str(portfolio.get("dispatch_band") or ""),
            "soft_preempt": bool(portfolio.get("soft_preempt")),
            "preemption_recovery": preemption_recovery,
            "subscription_timeout_recovery": subscription_timeout_recovery,
        },
    }
    input_payload = {
        "message": normalized_goal,
        "query": normalized_goal,
        "conversation_id": conversation_id,
        "assistant_turn_id": str(goal_row.get("last_turn_id") or "") or None,
        "metadata": {
            "goal_scheduler": True,
            "goal_id": goal_id,
            "continuation_count": continuation_no,
            "trigger_user_id": user_id,
        },
        "planner": planner,
        "retrieval_hits": retrieval_hits,
        "goal": runtime_state["goal"],
        "unified_task": unified_task,
        "task_state": task_state,
        "current_action": runtime_state["current_action"],
        "policy": policy,
        "episodes": episodes,
        "runtime_state": runtime_state,
        "wake_condition": wake_condition,
        "active_subgoal": active_subgoal,
        "ready_subgoals": _as_list(goal_state.get("ready_subgoals")),
        "blocked_subgoals": _as_list(goal_state.get("blocked_subgoals")),
        "wake_graph": wake_graph,
        "event_memory": event_memory,
        "event_subscriptions": event_subscriptions,
        "event_requirements": runtime_state["event_requirements"],
        "event_timeouts": event_timeouts,
        "agenda": agenda,
        "portfolio": portfolio,
    }
    return TaskCreateRequest(
        client_request_id=f"goal-wakeup-{goal_id}-{uuid.uuid4().hex[:10]}",
        task_type=task_type,
        input=input_payload,
        budget=max(0.5, float(portfolio.get("allocated_budget") or 1.0)),
        conversation_id=conversation_id,
        assistant_turn_id=str(goal_row.get("last_turn_id") or "") or None,
        goal_id=goal_id,
        origin="goal_scheduler",
    )


async def dispatch_schedulable_goals(
    *,
    goal_repo: GoalRepository,
    task_repo: TaskRepository,
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
    cancel_workflow: Callable[[str], Awaitable[None]] | None = None,
    policy_repo: PolicyMemoryRepository | None = None,
    max_items: int | None = None,
) -> dict[str, int]:
    processed = 0
    scheduled = 0
    skipped = 0
    failed = 0
    preempted = 0
    limit = max_items if max_items is not None else int(settings.goal_scheduler_batch_size)
    candidate_limit = max(1, limit * 4)
    shadow_portfolio_context: dict[str, Any] | None = None
    active_goal_count = goal_repo.count_goals_with_live_task() if hasattr(goal_repo, "count_goals_with_live_task") else 0
    if hasattr(goal_repo, "list_schedulable_goals"):
        candidates = goal_repo.list_schedulable_goals(
            cooldown_s=int(settings.goal_scheduler_cooldown_s),
            limit=candidate_limit,
        )
    else:  # pragma: no cover - compatibility shim
        candidates = []
        for _ in range(candidate_limit):
            candidate = goal_repo.claim_next_schedulable_goal(cooldown_s=int(settings.goal_scheduler_cooldown_s))
            if not candidate:
                break
            candidates.append(candidate)
    portfolio = select_goal_portfolio_slice(
        candidates,
        active_goal_count=active_goal_count,
        max_active_goals=int(settings.goal_scheduler_max_active_goals),
        dispatch_limit=max(1, limit),
        soft_preempt_threshold=float(settings.goal_scheduler_soft_preempt_threshold),
    )
    live_goals = goal_repo.list_live_goals(limit=max(1, limit * 3)) if hasattr(goal_repo, "list_live_goals") else []
    hold_recommendations = recommend_goal_holds(
        live_goals,
        selected_entries=portfolio["selected"],
        active_goal_count=active_goal_count,
        max_active_goals=int(settings.goal_scheduler_max_active_goals),
        hold_seconds=int(settings.goal_scheduler_hold_s),
        max_holds=max(1, len([item for item in portfolio["selected"] if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))])),
    )
    if policy_repo is not None and hasattr(policy_repo, "get_candidate_version"):
        candidate = policy_repo.get_candidate_version(tenant_id="default")
        if str(_as_dict(candidate).get("status") or "").upper() == "CANARY":
            shadow_bias = _policy_portfolio_bias(candidate)
            shadow_memory = _policy_memory_override(candidate)
            shadow_portfolio = select_goal_portfolio_slice(
                candidates,
                active_goal_count=active_goal_count,
                max_active_goals=int(settings.goal_scheduler_max_active_goals),
                dispatch_limit=max(1, limit),
                soft_preempt_threshold=float(settings.goal_scheduler_soft_preempt_threshold),
                portfolio_bias_override=shadow_bias,
                policy_memory_override=shadow_memory,
            )
            shadow_holds = recommend_goal_holds(
                live_goals,
                selected_entries=shadow_portfolio["selected"],
                active_goal_count=active_goal_count,
                max_active_goals=int(settings.goal_scheduler_max_active_goals),
                hold_seconds=int(settings.goal_scheduler_hold_s),
                max_holds=max(
                    1,
                    len(
                        [
                            item
                            for item in shadow_portfolio["selected"]
                            if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                        ]
                    ),
                ),
                portfolio_bias_override=shadow_bias,
                policy_memory_override=shadow_memory,
            )
            record_shadow_portfolio_probe(
                repo=policy_repo,
                tenant_id=str(_as_dict(candidate).get("tenant_id") or "default"),
                actor_user_id=None,
                candidate_version_id=str(_as_dict(candidate).get("version_id") or ""),
                probe={
                    "live_selected_goal_ids": [str(item.get("goal_id") or "") for item in portfolio["selected"]],
                    "shadow_selected_goal_ids": [str(item.get("goal_id") or "") for item in shadow_portfolio["selected"]],
                    "live_hold_goal_ids": [str(item.get("goal_id") or "") for item in hold_recommendations],
                    "shadow_hold_goal_ids": [str(item.get("goal_id") or "") for item in shadow_holds],
                    "live_soft_preempt_goal_ids": [
                        str(item.get("goal_id") or "")
                        for item in portfolio["selected"]
                        if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                    ],
                    "shadow_soft_preempt_goal_ids": [
                        str(item.get("goal_id") or "")
                        for item in shadow_portfolio["selected"]
                        if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                    ],
                    "live_external_wait_sources": _selected_external_wait_sources(portfolio["selected"]),
                    "shadow_external_wait_sources": _selected_external_wait_sources(shadow_portfolio["selected"]),
                    "high_urgency": any(
                        bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                        or float(_as_dict(item.get("portfolio")).get("portfolio_score") or 0.0) >= 0.85
                        for item in portfolio["selected"]
                    ),
                },
            )
            shadow_portfolio_context = {
                "version_id": str(_as_dict(candidate).get("version_id") or ""),
                "live_selected_goal_ids": [str(item.get("goal_id") or "") for item in portfolio["selected"]],
                "shadow_selected_goal_ids": [str(item.get("goal_id") or "") for item in shadow_portfolio["selected"]],
                "live_hold_goal_ids": [str(item.get("goal_id") or "") for item in hold_recommendations],
                "shadow_hold_goal_ids": [str(item.get("goal_id") or "") for item in shadow_holds],
                "live_soft_preempt_goal_ids": [
                    str(item.get("goal_id") or "")
                    for item in portfolio["selected"]
                    if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                ],
                "shadow_soft_preempt_goal_ids": [
                    str(item.get("goal_id") or "")
                    for item in shadow_portfolio["selected"]
                    if bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                ],
                "live_external_wait_sources": _selected_external_wait_sources(portfolio["selected"]),
                "shadow_external_wait_sources": _selected_external_wait_sources(shadow_portfolio["selected"]),
                "high_urgency": any(
                    bool(_as_dict(item.get("portfolio")).get("soft_preempt"))
                    or float(_as_dict(item.get("portfolio")).get("portfolio_score") or 0.0) >= 0.85
                    for item in portfolio["selected"]
                ),
            }
            shadow_portfolio_context["diverged"] = (
                shadow_portfolio_context["live_selected_goal_ids"] != shadow_portfolio_context["shadow_selected_goal_ids"]
                or shadow_portfolio_context["live_hold_goal_ids"] != shadow_portfolio_context["shadow_hold_goal_ids"]
                or shadow_portfolio_context["live_soft_preempt_goal_ids"] != shadow_portfolio_context["shadow_soft_preempt_goal_ids"]
                or shadow_portfolio_context["live_external_wait_sources"] != shadow_portfolio_context["shadow_external_wait_sources"]
            )
    if policy_repo is not None:
        starvation_candidate = next(
            (
                item
                for item in portfolio["deferred"]
                if float(_as_dict(item.get("portfolio")).get("portfolio_score") or 0.0)
                >= float(settings.goal_scheduler_starvation_score_threshold)
                and float(_as_dict(item.get("portfolio")).get("age_minutes") or 0.0)
                >= float(settings.goal_scheduler_starvation_min_age_min)
            ),
            None,
        )
        if starvation_candidate is not None:
            goal_state = _as_dict(_as_dict(starvation_candidate.get("goal_row")).get("goal_state"))
            record_portfolio_feedback(
                repo=policy_repo,
                tenant_id=str(_as_dict(starvation_candidate.get("goal_row")).get("tenant_id") or "default"),
                actor_user_id=None,
                feedback={
                    "event_kind": "goal_starved",
                    "goal_id": str(starvation_candidate.get("goal_id") or ""),
                    "urgency_score": float(_as_dict(goal_state.get("agenda")).get("priority_score") or 0.0),
                },
            )
    for hold in hold_recommendations:
        if not hasattr(goal_repo, "get_goal") or not hasattr(goal_repo, "update_goal_portfolio"):
            continue
        existing = goal_repo.get_goal(
            tenant_id=str(hold.get("tenant_id") or "default"),
            goal_id=str(hold.get("goal_id") or ""),
        )
        if not existing:
            continue
        existing_state = _as_dict(existing.get("goal_state"))
        existing_portfolio = {
            **_as_dict(existing_state.get("portfolio")),
            **dict(hold),
            "dispatch_decision": "hold",
        }
        if (
            bool(settings.goal_scheduler_active_preemption_enabled)
            and cancel_workflow is not None
            and str(existing.get("current_task_id") or "")
        ):
            run = task_repo.get_latest_run_for_task(
                str(hold.get("tenant_id") or existing.get("tenant_id") or "default"),
                str(existing.get("current_task_id") or ""),
            )
            workflow_id = str(_as_dict(run).get("workflow_id") or "")
            if workflow_id:
                await cancel_workflow(workflow_id)
                existing_portfolio["hold_status"] = "PREEMPTING"
                existing_portfolio["preempt_requested_at"] = datetime.now(timezone.utc).isoformat()
                existing_portfolio["preempted_task_id"] = str(existing.get("current_task_id") or "")
                existing_portfolio["preempted_workflow_id"] = workflow_id
                preempted += 1
                record_portfolio_feedback(
                    repo=policy_repo,
                    tenant_id=str(hold.get("tenant_id") or existing.get("tenant_id") or "default"),
                    actor_user_id=None,
                    feedback={
                        "event_kind": "preempt_cancel",
                        "goal_id": str(hold.get("held_by_goal_id") or ""),
                        "held_goal_id": str(hold.get("goal_id") or ""),
                        "urgency_score": float(_as_dict(existing_state.get("agenda")).get("priority_score") or 0.0),
                    },
                )
            else:
                record_portfolio_feedback(
                    repo=policy_repo,
                    tenant_id=str(hold.get("tenant_id") or existing.get("tenant_id") or "default"),
                    actor_user_id=None,
                    feedback={
                        "event_kind": "hold",
                        "goal_id": str(hold.get("held_by_goal_id") or ""),
                        "held_goal_id": str(hold.get("goal_id") or ""),
                        "urgency_score": float(_as_dict(existing_state.get("agenda")).get("priority_score") or 0.0),
                    },
                )
        elif policy_repo is not None:
            record_portfolio_feedback(
                repo=policy_repo,
                tenant_id=str(hold.get("tenant_id") or existing.get("tenant_id") or "default"),
                actor_user_id=None,
                feedback={
                    "event_kind": "hold",
                    "goal_id": str(hold.get("held_by_goal_id") or ""),
                    "held_goal_id": str(hold.get("goal_id") or ""),
                    "urgency_score": float(_as_dict(existing_state.get("agenda")).get("priority_score") or 0.0),
                },
            )
        goal_repo.update_goal_portfolio(
            tenant_id=str(hold.get("tenant_id") or existing.get("tenant_id") or "default"),
            goal_id=str(hold.get("goal_id") or ""),
            portfolio=existing_portfolio,
        )

    for selected in portfolio["selected"]:
        staged_row = dict(_as_dict(selected.get("goal_row")))
        staged_goal_state = _as_dict(staged_row.get("goal_state"))
        staged_goal_state["portfolio"] = _as_dict(selected.get("portfolio"))
        staged_row["goal_state"] = staged_goal_state
        staged_row["portfolio"] = _as_dict(selected.get("portfolio"))
        claimed_goal = (
            goal_repo.claim_goal_for_scheduler(
                tenant_id=str(staged_row.get("tenant_id") or ""),
                goal_id=str(staged_row.get("goal_id") or ""),
                cooldown_s=int(settings.goal_scheduler_cooldown_s),
            )
            if hasattr(goal_repo, "claim_goal_for_scheduler")
            else staged_row
        )
        if not claimed_goal:
            skipped += 1
            continue
        goal_row = dict(claimed_goal)
        goal_state = _as_dict(goal_row.get("goal_state"))
        prior_portfolio = _as_dict(goal_state.get("portfolio"))
        goal_state["portfolio"] = {
            **_as_dict(selected.get("portfolio")),
            "hold_status": "ACTIVE",
            "held_by_goal_id": None,
            "hold_reason": None,
            "hold_until": None,
            "preempt_requested_at": None,
            "preempted_task_id": None,
            "preempted_workflow_id": None,
            "resume_strategy": str(prior_portfolio.get("resume_strategy") or ""),
            "last_preempted_at": str(prior_portfolio.get("last_preempted_at") or prior_portfolio.get("preempt_requested_at") or ""),
            "last_held_by_goal_id": str(prior_portfolio.get("last_held_by_goal_id") or prior_portfolio.get("held_by_goal_id") or ""),
            "last_hold_reason": str(prior_portfolio.get("last_hold_reason") or prior_portfolio.get("hold_reason") or ""),
            "shadow_portfolio": dict(shadow_portfolio_context or {}),
        }
        goal_row["goal_state"] = goal_state
        goal_row["portfolio"] = _as_dict(goal_state.get("portfolio"))
        processed += 1
        if not goal_needs_continuation(goal_row):
            skipped += 1
            continue
        try:
            req = build_goal_continuation_request(goal_row)
            if policy_repo is not None and bool(_as_dict(req.input["runtime_state"].get("scheduler")).get("subscription_timeout_recovery")):
                record_portfolio_feedback(
                    repo=policy_repo,
                    tenant_id=str(goal_row.get("tenant_id") or "default"),
                    actor_user_id=None,
                    feedback={
                        "event_kind": "subscription_timeout",
                        "goal_id": str(goal_row.get("goal_id") or ""),
                        "urgency_score": float(_as_dict(_as_dict(goal_row.get("goal_state")).get("agenda")).get("priority_score") or 0.0),
                    },
                )
            user = {"id": str(goal_row.get("user_id") or "")}
            created = await service_create_task(
                task_repo=task_repo,
                req=req,
                tenant_id=str(goal_row.get("tenant_id") or ""),
                user=user,
                trace_id=f"goal-{str(goal_row.get('goal_id') or '')}",
                start_workflow=start_workflow,
            )
            goal_state = _as_dict(goal_row.get("goal_state"))
            scheduler_state = _as_dict(goal_state.get("scheduler"))
            scheduler_state["last_task_id"] = str(created.get("task_id") or "")
            scheduler_state["last_run_id"] = str(created.get("run_id") or "")
            scheduler_state["last_dispatch_status"] = str(created.get("status") or "")
            scheduler_state["portfolio_score"] = float(_as_dict(goal_state.get("portfolio")).get("portfolio_score") or 0.0)
            scheduler_state["dispatch_decision"] = str(_as_dict(goal_state.get("portfolio")).get("dispatch_decision") or "")
            scheduler_state["soft_preempt"] = bool(_as_dict(goal_state.get("portfolio")).get("soft_preempt"))
            goal_state["scheduler"] = scheduler_state
            goal_repo.attach_task_to_goal(
                tenant_id=str(goal_row.get("tenant_id") or ""),
                goal_id=str(goal_row.get("goal_id") or ""),
                task_id=str(created.get("task_id") or ""),
                goal_state=goal_state,
            )
            scheduled += 1
        except Exception as exc:  # pragma: no cover - service tests cover path with fakes
            failed += 1
            logger.exception(
                "goal_scheduler_dispatch_failed goal_id=%s error=%s",
                goal_row.get("goal_id"),
                exc,
            )

    skipped += len(portfolio["deferred"])

    return {
        "processed": processed,
        "scheduled": scheduled,
        "skipped": skipped,
        "failed": failed,
        "preempted": preempted,
    }


async def run_goal_scheduler(
    *,
    goal_repo: GoalRepository,
    task_repo: TaskRepository,
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
    cancel_workflow: Callable[[str], Awaitable[None]] | None = None,
    policy_repo: PolicyMemoryRepository | None = None,
) -> None:
    interval = max(1.0, float(settings.goal_scheduler_interval_s))
    while True:
        try:
            await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=task_repo,
                start_workflow=start_workflow,
                cancel_workflow=cancel_workflow,
                policy_repo=policy_repo,
            )
        except Exception as exc:  # pragma: no cover - defensive loop guard
            logger.exception("goal_scheduler_loop_error error=%s", exc)
        await asyncio.sleep(interval)
