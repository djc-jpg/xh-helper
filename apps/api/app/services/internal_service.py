from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from ..masking import mask_payload, summarize_payload
from ..metrics import (
    internal_status_ignored_total,
    internal_status_rejected_total,
    task_cost_usd,
    task_failure_total,
    task_success_total,
)
from ..repositories import (
    AssistantConversationRepository,
    AssistantEpisodeRepository,
    AssistantTurnRepository,
    GoalRepository,
    PolicyMemoryRepository,
    TaskRepository,
    normalize_task_failure_fields,
)
from ..state_machine import FINAL_STATES, is_valid_transition
from ..tool_gateway import ToolGateway
from .agent_runtime_core import build_episode, merge_runtime_state
from .goal_runtime_service import build_preempted_goal_runtime, sync_goal_progress
from .goal_runtime_service import resume_waiting_goals_for_event
from .policy_memory_service import (
    record_episode_feedback,
    record_portfolio_feedback,
    record_shadow_policy_outcome,
    record_shadow_portfolio_outcome,
)


def _validate_worker_binding(binding: dict[str, Any], worker_id: str) -> None:
    assigned_worker = str(binding.get("assigned_worker") or "")
    if not assigned_worker or assigned_worker != worker_id:
        raise HTTPException(status_code=403, detail="worker identity mismatch")


def _safe_internal_status_audit(
    *,
    task_repo: TaskRepository,
    tenant_id: str,
    task_id: str,
    run_id: str,
    trace_id: str,
    reason: str,
    worker_id: str,
    status_event_id: str,
    current_status: str | None,
    incoming_status: str | None,
    extra: dict[str, Any] | None = None,
) -> None:
    detail = {
        "reason": reason,
        "task_id": task_id,
        "run_id": run_id,
        "worker_id": summarize_payload(worker_id),
        "status_event_id": summarize_payload(status_event_id),
        "current_status": summarize_payload(current_status or ""),
        "incoming_status": summarize_payload(incoming_status or ""),
    }
    if extra:
        detail["extra"] = mask_payload(extra)
    try:
        task_repo.insert_audit_log(
            tenant_id=tenant_id,
            actor_user_id=None,
            action="internal_status_guardrail",
            target_type="task",
            target_id=task_id,
            detail_masked=detail,
            trace_id=trace_id or "worker",
        )
    except Exception:
        # Guardrail observability must not break internal status ingest path.
        return


async def execute_internal_tool(
    *,
    task_repo: TaskRepository,
    gateway: ToolGateway,
    req: Any,
    worker_id: str,
) -> dict[str, Any]:
    binding = task_repo.get_run_binding_any_tenant(task_id=req.task_id, run_id=req.run_id)
    if not binding:
        raise HTTPException(status_code=409, detail="task/run binding mismatch")
    tenant_id = str(binding["tenant_id"])
    if req.tenant_id and str(req.tenant_id) != tenant_id:
        raise HTTPException(status_code=403, detail="tenant mismatch")
    _validate_worker_binding(binding, worker_id)
    task = task_repo.get_task_by_id(tenant_id=tenant_id, task_id=req.task_id)
    if not task:
        raise HTTPException(status_code=409, detail="task/run binding mismatch")
    if str(req.task_type) != str(task["task_type"]):
        raise HTTPException(status_code=403, detail="task type mismatch")
    if str(req.caller_user_id) != str(task["created_by"]):
        raise HTTPException(status_code=403, detail="caller identity mismatch")
    payload = req.model_dump()
    payload["tenant_id"] = tenant_id
    payload["task_type"] = str(task["task_type"])
    payload["caller_user_id"] = str(task["created_by"])
    payload["worker_id"] = worker_id
    result = await gateway.execute(payload)
    if result.get("status") == "SUCCEEDED":
        return result

    reason = str(result.get("reason_code") or "")
    detail = {"reason_code": reason}
    if reason in {
        "write_requires_operator",
        "write_requires_approval",
        "approval_context_invalid",
        "approval_invalid",
        "approval_not_approved",
        "policy_deny",
        "policy_default_deny",
    }:
        raise HTTPException(status_code=403, detail=detail)
    if reason in {"schema_invalid", "unknown_tool", "output_schema_invalid", "EGRESS_DOMAIN_NOT_ALLOWLISTED"}:
        raise HTTPException(status_code=400, detail=detail)
    if reason in {"adapter_http_408"}:
        raise HTTPException(status_code=408, detail=detail)
    if reason in {"adapter_http_429", "rate_limited_user_tool", "idempotency_in_progress"}:
        raise HTTPException(status_code=429, detail=detail)
    if reason in {"adapter_http_5xx", "timeout", "adapter_error"}:
        raise HTTPException(status_code=502, detail=detail)
    if reason in {"adapter_http_4xx"}:
        raise HTTPException(status_code=400, detail=detail)
    raise HTTPException(status_code=424, detail=detail)


def update_internal_task_status(
    *,
    task_repo: TaskRepository,
    task_id: str,
    body: dict[str, Any],
    worker_id: str,
    conversation_repo: AssistantConversationRepository | None = None,
    turn_repo: AssistantTurnRepository | None = None,
    episode_repo: AssistantEpisodeRepository | None = None,
    policy_repo: PolicyMemoryRepository | None = None,
    goal_repo: GoalRepository | None = None,
) -> dict[str, Any]:
    run_id = body.get("run_id")
    status_text = body.get("status")
    step_key = body.get("step_key", "worker")
    payload = body.get("payload", {})
    trace_id = body.get("trace_id", body.get("traceId", ""))
    status_event_id = str(body.get("status_event_id") or "")
    body_tenant = str(body.get("tenant_id") or "")
    if not run_id or not status_text or not status_event_id:
        raise HTTPException(status_code=400, detail="run_id, status and status_event_id required")

    binding = task_repo.get_run_binding_any_tenant(task_id=task_id, run_id=run_id)
    if not binding:
        raise HTTPException(status_code=409, detail="task/run binding mismatch")
    tenant_id = str(binding["tenant_id"])
    if body_tenant and body_tenant != tenant_id:
        internal_status_rejected_total.labels(reason="tenant_mismatch").inc()
        _safe_internal_status_audit(
            task_repo=task_repo,
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=str(run_id),
            trace_id=trace_id or "worker",
            reason="tenant_mismatch",
            worker_id=worker_id,
            status_event_id=status_event_id,
            current_status=str(binding.get("status") or ""),
            incoming_status=str(status_text),
            extra={"body_tenant": body_tenant, "binding_tenant": tenant_id},
        )
        raise HTTPException(status_code=403, detail="tenant mismatch")
    try:
        _validate_worker_binding(binding, worker_id)
    except HTTPException:
        internal_status_rejected_total.labels(reason="worker_mismatch").inc()
        _safe_internal_status_audit(
            task_repo=task_repo,
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=str(run_id),
            trace_id=trace_id or "worker",
            reason="worker_mismatch",
            worker_id=worker_id,
            status_event_id=status_event_id,
            current_status=str(binding.get("status") or ""),
            incoming_status=str(status_text),
            extra={"assigned_worker": str(binding.get("assigned_worker") or "")},
        )
        raise
    if task_repo.has_status_event(tenant_id=tenant_id, run_id=run_id, status_event_id=status_event_id):
        return {"status": "ok", "idempotent": True}
    current_status = str(binding["status"])
    if current_status in FINAL_STATES and status_text != current_status:
        internal_status_ignored_total.inc()
        internal_status_rejected_total.labels(reason="ignored_terminal_update").inc()
        _safe_internal_status_audit(
            task_repo=task_repo,
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=str(run_id),
            trace_id=trace_id or "worker",
            reason="ignored_terminal_update",
            worker_id=worker_id,
            status_event_id=status_event_id,
            current_status=current_status,
            incoming_status=str(status_text),
        )
        return {
            "status": "ok",
            "idempotent": True,
            "ignored": True,
            "ignored_reason": "terminal_state_absorbed",
        }
    if not is_valid_transition(current_status, status_text):
        internal_status_rejected_total.labels(reason="invalid_transition").inc()
        _safe_internal_status_audit(
            task_repo=task_repo,
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=str(run_id),
            trace_id=trace_id or "worker",
            reason="invalid_transition",
            worker_id=worker_id,
            status_event_id=status_event_id,
            current_status=current_status,
            incoming_status=str(status_text),
        )
        raise HTTPException(
            status_code=409,
            detail=f"invalid status transition {current_status} -> {status_text}",
        )

    inserted = task_repo.append_step(
        tenant_id=tenant_id,
        run_id=run_id,
        status_text=status_text,
        step_key=step_key,
        payload_masked=mask_payload(payload),
        trace_id=trace_id or "worker",
        span_id=body.get("span_id"),
        attempt=int(body.get("attempt") or 1),
        status_event_id=status_event_id,
    )
    if not inserted:
        return {"status": "ok", "idempotent": True}

    if status_text == "SUCCEEDED":
        task_repo.mark_task_succeeded(tenant_id, task_id, mask_payload(payload))
    elif status_text in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
        raw_error_code = str(payload.get("error_code") or payload.get("reason_code") or "")
        raw_error_message = payload.get("error")
        if raw_error_message in (None, ""):
            raw_error_message = payload or {"status": status_text}
        error_code, error_message = normalize_task_failure_fields(
            status_text=status_text,
            error_code=raw_error_code,
            error_message=raw_error_message,
        )
        task_repo.mark_task_failed(
            tenant_id=tenant_id,
            task_id=task_id,
            status_text=status_text,
            error_code=error_code,
            error_message=error_message,
        )
    else:
        task_repo.update_task_status(tenant_id, task_id, status_text)

    task_repo.update_run_status(tenant_id, run_id, status_text)
    if status_text == "SUCCEEDED":
        task_success_total.inc()
    if status_text in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
        task_failure_total.inc()
    if "cost" in body:
        amount = float(body["cost"])
        task_repo.add_task_cost(tenant_id, task_id, run_id, amount)
        task_cost_usd.labels(task_id=task_id).set(task_repo.get_task_cost(tenant_id, task_id))

    try:
        task = task_repo.get_task_by_id(tenant_id=tenant_id, task_id=task_id, include_sensitive=True)
        if task:
            existing_runtime = dict(task.get("runtime_state") or {})
            agent_runtime = dict(payload.get("agent_runtime") or {})
            latest_result = dict(existing_runtime.get("task_state", {}).get("latest_result") or {})
            if status_text == "SUCCEEDED":
                latest_result = {"status": status_text, "output": payload.get("output") or payload}
            elif status_text == "CANCELLED":
                latest_result = {
                    "status": status_text,
                    "reason_code": payload.get("reason_code") or "cancelled",
                    "error": payload.get("error") or payload,
                }
            elif status_text in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
                latest_result = {
                    "status": status_text,
                    "reason_code": payload.get("reason_code") or payload.get("error_code"),
                    "error": payload.get("error") or payload,
                }
            runtime_patch = {
                **agent_runtime,
                "status": status_text,
                "current_phase": str(agent_runtime.get("current_phase") or step_key or existing_runtime.get("current_phase") or "observe"),
            }
            task_state = dict(agent_runtime.get("task_state") or existing_runtime.get("task_state") or {})
            if task_state:
                task_state["current_phase"] = runtime_patch["current_phase"]
                task_state["latest_result"] = latest_result
                runtime_patch["task_state"] = task_state
            if status_text == "SUCCEEDED":
                runtime_patch["final_output"] = dict(agent_runtime.get("final_output") or existing_runtime.get("final_output") or {})
                if payload.get("output"):
                    runtime_patch["final_output"]["message"] = str(payload.get("output"))
            merged_runtime = merge_runtime_state(existing_runtime, runtime_patch)
            goal_row = None
            if goal_repo and str(task.get("goal_id") or ""):
                goal_row = goal_repo.get_goal(tenant_id=tenant_id, goal_id=str(task.get("goal_id") or ""))
                if status_text == "CANCELLED":
                    merged_runtime = build_preempted_goal_runtime(
                        merged_runtime,
                        goal_row=goal_row,
                        task_id=str(task_id),
                    )
            task_repo.update_task_runtime_state(tenant_id=tenant_id, task_id=task_id, runtime_state=merged_runtime)
            goal_source = dict(merged_runtime.get("goal") or {})
            goal_id = str(goal_source.get("goal_id") or task.get("goal_id") or "")
            if goal_source and goal_repo:
                goal_row = sync_goal_progress(
                    repo=goal_repo,
                    tenant_id=tenant_id,
                    user_id=str(task.get("created_by") or ""),
                    conversation_id=str(task.get("conversation_id") or "") or None,
                    goal={**goal_source, "goal_id": goal_id},
                    runtime_state=merged_runtime,
                    task_id=str(task_id),
                    turn_id=str(task.get("assistant_turn_id") or "") or None,
                    goal_id=goal_id or None,
                )
                merged_goal = dict(merged_runtime.get("goal") or {})
                merged_goal["goal_id"] = str(goal_row.get("goal_id") or goal_id)
                merged_goal["lifecycle_state"] = str(goal_row.get("status") or merged_goal.get("lifecycle_state") or "")
                merged_runtime["goal"] = merged_goal
                task_repo.update_task_runtime_state(tenant_id=tenant_id, task_id=task_id, runtime_state=merged_runtime)
            if goal_repo and status_text in FINAL_STATES:
                resume_waiting_goals_for_event(
                    repo=goal_repo,
                    tenant_id=tenant_id,
                    event_kind="task_completion",
                    event_key=str(task_id),
                    event_payload={
                        "task_id": str(task_id),
                        "status": status_text,
                        "step_key": step_key,
                    },
                    limit=20,
                )
            if turn_repo and str(task.get("assistant_turn_id") or ""):
                turn = turn_repo.get_turn(tenant_id=tenant_id, turn_id=str(task.get("assistant_turn_id") or ""))
                if turn:
                    assistant_message: str | None = None
                    response_type = str(turn.get("response_type") or "task_created")
                    if status_text == "SUCCEEDED":
                        assistant_message = str((merged_runtime.get("final_output") or {}).get("message") or payload.get("output") or "")
                    elif status_text in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
                        assistant_message = str(payload.get("error") or payload.get("reason_code") or "Workflow execution failed.")
                    turn_repo.update_turn(
                        tenant_id=tenant_id,
                        turn_id=str(task.get("assistant_turn_id") or ""),
                        route=str(turn.get("route") or "workflow_task"),
                        status=status_text,
                        current_phase=str(merged_runtime.get("current_phase") or turn.get("current_phase") or "observe"),
                        response_type=response_type,
                        assistant_message=assistant_message or turn.get("assistant_message"),
                        task_id=str(task_id),
                        runtime_state=merged_runtime,
                    )
            if conversation_repo and str(task.get("conversation_id") or ""):
                conversation_repo.update_memory(
                    tenant_id=tenant_id,
                    user_id=str(task.get("created_by") or ""),
                    conversation_id=str(task.get("conversation_id") or ""),
                    last_task_result={
                        "task_id": task_id,
                        "status": status_text,
                        "step_key": step_key,
                        "output": payload.get("output"),
                        "error": payload.get("error") or payload.get("reason_code"),
                    },
                )
            if episode_repo and status_text in FINAL_STATES:
                episode = build_episode(
                    episode_id=f"episode-{str(task.get('assistant_turn_id') or task_id)}",
                    user_message=str((task.get("input_masked") or {}).get("message") or ""),
                    goal=dict(merged_runtime.get("goal") or {"normalized_goal": str((task.get("input_masked") or {}).get("message") or "")}),
                    action=dict(merged_runtime.get("current_action") or {"action_type": "workflow_call"}),
                    task_state=dict(merged_runtime.get("task_state") or {}),
                    reflection=dict(merged_runtime.get("reflection") or {}),
                    policy=dict(merged_runtime.get("policy") or {}),
                    tool_names=[
                        str(item.get("tool_id") or "")
                        for item in task_repo.list_tool_calls_for_task(tenant_id=tenant_id, task_id=task_id)
                        if str(item.get("tool_id") or "")
                    ],
                    outcome_status=status_text,
                    final_outcome=str((merged_runtime.get("final_output") or {}).get("message") or payload.get("output") or payload.get("error") or ""),
                )
                episode_repo.upsert_episode(
                    tenant_id=tenant_id,
                    user_id=str(task.get("created_by") or ""),
                    conversation_id=str(task.get("conversation_id") or "") or None,
                    turn_id=str(task.get("assistant_turn_id") or "") or None,
                    task_id=str(task_id),
                    episode=episode,
                )
                if policy_repo:
                    try:
                        record_episode_feedback(
                            repo=policy_repo,
                            tenant_id=tenant_id,
                            actor_user_id=str(task.get("created_by") or ""),
                            episode=episode,
                        )
                    except Exception:
                        pass
                    try:
                        policy_state = dict(merged_runtime.get("policy") or {})
                        shadow_policy = dict(policy_state.get("shadow_policy") or {})
                        candidate_shadow_version_id = (
                            str(policy_state.get("policy_version_id") or "")
                            if str((policy_state.get("policy_selector") or {}).get("mode") or "") == "canary"
                            else str(shadow_policy.get("version_id") or "")
                        )
                        if candidate_shadow_version_id:
                            record_shadow_policy_outcome(
                                repo=policy_repo,
                                tenant_id=tenant_id,
                                actor_user_id=str(task.get("created_by") or ""),
                                candidate_version_id=candidate_shadow_version_id,
                                outcome={
                                    "goal_id": str((merged_runtime.get("goal") or {}).get("goal_id") or task.get("goal_id") or ""),
                                    "conversation_id": str(task.get("conversation_id") or ""),
                                    "live_policy_version_id": str(policy_state.get("policy_version_id") or ""),
                                    "shadow_policy_version_id": str(shadow_policy.get("version_id") or ""),
                                    "live_action": str((merged_runtime.get("current_action") or {}).get("action_type") or ""),
                                    "shadow_action": str(shadow_policy.get("action_type") or ""),
                                    "outcome_status": status_text,
                                    "risk_level": str((merged_runtime.get("goal") or {}).get("risk_level") or ""),
                                    "diverged": (
                                        str((merged_runtime.get("current_action") or {}).get("action_type") or "")
                                        != str(shadow_policy.get("action_type") or "")
                                        or str(merged_runtime.get("route") or "")
                                        != str(shadow_policy.get("route") or "")
                                    ),
                                },
                            )
                    except Exception:
                        pass
                    try:
                        portfolio_state = dict(merged_runtime.get("portfolio") or {})
                        shadow_portfolio = dict(portfolio_state.get("shadow_portfolio") or {})
                        candidate_shadow_portfolio_version_id = str(shadow_portfolio.get("version_id") or "")
                        if candidate_shadow_portfolio_version_id:
                            record_shadow_portfolio_outcome(
                                repo=policy_repo,
                                tenant_id=tenant_id,
                                actor_user_id=str(task.get("created_by") or ""),
                                candidate_version_id=candidate_shadow_portfolio_version_id,
                                outcome={
                                    "goal_id": str((merged_runtime.get("goal") or {}).get("goal_id") or task.get("goal_id") or ""),
                                    "conversation_id": str(task.get("conversation_id") or ""),
                                    "live_policy_version_id": str(policy_state.get("policy_version_id") or ""),
                                    "shadow_policy_version_id": candidate_shadow_portfolio_version_id,
                                    "live_goal_id": str((merged_runtime.get("goal") or {}).get("goal_id") or task.get("goal_id") or ""),
                                    "shadow_selected_goal_ids": list(shadow_portfolio.get("shadow_selected_goal_ids") or []),
                                    "outcome_status": status_text,
                                    "risk_level": str((merged_runtime.get("goal") or {}).get("risk_level") or ""),
                                    "high_urgency": bool(shadow_portfolio.get("high_urgency")),
                                    "diverged": bool(shadow_portfolio.get("diverged")),
                                    "live_external_wait_sources": list(shadow_portfolio.get("live_external_wait_sources") or []),
                                    "shadow_external_wait_sources": list(shadow_portfolio.get("shadow_external_wait_sources") or []),
                                },
                            )
                    except Exception:
                        pass
                portfolio_state = dict(merged_runtime.get("portfolio") or {})
                if str(portfolio_state.get("resume_strategy") or "") == "replan_after_preemption":
                    if status_text == "SUCCEEDED":
                        try:
                            record_portfolio_feedback(
                                repo=policy_repo,
                                tenant_id=tenant_id,
                                actor_user_id=str(task.get("created_by") or ""),
                                feedback={
                                    "event_kind": "preempt_resume_success",
                                    "goal_id": str((merged_runtime.get("goal") or {}).get("goal_id") or task.get("goal_id") or ""),
                                    "held_goal_id": str(portfolio_state.get("last_held_by_goal_id") or ""),
                                    "urgency_score": float((merged_runtime.get("agenda") or {}).get("priority_score") or 0.0),
                                },
                            )
                        except Exception:
                            pass
                    elif status_text in {"FAILED_FINAL", "TIMED_OUT", "FAILED_RETRYABLE"}:
                        try:
                            record_portfolio_feedback(
                                repo=policy_repo,
                                tenant_id=tenant_id,
                                actor_user_id=str(task.get("created_by") or ""),
                                feedback={
                                    "event_kind": "preempt_resume_regret",
                                    "goal_id": str((merged_runtime.get("goal") or {}).get("goal_id") or task.get("goal_id") or ""),
                                    "held_goal_id": str(portfolio_state.get("last_held_by_goal_id") or ""),
                                    "urgency_score": float((merged_runtime.get("agenda") or {}).get("priority_score") or 0.0),
                                },
                            )
                        except Exception:
                            pass
    except Exception:
        # Runtime mirrors and episode extraction are best-effort. Internal status ingest
        # must stay compatible with older tests and environments that don't provide DB-backed task lookup.
        pass
    return {"status": "ok", "idempotent": False}
