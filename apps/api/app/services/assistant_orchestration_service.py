from __future__ import annotations

from datetime import datetime, timezone
import uuid
from typing import Any, Awaitable, Callable

from fastapi import HTTPException
from runtime_backbone import apply_runtime_event, select_next_runtime_step

from ..config import settings
from ..input_crypto import encrypt_input_payload
from ..masking import mask_payload
from ..qwen_client import qwen_client
from ..repositories import (
    AssistantConversationRepository,
    AssistantEpisodeRepository,
    AssistantTurnRepository,
    GoalRepository,
    PolicyMemoryRepository,
    TaskRepository,
    ToolRepository,
)
from ..schemas import AssistantChatRequest, TaskCreateRequest
from ..tool_gateway import ToolGateway
from .agent_runtime_core import (
    build_episode,
    build_task_state,
    build_unified_task,
    normalize_goal,
    retrieve_relevant_episodes,
)
from .assistant_runtime_service import build_turn_summary
from .goal_runtime_service import sync_goal_progress
from .goal_runtime_service import resume_waiting_goals_for_event
from .planner_service import PlannerService
from .policy_memory_service import (
    build_runtime_policy_memory,
    record_shadow_policy_probe,
    record_episode_feedback,
    select_shadow_policy_version,
    select_runtime_policy_version,
)
from .retrieval_service import RetrievalService
from .task_service import create_task as service_create_task
from .tool_registry_service import ToolRegistryService

RETRYABLE_TOOL_DENY_REASONS = {
    "adapter_http_408",
    "adapter_http_429",
    "adapter_http_5xx",
    "timeout",
    "adapter_network_error",
    "idempotency_in_progress",
}
MAX_CONVERSATION_HISTORY = 16
WORKFLOW_HISTORY_WINDOW = 8


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _status_event_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:24]}"


def _memory_view(conversation: dict[str, Any]) -> dict[str, Any]:
    return {
        "last_task_result": conversation.get("last_task_result") or {},
        "last_tool_result": conversation.get("last_tool_result") or {},
        "user_preferences": conversation.get("user_preferences") or {},
    }


def _memory_from_row(row: dict[str, Any] | None, fallback: dict[str, Any]) -> dict[str, Any]:
    if not row:
        return fallback
    return {
        "last_task_result": row.get("last_task_result") or {},
        "last_tool_result": row.get("last_tool_result") or {},
        "user_preferences": row.get("user_preferences") or {},
    }


def _fallback_response_with_retrieval(message: str, retrieval_hits: list[dict[str, Any]], memory: dict[str, Any]) -> str:
    lowered = message.lower()
    if "last tool" in lowered and memory.get("last_tool_result"):
        return f"Last tool result snapshot: {memory.get('last_tool_result')}"
    if "last task" in lowered and memory.get("last_task_result"):
        return f"Last task snapshot: {memory.get('last_task_result')}"
    if retrieval_hits:
        top = retrieval_hits[0]
        answer = f"{message}\n\nReference: {top.get('title', 'doc')} - {top.get('snippet', '')}"
    else:
        answer = "I can help with this request and use tools/workflows when needed."
    prefs = memory.get("user_preferences") or {}
    style = str(prefs.get("response_style") or "").strip().lower()
    if style == "concise":
        return answer[:220]
    return answer


def _recent_history_text(history: list[dict[str, Any]], current_message: str) -> str:
    rows = list(history[-6:])
    rows.append({"role": "user", "message": current_message})
    return "\n".join(f"- {item.get('role', 'user')}: {item.get('message', '')}" for item in rows if item.get("message"))


def _retrieval_text(retrieval_hits: list[dict[str, Any]]) -> str:
    if not retrieval_hits:
        return "None."
    return "\n".join(
        f"- {str(hit.get('title') or hit.get('source') or 'doc')}: {str(hit.get('snippet') or '')[:280]}"
        for hit in retrieval_hits[:3]
    )


async def _response_with_retrieval(
    message: str,
    retrieval_hits: list[dict[str, Any]],
    memory: dict[str, Any],
    history: list[dict[str, Any]],
) -> str:
    fallback = _fallback_response_with_retrieval(message, retrieval_hits, memory)
    if not qwen_client.is_enabled():
        return fallback

    preferences = memory.get("user_preferences") or {}
    system_prompt = (
        "You are XH Helper, a goal-driven agent runtime. "
        "Answer clearly and truthfully. Use provided retrieval context when present. "
        "Do not invent tools, approvals, workflow states, or sources that are not in the prompt."
    )
    user_prompt = (
        f"User request:\n{message}\n\n"
        f"Recent conversation:\n{_recent_history_text(history, message)}\n\n"
        f"Memory snapshot:\n"
        f"- last_task_result: {memory.get('last_task_result') or {}}\n"
        f"- last_tool_result: {memory.get('last_tool_result') or {}}\n"
        f"- user_preferences: {preferences}\n\n"
        f"Retrieved references:\n{_retrieval_text(retrieval_hits)}\n\n"
        "Respond in plain text. Keep it concise but useful. "
        "If references are provided, ground the answer in them and mention source titles briefly."
    )
    try:
        answer = await qwen_client.chat_text(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
            max_tokens=420,
            timeout_s=min(settings.qwen_timeout_s, 15.0),
        )
        return answer or fallback
    except Exception:
        return fallback


def _tool_payload_from_message(message: str) -> dict[str, Any]:
    lowered = message.strip().lower()
    if "record" in lowered:
        return {"method": "GET", "path": "/records", "params": {"q": lowered[:120]}}
    return {"query": lowered[:200], "domain": "example.com", "top_k": 3}


def _workflow_reply(task_id: str) -> str:
    return f"Long-running task started. Track it with task_id={task_id}."


def _confirmed(metadata: dict[str, Any]) -> bool:
    raw = metadata.get("confirmed")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "confirmed"}
    return False


def _resolve_task_type(plan: dict[str, Any]) -> str:
    task_type = str(plan.get("task_type") or "rag_qa")
    if plan.get("action") == "need_approval":
        return "ticket_email"
    return task_type


def _runtime_observation(kind: str, summary: str, *, source: str | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "kind": kind,
        "summary": summary,
        "source": source,
        "payload": payload or {},
    }


def _runtime_state_snapshot(
    *,
    goal: dict[str, Any] | None = None,
    task_state: dict[str, Any] | None = None,
    current_action: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    status: str | None = None,
    current_phase: str | None = None,
    latest_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    goal_data = _as_dict(goal)
    task_data = _as_dict(task_state)
    action_data = _as_dict(current_action)
    policy_data = _as_dict(policy)
    latest = _as_dict(latest_result) or _as_dict(task_data.get("latest_result"))
    return {
        "status": status or None,
        "current_phase": current_phase or task_data.get("current_phase"),
        "goal": {
            "goal_id": str(goal_data.get("goal_id") or "") or None,
            "normalized_goal": str(goal_data.get("normalized_goal") or ""),
            "risk_level": str(goal_data.get("risk_level") or "") or None,
            "unknowns": list(goal_data.get("unknowns") or []),
        },
        "blockers": list(task_data.get("blockers") or []),
        "pending_approvals": list(task_data.get("pending_approvals") or []),
        "available_actions": list(task_data.get("available_actions") or []),
        "latest_result": latest,
        "current_action": {
            "action_type": str(action_data.get("action_type") or "") or None,
            "target": str(action_data.get("target") or "") or None,
            "expected_result": str(action_data.get("expected_result") or "") or None,
            "fallback": str(action_data.get("fallback") or "") or None,
        },
        "policy": {
            "selected_action": str(policy_data.get("selected_action") or "") or None,
            "fallback_action": str(policy_data.get("fallback_action") or "") or None,
            "policy_version_id": str(policy_data.get("policy_version_id") or "") or None,
        },
    }


def _runtime_step(
    key: str,
    phase: str,
    title: str,
    status: str,
    summary: str,
    *,
    observation: dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    reflection: dict[str, Any] | None = None,
    state_before: dict[str, Any] | None = None,
    state_after: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "phase": phase,
        "title": title,
        "status": status,
        "summary": summary,
        "created_at": _now_iso(),
        "observation": observation,
        "decision": decision,
        "reflection": reflection,
        "state_before": state_before or {},
        "state_after": state_after or {},
    }


def _build_turn_runtime(
    *,
    turn_id: str,
    route: str,
    status: str,
    current_phase: str,
    plan: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    memory: dict[str, Any],
    decision: dict[str, Any],
    reflection: dict[str, Any],
    steps: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    final_output: dict[str, Any],
    goal: dict[str, Any] | None = None,
    unified_task: dict[str, Any] | None = None,
    task_state: dict[str, Any] | None = None,
    current_action: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    episodes: list[dict[str, Any]] | None = None,
    task_id: str | None = None,
    goal_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "turn_id": turn_id,
        "route": route,
        "status": status,
        "current_phase": current_phase,
        "task_id": task_id,
        "goal_ref": goal_ref or {},
        "planner": plan,
        "retrieval_hits": retrieval_hits,
        "memory": memory,
        "goal": goal or {},
        "unified_task": unified_task or {},
        "task_state": task_state or {},
        "current_action": current_action or {},
        "policy": policy or {},
        "episodes": episodes or [],
        "decision": decision,
        "reflection": reflection,
        "steps": steps,
        "observations": observations,
        "final_output": final_output,
    }


def _apply_turn_runtime_event(
    *,
    turn_id: str,
    route: str,
    status: str,
    current_phase: str,
    plan: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    memory: dict[str, Any],
    decision: dict[str, Any],
    steps: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    final_output: dict[str, Any],
    summary: str,
    goal: dict[str, Any] | None = None,
    unified_task: dict[str, Any] | None = None,
    task_state: dict[str, Any] | None = None,
    current_action: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    episodes: list[dict[str, Any]] | None = None,
    task_id: str | None = None,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    target: str | None = None,
    goal_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_runtime = _build_turn_runtime(
        turn_id=turn_id,
        route=route,
        status=status,
        current_phase=current_phase,
        plan=plan,
        retrieval_hits=retrieval_hits,
        memory=memory,
        decision=decision,
        reflection={},
        steps=steps,
        observations=observations,
        final_output=final_output,
        goal=goal,
        unified_task=unified_task,
        task_state=task_state,
        current_action=current_action,
        policy=policy,
        episodes=episodes,
        task_id=task_id,
        goal_ref=goal_ref,
    )
    return apply_runtime_event(
        base_runtime,
        event_type=f"assistant.{current_phase}",
        status=status,
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        final_output=final_output,
        decision=decision,
        route=route,
        observations=observations,
        steps=steps,
        summary=summary,
        target=target,
    )

def _persist_turn_messages(
    *,
    conversation_repo: AssistantConversationRepository,
    tenant_id: str,
    user_id: str,
    conversation_id: str,
    user_message: str,
    assistant_message: str,
    route: str,
    turn_id: str,
) -> None:
    conversation_repo.append_message(
        tenant_id=tenant_id,
        user_id=user_id,
        conversation_id=conversation_id,
        role="user",
        message=user_message,
        route=route,
        metadata={"turn_id": turn_id},
        created_at=_now_iso(),
        max_messages=MAX_CONVERSATION_HISTORY,
    )
    conversation_repo.append_message(
        tenant_id=tenant_id,
        user_id=user_id,
        conversation_id=conversation_id,
        role="assistant",
        message=assistant_message,
        route=route,
        metadata={"turn_id": turn_id},
        created_at=_now_iso(),
        max_messages=MAX_CONVERSATION_HISTORY,
    )


def _safe_update_memory(
    *,
    conversation_repo: AssistantConversationRepository,
    tenant_id: str,
    user_id: str,
    conversation_id: str,
    last_task_result: dict[str, Any] | None = None,
    last_tool_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = conversation_repo.update_memory(
        tenant_id=tenant_id,
        user_id=user_id,
        conversation_id=conversation_id,
        last_task_result=last_task_result,
        last_tool_result=last_tool_result,
    )
    return _memory_from_row(
        out,
        {"last_task_result": {}, "last_tool_result": {}, "user_preferences": {}},
    )


def _safe_upsert_episode(
    *,
    episode_repo: AssistantEpisodeRepository,
    tenant_id: str,
    user_id: str,
    conversation_id: str,
    turn_id: str,
    task_id: str | None,
    episode: dict[str, Any],
) -> None:
    episode_repo.upsert_episode(
        tenant_id=tenant_id,
        user_id=user_id,
        conversation_id=conversation_id,
        turn_id=turn_id,
        task_id=task_id,
        episode=episode,
    )


def _safe_record_policy_feedback(
    *,
    policy_repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str,
    episode: dict[str, Any],
) -> None:
    try:
        record_episode_feedback(
            repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=actor_user_id,
            episode=episode,
        )
    except Exception:
        return


def _safe_sync_goal(
    *,
    goal_repo: GoalRepository | None,
    tenant_id: str,
    user_id: str,
    conversation_id: str,
    goal: dict[str, Any],
    runtime_state: dict[str, Any],
    task_id: str | None = None,
    turn_id: str | None = None,
) -> dict[str, Any]:
    try:
        return sync_goal_progress(
            repo=goal_repo,
            tenant_id=tenant_id,
            user_id=user_id,
            conversation_id=conversation_id,
            goal=goal,
            runtime_state=runtime_state,
            task_id=task_id,
            turn_id=turn_id,
            goal_id=str(goal.get("goal_id") or "") or None,
        )
    except Exception:
        return {
            "goal_id": str(goal.get("goal_id") or ""),
            "status": str(goal.get("lifecycle_state") or ""),
            "goal_state": {},
            "continuation_count": int(goal.get("continuation_count") or 0),
        }


def _workflow_input(
    *,
    req: AssistantChatRequest,
    conversation_id: str,
    turn_id: str,
    history: list[dict[str, Any]],
    metadata: dict[str, Any],
    plan: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    selected_tool_name: str,
    goal: dict[str, Any],
    unified_task: dict[str, Any],
    task_state: dict[str, Any],
    current_action: dict[str, Any],
    policy: dict[str, Any],
    episodes: list[dict[str, Any]],
    decision: dict[str, Any],
    reflection: dict[str, Any],
    steps: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    goal_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_state = {
        "turn_id": turn_id,
        "route": "workflow_task",
        "status": "QUEUED",
        "current_phase": str(task_state.get("current_phase") or "plan"),
        "task_id": None,
        "goal_ref": goal_ref or {},
        "planner": plan,
        "retrieval_hits": retrieval_hits,
        "memory": {},
        "goal": goal,
        "unified_task": unified_task,
        "task_state": task_state,
        "current_action": current_action,
        "policy": policy,
        "episodes": episodes,
        "decision": decision,
        "reflection": reflection,
        "steps": list(steps),
        "observations": list(observations),
        "final_output": {},
    }
    return {
        "message": req.message,
        "question": req.message,
        "query": req.message,
        "conversation_id": conversation_id,
        "assistant_turn_id": turn_id,
        "history": (history + [{"role": "user", "message": req.message}])[-WORKFLOW_HISTORY_WINDOW:],
        "metadata": metadata,
        "planner": plan,
        "retrieval_hits": retrieval_hits,
        "selected_tool": selected_tool_name or None,
        "goal": goal,
        "unified_task": unified_task,
        "task_state": task_state,
        "current_action": current_action,
        "policy": policy,
        "episodes": episodes,
        "runtime_state": runtime_state,
        "goal_ref": goal_ref or {},
    }


async def orchestrate_assistant_chat(
    *,
    conversation_repo: AssistantConversationRepository,
    episode_repo: AssistantEpisodeRepository,
    turn_repo: AssistantTurnRepository,
    task_repo: TaskRepository,
    tool_repo: ToolRepository,
    policy_repo: PolicyMemoryRepository | None,
    goal_repo: GoalRepository | None,
    gateway: ToolGateway,
    req: AssistantChatRequest,
    tenant_id: str,
    user: dict[str, Any],
    trace_id: str,
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> dict[str, Any]:
    conversation_id = str(req.conversation_id or f"conv-{uuid.uuid4().hex[:16]}")
    turn_id = f"turn-{uuid.uuid4().hex[:16]}"
    try:
        conversation = conversation_repo.get_or_create_conversation(
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail="conversation ownership mismatch") from exc

    history = list(conversation.get("message_history") or [])
    memory = _memory_view(conversation)
    metadata = dict(req.metadata or {})
    confirmed = _confirmed(metadata)
    resumed_goals = resume_waiting_goals_for_event(
        repo=goal_repo,
        tenant_id=tenant_id,
        event_kind="user_message",
        event_key=conversation_id,
        event_payload={"message": req.message, "trace_id": trace_id, "turn_id": turn_id},
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=1,
    )
    resumed_goal = resumed_goals[0] if resumed_goals else None
    resumed_goal_state = dict((resumed_goal or {}).get("goal_state") or {})
    resumed_goal_payload = dict(resumed_goal_state.get("goal") or {})

    retrieval_service = RetrievalService()
    retrieval_hits = retrieval_service.retrieve(query=req.message)
    tool_registry = ToolRegistryService(tool_repo)
    registry_tools = tool_registry.list_tools(tenant_id=tenant_id, enabled_only=True)
    tool_candidates = tool_registry.select_candidates(message=req.message, tools=registry_tools)
    planner = PlannerService()
    plan = await planner.aplan(
        message=req.message,
        mode=req.mode,
        metadata=metadata,
        history=history,
        memory=memory,
        retrieval_hits=retrieval_hits,
        tool_candidates=tool_candidates,
    )
    selected_tool_name = str(plan.get("selected_tool") or (tool_candidates[0].get("tool_name") if tool_candidates else "") or "")

    raw_episode_rows = episode_repo.list_recent_episodes_for_user(
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        limit=25,
    )
    goal = normalize_goal(
        message=req.message,
        mode=req.mode,
        metadata=metadata,
        planner=plan,
        retrieval_hits=retrieval_hits,
        tool_candidates=tool_candidates,
        memory=memory,
    )
    if resumed_goal_payload:
        goal = {
            **goal,
            **{key: value for key, value in resumed_goal_payload.items() if value not in (None, "", [], {})},
        }
        goal["normalized_goal"] = str(resumed_goal_payload.get("normalized_goal") or goal.get("normalized_goal") or req.message)
        goal["goal_id"] = str(resumed_goal_payload.get("goal_id") or goal.get("goal_id") or "")
        if resumed_goal_state.get("subgoals"):
            goal["subgoals"] = list(resumed_goal_state.get("subgoals") or [])
    selected_policy_version, policy_selector = select_runtime_policy_version(
        repo=policy_repo,
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        goal=goal,
        conversation_id=conversation_id,
        preferred_version_id=(
            str((resumed_goal or {}).get("policy_version_id") or "")
            or str(_as_dict(resumed_goal_state.get("policy")).get("policy_version_id") or "")
            or str(metadata.get("policy_version_override") or "")
        ),
    )
    policy_memory = build_runtime_policy_memory(selected_policy_version)
    policy_memory["selection"] = dict(policy_selector)
    episodes = retrieve_relevant_episodes(
        normalized_goal=str(goal.get("normalized_goal") or req.message),
        episodes=raw_episode_rows,
        limit=3,
    )
    unified_task = build_unified_task(
        goal=goal,
        planner=plan,
        retrieval_hits=retrieval_hits,
        tool_candidates=tool_candidates,
        episodes=episodes,
        memory=memory,
        policy_memory=policy_memory,
    )
    observations: list[dict[str, Any]] = [
        _runtime_observation("memory", "Loaded session memory snapshot.", source="assistant_conversations", payload=memory),
        _runtime_observation(
            "retrieval",
            f"Retrieved {len(retrieval_hits)} knowledge hit(s).",
            source="local_docs",
            payload={"hits": retrieval_hits},
        ),
    ]
    if episodes:
        observations.append(
            _runtime_observation(
                "episode",
                f"Matched {len(episodes)} similar episode(s) from prior runs.",
                source="assistant_episodes",
                payload={"episodes": episodes},
            )
        )
    if resumed_goal:
        observations.append(
            _runtime_observation(
                "goal_resume",
                f"Resumed waiting goal `{goal.get('normalized_goal')}` from a user message event.",
                source="agent_goals",
                payload={
                    "goal_id": str((resumed_goal or {}).get("goal_id") or ""),
                    "active_subgoal": dict(resumed_goal_state.get("active_subgoal") or {}),
                    "wake_graph": dict(resumed_goal_state.get("wake_graph") or {}),
                },
            )
        )

    task_state = build_task_state(
        goal=goal,
        unified_task=unified_task,
        observations=observations,
        pending_approvals=[],
        latest_result={},
        current_phase="interpret",
        policy_memory=policy_memory,
    )
    selection = select_next_runtime_step(
        goal=goal,
        planner=plan,
        task_state=task_state,
        retrieval_hits=retrieval_hits,
        tool_candidates=tool_candidates,
        confirmed=confirmed,
        episodes=episodes,
        has_retrieval_observation=bool(retrieval_hits),
        latest_result=None,
        requested_mode=req.mode,
        selected_tool=selected_tool_name,
    )
    current_action = dict(selection["current_action"])
    policy = dict(selection["policy"])
    policy["policy_version_id"] = str(policy_memory.get("version_id") or "") or None
    policy["policy_memory"] = dict(policy_memory)
    policy["policy_selector"] = dict(policy_selector)
    task_state["current_action_candidate"] = current_action
    task_state["current_phase"] = "plan"
    route = str(selection["route"])
    decision = dict(selection["decision"])
    goal_runtime_seed = {
        "status": "RUNNING",
        "current_phase": "plan",
        "goal": goal,
        "task_state": task_state,
        "current_action": current_action,
        "policy": policy,
        "reflection": dict(selection["reflection"]),
    }
    goal_row = _safe_sync_goal(
        goal_repo=goal_repo,
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        goal=goal,
        runtime_state=goal_runtime_seed,
        turn_id=turn_id,
    )
    goal["goal_id"] = str(goal_row.get("goal_id") or goal.get("goal_id") or "")
    goal["lifecycle_state"] = str(goal_row.get("status") or "ACTIVE")
    goal["continuation_count"] = int(goal_row.get("continuation_count") or 0)
    goal_ref = {
        "goal_id": goal["goal_id"],
        "lifecycle_state": goal["lifecycle_state"],
        "continuation_count": goal["continuation_count"],
    }
    steps: list[dict[str, Any]] = [
        _runtime_step(
            "interpret_goal",
            "understand",
            "Normalize goal",
            "completed",
            f"Normalized goal as `{goal.get('normalized_goal')}` with risk={goal.get('risk_level')}.",
            observation=observations[0],
            state_before=_runtime_state_snapshot(
                goal=goal,
                task_state={"current_phase": "interpret", "available_actions": list(task_state.get("available_actions") or [])},
                status="RUNNING",
                current_phase="interpret",
            ),
            state_after=_runtime_state_snapshot(
                goal=goal,
                task_state=task_state,
                current_action=current_action,
                policy=policy,
                status="RUNNING",
                current_phase="plan",
            ),
        ),
        _runtime_step(
            "policy_action_selection",
            "plan",
            "Choose next action",
            "completed",
            f"Policy selected `{current_action.get('action_type')}`.",
            decision=decision,
            state_before=_runtime_state_snapshot(
                goal=goal,
                task_state={**task_state, "current_action_candidate": None},
                policy=policy,
                status="RUNNING",
                current_phase="plan",
            ),
            state_after=_runtime_state_snapshot(
                goal=goal,
                task_state=task_state,
                current_action=current_action,
                policy=policy,
                status="RUNNING",
                current_phase="plan",
            ),
        ),
    ]

    if str(current_action.get("action_type") or "") == "retrieve" and not retrieval_hits:
        retrieval_hits = retrieval_service.retrieve(query=str(goal.get("normalized_goal") or req.message))
        retrieval_observation = _runtime_observation(
            "retrieval",
            f"Executed runtime retrieval and collected {len(retrieval_hits)} hit(s).",
            source="local_docs",
            payload={"hits": retrieval_hits},
        )
        observations.append(retrieval_observation)
        steps.append(
            _runtime_step(
                "runtime_retrieve",
                "observe",
                "Retrieve grounding context",
                "completed",
                retrieval_observation["summary"],
                observation=retrieval_observation,
                state_before=_runtime_state_snapshot(
                    goal=goal,
                    task_state=task_state,
                    current_action=current_action,
                    policy=policy,
                    status="RUNNING",
                    current_phase="observe",
                ),
                state_after=_runtime_state_snapshot(
                    goal=goal,
                    task_state={**task_state, "latest_result": {"status": "IN_PROGRESS", "retrieval_hits": len(retrieval_hits)}},
                    current_action=current_action,
                    policy=policy,
                    status="RUNNING",
                    current_phase="observe",
                    latest_result={"status": "IN_PROGRESS", "retrieval_hits": len(retrieval_hits)},
                ),
            )
        )
        retrieval_reflection = dict(
            apply_runtime_event(
                {
                    "goal": goal,
                    "task_state": task_state,
                    "current_action": current_action,
                    "policy": policy,
                },
                event_type="assistant.retrieve",
                status="IN_PROGRESS",
                current_phase="observe",
                latest_result={"status": "IN_PROGRESS", "retrieval_hits": len(retrieval_hits)},
                summary=retrieval_observation["summary"],
            ).get("reflection")
            or {}
        )
        steps.append(
            _runtime_step(
                "runtime_reflect",
                "reflect",
                "Reflect on retrieval",
                "completed",
                str(retrieval_reflection.get("summary") or "Reflection complete."),
                reflection=retrieval_reflection,
            )
        )
        unified_task = build_unified_task(
            goal=goal,
            planner=plan,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
            episodes=episodes,
            memory=memory,
            policy_memory=policy_memory,
        )
        task_state = build_task_state(
            goal=goal,
            unified_task=unified_task,
            observations=observations,
            pending_approvals=[],
            latest_result={},
            current_phase="replan",
            policy_memory=policy_memory,
        )
        selection = select_next_runtime_step(
            goal=goal,
            planner=plan,
            task_state=task_state,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
            confirmed=confirmed,
            episodes=episodes,
            has_retrieval_observation=True,
            latest_result=None,
            requested_mode=req.mode,
            selected_tool=selected_tool_name,
        )
        current_action = dict(selection["current_action"])
        policy = dict(selection["policy"])
        policy["policy_version_id"] = str(policy_memory.get("version_id") or "") or None
        policy["policy_memory"] = dict(policy_memory)
        policy["policy_selector"] = dict(policy_selector)
        task_state["current_action_candidate"] = current_action
        task_state["current_phase"] = "plan"
        route = str(selection["route"])
        decision = dict(selection["decision"])
        steps.append(
            _runtime_step(
                "runtime_replan",
                "replan",
                "Replan after observation",
                "completed",
                f"Updated next action to `{current_action.get('action_type')}`.",
                decision=decision,
                state_before=_runtime_state_snapshot(
                    goal=goal,
                    task_state={**task_state, "current_phase": "replan"},
                    policy=policy,
                    status="RUNNING",
                    current_phase="replan",
                ),
                state_after=_runtime_state_snapshot(
                    goal=goal,
                    task_state=task_state,
                    current_action=current_action,
                    policy=policy,
                    status="RUNNING",
                    current_phase="plan",
                ),
            )
        )

    final_reflection = dict(selection["reflection"])
    shadow_version, shadow_selector = select_shadow_policy_version(
        repo=policy_repo,
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        selected_version_id=str(policy.get("policy_version_id") or ""),
    )
    if shadow_version is not None:
        shadow_memory = build_runtime_policy_memory(shadow_version)
        shadow_memory["selection"] = dict(shadow_selector)
        shadow_unified_task = build_unified_task(
            goal=goal,
            planner=plan,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
            episodes=episodes,
            memory=memory,
            policy_memory=shadow_memory,
        )
        shadow_task_state = build_task_state(
            goal=goal,
            unified_task=shadow_unified_task,
            observations=observations,
            pending_approvals=[],
            latest_result={},
            current_phase=str(task_state.get("current_phase") or "plan"),
            policy_memory=shadow_memory,
        )
        shadow_selection = select_next_runtime_step(
            goal=goal,
            planner=plan,
            task_state=shadow_task_state,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
            confirmed=confirmed,
            episodes=episodes,
            has_retrieval_observation=bool(retrieval_hits),
            latest_result=None,
            requested_mode=req.mode,
            selected_tool=selected_tool_name,
        )
        shadow_action = dict(shadow_selection["current_action"])
        shadow_route = str(shadow_selection["route"])
        policy["shadow_policy"] = {
            "version_id": str(shadow_memory.get("version_id") or ""),
            "status": str(shadow_memory.get("status") or ""),
            "selector": dict(shadow_selector),
            "action_type": str(shadow_action.get("action_type") or ""),
            "route": shadow_route,
        }
        candidate_probe_version_id = (
            str(policy.get("policy_version_id") or "")
            if str(policy_selector.get("mode") or "") == "canary"
            else str(shadow_memory.get("version_id") or "")
        )
        record_shadow_policy_probe(
            repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            candidate_version_id=candidate_probe_version_id,
            probe={
                "live_mode": str(policy_selector.get("mode") or ""),
                "live_policy_version_id": str(policy.get("policy_version_id") or ""),
                "live_action": str(current_action.get("action_type") or ""),
                "live_route": route,
                "shadow_policy_version_id": str(shadow_memory.get("version_id") or ""),
                "shadow_action": str(shadow_action.get("action_type") or ""),
                "shadow_route": shadow_route,
                "risk_level": str(goal.get("risk_level") or ""),
                "goal_id": str(goal.get("goal_id") or ""),
                "conversation_id": conversation_id,
            },
        )

    if str(current_action.get("action_type") or "") == "ask_user":
        answer = "I can help, but I need one more detail before I act. What exactly should I focus on?"
        steps.append(
            _runtime_step(
                "ask_user",
                "act",
                "Request clarification",
                "completed",
                "Asked the user for missing context before continuing.",
                reflection=final_reflection,
            )
        )
        runtime_state = _apply_turn_runtime_event(
            turn_id=turn_id,
            route="direct_answer",
            status="SUCCEEDED",
            current_phase="ask_user",
            plan=plan,
            retrieval_hits=retrieval_hits,
            memory=memory,
            decision=decision,
            steps=steps,
            observations=observations,
            final_output={"message": answer},
            summary="Asked the user for missing context before continuing.",
            goal=goal,
            unified_task=unified_task,
            task_state=task_state,
            current_action=current_action,
            policy=policy,
            episodes=episodes,
            latest_result={"status": "NEED_INFO", "reason": "missing_user_context"},
            goal_ref=goal_ref,
        )
        goal_row = _safe_sync_goal(
            goal_repo=goal_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            goal=goal,
            runtime_state=runtime_state,
            turn_id=turn_id,
        )
        goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
        turn = turn_repo.create_turn(
            tenant_id=tenant_id,
            turn_id=turn_id,
            conversation_id=conversation_id,
            user_id=str(user["id"]),
            route="direct_answer",
            status="SUCCEEDED",
            current_phase="ask_user",
            response_type="direct_answer",
            user_message=req.message,
            assistant_message=answer,
            task_id=None,
            trace_id=trace_id,
            runtime_state=runtime_state,
        )
        _persist_turn_messages(
            conversation_repo=conversation_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            user_message=req.message,
            assistant_message=answer,
            route="direct_answer",
            turn_id=turn_id,
        )
        episode = build_episode(
            episode_id=f"episode-{turn_id}",
            user_message=req.message,
            goal=goal,
            action=current_action,
            task_state=task_state,
            reflection=final_reflection,
            policy=policy,
            tool_names=[],
            outcome_status="SUCCEEDED",
            final_outcome=answer,
        )
        _safe_upsert_episode(
            episode_repo=episode_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            turn_id=turn_id,
            task_id=None,
            episode=episode,
        )
        _safe_record_policy_feedback(
            policy_repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            episode=episode,
        )
        return {
            "conversation_id": conversation_id,
            "route": "direct_answer",
            "response_type": "direct_answer",
            "message": answer,
            "task": None,
            "tool_result": None,
            "planner": plan,
            "retrieval_hits": retrieval_hits,
            "memory": memory,
            "need_confirmation": False,
            "trace_id": trace_id,
            "turn": build_turn_summary(turn),
        }

    if str(current_action.get("action_type") or "") == "approval_request":
        answer = (
            f"Tool `{selected_tool_name or 'selected_tool'}` is high-risk and needs confirmation. "
            "Re-send with metadata.confirmed=true to continue, or let me create a workflow task."
        )
        task_state["pending_approvals"] = [selected_tool_name or "selected_tool"]
        task_state["current_phase"] = "approval_request"
        steps.append(
            _runtime_step(
                "request_approval",
                "act",
                "Request approval",
                "waiting",
                f"Paused execution for `{selected_tool_name or 'selected_tool'}` pending approval.",
                reflection=final_reflection,
            )
        )
        runtime_state = _apply_turn_runtime_event(
            turn_id=turn_id,
            route="tool_task",
            status="WAITING_HUMAN",
            current_phase="approval_request",
            plan=plan,
            retrieval_hits=retrieval_hits,
            memory=memory,
            decision=decision,
            steps=steps,
            observations=observations,
            final_output={"message": answer},
            summary=f"Paused execution for `{selected_tool_name or 'selected_tool'}` pending approval.",
            goal=goal,
            unified_task=unified_task,
            task_state=task_state,
            current_action=current_action,
            policy=policy,
            episodes=episodes,
            latest_result={"status": "WAITING_HUMAN", "approval_target": selected_tool_name or "selected_tool"},
            pending_approvals=task_state["pending_approvals"],
            target=selected_tool_name or "selected_tool",
            goal_ref=goal_ref,
        )
        goal_row = _safe_sync_goal(
            goal_repo=goal_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            goal=goal,
            runtime_state=runtime_state,
            turn_id=turn_id,
        )
        goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
        turn = turn_repo.create_turn(
            tenant_id=tenant_id,
            turn_id=turn_id,
            conversation_id=conversation_id,
            user_id=str(user["id"]),
            route="tool_task",
            status="WAITING_HUMAN",
            current_phase="approval_request",
            response_type="direct_answer",
            user_message=req.message,
            assistant_message=answer,
            task_id=None,
            trace_id=trace_id,
            runtime_state=runtime_state,
        )
        _persist_turn_messages(
            conversation_repo=conversation_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            user_message=req.message,
            assistant_message=answer,
            route="tool_task",
            turn_id=turn_id,
        )
        return {
            "conversation_id": conversation_id,
            "route": "tool_task",
            "response_type": "direct_answer",
            "message": answer,
            "task": None,
            "tool_result": None,
            "planner": plan,
            "retrieval_hits": retrieval_hits,
            "memory": memory,
            "need_confirmation": True,
            "trace_id": trace_id,
            "turn": build_turn_summary(turn),
        }

    if str(current_action.get("action_type") or "") == "respond":
        answer = await _response_with_retrieval(req.message, retrieval_hits, memory, history)
        steps.append(
            _runtime_step(
                "respond",
                "act",
                "Respond to user",
                "completed",
                "Returned a direct user-facing answer.",
                reflection=final_reflection,
            )
        )
        runtime_state = _apply_turn_runtime_event(
            turn_id=turn_id,
            route="direct_answer",
            status="SUCCEEDED",
            current_phase="respond",
            plan=plan,
            retrieval_hits=retrieval_hits,
            memory=memory,
            decision=decision,
            steps=steps,
            observations=observations,
            final_output={"message": answer, "retrieval_hits": retrieval_hits},
            summary="Returned a direct user-facing answer.",
            goal=goal,
            unified_task=unified_task,
            task_state=task_state,
            current_action=current_action,
            policy=policy,
            episodes=episodes,
            latest_result={"status": "SUCCEEDED", "retrieval_hits": len(retrieval_hits)},
            goal_ref=goal_ref,
        )
        goal_row = _safe_sync_goal(
            goal_repo=goal_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            goal=goal,
            runtime_state=runtime_state,
            turn_id=turn_id,
        )
        goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
        turn = turn_repo.create_turn(
            tenant_id=tenant_id,
            turn_id=turn_id,
            conversation_id=conversation_id,
            user_id=str(user["id"]),
            route="direct_answer",
            status="SUCCEEDED",
            current_phase="respond",
            response_type="direct_answer",
            user_message=req.message,
            assistant_message=answer,
            task_id=None,
            trace_id=trace_id,
            runtime_state=runtime_state,
        )
        _persist_turn_messages(
            conversation_repo=conversation_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            user_message=req.message,
            assistant_message=answer,
            route="direct_answer",
            turn_id=turn_id,
        )
        episode = build_episode(
            episode_id=f"episode-{turn_id}",
            user_message=req.message,
            goal=goal,
            action=current_action,
            task_state=task_state,
            reflection=final_reflection,
            policy=policy,
            tool_names=[],
            outcome_status="SUCCEEDED",
            final_outcome=answer,
        )
        _safe_upsert_episode(
            episode_repo=episode_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            turn_id=turn_id,
            task_id=None,
            episode=episode,
        )
        _safe_record_policy_feedback(
            policy_repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            episode=episode,
        )
        return {
            "conversation_id": conversation_id,
            "route": "direct_answer",
            "response_type": "direct_answer",
            "message": answer,
            "task": None,
            "tool_result": None,
            "planner": plan,
            "retrieval_hits": retrieval_hits,
            "memory": memory,
            "need_confirmation": False,
            "trace_id": trace_id,
            "turn": build_turn_summary(turn),
        }

    if str(current_action.get("action_type") or "") == "tool_call":
        task = task_repo.create_task(
            tenant_id=tenant_id,
            client_request_id=f"assistant-tool-{uuid.uuid4().hex[:16]}",
            task_type="tool_flow",
            created_by=str(user["id"]),
            input_masked=mask_payload(
                {
                    "message": req.message,
                    "query": req.message,
                    "conversation_id": conversation_id,
                    "assistant_turn_id": turn_id,
                    "planner": plan,
                    "retrieval_hits": retrieval_hits,
                    "goal": goal,
                    "unified_task": unified_task,
                    "task_state": task_state,
                    "current_action": current_action,
                    "policy": policy,
                    "episodes": episodes,
                    "selected_tool": selected_tool_name,
                    "metadata": metadata,
                }
            ),
            input_raw_encrypted=encrypt_input_payload({"message": req.message, "query": req.message, "metadata": metadata}),
            trace_id=trace_id,
            budget=1.0,
            requires_hitl=False,
            conversation_id=conversation_id,
            assistant_turn_id=turn_id,
            goal_id=goal_ref.get("goal_id"),
            origin="assistant_chat",
        )
        task_id = str(task["id"])
        run = task_repo.create_run(
            tenant_id=tenant_id,
            task_id=task_id,
            run_no=1,
            workflow_id=f"assistant-fast-tool-{task_id}",
            trace_id=trace_id,
            assigned_worker=settings.default_worker_id,
        )
        run_id = str(run["id"])
        task_repo.update_task_status(tenant_id, task_id, "RUNNING")
        task_repo.append_step(
            tenant_id=tenant_id,
            run_id=run_id,
            status_text="RUNNING",
            step_key="assistant_tool_run",
            payload_masked=mask_payload({"goal": goal, "selected_tool": selected_tool_name}),
            trace_id=trace_id,
            status_event_id=_status_event_id("assistant-tool-run"),
        )

        tool_payload = dict(current_action.get("input") or {})
        if not tool_payload or tool_payload == {
            "goal": goal.get("normalized_goal"),
            "unknowns": goal.get("unknowns"),
            "selected_tool": selected_tool_name,
        }:
            tool_payload = _tool_payload_from_message(req.message)
            if selected_tool_name == "web_search":
                tool_payload = {
                    "query": req.message,
                    "domain": str(metadata.get("domain") or "example.com"),
                    "top_k": int(metadata.get("top_k") or 3),
                }

        tool_response = await gateway.execute(
            {
                "tenant_id": tenant_id,
                "tool_call_id": f"assistant-toolcall-{uuid.uuid4().hex[:16]}",
                "task_id": task_id,
                "run_id": run_id,
                "task_type": "tool_flow",
                "tool_id": selected_tool_name or "web_search",
                "payload": tool_payload,
                "caller_user_id": str(user["id"]),
                "approval_id": None,
                "trace_id": trace_id,
            }
        )

        if str(tool_response.get("status")) == "SUCCEEDED":
            tool_result = dict(tool_response.get("result") or {})
            task_repo.mark_task_succeeded(
                tenant_id,
                task_id,
                mask_payload({"tool_result": tool_result, "planner": plan, "retrieval_hits": retrieval_hits}),
            )
            task_repo.update_run_status(tenant_id, run_id, "SUCCEEDED")
            tool_obs = _runtime_observation(
                "tool_result",
                f"Tool `{selected_tool_name or 'selected_tool'}` completed successfully.",
                source=selected_tool_name or "tool",
                payload={"tool_result": tool_result},
            )
            observations.append(tool_obs)
            latest_result = {"status": "SUCCEEDED", "tool_result": tool_result}
            task_state = build_task_state(
                goal=goal,
                unified_task=unified_task,
                observations=observations,
                pending_approvals=[],
                latest_result=latest_result,
                current_phase="observe",
                policy_memory=policy_memory,
            )
            task_state["current_action_candidate"] = current_action
            answer = f"Tool `{selected_tool_name or 'selected_tool'}` completed successfully."
            success_runtime = apply_runtime_event(
                {
                    "goal": goal,
                    "task_state": task_state,
                    "current_action": current_action,
                    "policy": policy,
                    "episodes": episodes,
                },
                event_type="assistant.tool_success",
                status="SUCCEEDED",
                current_phase="respond",
                latest_result=latest_result,
                final_output={"message": answer, "tool_result": tool_result},
                summary=tool_obs["summary"],
                route="tool_task",
            )
            success_reflection = dict(success_runtime.get("reflection") or {})
            current_action = dict(success_runtime.get("current_action") or current_action)
            policy = dict(success_runtime.get("policy") or policy)
            steps.extend(
                [
                    _runtime_step(
                        "tool_call",
                        "act",
                        "Execute tool",
                        "completed",
                        tool_obs["summary"],
                        observation=tool_obs,
                    ),
                    _runtime_step(
                        "tool_reflect",
                        "reflect",
                        "Reflect on tool result",
                        "completed",
                        str(success_reflection.get("summary") or "Reflection complete."),
                        reflection=success_reflection,
                    ),
                ]
            )
            memory_out = _safe_update_memory(
                conversation_repo=conversation_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                last_tool_result={
                    "task_id": task_id,
                    "tool_name": selected_tool_name,
                    "tool_result": tool_result,
                },
                last_task_result={
                    "task_id": task_id,
                    "status": "SUCCEEDED",
                    "tool_name": selected_tool_name,
                },
            )
            runtime_state = _apply_turn_runtime_event(
                turn_id=turn_id,
                route="tool_task",
                status="SUCCEEDED",
                current_phase="respond",
                plan=plan,
                retrieval_hits=retrieval_hits,
                memory=memory_out,
                decision=decision,
                steps=steps,
                observations=observations,
                final_output={"message": answer, "tool_result": tool_result},
                summary=tool_obs["summary"],
                goal=goal,
                unified_task=unified_task,
                task_state=task_state,
                current_action=current_action,
                policy=policy,
                episodes=episodes,
                task_id=task_id,
                latest_result=latest_result,
                goal_ref=goal_ref,
            )
            goal_row = _safe_sync_goal(
                goal_repo=goal_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                goal=goal,
                runtime_state=runtime_state,
                task_id=task_id,
                turn_id=turn_id,
            )
            goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
            turn = turn_repo.create_turn(
                tenant_id=tenant_id,
                turn_id=turn_id,
                conversation_id=conversation_id,
                user_id=str(user["id"]),
                route="tool_task",
                status="SUCCEEDED",
                current_phase="respond",
                response_type="direct_answer",
                user_message=req.message,
                assistant_message=answer,
                task_id=task_id,
                trace_id=trace_id,
                runtime_state=runtime_state,
            )
            _persist_turn_messages(
                conversation_repo=conversation_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                user_message=req.message,
                assistant_message=answer,
                route="tool_task",
                turn_id=turn_id,
            )
            episode = build_episode(
                episode_id=f"episode-{turn_id}",
                user_message=req.message,
                goal=goal,
                action=current_action,
                task_state=task_state,
                reflection=success_reflection,
                policy=policy,
                tool_names=[selected_tool_name],
                outcome_status="SUCCEEDED",
                final_outcome=answer,
            )
            _safe_upsert_episode(
                episode_repo=episode_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                turn_id=turn_id,
                task_id=task_id,
                episode=episode,
            )
            _safe_record_policy_feedback(
                policy_repo=policy_repo,
                tenant_id=tenant_id,
                actor_user_id=str(user["id"]),
                episode=episode,
            )
            return {
                "conversation_id": conversation_id,
                "route": "tool_task",
                "response_type": "direct_answer",
                "message": answer,
                "task": {"task_id": task_id, "run_id": run_id, "status": "SUCCEEDED"},
                "tool_result": tool_result,
                "planner": plan,
                "retrieval_hits": retrieval_hits,
                "memory": memory_out,
                "need_confirmation": False,
                "trace_id": trace_id,
                "turn": build_turn_summary(turn),
            }

        reason_code = str(tool_response.get("reason_code") or "tool_denied")
        failure_status = "FAILED_RETRYABLE" if reason_code in RETRYABLE_TOOL_DENY_REASONS else "FAILED_FINAL"
        task_repo.mark_task_failed(
            tenant_id=tenant_id,
            task_id=task_id,
            status_text=failure_status,
            error_code=reason_code,
            error_message=tool_response.get("result") or {"reason_code": reason_code},
        )
        task_repo.update_run_status(tenant_id, run_id, failure_status)
        latest_result = {"status": "retryable_tool_failure" if failure_status == "FAILED_RETRYABLE" else failure_status, "reason_code": reason_code}
        failure_runtime = apply_runtime_event(
            {
                "goal": goal,
                "task_state": task_state,
                "current_action": current_action,
                "policy": policy,
                "episodes": episodes,
            },
            event_type="assistant.tool_failure",
            status=failure_status,
            current_phase="reflect",
            latest_result=latest_result,
            summary=f"Tool execution failed with `{reason_code}`.",
            route="tool_task",
        )
        failure_reflection = dict(failure_runtime.get("reflection") or {})
        failed_action = dict(failure_runtime.get("current_action") or current_action)
        failed_policy = dict(failure_runtime.get("policy") or policy)
        if failure_status == "FAILED_RETRYABLE" and (
            str(failure_reflection.get("next_action") or "") == "workflow_call"
            or str(failed_policy.get("fallback_action") or "") == "workflow_call"
        ):
            escalation_steps = steps + [
                _runtime_step(
                    "tool_failure",
                    "observe",
                    "Observe tool failure",
                    "completed",
                    f"Tool execution failed with `{reason_code}`.",
                    reflection=failure_reflection,
                    state_before=_runtime_state_snapshot(
                        goal=goal,
                        task_state=task_state,
                        current_action=current_action,
                        policy=policy,
                        status=failure_status,
                        current_phase="reflect",
                        latest_result=latest_result,
                    ),
                    state_after=_runtime_state_snapshot(
                        goal=goal,
                        task_state={**task_state, "latest_result": latest_result},
                        current_action=failed_action,
                        policy=failed_policy,
                        status=failure_status,
                        current_phase="reflect",
                        latest_result=latest_result,
                    ),
                ),
                _runtime_step(
                    "workflow_replan",
                    "replan",
                    "Escalate into workflow",
                    "completed",
                    "Escalating the goal into a durable workflow after a retryable tool failure.",
                    reflection=failure_reflection,
                    state_before=_runtime_state_snapshot(
                        goal=goal,
                        task_state={**task_state, "latest_result": latest_result},
                        current_action=failed_action,
                        policy=failed_policy,
                        status=failure_status,
                        current_phase="replan",
                        latest_result=latest_result,
                    ),
                    state_after=_runtime_state_snapshot(
                        goal=goal,
                        task_state={**task_state, "fallback_state": "workflow_replan", "current_phase": "replan", "latest_result": latest_result},
                        current_action=failed_action,
                        policy=failed_policy,
                        status="QUEUED",
                        current_phase="wait",
                        latest_result=latest_result,
                    ),
                ),
            ]
            workflow_req = TaskCreateRequest(
                client_request_id=f"assistant-wf-replan-{uuid.uuid4().hex[:16]}",
                task_type=_resolve_task_type(plan),
                input=_workflow_input(
                    req=req,
                    conversation_id=conversation_id,
                    turn_id=turn_id,
                    history=history,
                    metadata=metadata,
                    plan=plan,
                    retrieval_hits=retrieval_hits,
                    selected_tool_name=selected_tool_name,
                    goal=goal,
                    unified_task=unified_task,
                    task_state=task_state,
                    current_action=failed_action,
                    policy=failed_policy,
                    episodes=episodes,
                    decision={
                        "action": str(failed_action.get("action_type") or failed_policy.get("selected_action") or ""),
                        "route": "workflow_task",
                        "selected_tool": selected_tool_name or None,
                        "confidence": plan.get("confidence"),
                        "summary": str(failed_action.get("rationale") or "Workflow escalation created after retryable tool failure."),
                    },
                    reflection={
                        "summary": str(failure_reflection.get("summary") or "Retryable failure escalated into workflow."),
                        "requires_replan": bool(failure_reflection.get("requires_replan")),
                        "next_action": "wait",
                    },
                    steps=escalation_steps,
                    observations=observations,
                    goal_ref=goal_ref,
                ),
                budget=1.0,
                conversation_id=conversation_id,
                assistant_turn_id=turn_id,
                goal_id=goal_ref.get("goal_id"),
                origin="assistant_chat",
            )
            workflow_created = await service_create_task(
                task_repo=task_repo,
                req=workflow_req,
                tenant_id=tenant_id,
                user=user,
                trace_id=trace_id,
                start_workflow=start_workflow,
            )
            answer = (
                f"Fast tool execution failed with `{reason_code}`, so I escalated the goal into workflow task "
                f"{workflow_created['task_id']} for a safer retry path."
            )
            task_state["fallback_state"] = "workflow_replan"
            task_state["current_phase"] = "replan"
            escalation_steps[-1]["summary"] = f"Created workflow task `{workflow_created['task_id']}` after retryable tool failure."
            steps = escalation_steps
            memory_out = _safe_update_memory(
                conversation_repo=conversation_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                last_task_result={
                    "task_id": str(workflow_created["task_id"]),
                    "status": str(workflow_created["status"]),
                    "fallback_from": task_id,
                    "reason_code": reason_code,
                },
            )
            runtime_state = _apply_turn_runtime_event(
                turn_id=turn_id,
                route="workflow_task",
                status=str(workflow_created["status"]),
                current_phase="replan",
                plan=plan,
                retrieval_hits=retrieval_hits,
                memory=memory_out,
                decision=decision,
                steps=steps,
                observations=observations,
                final_output={"message": answer},
                summary=f"Created workflow task `{workflow_created['task_id']}` after retryable tool failure.",
                goal=goal,
                unified_task=unified_task,
                task_state=task_state,
                current_action=failed_action,
                policy=failed_policy,
                episodes=episodes,
                task_id=str(workflow_created["task_id"]),
                latest_result={"status": str(workflow_created["status"]), "task_id": str(workflow_created["task_id"]), "reason_code": reason_code},
                goal_ref=goal_ref,
            )
            goal_row = _safe_sync_goal(
                goal_repo=goal_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                goal=goal,
                runtime_state=runtime_state,
                task_id=str(workflow_created["task_id"]),
                turn_id=turn_id,
            )
            goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
            turn = turn_repo.create_turn(
                tenant_id=tenant_id,
                turn_id=turn_id,
                conversation_id=conversation_id,
                user_id=str(user["id"]),
                route="workflow_task",
                status=str(workflow_created["status"]),
                current_phase="replan",
                response_type="task_created",
                user_message=req.message,
                assistant_message=answer,
                task_id=str(workflow_created["task_id"]),
                trace_id=trace_id,
                runtime_state=runtime_state,
            )
            _persist_turn_messages(
                conversation_repo=conversation_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                user_message=req.message,
                assistant_message=answer,
                route="workflow_task",
                turn_id=turn_id,
            )
            episode = build_episode(
                episode_id=f"episode-{turn_id}",
                user_message=req.message,
                goal=goal,
                action={"action_type": "workflow_call", "status": "planned"},
                task_state=task_state,
                reflection=failure_reflection,
                policy=failed_policy,
                tool_names=[selected_tool_name],
                outcome_status=str(workflow_created["status"]),
                final_outcome=answer,
            )
            _safe_upsert_episode(
                episode_repo=episode_repo,
                tenant_id=tenant_id,
                user_id=str(user["id"]),
                conversation_id=conversation_id,
                turn_id=turn_id,
                task_id=str(workflow_created["task_id"]),
                episode=episode,
            )
            _safe_record_policy_feedback(
                policy_repo=policy_repo,
                tenant_id=tenant_id,
                actor_user_id=str(user["id"]),
                episode=episode,
            )
            return {
                "conversation_id": conversation_id,
                "route": "workflow_task",
                "response_type": "task_created",
                "message": answer,
                "task": {
                    "task_id": str(workflow_created["task_id"]),
                    "run_id": workflow_created.get("run_id"),
                    "status": str(workflow_created["status"]),
                },
                "tool_result": None,
                "planner": plan,
                "retrieval_hits": retrieval_hits,
                "memory": memory_out,
                "need_confirmation": False,
                "trace_id": trace_id,
                "turn": build_turn_summary(turn),
            }

        answer = f"Tool task failed with reason={reason_code}."
        task_state["current_phase"] = "reflect"
        task_state["latest_result"] = latest_result
        steps.extend(
            [
                _runtime_step(
                    "tool_failure",
                    "observe",
                    "Observe tool failure",
                    "completed",
                    f"Tool execution failed with `{reason_code}`.",
                    reflection=failure_reflection,
                ),
                _runtime_step(
                    "tool_failure_reflect",
                    "reflect",
                    "Reflect on failure",
                    "completed",
                    str(failure_reflection.get("summary") or "Reflection complete."),
                    reflection=failure_reflection,
                ),
            ]
        )
        memory_out = _safe_update_memory(
            conversation_repo=conversation_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            last_task_result={"task_id": task_id, "status": failure_status, "reason_code": reason_code},
        )
        runtime_state = _apply_turn_runtime_event(
            turn_id=turn_id,
            route="tool_task",
            status=failure_status,
            current_phase="reflect",
            plan=plan,
            retrieval_hits=retrieval_hits,
            memory=memory_out,
            decision=decision,
            steps=steps,
            observations=observations,
            final_output={"message": answer, "reason_code": reason_code},
            summary=str(failure_reflection.get("summary") or f"Tool execution failed with `{reason_code}`."),
            goal=goal,
            unified_task=unified_task,
            task_state=task_state,
            current_action=failed_action,
            policy=failed_policy,
            episodes=episodes,
            task_id=task_id,
            latest_result=latest_result,
            goal_ref=goal_ref,
        )
        goal_row = _safe_sync_goal(
            goal_repo=goal_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            goal=goal,
            runtime_state=runtime_state,
            task_id=task_id,
            turn_id=turn_id,
        )
        goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
        turn = turn_repo.create_turn(
            tenant_id=tenant_id,
            turn_id=turn_id,
            conversation_id=conversation_id,
            user_id=str(user["id"]),
            route="tool_task",
            status=failure_status,
            current_phase="reflect",
            response_type="direct_answer",
            user_message=req.message,
            assistant_message=answer,
            task_id=task_id,
            trace_id=trace_id,
            runtime_state=runtime_state,
        )
        _persist_turn_messages(
            conversation_repo=conversation_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            user_message=req.message,
            assistant_message=answer,
            route="tool_task",
            turn_id=turn_id,
        )
        episode = build_episode(
            episode_id=f"episode-{turn_id}",
            user_message=req.message,
            goal=goal,
            action=failed_action,
            task_state=task_state,
            reflection=failure_reflection,
            policy=failed_policy,
            tool_names=[selected_tool_name],
            outcome_status=failure_status,
            final_outcome=answer,
        )
        _safe_upsert_episode(
            episode_repo=episode_repo,
            tenant_id=tenant_id,
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            turn_id=turn_id,
            task_id=task_id,
            episode=episode,
        )
        _safe_record_policy_feedback(
            policy_repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            episode=episode,
        )
        return {
            "conversation_id": conversation_id,
            "route": "tool_task",
            "response_type": "direct_answer",
            "message": answer,
            "task": {"task_id": task_id, "run_id": run_id, "status": failure_status},
            "tool_result": None,
            "planner": plan,
            "retrieval_hits": retrieval_hits,
            "memory": memory_out,
            "need_confirmation": False,
            "trace_id": trace_id,
            "turn": build_turn_summary(turn),
        }

    workflow_req = TaskCreateRequest(
        client_request_id=f"assistant-wf-{uuid.uuid4().hex[:16]}",
        task_type=_resolve_task_type(plan),
        input=_workflow_input(
            req=req,
            conversation_id=conversation_id,
            turn_id=turn_id,
            history=history,
            metadata=metadata,
            plan=plan,
            retrieval_hits=retrieval_hits,
            selected_tool_name=selected_tool_name,
            goal=goal,
            unified_task=unified_task,
            task_state=task_state,
            current_action=current_action,
            policy=policy,
            episodes=episodes,
            decision=decision,
            reflection={
                "summary": str(final_reflection.get("summary") or "Assistant handed off the goal into the durable runtime."),
                "requires_replan": bool(final_reflection.get("requires_replan")),
                "next_action": "wait",
            },
            steps=steps,
            observations=observations,
            goal_ref=goal_ref,
        ),
        budget=1.0,
        conversation_id=conversation_id,
        assistant_turn_id=turn_id,
        goal_id=goal_ref.get("goal_id"),
        origin="assistant_chat",
    )
    workflow_created = await service_create_task(
        task_repo=task_repo,
        req=workflow_req,
        tenant_id=tenant_id,
        user=user,
        trace_id=trace_id,
        start_workflow=start_workflow,
    )
    answer = _workflow_reply(str(workflow_created["task_id"]))
    steps.append(
        _runtime_step(
            "workflow_handoff",
            "act",
            "Hand off to workflow",
            "completed",
            f"Created workflow task `{workflow_created['task_id']}`.",
            reflection=final_reflection,
            state_before=_runtime_state_snapshot(
                goal=goal,
                task_state=task_state,
                current_action=current_action,
                policy=policy,
                status="RUNNING",
                current_phase="act",
            ),
            state_after=_runtime_state_snapshot(
                goal=goal,
                task_state={**task_state, "current_phase": "wait", "latest_result": {"status": str(workflow_created["status"]), "task_id": str(workflow_created["task_id"]) }},
                current_action=current_action,
                policy=policy,
                status=str(workflow_created["status"]),
                current_phase="wait",
                latest_result={"status": str(workflow_created["status"]), "task_id": str(workflow_created["task_id"])},
            ),
        )
    )
    task_state["current_phase"] = "wait"
    task_state["latest_result"] = {"status": str(workflow_created["status"]), "task_id": str(workflow_created["task_id"])}
    memory_out = _safe_update_memory(
        conversation_repo=conversation_repo,
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        last_task_result={
            "task_id": str(workflow_created["task_id"]),
            "status": str(workflow_created["status"]),
            "task_type": workflow_req.task_type,
        },
    )
    runtime_state = _apply_turn_runtime_event(
        turn_id=turn_id,
        route="workflow_task",
        status=str(workflow_created["status"]),
        current_phase="wait",
        plan=plan,
        retrieval_hits=retrieval_hits,
        memory=memory_out,
        decision=decision,
        steps=steps,
        observations=observations,
        final_output={"message": answer},
        summary=f"Created workflow task `{workflow_created['task_id']}`.",
        goal=goal,
        unified_task=unified_task,
        task_state=task_state,
        current_action=current_action,
        policy=policy,
        episodes=episodes,
        task_id=str(workflow_created["task_id"]),
        latest_result={"status": str(workflow_created["status"]), "task_id": str(workflow_created["task_id"])},
        goal_ref=goal_ref,
    )
    goal_row = _safe_sync_goal(
        goal_repo=goal_repo,
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        goal=goal,
        runtime_state=runtime_state,
        task_id=str(workflow_created["task_id"]),
        turn_id=turn_id,
    )
    goal["lifecycle_state"] = str(goal_row.get("status") or goal.get("lifecycle_state") or "")
    turn = turn_repo.create_turn(
        tenant_id=tenant_id,
        turn_id=turn_id,
        conversation_id=conversation_id,
        user_id=str(user["id"]),
        route="workflow_task",
        status=str(workflow_created["status"]),
        current_phase="wait",
        response_type="task_created",
        user_message=req.message,
        assistant_message=answer,
        task_id=str(workflow_created["task_id"]),
        trace_id=trace_id,
        runtime_state=runtime_state,
    )
    _persist_turn_messages(
        conversation_repo=conversation_repo,
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        user_message=req.message,
        assistant_message=answer,
        route="workflow_task",
        turn_id=turn_id,
    )
    _safe_upsert_episode(
        episode_repo=episode_repo,
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        turn_id=turn_id,
        task_id=str(workflow_created["task_id"]),
        episode=build_episode(
            episode_id=f"episode-{turn_id}",
            user_message=req.message,
            goal=goal,
            action=current_action,
            task_state=task_state,
            reflection=final_reflection,
            policy=policy,
            tool_names=[selected_tool_name] if selected_tool_name else [],
            outcome_status=str(workflow_created["status"]),
            final_outcome=answer,
        ),
    )
    return {
        "conversation_id": conversation_id,
        "route": "workflow_task",
        "response_type": "task_created",
        "message": answer,
        "task": {
            "task_id": str(workflow_created["task_id"]),
            "run_id": workflow_created.get("run_id"),
            "status": str(workflow_created["status"]),
        },
        "tool_result": None,
        "planner": plan,
        "retrieval_hits": retrieval_hits,
        "memory": memory_out,
        "need_confirmation": bool(plan.get("need_confirmation")),
        "trace_id": trace_id,
        "turn": build_turn_summary(turn),
    }
