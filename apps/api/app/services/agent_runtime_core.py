from __future__ import annotations

import re
from typing import Any

from runtime_backbone import (
    ACTION_TYPES,
    COMPLEX_HINTS,
    choose_next_action as _shared_choose_next_action,
    derive_experience_profile,
    merge_runtime_state as _shared_merge_runtime_state,
    reflect_and_replan as _shared_reflect_and_replan,
)

STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "be",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "please",
    "the",
    "this",
    "to",
    "what",
    "with",
}
QUESTION_HINTS = {"what", "why", "how", "when", "where", "which", "who"}
HIGH_RISK_HINTS = {"send", "email", "ticket", "delete", "write", "update", "create"}
AMBIGUOUS_HINTS = {"it", "this", "that", "them", "something", "stuff"}
GENERIC_REQUEST_HINTS = {
    "help",
    "need",
    "want",
    "show",
    "tell",
    "make",
    "do",
    "fix",
    "work",
    "handle",
    "support",
    "issue",
    "problem",
    "thing",
}


def _tokenize(text: str) -> set[str]:
    words = {item for item in re.findall(r"[a-z0-9_]{2,}", text.lower()) if item not in STOP_WORDS}
    return words


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def normalize_goal(
    *,
    message: str,
    mode: str | None,
    metadata: dict[str, Any],
    planner: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    tool_candidates: list[dict[str, Any]],
    memory: dict[str, Any],
) -> dict[str, Any]:
    normalized_message = " ".join(message.strip().split())
    lowered = normalized_message.lower()
    selected_tool = str(planner.get("selected_tool") or "")
    risk_level = "low"
    if any(hint in lowered for hint in HIGH_RISK_HINTS):
        risk_level = "medium"
    if any(bool(tool.get("requires_approval")) for tool in tool_candidates[:1]):
        risk_level = "high"
    constraints = []
    if mode and mode != "auto":
        constraints.append(f"requested_mode:{mode}")
    if selected_tool:
        constraints.append(f"preferred_tool:{selected_tool}")
    if isinstance(metadata.get("domain"), str) and metadata.get("domain"):
        constraints.append(f"domain:{metadata['domain']}")
    preferences = dict(memory.get("user_preferences") or {})
    if preferences.get("response_style"):
        constraints.append(f"response_style:{preferences['response_style']}")

    success_criteria = []
    planner_steps = [str(step).strip() for step in list(planner.get("plan_steps") or []) if str(step).strip()]
    if planner_steps:
        success_criteria.extend(planner_steps[:3])
    else:
        if retrieval_hits:
            success_criteria.append("Ground the answer in retrieved evidence.")
        if tool_candidates:
            success_criteria.append("Select the safest viable action for the current goal.")
        success_criteria.append("Return a user-visible result or a clear next step.")

    unknowns: list[str] = []
    tokens = _tokenize(lowered)
    raw_terms = set(re.findall(r"[a-z0-9_]{2,}", lowered))
    grounding_tokens = {
        token
        for token in tokens
        if token not in AMBIGUOUS_HINTS
        and token not in QUESTION_HINTS
        and token not in GENERIC_REQUEST_HINTS
        and token not in COMPLEX_HINTS
        and token not in HIGH_RISK_HINTS
    }
    if not retrieval_hits and any(lowered.startswith(q + " ") or lowered == q for q in QUESTION_HINTS):
        unknowns.append("missing_grounding_evidence")
    if any(hint in raw_terms for hint in AMBIGUOUS_HINTS) and not grounding_tokens:
        unknowns.append("ambiguous_user_reference")
    if selected_tool == "web_search" and "domain:" not in " ".join(constraints):
        unknowns.append("search_scope_not_explicit")
    if any(bool(tool.get("requires_approval")) for tool in tool_candidates[:1]) and not bool(metadata.get("confirmed")):
        unknowns.append("approval_not_granted")

    return {
        "user_intent": str(planner.get("intent") or "general_qna"),
        "normalized_goal": normalized_message,
        "constraints": _unique(constraints),
        "success_criteria": _unique(success_criteria),
        "unknowns": _unique(unknowns),
        "risk_level": risk_level,
    }


def retrieve_relevant_episodes(
    *,
    normalized_goal: str,
    episodes: list[dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    goal_tokens = _tokenize(normalized_goal)
    scored: list[tuple[float, dict[str, Any]]] = []
    for row in episodes:
        episode_goal = str(row.get("normalized_goal") or row.get("task_summary") or "")
        episode_tokens = _tokenize(episode_goal)
        overlap = len(goal_tokens & episode_tokens)
        if overlap <= 0:
            continue
        tool_bonus = 0.1 if row.get("tool_names") else 0.0
        success_bonus = 0.15 if str(row.get("outcome_status") or "") == "SUCCEEDED" else 0.0
        score = round(overlap / max(1, len(goal_tokens)) + tool_bonus + success_bonus, 4)
        scored.append(
            (
                score,
                {
                    "episode_id": str(row.get("episode_id") or ""),
                    "task_summary": str(row.get("task_summary") or ""),
                    "chosen_strategy": str(row.get("chosen_strategy") or ""),
                    "steps_taken": list(row.get("action_types") or []),
                    "tool_usage": list(row.get("tool_names") or []),
                    "final_outcome": str(row.get("final_outcome") or ""),
                    "useful_lessons": list(row.get("useful_lessons") or []),
                    "similarity": score,
                    "outcome_status": str(row.get("outcome_status") or ""),
                },
            )
        )
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[: max(1, int(limit))]]


def build_unified_task(
    *,
    goal: dict[str, Any],
    planner: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    tool_candidates: list[dict[str, Any]],
    episodes: list[dict[str, Any]],
    memory: dict[str, Any],
    policy_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    beliefs: list[str] = []
    experience = derive_experience_profile(
        episodes,
        latest_result=dict(memory.get("last_task_result") or {}),
        selected_tool=str(planner.get("selected_tool") or ""),
    )
    if retrieval_hits:
        beliefs.append(f"retrieval_hits:{len(retrieval_hits)}")
    if memory.get("last_task_result"):
        beliefs.append("has_last_task_result")
    if memory.get("last_tool_result"):
        beliefs.append("has_last_tool_result")
    if episodes:
        beliefs.append(f"similar_episodes:{len(episodes)}")
    if policy_memory:
        beliefs.append(f"policy_version:{str(policy_memory.get('version_tag') or policy_memory.get('version_id') or 'unknown')}")
    available_actions = ["ask_user", "retrieve", "respond", "reflect", "replan"]
    if tool_candidates:
        available_actions.extend(["tool_call", "approval_request"])
    if str(planner.get("task_type") or "") in {"research_summary", "ticket_email"} or any(
        hint in goal.get("normalized_goal", "").lower() for hint in COMPLEX_HINTS
    ):
        available_actions.append("workflow_call")
    if (
        experience.get("preferred_action") == "workflow_call"
        or int(experience.get("tool_retry_failures") or 0) > 0
        or int(experience.get("retryable_failures") or 0) > 0
    ):
        available_actions.append("workflow_call")
    if goal.get("unknowns"):
        available_actions.append("wait")
    return {
        "goal": goal,
        "available_actions": _unique(available_actions),
        "current_beliefs": beliefs,
        "current_facts": _unique(
            [str(hit.get("title") or hit.get("source") or "") for hit in retrieval_hits[:3] if hit.get("title") or hit.get("source")]
        ),
        "planner_signal": dict(planner),
        "episode_context": episodes,
        "experience_profile": experience,
        "policy_memory": dict(policy_memory or {}),
    }


def build_task_state(
    *,
    goal: dict[str, Any],
    unified_task: dict[str, Any],
    observations: list[dict[str, Any]],
    pending_approvals: list[str],
    latest_result: dict[str, Any] | None = None,
    current_phase: str = "interpret",
    blockers: list[str] | None = None,
    policy_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers_out = list(blockers or [])
    if goal.get("unknowns"):
        blockers_out.extend(str(item) for item in goal.get("unknowns") or [])
    return {
        "current_goal": goal,
        "current_subgoals": list(goal.get("success_criteria") or []),
        "current_phase": current_phase,
        "current_action_candidate": None,
        "observations": observations,
        "beliefs": list(unified_task.get("current_beliefs") or []),
        "known_facts": list(unified_task.get("current_facts") or []),
        "blockers": _unique(blockers_out),
        "pending_approvals": _unique(pending_approvals),
        "fallback_state": "idle",
        "latest_result": latest_result or {},
        "available_actions": list(unified_task.get("available_actions") or []),
        "unknowns": list(goal.get("unknowns") or []),
        "policy_memory": dict(policy_memory or unified_task.get("policy_memory") or {}),
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
    return _shared_choose_next_action(
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


def reflect_and_replan(
    *,
    action: dict[str, Any],
    goal: dict[str, Any],
    retrieval_hits: list[dict[str, Any]],
    latest_result: dict[str, Any] | None,
    fallback_action: str | None,
) -> dict[str, Any]:
    return _shared_reflect_and_replan(
        action=action,
        goal=goal,
        retrieval_hits=retrieval_hits,
        latest_result=latest_result,
        fallback_action=fallback_action,
    )


def merge_runtime_state(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    return _shared_merge_runtime_state(base, patch)


def build_episode(
    *,
    episode_id: str,
    user_message: str,
    goal: dict[str, Any],
    action: dict[str, Any],
    task_state: dict[str, Any],
    reflection: dict[str, Any],
    policy: dict[str, Any],
    tool_names: list[str],
    outcome_status: str,
    final_outcome: str,
) -> dict[str, Any]:
    lessons: list[str] = []
    latest_result = dict(task_state.get("latest_result") or {})
    if task_state.get("unknowns"):
        lessons.append("Unknowns should be surfaced before expensive execution.")
    if policy.get("approval_triggered"):
        lessons.append("High-risk actions require explicit approval before continuing.")
    if action.get("action_type") == "workflow_call":
        lessons.append("Durable workflow handoff works better for multi-step open goals.")
    if reflection.get("requires_replan"):
        lessons.append("Retryable failures should escalate into a different action type.")
    if str(latest_result.get("status") or "") == "NEED_INFO":
        lessons.append("Clarify missing inputs before attempting execution.")
    if str(latest_result.get("status") or "") == "retryable_tool_failure":
        lessons.append("Repeated fast-path tool failures should bias toward durable execution.")
    return {
        "episode_id": episode_id,
        "normalized_goal": str(goal.get("normalized_goal") or user_message),
        "task_summary": str(goal.get("normalized_goal") or user_message)[:240],
        "chosen_strategy": str(action.get("action_type") or ""),
        "action_types": _unique([str(action.get("action_type") or ""), str(policy.get("fallback_action") or "")]),
        "tool_names": _unique([name for name in tool_names if name]),
        "outcome_status": outcome_status,
        "final_outcome": final_outcome[:400],
        "useful_lessons": _unique(lessons or [str(reflection.get("summary") or "Captured runtime outcome.")]),
        "episode_payload": {
            "goal": goal,
            "action": action,
            "task_state": task_state,
            "reflection": reflection,
            "policy": policy,
            "goal_ref": {
                "goal_id": str(goal.get("goal_id") or "") or None,
                "lifecycle_state": str(goal.get("lifecycle_state") or "") or None,
            },
            "outcome_signal": {
                "latest_result": latest_result,
                "requires_replan": bool(reflection.get("requires_replan")),
                "next_action": str(reflection.get("next_action") or ""),
                "policy_version_id": str(policy.get("policy_version_id") or "") or None,
            },
        },
    }
