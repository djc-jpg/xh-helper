from __future__ import annotations

from copy import deepcopy
import hashlib
from typing import Any
import uuid

from ..config import settings
from ..repositories import PolicyMemoryRepository

POLICY_ACTION_KEYS = (
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


def _default_memory_payload() -> dict[str, Any]:
    return {
        "action_bias": {key: 0 for key in POLICY_ACTION_KEYS},
        "tool_failure_counts": {},
        "tool_success_counts": {},
        "tool_last_seen": {},
        "tool_reliability": {},
        "external_source_failure_counts": {},
        "external_source_success_counts": {},
        "external_source_last_seen": {},
        "external_source_reliability": {},
        "critic_patterns": {},
        "portfolio_bias": {
            "dynamic_subgoal_boost": 0,
            "stalled_goal_boost": 0,
            "replan_goal_boost": 0,
            "continuation_penalty": 0,
        },
        "lesson_hints": [],
        "lesson_catalog": [],
        "retired_lessons": [],
        "feedback_counts": {
            "episodes": 0,
            "eval_runs": 0,
            "portfolio_events": 0,
            "external_signals": 0,
        },
        "portfolio_outcomes": {
            "hold_events": 0,
            "preempt_cancel_events": 0,
            "preempt_resume_success": 0,
            "preempt_resume_regret": 0,
            "subscription_timeout_events": 0,
            "external_wait_success_events": 0,
            "external_wait_failure_events": 0,
            "goal_starvation_events": 0,
        },
        "external_signal_outcomes": {
            "success": 0,
            "failure": 0,
            "timeout": 0,
            "progress": 0,
            "update": 0,
        },
        "portfolio_learning": {
            "scheduler_confidence": 0.0,
            "preempt_success_rate": 0.0,
            "preempt_regret_rate": 0.0,
            "hold_adaptation_rate": 0.0,
            "starvation_rate": 0.0,
            "subscription_timeout_rate": 0.0,
            "external_wait_success_rate": 0.0,
            "external_wait_failure_rate": 0.0,
            "portfolio_throughput_score": 0.0,
        },
        "memory_hygiene": {
            "update_index": 0,
            "memory_confidence": 0.0,
            "pruned_lessons": 0,
            "pruned_tools": 0,
            "conflict_count": 0,
            "tool_conflict_count": 0,
            "action_conflict_count": 0,
            "forgotten_lessons": 0,
            "forgotten_tools": 0,
            "forgotten_external_sources": 0,
        },
        "eval_summary": {},
    }


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalized_memory_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")


def _external_source_feedback_keys(*, source: str, event_topic: str | None = None) -> list[str]:
    normalized_source = _normalized_memory_key(source) or "external_signal"
    keys = [normalized_source]
    normalized_topic = _normalized_memory_key(event_topic)
    if normalized_topic:
        keys.append(f"{normalized_source}:topic:{normalized_topic}")
    return keys


def _top_int_dict(
    raw_mapping: dict[str, Any],
    *,
    limit: int,
    last_seen: dict[str, Any] | None = None,
) -> tuple[dict[str, int], int]:
    items: list[tuple[str, int, int]] = []
    for raw_key, raw_value in raw_mapping.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        score = _safe_int(raw_value)
        if score <= 0:
            continue
        items.append((key, score, _safe_int(_as_dict(last_seen).get(key))))
    items.sort(key=lambda item: (-item[1], -item[2], item[0]))
    kept = items[: max(1, int(limit))]
    return {key: score for key, score, _ in kept}, max(0, len(items) - len(kept))


def _lesson_catalog_entries(value: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    raw_items = value if isinstance(value, list) else []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        lesson = str(raw_item.get("lesson") or "").strip()
        if not lesson:
            continue
        entries.append(
            {
                "lesson": lesson,
                "support_count": max(1, _safe_int(raw_item.get("support_count"))),
                "last_seen_index": max(0, _safe_int(raw_item.get("last_seen_index"))),
                "confidence": max(0.0, min(1.0, _safe_float(raw_item.get("confidence") or 0.0))),
                "source": str(raw_item.get("source") or ""),
            }
        )
    return entries


def _retired_lesson_entries(value: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    raw_items = value if isinstance(value, list) else []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        lesson = str(raw_item.get("lesson") or "").strip()
        if not lesson:
            continue
        entries.append(
            {
                "lesson": lesson,
                "reason": str(raw_item.get("reason") or "").strip() or "retired",
                "retired_at_index": max(0, _safe_int(raw_item.get("retired_at_index"))),
            }
        )
    return entries


def _tool_reliability_scores(
    *,
    success_counts: dict[str, Any],
    failure_counts: dict[str, Any],
    last_seen: dict[str, Any],
    update_index: int,
) -> dict[str, dict[str, Any]]:
    scores: dict[str, dict[str, Any]] = {}
    confidence_floor = max(1, int(settings.policy_memory_confidence_feedback_floor))
    evidence_floor = max(1, int(settings.policy_memory_min_tool_evidence))
    forget_after = max(2, int(settings.policy_memory_forget_after_updates))
    for tool_name in sorted(set(_as_dict(success_counts).keys()) | set(_as_dict(failure_counts).keys())):
        success_count = max(0, _safe_int(_as_dict(success_counts).get(tool_name)))
        failure_count = max(0, _safe_int(_as_dict(failure_counts).get(tool_name)))
        evidence = success_count + failure_count
        if evidence <= 0:
            continue
        age = max(0, update_index - _safe_int(_as_dict(last_seen).get(tool_name)))
        recency_weight = max(0.15, 1.0 - (age / max(1.0, float(forget_after))))
        evidence_weight = min(1.0, evidence / max(1.0, float(evidence_floor)))
        score = ((success_count - failure_count) / max(1.0, float(evidence))) * recency_weight * evidence_weight
        confidence = min(1.0, (evidence / max(1.0, float(confidence_floor))) * recency_weight)
        scores[str(tool_name)] = {
            "score": round(max(-1.0, min(1.0, score)), 3),
            "confidence": round(max(0.0, min(1.0, confidence)), 3),
            "evidence": evidence,
            "successes": success_count,
            "failures": failure_count,
            "age": age,
        }
    return scores


def _external_source_reliability_scores(
    *,
    success_counts: dict[str, Any],
    failure_counts: dict[str, Any],
    last_seen: dict[str, Any],
    update_index: int,
) -> dict[str, dict[str, Any]]:
    return _tool_reliability_scores(
        success_counts=success_counts,
        failure_counts=failure_counts,
        last_seen=last_seen,
        update_index=update_index,
    )


def _portfolio_learning_summary(
    *,
    portfolio_outcomes: dict[str, Any],
    feedback_counts: dict[str, Any],
) -> dict[str, float]:
    portfolio_outcomes = _as_dict(portfolio_outcomes)
    feedback_counts = _as_dict(feedback_counts)
    hold_events = max(0, _safe_int(portfolio_outcomes.get("hold_events")))
    preempt_cancel_events = max(0, _safe_int(portfolio_outcomes.get("preempt_cancel_events")))
    preempt_resume_success = max(0, _safe_int(portfolio_outcomes.get("preempt_resume_success")))
    preempt_resume_regret = max(0, _safe_int(portfolio_outcomes.get("preempt_resume_regret")))
    subscription_timeout_events = max(0, _safe_int(portfolio_outcomes.get("subscription_timeout_events")))
    external_wait_success_events = max(0, _safe_int(portfolio_outcomes.get("external_wait_success_events")))
    external_wait_failure_events = max(0, _safe_int(portfolio_outcomes.get("external_wait_failure_events")))
    goal_starvation_events = max(0, _safe_int(portfolio_outcomes.get("goal_starvation_events")))
    portfolio_events = max(0, _safe_int(feedback_counts.get("portfolio_events")))
    preempt_total = preempt_cancel_events + preempt_resume_success + preempt_resume_regret
    external_wait_total = subscription_timeout_events + external_wait_success_events + external_wait_failure_events
    scheduler_confidence = 0.0
    if portfolio_events > 0:
        scheduler_confidence = max(
            0.0,
            min(
                1.0,
                (
                    (preempt_resume_success * 1.5) + max(0, hold_events - preempt_cancel_events)
                )
                / max(1.0, float(portfolio_events + preempt_total)),
            ),
        )
    throughput_denominator = (
        hold_events
        + preempt_total
        + external_wait_total
        + goal_starvation_events
        + max(0, _safe_int(feedback_counts.get("episodes")))
    )
    throughput_score = 0.0
    if throughput_denominator > 0:
        throughput_score = max(
            0.0,
            min(
                1.0,
                (
                    (preempt_resume_success * 1.5)
                    + external_wait_success_events
                    + max(0, hold_events - preempt_cancel_events)
                )
                / max(1.0, float(throughput_denominator)),
            ),
        )
    return {
        "scheduler_confidence": round(scheduler_confidence, 3),
        "preempt_success_rate": round(preempt_resume_success / max(1.0, float(preempt_total)), 3) if preempt_total else 0.0,
        "preempt_regret_rate": round((preempt_resume_regret + preempt_cancel_events) / max(1.0, float(preempt_total)), 3)
        if preempt_total
        else 0.0,
        "hold_adaptation_rate": round(hold_events / max(1.0, float(portfolio_events)), 3) if portfolio_events else 0.0,
        "starvation_rate": round(goal_starvation_events / max(1.0, float(portfolio_events)), 3) if portfolio_events else 0.0,
        "subscription_timeout_rate": round(subscription_timeout_events / max(1.0, float(external_wait_total)), 3)
        if external_wait_total
        else 0.0,
        "external_wait_success_rate": round(external_wait_success_events / max(1.0, float(external_wait_total)), 3)
        if external_wait_total
        else 0.0,
        "external_wait_failure_rate": round(external_wait_failure_events / max(1.0, float(external_wait_total)), 3)
        if external_wait_total
        else 0.0,
        "portfolio_throughput_score": round(throughput_score, 3),
    }


def _update_lesson_catalog(
    *,
    payload: dict[str, Any],
    lessons: list[str],
    source: str,
) -> dict[str, Any]:
    hygiene = _as_dict(payload.get("memory_hygiene"))
    feedback_counts = _as_dict(payload.get("feedback_counts"))
    update_index = max(
        1,
        _safe_int(hygiene.get("update_index")),
        _safe_int(feedback_counts.get("episodes"))
        + _safe_int(feedback_counts.get("portfolio_events"))
        + _safe_int(feedback_counts.get("eval_runs")),
    )
    catalog: dict[str, dict[str, Any]] = {
        str(item.get("lesson") or ""): dict(item)
        for item in _lesson_catalog_entries(payload.get("lesson_catalog"))
        if str(item.get("lesson") or "")
    }
    for lesson in lessons:
        entry = dict(catalog.get(lesson) or {})
        support_count = max(0, _safe_int(entry.get("support_count"))) + 1
        entry.update(
            {
                "lesson": lesson,
                "support_count": support_count,
                "last_seen_index": update_index,
                "source": source,
            }
        )
        catalog[lesson] = entry
    payload["lesson_catalog"] = list(catalog.values())
    return payload


def _apply_memory_hygiene(payload: dict[str, Any]) -> dict[str, Any]:
    next_payload = deepcopy(payload)
    feedback_counts = _as_dict(next_payload.get("feedback_counts"))
    total_feedback = (
        _safe_int(feedback_counts.get("episodes"))
        + _safe_int(feedback_counts.get("portfolio_events"))
        + _safe_int(feedback_counts.get("eval_runs"))
        + _safe_int(feedback_counts.get("external_signals"))
    )
    hygiene = {
        **_as_dict(_default_memory_payload().get("memory_hygiene")),
        **_as_dict(next_payload.get("memory_hygiene")),
    }
    hygiene["update_index"] = max(_safe_int(hygiene.get("update_index")), total_feedback)
    forget_after = max(2, int(settings.policy_memory_forget_after_updates))

    max_lessons = max(1, int(settings.policy_memory_max_lessons))
    lesson_entries = _lesson_catalog_entries(next_payload.get("lesson_catalog"))
    retired_lessons = _retired_lesson_entries(next_payload.get("retired_lessons"))
    lesson_entries.sort(
        key=lambda item: (
            -(
                (max(1, _safe_int(item.get("support_count"))) * 2.0)
                - max(0, hygiene["update_index"] - _safe_int(item.get("last_seen_index"))) * 0.1
            ),
            item["lesson"],
        )
    )
    kept_lessons: list[dict[str, Any]] = []
    forgotten_lessons = 0
    for item in lesson_entries:
        age = max(0, hygiene["update_index"] - _safe_int(item.get("last_seen_index")))
        support_count = max(1, _safe_int(item.get("support_count")))
        if age > forget_after and support_count <= 1:
            retired_lessons.append(
                {
                    "lesson": str(item.get("lesson") or ""),
                    "reason": "stale_low_support",
                    "retired_at_index": hygiene["update_index"],
                }
            )
            forgotten_lessons += 1
            continue
        kept_lessons.append(item)
    kept_lessons = kept_lessons[:max_lessons]
    hygiene["pruned_lessons"] = max(0, len(lesson_entries) - len(kept_lessons) - forgotten_lessons)
    hygiene["forgotten_lessons"] = forgotten_lessons
    confidence_floor = max(1, int(settings.policy_memory_confidence_feedback_floor))
    next_payload["lesson_catalog"] = [
        {
            **item,
            "confidence": round(
                min(
                    1.0,
                    (
                        max(1, _safe_int(item.get("support_count"))) * 2.0
                        + max(0, confidence_floor - max(0, hygiene["update_index"] - _safe_int(item.get("last_seen_index"))))
                    )
                    / max(1.0, confidence_floor * 2.0),
                ),
                3,
            ),
        }
        for item in kept_lessons
    ]
    next_payload["lesson_hints"] = [str(item.get("lesson") or "") for item in next_payload["lesson_catalog"][:max_lessons]]
    max_retired_lessons = max(1, int(settings.policy_memory_max_retired_lessons))
    retired_lessons = sorted(
        retired_lessons,
        key=lambda item: (-_safe_int(item.get("retired_at_index")), str(item.get("lesson") or "")),
    )[:max_retired_lessons]
    next_payload["retired_lessons"] = retired_lessons

    max_tool_entries = max(1, int(settings.policy_memory_max_tool_entries))
    tool_last_seen = _as_dict(next_payload.get("tool_last_seen"))
    tool_failures, pruned_failures = _top_int_dict(
        _as_dict(next_payload.get("tool_failure_counts")),
        limit=max_tool_entries,
        last_seen=tool_last_seen,
    )
    tool_successes, pruned_successes = _top_int_dict(
        _as_dict(next_payload.get("tool_success_counts")),
        limit=max_tool_entries,
        last_seen=tool_last_seen,
    )
    forgotten_tools = 0
    for tool_name in list(set(tool_failures.keys()) | set(tool_successes.keys())):
        last_seen_index = _safe_int(tool_last_seen.get(tool_name))
        age = max(0, hygiene["update_index"] - last_seen_index)
        evidence = _safe_int(tool_failures.get(tool_name)) + _safe_int(tool_successes.get(tool_name))
        if age > forget_after and evidence <= 1:
            tool_failures.pop(tool_name, None)
            tool_successes.pop(tool_name, None)
            forgotten_tools += 1
    next_payload["tool_failure_counts"] = tool_failures
    next_payload["tool_success_counts"] = tool_successes
    next_payload["tool_last_seen"] = {
        key: _safe_int(tool_last_seen.get(key))
        for key in {**tool_failures, **tool_successes}.keys()
        if _safe_int(tool_last_seen.get(key)) > 0
    }
    hygiene["pruned_tools"] = pruned_failures + pruned_successes
    hygiene["forgotten_tools"] = forgotten_tools
    next_payload["tool_reliability"] = _tool_reliability_scores(
        success_counts=next_payload.get("tool_success_counts"),
        failure_counts=next_payload.get("tool_failure_counts"),
        last_seen=next_payload.get("tool_last_seen"),
        update_index=hygiene["update_index"],
    )
    external_source_success = _as_dict(next_payload.get("external_source_success_counts"))
    external_source_failure = _as_dict(next_payload.get("external_source_failure_counts"))
    external_source_last_seen = _as_dict(next_payload.get("external_source_last_seen"))
    forgotten_external_sources = 0
    for source_name in list(set(external_source_success.keys()) | set(external_source_failure.keys())):
        last_seen_index = _safe_int(external_source_last_seen.get(source_name))
        age = max(0, hygiene["update_index"] - last_seen_index)
        evidence = _safe_int(external_source_success.get(source_name)) + _safe_int(external_source_failure.get(source_name))
        if age > forget_after and evidence <= 1:
            external_source_success.pop(source_name, None)
            external_source_failure.pop(source_name, None)
            external_source_last_seen.pop(source_name, None)
            forgotten_external_sources += 1
    next_payload["external_source_success_counts"] = external_source_success
    next_payload["external_source_failure_counts"] = external_source_failure
    next_payload["external_source_last_seen"] = external_source_last_seen
    next_payload["external_source_reliability"] = _external_source_reliability_scores(
        success_counts=external_source_success,
        failure_counts=external_source_failure,
        last_seen=external_source_last_seen,
        update_index=hygiene["update_index"],
    )
    hygiene["forgotten_external_sources"] = forgotten_external_sources
    tool_conflict_count = sum(
        1
        for key in set(_as_dict(next_payload.get("tool_failure_counts")).keys()).intersection(
            _as_dict(next_payload.get("tool_success_counts")).keys()
        )
        if _safe_int(_as_dict(next_payload.get("tool_failure_counts")).get(key)) > 0
        and _safe_int(_as_dict(next_payload.get("tool_success_counts")).get(key)) > 0
    )
    action_bias = _as_dict(next_payload.get("action_bias"))
    ranked_actions = sorted(
        [
            (str(key), _safe_int(value))
            for key, value in action_bias.items()
            if _safe_int(value) > 0
        ],
        key=lambda item: (-item[1], item[0]),
    )
    action_conflict_count = 0
    if len(ranked_actions) >= 2 and abs(ranked_actions[0][1] - ranked_actions[1][1]) <= 1:
        action_conflict_count = 1
    conflict_count = tool_conflict_count + action_conflict_count
    confidence_penalty = min(0.4, conflict_count * 0.1)
    hygiene["tool_conflict_count"] = tool_conflict_count
    hygiene["action_conflict_count"] = action_conflict_count
    hygiene["conflict_count"] = conflict_count
    hygiene["memory_confidence"] = round(max(0.0, min(1.0, total_feedback / max(1, confidence_floor)) - confidence_penalty), 3)
    next_payload["portfolio_learning"] = _portfolio_learning_summary(
        portfolio_outcomes=next_payload.get("portfolio_outcomes"),
        feedback_counts=next_payload.get("feedback_counts"),
    )
    next_payload["memory_hygiene"] = hygiene
    return next_payload


def derive_policy_eval_summary(
    *,
    memory_payload: dict[str, Any],
    base_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    defaults = _default_memory_payload()
    payload = {
        **defaults,
        **(deepcopy(_as_dict(memory_payload)) or {}),
    }
    feedback_counts = {
        **_as_dict(defaults.get("feedback_counts")),
        **_as_dict(payload.get("feedback_counts")),
    }
    critic_patterns = _as_dict(payload.get("critic_patterns"))
    portfolio_bias = {
        **_as_dict(defaults.get("portfolio_bias")),
        **_as_dict(payload.get("portfolio_bias")),
    }
    portfolio_outcomes = {
        **_as_dict(defaults.get("portfolio_outcomes")),
        **_as_dict(payload.get("portfolio_outcomes")),
    }
    portfolio_learning = {
        **_as_dict(defaults.get("portfolio_learning")),
        **_as_dict(payload.get("portfolio_learning")),
    }
    external_signal_outcomes = {
        **_as_dict(defaults.get("external_signal_outcomes")),
        **_as_dict(payload.get("external_signal_outcomes")),
    }
    summary = dict(base_summary or {})

    success_count = _safe_int(critic_patterns.get("SUCCEEDED"))
    retryable_failures = _safe_int(critic_patterns.get("FAILED_RETRYABLE"))
    failed_final = _safe_int(critic_patterns.get("FAILED_FINAL"))
    timed_out = _safe_int(critic_patterns.get("TIMED_OUT"))
    cancelled = _safe_int(critic_patterns.get("CANCELLED"))
    total_terminal = success_count + retryable_failures + failed_final + timed_out + cancelled
    if total_terminal > 0 and "success_rate" not in summary:
        summary["success_rate"] = round(success_count / total_terminal, 3)

    if (total_terminal > 0 or _safe_int(feedback_counts.get("episodes")) > 0) and "trace_coverage" not in summary:
        summary["trace_coverage"] = 1.0
    summary.setdefault("prompt_leak_count", 0)
    summary.setdefault("unauthorized_tool_calls", 0)
    summary.setdefault("status_mismatch_count", 0)

    hold_events = _safe_int(portfolio_outcomes.get("hold_events"))
    preempt_cancel_events = _safe_int(portfolio_outcomes.get("preempt_cancel_events"))
    preempt_resume_success = _safe_int(portfolio_outcomes.get("preempt_resume_success"))
    preempt_resume_regret = _safe_int(portfolio_outcomes.get("preempt_resume_regret"))
    subscription_timeout_events = _safe_int(portfolio_outcomes.get("subscription_timeout_events"))
    external_wait_success_events = _safe_int(portfolio_outcomes.get("external_wait_success_events"))
    external_wait_failure_events = _safe_int(portfolio_outcomes.get("external_wait_failure_events"))
    goal_starvation_events = _safe_int(portfolio_outcomes.get("goal_starvation_events"))
    continuation_penalty = _safe_int(portfolio_bias.get("continuation_penalty"))

    completed_pressure = success_count + preempt_resume_success
    stalled_pressure = retryable_failures + failed_final + timed_out + preempt_cancel_events + preempt_resume_regret
    if completed_pressure + stalled_pressure > 0 and "portfolio_goal_completion_rate" not in summary:
        summary["portfolio_goal_completion_rate"] = round(
            completed_pressure / (completed_pressure + stalled_pressure),
            3,
        )

    recovery_denominator = preempt_resume_success + preempt_resume_regret
    if recovery_denominator > 0 and "preempt_recovery_success_rate" not in summary:
        summary["preempt_recovery_success_rate"] = round(
            preempt_resume_success / recovery_denominator,
            3,
        )

    regret_denominator = max(1, preempt_resume_success + preempt_resume_regret + preempt_cancel_events)
    if (
        preempt_resume_success > 0
        or preempt_resume_regret > 0
        or preempt_cancel_events > 0
    ) and "preempt_regret_rate" not in summary:
        summary["preempt_regret_rate"] = round(preempt_resume_regret / regret_denominator, 3)

    agenda_pressure = hold_events + preempt_cancel_events + continuation_penalty
    agenda_support = completed_pressure + _safe_int(feedback_counts.get("episodes"))
    if agenda_pressure > 0 or agenda_support > 0:
        stability = 1.0 - min(1.0, agenda_pressure / max(1.0, agenda_pressure + agenda_support))
        summary.setdefault("agenda_stability", round(stability, 3))

    hygiene = _as_dict(payload.get("memory_hygiene"))
    summary["memory_confidence"] = round(max(0.0, min(1.0, _safe_float(hygiene.get("memory_confidence")))), 3)
    summary["memory_conflict_count"] = _safe_int(hygiene.get("conflict_count"))
    summary["forgotten_lesson_count"] = _safe_int(hygiene.get("forgotten_lessons"))
    summary["forgotten_tool_count"] = _safe_int(hygiene.get("forgotten_tools"))
    summary["policy_lesson_count"] = len(_lesson_catalog_entries(payload.get("lesson_catalog")))
    summary["retired_lesson_count"] = len(_retired_lesson_entries(payload.get("retired_lessons")))
    summary["policy_tool_memory_count"] = len(_as_dict(payload.get("tool_failure_counts"))) + len(
        _as_dict(payload.get("tool_success_counts"))
    )
    summary["tool_reliability_count"] = len(_as_dict(payload.get("tool_reliability")))
    summary["scheduler_confidence"] = round(_safe_float(portfolio_learning.get("scheduler_confidence")), 3)
    summary["portfolio_preempt_success_rate"] = round(_safe_float(portfolio_learning.get("preempt_success_rate")), 3)
    summary["portfolio_preempt_regret_rate"] = round(_safe_float(portfolio_learning.get("preempt_regret_rate")), 3)
    summary["portfolio_hold_adaptation_rate"] = round(_safe_float(portfolio_learning.get("hold_adaptation_rate")), 3)
    summary["portfolio_starvation_rate"] = round(_safe_float(portfolio_learning.get("starvation_rate")), 3)
    summary["portfolio_subscription_timeout_rate"] = round(_safe_float(portfolio_learning.get("subscription_timeout_rate")), 3)
    summary["portfolio_external_wait_success_rate"] = round(_safe_float(portfolio_learning.get("external_wait_success_rate")), 3)
    summary["portfolio_external_wait_failure_rate"] = round(_safe_float(portfolio_learning.get("external_wait_failure_rate")), 3)
    summary["portfolio_throughput_score"] = round(_safe_float(portfolio_learning.get("portfolio_throughput_score")), 3)
    summary["portfolio_external_wait_event_count"] = (
        subscription_timeout_events + external_wait_success_events + external_wait_failure_events
    )
    summary["portfolio_starved_goal_count"] = goal_starvation_events
    summary["feedback_episode_count"] = _safe_int(feedback_counts.get("episodes"))
    summary["feedback_eval_run_count"] = _safe_int(feedback_counts.get("eval_runs"))
    summary["feedback_portfolio_event_count"] = _safe_int(feedback_counts.get("portfolio_events"))
    summary["feedback_external_signal_count"] = _safe_int(feedback_counts.get("external_signals"))
    external_total = sum(max(0, _safe_int(value)) for value in external_signal_outcomes.values())
    external_failures = _safe_int(external_signal_outcomes.get("failure")) + _safe_int(external_signal_outcomes.get("timeout"))
    summary["external_signal_failure_rate"] = round(external_failures / max(1.0, float(external_total)), 3) if external_total else 0.0
    summary["external_source_reliability_count"] = len(_as_dict(payload.get("external_source_reliability")))
    return summary


def _refresh_candidate_eval_summary(
    *,
    payload: dict[str, Any],
    comparison_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    next_payload = deepcopy(payload)
    next_comparison = deepcopy(comparison_payload)
    eval_summary = derive_policy_eval_summary(
        memory_payload=next_payload,
        base_summary=_as_dict(next_payload.get("eval_summary")),
    )
    eval_summary.update({key: value for key, value in _shadow_probe_summary(comparison_payload=next_comparison).items() if key not in eval_summary})
    eval_summary.update({key: value for key, value in _shadow_outcome_summary(comparison_payload=next_comparison).items() if key not in eval_summary})
    eval_summary.update({key: value for key, value in _shadow_portfolio_summary(comparison_payload=next_comparison).items() if key not in eval_summary})
    eval_summary.update(
        {key: value for key, value in _shadow_portfolio_outcome_summary(comparison_payload=next_comparison).items() if key not in eval_summary}
    )
    next_payload["eval_summary"] = eval_summary
    next_comparison["auto_eval_summary"] = dict(eval_summary)
    return next_payload, next_comparison


def _repo_supports_eval_loop(repo: PolicyMemoryRepository | None) -> bool:
    if repo is None:
        return False
    return all(
        hasattr(repo, attr)
        for attr in (
            "get_policy_version",
            "create_eval_run",
            "activate_policy_version",
            "mark_policy_version_status",
            "update_policy_version",
        )
    )


def maybe_auto_evaluate_candidate_policy(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
) -> dict[str, Any] | None:
    if not bool(settings.policy_auto_eval_enabled):
        return None
    if not _repo_supports_eval_loop(repo):
        return None
    candidate_id = str(candidate_version_id or "").strip()
    if not candidate_id:
        return None
    candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=candidate_id)
    if not candidate:
        return None
    candidate_status = str(candidate.get("status") or "").upper()
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    if str(active.get("version_id") or "") == candidate_id:
        return None

    memory_payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    feedback_counts = _as_dict(memory_payload.get("feedback_counts"))
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    last_counts = _as_dict(comparison_payload.get("last_auto_eval_feedback_counts"))

    episode_count = _safe_int(feedback_counts.get("episodes"))
    portfolio_event_count = _safe_int(feedback_counts.get("portfolio_events"))
    total_feedback = episode_count + portfolio_event_count
    last_total_feedback = _safe_int(last_counts.get("episodes")) + _safe_int(last_counts.get("portfolio_events"))
    new_feedback = max(0, total_feedback - last_total_feedback)
    shadow_summary = _shadow_probe_summary(comparison_payload=comparison_payload)
    shadow_probe_count = _safe_int(shadow_summary.get("shadow_probe_count"))
    shadow_counts = _as_dict(comparison_payload.get("shadow_probe_counts"))
    shadow_outcome_summary = _shadow_outcome_summary(comparison_payload=comparison_payload)
    shadow_outcome_guardrail = _shadow_outcome_guardrail_verdict(comparison_payload=comparison_payload)
    shadow_portfolio_summary = _shadow_portfolio_summary(comparison_payload=comparison_payload)
    shadow_portfolio_guardrail = _shadow_portfolio_guardrail_verdict(comparison_payload=comparison_payload)
    shadow_portfolio_outcome_summary = _shadow_portfolio_outcome_summary(comparison_payload=comparison_payload)
    shadow_portfolio_outcome_guardrail = _shadow_portfolio_outcome_guardrail_verdict(
        comparison_payload=comparison_payload
    )
    shadow_high_risk_probe_count = _safe_int(shadow_counts.get("high_risk_total"))
    shadow_ready = (
        not bool(settings.policy_shadow_enabled)
        or shadow_probe_count >= max(0, int(settings.policy_shadow_min_probe_count))
    )
    if shadow_ready and shadow_high_risk_probe_count > 0:
        shadow_ready = shadow_high_risk_probe_count >= max(0, int(settings.policy_shadow_min_high_risk_probe_count))
    if (
        shadow_ready
        and candidate_status == "CANARY"
        and bool(shadow_outcome_guardrail)
        and not bool(shadow_outcome_guardrail.get("ready"))
    ):
        shadow_ready = False
    if (
        shadow_ready
        and bool(shadow_outcome_guardrail)
        and bool(shadow_outcome_guardrail.get("ready"))
        and not bool(shadow_outcome_guardrail.get("passed"))
    ):
        shadow_ready = False
    if (
        shadow_ready
        and candidate_status == "CANARY"
        and portfolio_event_count > 0
        and bool(shadow_portfolio_guardrail)
        and not bool(shadow_portfolio_guardrail.get("ready"))
    ):
        shadow_ready = False
    if (
        shadow_ready
        and portfolio_event_count > 0
        and bool(shadow_portfolio_guardrail)
        and bool(shadow_portfolio_guardrail.get("ready"))
        and not bool(shadow_portfolio_guardrail.get("passed"))
    ):
        shadow_ready = False
    if (
        shadow_ready
        and candidate_status == "CANARY"
        and portfolio_event_count > 0
        and bool(shadow_portfolio_outcome_guardrail)
        and not bool(shadow_portfolio_outcome_guardrail.get("ready"))
    ):
        shadow_ready = False
    if (
        shadow_ready
        and portfolio_event_count > 0
        and bool(shadow_portfolio_outcome_guardrail)
        and bool(shadow_portfolio_outcome_guardrail.get("ready"))
        and not bool(shadow_portfolio_outcome_guardrail.get("passed"))
    ):
        shadow_ready = False

    ready = (
        total_feedback >= max(1, int(settings.policy_auto_eval_min_total_feedback))
        and (
            episode_count >= max(0, int(settings.policy_auto_eval_min_episode_feedback))
            or portfolio_event_count >= max(0, int(settings.policy_auto_eval_min_portfolio_feedback))
        )
        and new_feedback >= max(1, int(settings.policy_auto_eval_feedback_delta))
        and shadow_ready
    )
    if not ready:
        return None

    eval_summary = derive_policy_eval_summary(
        memory_payload=memory_payload,
        base_summary=_as_dict(memory_payload.get("eval_summary")),
    )
    eval_summary.update({key: value for key, value in shadow_summary.items() if key not in eval_summary})
    eval_summary.update({key: value for key, value in shadow_outcome_summary.items() if key not in eval_summary})
    eval_summary.update({key: value for key, value in shadow_portfolio_summary.items() if key not in eval_summary})
    eval_summary.update({key: value for key, value in shadow_portfolio_outcome_summary.items() if key not in eval_summary})
    result = record_policy_eval(
        repo=repo,
        tenant_id=tenant_id,
        actor_user_id=actor_user_id,
        candidate_version_id=candidate_id,
        summary=eval_summary,
        auto_promote=bool(settings.policy_auto_eval_promote),
    )
    latest = repo.get_policy_version(tenant_id=tenant_id, version_id=candidate_id)
    if latest:
        repo.update_policy_version(
            tenant_id=tenant_id,
            version_id=candidate_id,
            memory_payload=deepcopy(_as_dict(latest.get("memory_payload"))) or _default_memory_payload(),
            comparison_payload={
                **_as_dict(latest.get("comparison_payload")),
                "last_auto_eval_feedback_counts": {
                    "episodes": episode_count,
                    "portfolio_events": portfolio_event_count,
                    "shadow_probes": shadow_probe_count,
                    "shadow_outcomes": _safe_int(shadow_outcome_summary.get("shadow_outcome_count")),
                    "shadow_portfolio_probes": _safe_int(shadow_portfolio_summary.get("shadow_portfolio_probe_count")),
                    "shadow_portfolio_outcomes": _safe_int(
                        shadow_portfolio_outcome_summary.get("shadow_portfolio_outcome_count")
                    ),
                },
                "last_auto_eval_result": {
                    "eval_run_id": str(result.get("eval_run_id") or ""),
                    "passed": bool(_as_dict(result.get("verdict")).get("passed")),
                    "promoted": bool(result.get("promoted")),
                    "candidate_status": str(result.get("candidate_status") or ""),
                },
            },
        )
    return result


def ensure_active_policy_version(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None = None,
) -> dict[str, Any]:
    if repo is None:
        return {
            "version_id": "policy-memory-ephemeral",
            "version_tag": "ephemeral",
            "status": "ACTIVE",
            "memory_payload": _default_memory_payload(),
            "comparison_payload": {},
        }
    active = repo.get_active_version(tenant_id=tenant_id)
    if active:
        return active
    return repo.create_policy_version(
        tenant_id=tenant_id,
        version_id=f"policy-{uuid.uuid4().hex[:16]}",
        version_tag="baseline",
        status="ACTIVE",
        base_version_id=None,
        source="bootstrap",
        memory_payload=_default_memory_payload(),
        comparison_payload={"baseline_version_id": None},
        created_by=actor_user_id,
    )


def _stable_rollout_bucket(identity: str) -> int:
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


def _portfolio_rollout_guardrail_reason(
    *,
    active_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
) -> str | None:
    active_starvation_rate = _safe_float(active_summary.get("portfolio_starvation_rate"))
    candidate_starvation_rate = _safe_float(candidate_summary.get("portfolio_starvation_rate"))
    if candidate_starvation_rate > max(
        float(settings.policy_canary_max_starvation_rate),
        active_starvation_rate + float(settings.policy_canary_starvation_rate_delta),
    ):
        return "canary_starvation_rate_too_high"

    active_subscription_timeout_rate = _safe_float(active_summary.get("portfolio_subscription_timeout_rate"))
    candidate_subscription_timeout_rate = _safe_float(candidate_summary.get("portfolio_subscription_timeout_rate"))
    if candidate_subscription_timeout_rate > max(
        float(settings.policy_canary_max_subscription_timeout_rate),
        active_subscription_timeout_rate + float(settings.policy_canary_subscription_timeout_rate_delta),
    ):
        return "canary_subscription_timeout_rate_too_high"

    active_throughput_score = _safe_float(active_summary.get("portfolio_throughput_score"))
    candidate_throughput_score = _safe_float(candidate_summary.get("portfolio_throughput_score"))
    if candidate_throughput_score > 0.0 and candidate_throughput_score < max(
        float(settings.policy_canary_min_throughput_score),
        active_throughput_score - float(settings.policy_canary_throughput_score_delta),
    ):
        return "canary_portfolio_throughput_too_low"

    return None


def select_runtime_policy_version(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    goal: dict[str, Any] | None = None,
    conversation_id: str | None = None,
    preferred_version_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    preferred_id = str(preferred_version_id or "").strip()
    if repo is None:
        return active, {"mode": "active", "reason": "ephemeral_runtime", "rollout_bucket": None}
    if preferred_id:
        preferred = repo.get_policy_version(tenant_id=tenant_id, version_id=preferred_id)
        if preferred and str(preferred.get("status") or "").upper() in {"ACTIVE", "CANARY"}:
            return preferred, {
                "mode": str(preferred.get("status") or "ACTIVE").lower(),
                "reason": "goal_continuity",
                "rollout_bucket": None,
            }

    candidate = repo.get_candidate_version(tenant_id=tenant_id)
    candidate_status = str((candidate or {}).get("status") or "").upper()
    if not bool(settings.policy_canary_enabled) or candidate is None or candidate_status != "CANARY":
        return active, {"mode": "active", "reason": "no_canary_available", "rollout_bucket": None}

    goal_payload = _as_dict(goal)
    risk_level = str(goal_payload.get("risk_level") or "low").lower()
    if risk_level == "high" and not bool(settings.policy_canary_allow_high_risk):
        return active, {"mode": "active", "reason": "high_risk_goal", "rollout_bucket": None}

    last_eval_verdict = _as_dict(_as_dict(candidate.get("comparison_payload")).get("last_eval_verdict"))
    shadow_guardrail = _as_dict(_as_dict(candidate.get("comparison_payload")).get("shadow_guardrail"))
    shadow_outcome_guardrail = _as_dict(_as_dict(candidate.get("comparison_payload")).get("shadow_outcome_guardrail"))
    shadow_portfolio_guardrail = _as_dict(_as_dict(candidate.get("comparison_payload")).get("shadow_portfolio_guardrail"))
    shadow_portfolio_outcome_guardrail = _as_dict(
        _as_dict(candidate.get("comparison_payload")).get("shadow_portfolio_outcome_guardrail")
    )
    eval_summary = _as_dict(_as_dict(candidate.get("memory_payload")).get("eval_summary"))
    active_eval_summary = _as_dict(_as_dict(active.get("memory_payload")).get("eval_summary"))
    shadow_counts = _as_dict(_as_dict(candidate.get("comparison_payload")).get("shadow_probe_counts"))
    shadow_summary = _shadow_probe_summary(comparison_payload=_as_dict(candidate.get("comparison_payload")))
    shadow_outcome_summary = _shadow_outcome_summary(comparison_payload=_as_dict(candidate.get("comparison_payload")))
    shadow_portfolio_outcome_summary = _shadow_portfolio_outcome_summary(
        comparison_payload=_as_dict(candidate.get("comparison_payload"))
    )
    shadow_probe_count = _safe_int(shadow_summary.get("shadow_probe_count"))
    if shadow_guardrail and not bool(shadow_guardrail.get("passed")) and bool(shadow_guardrail.get("ready")):
        return active, {"mode": "active", "reason": "shadow_guardrail_failed", "rollout_bucket": None}
    if shadow_outcome_guardrail and not bool(shadow_outcome_guardrail.get("passed")) and bool(shadow_outcome_guardrail.get("ready")):
        return active, {"mode": "active", "reason": "shadow_outcome_guardrail_failed", "rollout_bucket": None}
    if shadow_portfolio_guardrail and not bool(shadow_portfolio_guardrail.get("passed")) and bool(shadow_portfolio_guardrail.get("ready")):
        return active, {"mode": "active", "reason": "shadow_portfolio_guardrail_failed", "rollout_bucket": None}
    if shadow_portfolio_outcome_guardrail and not bool(shadow_portfolio_outcome_guardrail.get("passed")) and bool(shadow_portfolio_outcome_guardrail.get("ready")):
        return active, {"mode": "active", "reason": "shadow_portfolio_outcome_guardrail_failed", "rollout_bucket": None}
    if shadow_outcome_summary and (
        _safe_float(shadow_outcome_summary.get("shadow_regret_signal_rate"))
        > float(settings.policy_shadow_max_regret_signal_rate)
    ):
        return active, {"mode": "active", "reason": "shadow_outcome_regret_too_high", "rollout_bucket": None}
    if shadow_portfolio_outcome_summary and (
        _safe_float(shadow_portfolio_outcome_summary.get("shadow_portfolio_regret_signal_rate"))
        > float(settings.policy_shadow_max_regret_signal_rate)
    ):
        return active, {"mode": "active", "reason": "shadow_portfolio_outcome_regret_too_high", "rollout_bucket": None}
    portfolio_rollout_reason = _portfolio_rollout_guardrail_reason(
        active_summary=active_eval_summary,
        candidate_summary=eval_summary,
    )
    if portfolio_rollout_reason:
        return active, {"mode": "active", "reason": portfolio_rollout_reason, "rollout_bucket": None}
    if bool(settings.policy_shadow_enabled) and shadow_probe_count < max(0, int(settings.policy_shadow_min_probe_count)):
        return active, {"mode": "active", "reason": "shadow_probe_floor_not_met", "rollout_bucket": None}
    if last_eval_verdict and not bool(last_eval_verdict.get("passed")):
        return active, {"mode": "active", "reason": "canary_eval_failed", "rollout_bucket": None}
    if float(eval_summary.get("success_rate") or 0.0) and float(eval_summary.get("success_rate") or 0.0) < 0.9:
        return active, {"mode": "active", "reason": "canary_below_success_floor", "rollout_bucket": None}
    if shadow_probe_count > 0 and (
        float(shadow_summary.get("shadow_action_agreement_rate") or 0.0)
        < float(settings.policy_shadow_min_action_agreement_rate)
    ):
        return active, {"mode": "active", "reason": "shadow_action_agreement_too_low", "rollout_bucket": None}
    if risk_level == "high" and bool(settings.policy_canary_allow_high_risk):
        high_risk_probe_count = _safe_int(shadow_counts.get("high_risk_total"))
        if high_risk_probe_count < max(0, int(settings.policy_shadow_min_high_risk_probe_count)):
            return active, {"mode": "active", "reason": "shadow_high_risk_probe_floor_not_met", "rollout_bucket": None}
        if high_risk_probe_count > 0 and (
            float(shadow_summary.get("shadow_high_risk_action_agreement_rate") or 0.0)
            < float(settings.policy_shadow_min_high_risk_action_agreement_rate)
        ):
            return active, {"mode": "active", "reason": "shadow_high_risk_agreement_too_low", "rollout_bucket": None}

    identity = "|".join(
        [
            str(goal_payload.get("goal_id") or ""),
            str(conversation_id or ""),
            str(goal_payload.get("normalized_goal") or ""),
            str(actor_user_id or ""),
        ]
    ).strip("|") or str(candidate.get("version_id") or "")
    bucket = _stable_rollout_bucket(identity)
    rollout_pct = max(0, min(100, int(settings.policy_canary_rollout_pct)))
    if bucket >= rollout_pct:
        return active, {"mode": "active", "reason": "outside_canary_rollout", "rollout_bucket": bucket}

    return candidate, {
        "mode": "canary",
        "reason": "deterministic_rollout",
        "rollout_bucket": bucket,
        "rollout_pct": rollout_pct,
    }


def select_shadow_policy_version(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    selected_version_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not bool(settings.policy_shadow_enabled) or repo is None:
        return None, {"mode": "disabled", "reason": "shadow_disabled"}
    selected_id = str(selected_version_id or "").strip()
    if not selected_id:
        return None, {"mode": "disabled", "reason": "missing_selected_version"}
    selected = repo.get_policy_version(tenant_id=tenant_id, version_id=selected_id)
    if not selected:
        return None, {"mode": "disabled", "reason": "selected_version_not_found"}
    selected_status = str(selected.get("status") or "").upper()
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    candidate = repo.get_candidate_version(tenant_id=tenant_id)
    candidate_status = str((candidate or {}).get("status") or "").upper()
    if selected_status == "CANARY":
        active_id = str(active.get("version_id") or "")
        if active_id and active_id != selected_id:
            return active, {"mode": "active_shadow", "reason": "compare_canary_to_active"}
        return None, {"mode": "disabled", "reason": "missing_active_shadow"}
    if selected_status == "ACTIVE" and candidate is not None and candidate_status == "CANARY":
        candidate_id = str(candidate.get("version_id") or "")
        if candidate_id and candidate_id != selected_id:
            return candidate, {"mode": "canary_shadow", "reason": "compare_active_to_canary"}
    return None, {"mode": "disabled", "reason": "no_shadow_candidate"}


def _shadow_probe_summary(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    counts = _as_dict(comparison_payload.get("shadow_probe_counts"))
    total = _safe_int(counts.get("total"))
    action_divergence = _safe_int(counts.get("action_divergence"))
    route_divergence = _safe_int(counts.get("route_divergence"))
    high_risk_total = _safe_int(counts.get("high_risk_total"))
    high_risk_action_divergence = _safe_int(counts.get("high_risk_action_divergence"))
    if total <= 0:
        return {}
    summary = {
        "shadow_probe_count": total,
        "shadow_action_agreement_rate": round(max(0.0, (total - action_divergence) / total), 3),
        "shadow_route_agreement_rate": round(max(0.0, (total - route_divergence) / total), 3),
    }
    if high_risk_total > 0:
        summary["shadow_high_risk_action_agreement_rate"] = round(
            max(0.0, (high_risk_total - high_risk_action_divergence) / high_risk_total),
            3,
        )
    return summary


def _shadow_outcome_summary(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    counts = _as_dict(comparison_payload.get("shadow_outcome_counts"))
    total = _safe_int(counts.get("total"))
    divergent_total = _safe_int(counts.get("divergent_total"))
    live_success_divergent = _safe_int(counts.get("live_success_divergent"))
    live_failure_divergent = _safe_int(counts.get("live_failure_divergent"))
    if total <= 0:
        return {}
    summary = {
        "shadow_outcome_count": total,
        "shadow_divergence_count": divergent_total,
        "shadow_alignment_rate": round(max(0.0, (total - divergent_total) / total), 3),
    }
    if divergent_total > 0:
        summary["shadow_regret_signal_rate"] = round(max(0.0, live_success_divergent / divergent_total), 3)
        summary["shadow_opportunity_signal_rate"] = round(max(0.0, live_failure_divergent / divergent_total), 3)
    return summary


def _shadow_outcome_guardrail_verdict(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    summary = _shadow_outcome_summary(comparison_payload=comparison_payload)
    outcome_count = _safe_int(summary.get("shadow_outcome_count"))
    min_outcome_count = max(0, int(settings.policy_shadow_min_outcome_count))
    ready = outcome_count >= min_outcome_count if min_outcome_count > 0 else outcome_count > 0
    passed = True
    reasons: list[str] = []
    if (
        ready
        and _safe_float(summary.get("shadow_regret_signal_rate"))
        > float(settings.policy_shadow_max_regret_signal_rate)
    ):
        passed = False
        reasons.append("shadow outcome regret signal stayed above the rollout floor")
    return {
        "ready": ready,
        "passed": passed,
        "reasons": reasons,
        "outcome_count": outcome_count,
    }


def _shadow_portfolio_summary(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    counts = _as_dict(comparison_payload.get("shadow_portfolio_counts"))
    total = _safe_int(counts.get("total"))
    divergent_total = _safe_int(counts.get("divergent_total"))
    selected_divergence = _safe_int(counts.get("selected_divergence"))
    hold_divergence = _safe_int(counts.get("hold_divergence"))
    soft_preempt_divergence = _safe_int(counts.get("soft_preempt_divergence"))
    external_wait_total = _safe_int(counts.get("external_wait_total"))
    external_wait_divergence = _safe_int(counts.get("external_wait_divergence"))
    high_urgency_total = _safe_int(counts.get("high_urgency_total"))
    high_urgency_divergence = _safe_int(counts.get("high_urgency_divergence"))
    if total <= 0:
        return {}
    summary = {
        "shadow_portfolio_probe_count": total,
        "shadow_portfolio_agreement_rate": round(max(0.0, (total - divergent_total) / total), 3),
        "shadow_portfolio_selected_agreement_rate": round(max(0.0, (total - selected_divergence) / total), 3),
        "shadow_portfolio_hold_agreement_rate": round(max(0.0, (total - hold_divergence) / total), 3),
        "shadow_portfolio_soft_preempt_agreement_rate": round(max(0.0, (total - soft_preempt_divergence) / total), 3),
    }
    if external_wait_total > 0:
        summary["shadow_portfolio_external_wait_agreement_rate"] = round(
            max(0.0, (external_wait_total - external_wait_divergence) / external_wait_total),
            3,
        )
    if high_urgency_total > 0:
        summary["shadow_portfolio_high_urgency_agreement_rate"] = round(
            max(0.0, (high_urgency_total - high_urgency_divergence) / high_urgency_total),
            3,
        )
    return summary


def _shadow_portfolio_guardrail_verdict(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    summary = _shadow_portfolio_summary(comparison_payload=comparison_payload)
    probe_count = _safe_int(summary.get("shadow_portfolio_probe_count"))
    ready = probe_count >= max(0, int(settings.policy_shadow_min_portfolio_probe_count))
    passed = True
    reasons: list[str] = []
    if ready and (
        _safe_float(summary.get("shadow_portfolio_agreement_rate"))
        < float(settings.policy_shadow_min_portfolio_agreement_rate)
    ):
        passed = False
        reasons.append("shadow portfolio agreement stayed below the rollout floor")
    if ready and _safe_float(summary.get("shadow_portfolio_high_urgency_agreement_rate")) and (
        _safe_float(summary.get("shadow_portfolio_high_urgency_agreement_rate"))
        < float(settings.policy_shadow_min_portfolio_agreement_rate)
    ):
        passed = False
        reasons.append("shadow portfolio agreement stayed too low for urgent scheduling decisions")
    if ready and _safe_float(summary.get("shadow_portfolio_external_wait_agreement_rate")) and (
        _safe_float(summary.get("shadow_portfolio_external_wait_agreement_rate"))
        < float(settings.policy_shadow_min_portfolio_agreement_rate)
    ):
        passed = False
        reasons.append("shadow portfolio agreement stayed too low for external dependency scheduling")
    return {
        "ready": ready,
        "passed": passed,
        "reasons": reasons,
        "probe_count": probe_count,
    }


def _shadow_portfolio_outcome_summary(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    counts = _as_dict(comparison_payload.get("shadow_portfolio_outcome_counts"))
    total = _safe_int(counts.get("total"))
    divergent_total = _safe_int(counts.get("divergent_total"))
    live_success_divergent = _safe_int(counts.get("live_success_divergent"))
    live_failure_divergent = _safe_int(counts.get("live_failure_divergent"))
    external_wait_total = _safe_int(counts.get("external_wait_total"))
    external_wait_success_divergent = _safe_int(counts.get("external_wait_success_divergent"))
    if total <= 0:
        return {}
    summary = {
        "shadow_portfolio_outcome_count": total,
        "shadow_portfolio_divergence_count": divergent_total,
        "shadow_portfolio_alignment_rate": round(max(0.0, (total - divergent_total) / total), 3),
    }
    if divergent_total > 0:
        summary["shadow_portfolio_regret_signal_rate"] = round(
            max(0.0, live_success_divergent / divergent_total),
            3,
        )
        summary["shadow_portfolio_opportunity_signal_rate"] = round(
            max(0.0, live_failure_divergent / divergent_total),
            3,
        )
    if external_wait_total > 0:
        summary["shadow_portfolio_external_wait_regret_signal_rate"] = round(
            max(0.0, external_wait_success_divergent / external_wait_total),
            3,
        )
    return summary


def _shadow_portfolio_outcome_guardrail_verdict(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    summary = _shadow_portfolio_outcome_summary(comparison_payload=comparison_payload)
    outcome_count = _safe_int(summary.get("shadow_portfolio_outcome_count"))
    min_outcome_count = max(0, int(settings.policy_shadow_min_outcome_count))
    ready = outcome_count >= min_outcome_count if min_outcome_count > 0 else outcome_count > 0
    passed = True
    reasons: list[str] = []
    if (
        ready
        and _safe_float(summary.get("shadow_portfolio_regret_signal_rate"))
        > float(settings.policy_shadow_max_regret_signal_rate)
    ):
        passed = False
        reasons.append("shadow portfolio regret signal stayed above the rollout floor")
    if (
        ready
        and _safe_float(summary.get("shadow_portfolio_external_wait_regret_signal_rate"))
        > float(settings.policy_shadow_max_regret_signal_rate)
    ):
        passed = False
        reasons.append("shadow portfolio external dependency regret stayed above the rollout floor")
    return {
        "ready": ready,
        "passed": passed,
        "reasons": reasons,
        "outcome_count": outcome_count,
    }


def _shadow_guardrail_verdict(*, comparison_payload: dict[str, Any]) -> dict[str, Any]:
    counts = _as_dict(comparison_payload.get("shadow_probe_counts"))
    summary = _shadow_probe_summary(comparison_payload=comparison_payload)
    probe_count = _safe_int(summary.get("shadow_probe_count"))
    high_risk_probe_count = _safe_int(counts.get("high_risk_total"))
    probe_ready = probe_count >= max(0, int(settings.policy_shadow_min_probe_count))
    high_risk_ready = (
        high_risk_probe_count <= 0
        or high_risk_probe_count >= max(0, int(settings.policy_shadow_min_high_risk_probe_count))
    )
    passed = True
    reasons: list[str] = []
    if probe_ready and (
        float(summary.get("shadow_action_agreement_rate") or 0.0)
        < float(settings.policy_shadow_min_action_agreement_rate)
    ):
        passed = False
        reasons.append("shadow action agreement fell below the rollout floor")
    if high_risk_probe_count > 0 and high_risk_ready and (
        float(summary.get("shadow_high_risk_action_agreement_rate") or 0.0)
        < float(settings.policy_shadow_min_high_risk_action_agreement_rate)
    ):
        passed = False
        reasons.append("shadow agreement fell below the high-risk rollout floor")
    return {
        "ready": probe_ready and high_risk_ready,
        "passed": passed,
        "reasons": reasons,
        "probe_count": probe_count,
        "high_risk_probe_count": high_risk_probe_count,
    }


def record_shadow_policy_probe(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
    probe: dict[str, Any],
) -> dict[str, Any] | None:
    del actor_user_id
    if repo is None:
        return None
    version_id = str(candidate_version_id or "").strip()
    if not version_id:
        return None
    candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)
    if not candidate:
        return None
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    counts = _as_dict(comparison_payload.get("shadow_probe_counts"))
    counts["total"] = _safe_int(counts.get("total")) + 1
    action_diverged = str(probe.get("live_action") or "") != str(probe.get("shadow_action") or "")
    route_diverged = str(probe.get("live_route") or "") != str(probe.get("shadow_route") or "")
    if action_diverged:
        counts["action_divergence"] = _safe_int(counts.get("action_divergence")) + 1
    if route_diverged:
        counts["route_divergence"] = _safe_int(counts.get("route_divergence")) + 1
    if str(probe.get("risk_level") or "").lower() == "high":
        counts["high_risk_total"] = _safe_int(counts.get("high_risk_total")) + 1
        if action_diverged:
            counts["high_risk_action_divergence"] = _safe_int(counts.get("high_risk_action_divergence")) + 1
    comparison_payload["shadow_probe_counts"] = counts
    comparison_payload["shadow_last_probe"] = {
        "live_mode": str(probe.get("live_mode") or ""),
        "live_policy_version_id": str(probe.get("live_policy_version_id") or ""),
        "live_action": str(probe.get("live_action") or ""),
        "live_route": str(probe.get("live_route") or ""),
        "shadow_policy_version_id": str(probe.get("shadow_policy_version_id") or ""),
        "shadow_action": str(probe.get("shadow_action") or ""),
        "shadow_route": str(probe.get("shadow_route") or ""),
        "risk_level": str(probe.get("risk_level") or ""),
        "goal_id": str(probe.get("goal_id") or ""),
        "conversation_id": str(probe.get("conversation_id") or ""),
        "diverged": action_diverged or route_diverged,
    }
    comparison_payload["shadow_eval_summary"] = _shadow_probe_summary(comparison_payload=comparison_payload)
    comparison_payload["shadow_guardrail"] = _shadow_guardrail_verdict(comparison_payload=comparison_payload)
    repo.update_policy_version(
        tenant_id=tenant_id,
        version_id=version_id,
        memory_payload=payload,
        comparison_payload=comparison_payload,
    )
    if (
        bool(settings.policy_shadow_auto_rollback_enabled)
        and str(candidate.get("status") or "").upper() == "CANARY"
        and _repo_supports_eval_loop(repo)
        and not bool(_as_dict(comparison_payload.get("shadow_guardrail")).get("passed"))
        and bool(_as_dict(comparison_payload.get("shadow_guardrail")).get("ready"))
    ):
        repo.mark_policy_version_status(
            tenant_id=tenant_id,
            version_id=version_id,
            status="ROLLED_BACK",
        )
    return repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)


def record_shadow_policy_outcome(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
    outcome: dict[str, Any],
) -> dict[str, Any] | None:
    del actor_user_id
    if repo is None:
        return None
    version_id = str(candidate_version_id or "").strip()
    if not version_id:
        return None
    candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)
    if not candidate:
        return None
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    counts = _as_dict(comparison_payload.get("shadow_outcome_counts"))
    counts["total"] = _safe_int(counts.get("total")) + 1
    divergent = bool(outcome.get("diverged"))
    success = str(outcome.get("outcome_status") or "").upper() == "SUCCEEDED"
    if divergent:
        counts["divergent_total"] = _safe_int(counts.get("divergent_total")) + 1
        if success:
            counts["live_success_divergent"] = _safe_int(counts.get("live_success_divergent")) + 1
        else:
            counts["live_failure_divergent"] = _safe_int(counts.get("live_failure_divergent")) + 1
    else:
        counts["aligned_total"] = _safe_int(counts.get("aligned_total")) + 1
        if success:
            counts["live_success_aligned"] = _safe_int(counts.get("live_success_aligned")) + 1
        else:
            counts["live_failure_aligned"] = _safe_int(counts.get("live_failure_aligned")) + 1
    comparison_payload["shadow_outcome_counts"] = counts
    comparison_payload["shadow_last_outcome"] = {
        "goal_id": str(outcome.get("goal_id") or ""),
        "conversation_id": str(outcome.get("conversation_id") or ""),
        "live_policy_version_id": str(outcome.get("live_policy_version_id") or ""),
        "shadow_policy_version_id": str(outcome.get("shadow_policy_version_id") or ""),
        "live_action": str(outcome.get("live_action") or ""),
        "shadow_action": str(outcome.get("shadow_action") or ""),
        "outcome_status": str(outcome.get("outcome_status") or ""),
        "diverged": divergent,
        "risk_level": str(outcome.get("risk_level") or ""),
    }
    comparison_payload["shadow_outcome_summary"] = _shadow_outcome_summary(comparison_payload=comparison_payload)
    comparison_payload["shadow_outcome_guardrail"] = _shadow_outcome_guardrail_verdict(
        comparison_payload=comparison_payload
    )
    repo.update_policy_version(
        tenant_id=tenant_id,
        version_id=version_id,
        memory_payload=payload,
        comparison_payload=comparison_payload,
    )
    if (
        bool(settings.policy_shadow_auto_rollback_enabled)
        and str(candidate.get("status") or "").upper() == "CANARY"
        and _repo_supports_eval_loop(repo)
        and not bool(_as_dict(comparison_payload.get("shadow_outcome_guardrail")).get("passed"))
        and bool(_as_dict(comparison_payload.get("shadow_outcome_guardrail")).get("ready"))
    ):
        repo.mark_policy_version_status(
            tenant_id=tenant_id,
            version_id=version_id,
            status="ROLLED_BACK",
        )
    return repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)


def record_shadow_portfolio_probe(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
    probe: dict[str, Any],
) -> dict[str, Any] | None:
    del actor_user_id
    if repo is None:
        return None
    version_id = str(candidate_version_id or "").strip()
    if not version_id:
        return None
    candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)
    if not candidate:
        return None
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    counts = _as_dict(comparison_payload.get("shadow_portfolio_counts"))
    counts["total"] = _safe_int(counts.get("total")) + 1

    live_selected = [str(item).strip() for item in _as_list(probe.get("live_selected_goal_ids")) if str(item).strip()]
    shadow_selected = [str(item).strip() for item in _as_list(probe.get("shadow_selected_goal_ids")) if str(item).strip()]
    live_holds = sorted(str(item).strip() for item in _as_list(probe.get("live_hold_goal_ids")) if str(item).strip())
    shadow_holds = sorted(str(item).strip() for item in _as_list(probe.get("shadow_hold_goal_ids")) if str(item).strip())
    live_soft_preempt = sorted(
        str(item).strip() for item in _as_list(probe.get("live_soft_preempt_goal_ids")) if str(item).strip()
    )
    shadow_soft_preempt = sorted(
        str(item).strip() for item in _as_list(probe.get("shadow_soft_preempt_goal_ids")) if str(item).strip()
    )
    live_external_wait_sources = sorted(
        str(item).strip() for item in _as_list(probe.get("live_external_wait_sources")) if str(item).strip()
    )
    shadow_external_wait_sources = sorted(
        str(item).strip() for item in _as_list(probe.get("shadow_external_wait_sources")) if str(item).strip()
    )
    selected_diverged = live_selected != shadow_selected
    hold_diverged = live_holds != shadow_holds
    soft_preempt_diverged = live_soft_preempt != shadow_soft_preempt
    external_wait_diverged = live_external_wait_sources != shadow_external_wait_sources
    divergent = selected_diverged or hold_diverged or soft_preempt_diverged or external_wait_diverged
    if selected_diverged:
        counts["selected_divergence"] = _safe_int(counts.get("selected_divergence")) + 1
    if hold_diverged:
        counts["hold_divergence"] = _safe_int(counts.get("hold_divergence")) + 1
    if soft_preempt_diverged:
        counts["soft_preempt_divergence"] = _safe_int(counts.get("soft_preempt_divergence")) + 1
    if live_external_wait_sources or shadow_external_wait_sources:
        counts["external_wait_total"] = _safe_int(counts.get("external_wait_total")) + 1
        if external_wait_diverged:
            counts["external_wait_divergence"] = _safe_int(counts.get("external_wait_divergence")) + 1
    if divergent:
        counts["divergent_total"] = _safe_int(counts.get("divergent_total")) + 1
    if bool(probe.get("high_urgency")):
        counts["high_urgency_total"] = _safe_int(counts.get("high_urgency_total")) + 1
        if divergent:
            counts["high_urgency_divergence"] = _safe_int(counts.get("high_urgency_divergence")) + 1

    comparison_payload["shadow_portfolio_counts"] = counts
    comparison_payload["shadow_portfolio_last_probe"] = {
        "live_selected_goal_ids": live_selected,
        "shadow_selected_goal_ids": shadow_selected,
        "live_hold_goal_ids": live_holds,
        "shadow_hold_goal_ids": shadow_holds,
        "live_soft_preempt_goal_ids": live_soft_preempt,
        "shadow_soft_preempt_goal_ids": shadow_soft_preempt,
        "live_external_wait_sources": live_external_wait_sources,
        "shadow_external_wait_sources": shadow_external_wait_sources,
        "high_urgency": bool(probe.get("high_urgency")),
        "diverged": divergent,
    }
    comparison_payload["shadow_portfolio_summary"] = _shadow_portfolio_summary(comparison_payload=comparison_payload)
    comparison_payload["shadow_portfolio_guardrail"] = _shadow_portfolio_guardrail_verdict(
        comparison_payload=comparison_payload
    )
    repo.update_policy_version(
        tenant_id=tenant_id,
        version_id=version_id,
        memory_payload=payload,
        comparison_payload=comparison_payload,
    )
    if (
        bool(settings.policy_shadow_auto_rollback_enabled)
        and str(candidate.get("status") or "").upper() == "CANARY"
        and _repo_supports_eval_loop(repo)
        and not bool(_as_dict(comparison_payload.get("shadow_portfolio_guardrail")).get("passed"))
        and bool(_as_dict(comparison_payload.get("shadow_portfolio_guardrail")).get("ready"))
    ):
        repo.mark_policy_version_status(
            tenant_id=tenant_id,
            version_id=version_id,
            status="ROLLED_BACK",
        )
    return repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)


def record_shadow_portfolio_outcome(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
    outcome: dict[str, Any],
) -> dict[str, Any] | None:
    del actor_user_id
    if repo is None:
        return None
    version_id = str(candidate_version_id or "").strip()
    if not version_id:
        return None
    candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)
    if not candidate:
        return None
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    counts = _as_dict(comparison_payload.get("shadow_portfolio_outcome_counts"))
    counts["total"] = _safe_int(counts.get("total")) + 1

    live_goal_id = str(outcome.get("live_goal_id") or outcome.get("goal_id") or "").strip()
    shadow_selected_goal_ids = [
        str(item).strip() for item in _as_list(outcome.get("shadow_selected_goal_ids")) if str(item).strip()
    ]
    live_external_wait_sources = sorted(
        str(item).strip() for item in _as_list(outcome.get("live_external_wait_sources")) if str(item).strip()
    )
    shadow_external_wait_sources = sorted(
        str(item).strip() for item in _as_list(outcome.get("shadow_external_wait_sources")) if str(item).strip()
    )
    divergent = bool(outcome.get("diverged"))
    if not divergent and live_goal_id and shadow_selected_goal_ids:
        divergent = live_goal_id not in shadow_selected_goal_ids
    success = str(outcome.get("outcome_status") or "").upper() == "SUCCEEDED"
    external_wait_diverged = live_external_wait_sources != shadow_external_wait_sources
    if divergent:
        counts["divergent_total"] = _safe_int(counts.get("divergent_total")) + 1
        if success:
            counts["live_success_divergent"] = _safe_int(counts.get("live_success_divergent")) + 1
        else:
            counts["live_failure_divergent"] = _safe_int(counts.get("live_failure_divergent")) + 1
    else:
        counts["aligned_total"] = _safe_int(counts.get("aligned_total")) + 1
        if success:
            counts["live_success_aligned"] = _safe_int(counts.get("live_success_aligned")) + 1
        else:
            counts["live_failure_aligned"] = _safe_int(counts.get("live_failure_aligned")) + 1
    if bool(outcome.get("high_urgency")):
        counts["high_urgency_total"] = _safe_int(counts.get("high_urgency_total")) + 1
        if divergent:
            counts["high_urgency_divergent"] = _safe_int(counts.get("high_urgency_divergent")) + 1
    if live_external_wait_sources or shadow_external_wait_sources:
        counts["external_wait_total"] = _safe_int(counts.get("external_wait_total")) + 1
        if external_wait_diverged:
            counts["external_wait_divergence"] = _safe_int(counts.get("external_wait_divergence")) + 1
            if success:
                counts["external_wait_success_divergent"] = _safe_int(
                    counts.get("external_wait_success_divergent")
                ) + 1
            else:
                counts["external_wait_failure_divergent"] = _safe_int(
                    counts.get("external_wait_failure_divergent")
                ) + 1
    comparison_payload["shadow_portfolio_outcome_counts"] = counts
    comparison_payload["shadow_portfolio_last_outcome"] = {
        "goal_id": str(outcome.get("goal_id") or live_goal_id),
        "conversation_id": str(outcome.get("conversation_id") or ""),
        "live_policy_version_id": str(outcome.get("live_policy_version_id") or ""),
        "shadow_policy_version_id": str(outcome.get("shadow_policy_version_id") or ""),
        "live_goal_id": live_goal_id,
        "shadow_selected_goal_ids": shadow_selected_goal_ids,
        "outcome_status": str(outcome.get("outcome_status") or ""),
        "diverged": divergent,
        "high_urgency": bool(outcome.get("high_urgency")),
        "risk_level": str(outcome.get("risk_level") or ""),
        "live_external_wait_sources": live_external_wait_sources,
        "shadow_external_wait_sources": shadow_external_wait_sources,
    }
    comparison_payload["shadow_portfolio_outcome_summary"] = _shadow_portfolio_outcome_summary(
        comparison_payload=comparison_payload
    )
    comparison_payload["shadow_portfolio_outcome_guardrail"] = _shadow_portfolio_outcome_guardrail_verdict(
        comparison_payload=comparison_payload
    )
    repo.update_policy_version(
        tenant_id=tenant_id,
        version_id=version_id,
        memory_payload=payload,
        comparison_payload=comparison_payload,
    )
    if (
        bool(settings.policy_shadow_auto_rollback_enabled)
        and str(candidate.get("status") or "").upper() == "CANARY"
        and _repo_supports_eval_loop(repo)
        and not bool(_as_dict(comparison_payload.get("shadow_portfolio_outcome_guardrail")).get("passed"))
        and bool(_as_dict(comparison_payload.get("shadow_portfolio_outcome_guardrail")).get("ready"))
    ):
        repo.mark_policy_version_status(
            tenant_id=tenant_id,
            version_id=version_id,
            status="ROLLED_BACK",
        )
    return repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)


def build_runtime_policy_memory(version_row: dict[str, Any] | None) -> dict[str, Any]:
    if not version_row:
        return {
            "version_id": "policy-memory-ephemeral",
            "version_tag": "ephemeral",
            "status": "ACTIVE",
            "source": "ephemeral",
            **_default_memory_payload(),
        }
    defaults = _default_memory_payload()
    payload = {
        **defaults,
        **(deepcopy(_as_dict(version_row.get("memory_payload"))) or {}),
    }
    payload["action_bias"] = {
        **_as_dict(defaults.get("action_bias")),
        **_as_dict(payload.get("action_bias")),
    }
    payload["feedback_counts"] = {
        **_as_dict(defaults.get("feedback_counts")),
        **_as_dict(payload.get("feedback_counts")),
    }
    payload["portfolio_bias"] = {
        **_as_dict(defaults.get("portfolio_bias")),
        **_as_dict(payload.get("portfolio_bias")),
    }
    payload["portfolio_outcomes"] = {
        **_as_dict(defaults.get("portfolio_outcomes")),
        **_as_dict(payload.get("portfolio_outcomes")),
    }
    payload["portfolio_learning"] = {
        **_as_dict(defaults.get("portfolio_learning")),
        **_as_dict(payload.get("portfolio_learning")),
    }
    payload["external_signal_outcomes"] = {
        **_as_dict(defaults.get("external_signal_outcomes")),
        **_as_dict(payload.get("external_signal_outcomes")),
    }
    payload["external_source_reliability"] = {
        **_as_dict(defaults.get("external_source_reliability")),
        **_as_dict(payload.get("external_source_reliability")),
    }
    return {
        "version_id": str(version_row.get("version_id") or ""),
        "version_tag": str(version_row.get("version_tag") or ""),
        "status": str(version_row.get("status") or ""),
        "source": str(version_row.get("source") or ""),
        **payload,
    }


def _candidate_from_feedback(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    active_version: dict[str, Any],
    actor_user_id: str | None,
    source: str,
) -> dict[str, Any]:
    if repo is None:
        return active_version
    candidate = repo.get_candidate_version(tenant_id=tenant_id)
    if candidate:
        return candidate
    return repo.create_policy_version(
        tenant_id=tenant_id,
        version_id=f"policy-{uuid.uuid4().hex[:16]}",
        version_tag=f"candidate-{uuid.uuid4().hex[:8]}",
        status="CANDIDATE",
        base_version_id=str(active_version.get("version_id") or "") or None,
        source=source,
        memory_payload=deepcopy(_as_dict(active_version.get("memory_payload"))) or _default_memory_payload(),
        comparison_payload={"baseline_version_id": str(active_version.get("version_id") or "") or None},
        created_by=actor_user_id,
    )


def record_episode_feedback(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    episode: dict[str, Any],
) -> dict[str, Any]:
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    candidate = _candidate_from_feedback(
        repo=repo,
        tenant_id=tenant_id,
        active_version=active,
        actor_user_id=actor_user_id,
        source="episode_feedback",
    )
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    feedback_counts = _as_dict(payload.get("feedback_counts"))
    feedback_counts["episodes"] = int(feedback_counts.get("episodes") or 0) + 1
    payload["feedback_counts"] = feedback_counts
    observation_index = int(feedback_counts.get("episodes") or 0) + int(feedback_counts.get("portfolio_events") or 0)

    action_bias = _as_dict(payload.get("action_bias"))
    chosen_strategy = str(episode.get("chosen_strategy") or "")
    outcome_status = str(episode.get("outcome_status") or "")
    if chosen_strategy in action_bias:
        action_bias[chosen_strategy] = int(action_bias.get(chosen_strategy) or 0) + (2 if outcome_status == "SUCCEEDED" else 0)
    if outcome_status == "FAILED_RETRYABLE":
        action_bias["workflow_call"] = int(action_bias.get("workflow_call") or 0) + 1
        action_bias["replan"] = int(action_bias.get("replan") or 0) + 1
    if outcome_status in {"FAILED_FINAL", "TIMED_OUT"}:
        action_bias["ask_user"] = int(action_bias.get("ask_user") or 0) + 1
    if any("approval" in str(item).lower() for item in list(episode.get("useful_lessons") or [])):
        action_bias["approval_request"] = int(action_bias.get("approval_request") or 0) + 1
    payload["action_bias"] = action_bias

    tool_failure_counts = _as_dict(payload.get("tool_failure_counts"))
    tool_success_counts = _as_dict(payload.get("tool_success_counts"))
    tool_last_seen = _as_dict(payload.get("tool_last_seen"))
    if outcome_status in {"FAILED_RETRYABLE", "FAILED_FINAL"}:
        for tool_name in list(episode.get("tool_names") or []):
            name = str(tool_name or "").strip()
            if name:
                tool_failure_counts[name] = int(tool_failure_counts.get(name) or 0) + 1
                tool_last_seen[name] = observation_index
    if outcome_status == "SUCCEEDED":
        for tool_name in list(episode.get("tool_names") or []):
            name = str(tool_name or "").strip()
            if name:
                tool_success_counts[name] = int(tool_success_counts.get(name) or 0) + 1
                tool_last_seen[name] = observation_index
    payload["tool_failure_counts"] = tool_failure_counts
    payload["tool_success_counts"] = tool_success_counts
    payload["tool_last_seen"] = tool_last_seen

    critic_patterns = _as_dict(payload.get("critic_patterns"))
    outcome_signal = _as_dict(_as_dict(episode.get("episode_payload")).get("outcome_signal"))
    latest_result = _as_dict(outcome_signal.get("latest_result"))
    result_status = str(latest_result.get("status") or outcome_signal.get("next_action") or outcome_status)
    if result_status:
        critic_patterns[result_status] = int(critic_patterns.get(result_status) or 0) + 1
    payload["critic_patterns"] = critic_patterns

    lessons = [str(item).strip() for item in list(episode.get("useful_lessons") or []) if str(item).strip()]
    portfolio_bias = _as_dict(payload.get("portfolio_bias"))
    if outcome_status == "FAILED_RETRYABLE":
        portfolio_bias["dynamic_subgoal_boost"] = int(portfolio_bias.get("dynamic_subgoal_boost") or 0) + 1
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
    if outcome_status in {"FAILED_FINAL", "TIMED_OUT"}:
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
    if outcome_status == "SUCCEEDED" and chosen_strategy == "workflow_call":
        portfolio_bias["stalled_goal_boost"] = max(0, int(portfolio_bias.get("stalled_goal_boost") or 0) - 1)
        portfolio_bias["continuation_penalty"] = max(0, int(portfolio_bias.get("continuation_penalty") or 0) - 1)
    if any("follow-up" in lesson.lower() or "stalled" in lesson.lower() for lesson in lessons):
        portfolio_bias["stalled_goal_boost"] = int(portfolio_bias.get("stalled_goal_boost") or 0) + 1
    payload["portfolio_bias"] = portfolio_bias

    existing_lessons = [str(item).strip() for item in list(payload.get("lesson_hints") or []) if str(item).strip()]
    merged_lessons: list[str] = []
    for lesson in existing_lessons + lessons:
        if lesson and lesson not in merged_lessons:
            merged_lessons.append(lesson)
    payload["lesson_hints"] = merged_lessons[-20:]
    payload = _update_lesson_catalog(payload=payload, lessons=lessons, source="episode_feedback")

    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    comparison_payload["last_feedback"] = {
        "kind": "episode",
        "chosen_strategy": chosen_strategy,
        "outcome_status": outcome_status,
    }
    payload = _apply_memory_hygiene(payload)
    payload, comparison_payload = _refresh_candidate_eval_summary(
        payload=payload,
        comparison_payload=comparison_payload,
    )

    if repo is not None:
        repo.update_policy_version(
            tenant_id=tenant_id,
            version_id=str(candidate.get("version_id") or ""),
            memory_payload=payload,
            comparison_payload=comparison_payload,
        )
        candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or "")) or {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    else:
        candidate = {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    auto_eval_result = maybe_auto_evaluate_candidate_policy(
        repo=repo,
        tenant_id=tenant_id,
        actor_user_id=actor_user_id,
        candidate_version_id=str(candidate.get("version_id") or ""),
    )
    if repo is not None:
        refreshed = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or ""))
        if refreshed:
            candidate = refreshed
    if auto_eval_result is not None and isinstance(candidate, dict):
        candidate = {**candidate, "auto_eval_result": auto_eval_result}
    return candidate


def record_portfolio_feedback(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    feedback: dict[str, Any],
) -> dict[str, Any]:
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    candidate = _candidate_from_feedback(
        repo=repo,
        tenant_id=tenant_id,
        active_version=active,
        actor_user_id=actor_user_id,
        source="portfolio_feedback",
    )
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    feedback_counts = _as_dict(payload.get("feedback_counts"))
    feedback_counts["episodes"] = int(feedback_counts.get("episodes") or 0)
    feedback_counts["portfolio_events"] = int(feedback_counts.get("portfolio_events") or 0) + 1
    payload["feedback_counts"] = feedback_counts

    portfolio_bias = _as_dict(payload.get("portfolio_bias"))
    portfolio_outcomes = _as_dict(payload.get("portfolio_outcomes"))
    event_kind = str(feedback.get("event_kind") or "").strip().lower()
    urgency = max(0.0, min(1.0, float(feedback.get("urgency_score") or 0.0)))
    if event_kind == "soft_preempt":
        portfolio_bias["dynamic_subgoal_boost"] = int(portfolio_bias.get("dynamic_subgoal_boost") or 0) + 1
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
        if urgency >= 0.85:
            portfolio_bias["stalled_goal_boost"] = int(portfolio_bias.get("stalled_goal_boost") or 0) + 1
        portfolio_outcomes["hold_events"] = int(portfolio_outcomes.get("hold_events") or 0) + 1
    elif event_kind == "hold":
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
        portfolio_outcomes["hold_events"] = int(portfolio_outcomes.get("hold_events") or 0) + 1
    elif event_kind == "preempt_cancel":
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 2
        portfolio_bias["stalled_goal_boost"] = int(portfolio_bias.get("stalled_goal_boost") or 0) + 1
        portfolio_outcomes["preempt_cancel_events"] = int(portfolio_outcomes.get("preempt_cancel_events") or 0) + 1
    elif event_kind == "preempt_resume_success":
        portfolio_bias["dynamic_subgoal_boost"] = max(0, int(portfolio_bias.get("dynamic_subgoal_boost") or 0) - 1)
        portfolio_bias["stalled_goal_boost"] = max(0, int(portfolio_bias.get("stalled_goal_boost") or 0) - 1)
        portfolio_outcomes["preempt_resume_success"] = int(portfolio_outcomes.get("preempt_resume_success") or 0) + 1
    elif event_kind == "preempt_resume_regret":
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
        portfolio_outcomes["preempt_resume_regret"] = int(portfolio_outcomes.get("preempt_resume_regret") or 0) + 1
    elif event_kind == "subscription_timeout":
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
        portfolio_outcomes["subscription_timeout_events"] = int(
            portfolio_outcomes.get("subscription_timeout_events") or 0
        ) + 1
    elif event_kind == "external_wait_success":
        portfolio_bias["continuation_penalty"] = max(0, int(portfolio_bias.get("continuation_penalty") or 0) - 1)
        portfolio_bias["stalled_goal_boost"] = max(0, int(portfolio_bias.get("stalled_goal_boost") or 0) - 1)
        portfolio_outcomes["external_wait_success_events"] = int(
            portfolio_outcomes.get("external_wait_success_events") or 0
        ) + 1
    elif event_kind == "external_wait_failure":
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
        portfolio_bias["stalled_goal_boost"] = int(portfolio_bias.get("stalled_goal_boost") or 0) + 1
        portfolio_outcomes["external_wait_failure_events"] = int(
            portfolio_outcomes.get("external_wait_failure_events") or 0
        ) + 1
    elif event_kind == "goal_starved":
        portfolio_bias["stalled_goal_boost"] = int(portfolio_bias.get("stalled_goal_boost") or 0) + 1
        portfolio_outcomes["goal_starvation_events"] = int(portfolio_outcomes.get("goal_starvation_events") or 0) + 1
    payload["portfolio_bias"] = portfolio_bias
    payload["portfolio_outcomes"] = portfolio_outcomes

    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    comparison_payload["last_feedback"] = {
        "kind": "portfolio",
        "event_kind": event_kind,
        "goal_id": str(feedback.get("goal_id") or ""),
        "held_goal_id": str(feedback.get("held_goal_id") or ""),
    }
    payload = _apply_memory_hygiene(payload)
    payload, comparison_payload = _refresh_candidate_eval_summary(
        payload=payload,
        comparison_payload=comparison_payload,
    )

    if repo is not None:
        repo.update_policy_version(
            tenant_id=tenant_id,
            version_id=str(candidate.get("version_id") or ""),
            memory_payload=payload,
            comparison_payload=comparison_payload,
        )
        candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or "")) or {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    else:
        candidate = {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    auto_eval_result = maybe_auto_evaluate_candidate_policy(
        repo=repo,
        tenant_id=tenant_id,
        actor_user_id=actor_user_id,
        candidate_version_id=str(candidate.get("version_id") or ""),
    )
    if repo is not None:
        refreshed = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or ""))
        if refreshed:
            candidate = refreshed
    if auto_eval_result is not None and isinstance(candidate, dict):
        candidate = {**candidate, "auto_eval_result": auto_eval_result}
    return candidate


def record_external_signal_feedback(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    feedback: dict[str, Any],
) -> dict[str, Any]:
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    candidate = _candidate_from_feedback(
        repo=repo,
        tenant_id=tenant_id,
        active_version=active,
        actor_user_id=actor_user_id,
        source="external_signal_feedback",
    )
    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    feedback_counts = _as_dict(payload.get("feedback_counts"))
    feedback_counts["episodes"] = int(feedback_counts.get("episodes") or 0)
    feedback_counts["portfolio_events"] = int(feedback_counts.get("portfolio_events") or 0)
    feedback_counts["external_signals"] = int(feedback_counts.get("external_signals") or 0) + 1
    payload["feedback_counts"] = feedback_counts
    observation_index = (
        int(feedback_counts.get("episodes") or 0)
        + int(feedback_counts.get("portfolio_events") or 0)
        + int(feedback_counts.get("external_signals") or 0)
    )

    outcome = str(feedback.get("adapter_outcome") or "update").strip().lower() or "update"
    source = str(feedback.get("source") or "external_signal").strip() or "external_signal"
    event_topic = str(feedback.get("event_topic") or "").strip()
    source_keys = _external_source_feedback_keys(source=source, event_topic=event_topic)

    external_outcomes = _as_dict(payload.get("external_signal_outcomes"))
    external_outcomes[outcome] = int(external_outcomes.get(outcome) or 0) + 1
    payload["external_signal_outcomes"] = external_outcomes

    external_success = _as_dict(payload.get("external_source_success_counts"))
    external_failure = _as_dict(payload.get("external_source_failure_counts"))
    external_last_seen = _as_dict(payload.get("external_source_last_seen"))
    if outcome in {"success", "update"}:
        for source_key in source_keys:
            external_success[source_key] = int(external_success.get(source_key) or 0) + 1
            external_last_seen[source_key] = observation_index
    elif outcome in {"failure", "timeout"}:
        for source_key in source_keys:
            external_failure[source_key] = int(external_failure.get(source_key) or 0) + 1
            external_last_seen[source_key] = observation_index
    elif outcome == "progress":
        for source_key in source_keys:
            external_last_seen[source_key] = observation_index
    payload["external_source_success_counts"] = external_success
    payload["external_source_failure_counts"] = external_failure
    payload["external_source_last_seen"] = external_last_seen

    portfolio_bias = _as_dict(payload.get("portfolio_bias"))
    if outcome in {"failure", "timeout"}:
        portfolio_bias["replan_goal_boost"] = int(portfolio_bias.get("replan_goal_boost") or 0) + 1
        portfolio_bias["continuation_penalty"] = int(portfolio_bias.get("continuation_penalty") or 0) + 1
    elif outcome == "success":
        portfolio_bias["continuation_penalty"] = max(0, int(portfolio_bias.get("continuation_penalty") or 0) - 1)
    payload["portfolio_bias"] = portfolio_bias

    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    comparison_payload["last_feedback"] = {
        "kind": "external_signal",
        "source": source,
        "event_topic": event_topic,
        "adapter_outcome": outcome,
        "matched_goal_count": int(feedback.get("matched_goal_count") or 0),
        "requires_replan": bool(feedback.get("requires_replan")),
    }
    payload = _apply_memory_hygiene(payload)
    payload, comparison_payload = _refresh_candidate_eval_summary(
        payload=payload,
        comparison_payload=comparison_payload,
    )

    if repo is not None:
        repo.update_policy_version(
            tenant_id=tenant_id,
            version_id=str(candidate.get("version_id") or ""),
            memory_payload=payload,
            comparison_payload=comparison_payload,
        )
        candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or "")) or {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    else:
        candidate = {
            **candidate,
            "memory_payload": payload,
            "comparison_payload": comparison_payload,
        }
    auto_eval_result = maybe_auto_evaluate_candidate_policy(
        repo=repo,
        tenant_id=tenant_id,
        actor_user_id=actor_user_id,
        candidate_version_id=str(candidate.get("version_id") or ""),
    )
    if repo is not None:
        refreshed = repo.get_policy_version(tenant_id=tenant_id, version_id=str(candidate.get("version_id") or ""))
        if refreshed:
            candidate = refreshed
    if auto_eval_result is not None and isinstance(candidate, dict):
        candidate = {**candidate, "auto_eval_result": auto_eval_result}
    return candidate


def compare_eval_summaries(
    *,
    active_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
    min_success_rate: float = 0.9,
) -> dict[str, Any]:
    active_success = float(active_summary.get("success_rate") or 0.0)
    candidate_success = float(candidate_summary.get("success_rate") or 0.0)
    active_trace = float(active_summary.get("trace_coverage") or 0.0)
    candidate_trace = float(candidate_summary.get("trace_coverage") or 0.0)
    active_prompt_leaks = int(active_summary.get("prompt_leak_count") or 0)
    candidate_prompt_leaks = int(candidate_summary.get("prompt_leak_count") or 0)
    active_unauthorized = int(active_summary.get("unauthorized_tool_calls") or 0)
    candidate_unauthorized = int(candidate_summary.get("unauthorized_tool_calls") or 0)
    active_mismatches = int(active_summary.get("status_mismatch_count") or 0)
    candidate_mismatches = int(candidate_summary.get("status_mismatch_count") or 0)
    active_portfolio_completion = float(active_summary.get("portfolio_goal_completion_rate") or 0.0)
    candidate_portfolio_completion = float(candidate_summary.get("portfolio_goal_completion_rate") or 0.0)
    active_preempt_recovery = float(active_summary.get("preempt_recovery_success_rate") or 0.0)
    candidate_preempt_recovery = float(candidate_summary.get("preempt_recovery_success_rate") or 0.0)
    active_preempt_regret = float(active_summary.get("preempt_regret_rate") or 0.0)
    candidate_preempt_regret = float(candidate_summary.get("preempt_regret_rate") or 0.0)
    active_agenda_stability = float(active_summary.get("agenda_stability") or 0.0)
    candidate_agenda_stability = float(candidate_summary.get("agenda_stability") or 0.0)
    active_starvation_rate = float(active_summary.get("portfolio_starvation_rate") or 0.0)
    candidate_starvation_rate = float(candidate_summary.get("portfolio_starvation_rate") or 0.0)
    active_subscription_timeout_rate = float(active_summary.get("portfolio_subscription_timeout_rate") or 0.0)
    candidate_subscription_timeout_rate = float(candidate_summary.get("portfolio_subscription_timeout_rate") or 0.0)
    active_throughput_score = float(active_summary.get("portfolio_throughput_score") or 0.0)
    candidate_throughput_score = float(candidate_summary.get("portfolio_throughput_score") or 0.0)
    candidate_shadow_probe_count = int(candidate_summary.get("shadow_probe_count") or 0)
    candidate_shadow_action_agreement = float(candidate_summary.get("shadow_action_agreement_rate") or 0.0)
    candidate_shadow_high_risk_action_agreement = float(
        candidate_summary.get("shadow_high_risk_action_agreement_rate") or 0.0
    )
    candidate_shadow_regret_signal_rate = float(candidate_summary.get("shadow_regret_signal_rate") or 0.0)
    candidate_shadow_portfolio_probe_count = int(candidate_summary.get("shadow_portfolio_probe_count") or 0)
    candidate_shadow_portfolio_agreement = float(candidate_summary.get("shadow_portfolio_agreement_rate") or 0.0)
    candidate_shadow_portfolio_high_urgency_agreement = float(
        candidate_summary.get("shadow_portfolio_high_urgency_agreement_rate") or 0.0
    )
    candidate_shadow_portfolio_outcome_count = int(candidate_summary.get("shadow_portfolio_outcome_count") or 0)
    candidate_shadow_portfolio_regret_signal_rate = float(
        candidate_summary.get("shadow_portfolio_regret_signal_rate") or 0.0
    )

    passed = True
    reasons: list[str] = []
    if candidate_success < max(min_success_rate, active_success):
        passed = False
        reasons.append("candidate success rate regressed below the required threshold")
    if candidate_trace < max(0.8, active_trace - 0.02):
        passed = False
        reasons.append("candidate trace coverage regressed")
    if candidate_prompt_leaks > active_prompt_leaks:
        passed = False
        reasons.append("candidate increased prompt leak count")
    if candidate_unauthorized > active_unauthorized:
        passed = False
        reasons.append("candidate increased unauthorized tool calls")
    if candidate_mismatches > active_mismatches:
        passed = False
        reasons.append("candidate increased status mismatches")
    if candidate_portfolio_completion and candidate_portfolio_completion < max(0.6, active_portfolio_completion - 0.03):
        passed = False
        reasons.append("candidate regressed portfolio goal completion rate")
    if candidate_preempt_recovery and candidate_preempt_recovery < max(0.55, active_preempt_recovery - 0.05):
        passed = False
        reasons.append("candidate regressed preemption recovery success rate")
    if candidate_preempt_regret > max(active_preempt_regret, 0.15):
        passed = False
        reasons.append("candidate increased preemption regret rate")
    if candidate_agenda_stability and candidate_agenda_stability < max(0.5, active_agenda_stability - 0.08):
        passed = False
        reasons.append("candidate regressed agenda stability")
    if candidate_starvation_rate > max(0.2, active_starvation_rate + 0.05):
        passed = False
        reasons.append("candidate increased portfolio starvation rate")
    if candidate_subscription_timeout_rate > max(0.2, active_subscription_timeout_rate + 0.05):
        passed = False
        reasons.append("candidate increased subscription timeout rate")
    if candidate_throughput_score and candidate_throughput_score < max(0.35, active_throughput_score - 0.06):
        passed = False
        reasons.append("candidate regressed portfolio throughput score")
    if bool(settings.policy_shadow_enabled):
        if candidate_shadow_probe_count < max(0, int(settings.policy_shadow_min_probe_count)):
            passed = False
            reasons.append("candidate lacks enough shadow probes before promotion")
        if candidate_shadow_probe_count > 0 and (
            candidate_shadow_action_agreement < float(settings.policy_shadow_min_action_agreement_rate)
        ):
            passed = False
            reasons.append("candidate shadow action agreement is below the rollout floor")
        if candidate_shadow_high_risk_action_agreement and (
            candidate_shadow_high_risk_action_agreement
            < float(settings.policy_shadow_min_high_risk_action_agreement_rate)
        ):
            passed = False
            reasons.append("candidate shadow agreement is too low for high-risk goals")
        if candidate_shadow_regret_signal_rate > float(settings.policy_shadow_max_regret_signal_rate):
            passed = False
            reasons.append("candidate shadow regret signal rate is too high")
        portfolio_shadow_relevant = (
            candidate_shadow_portfolio_probe_count > 0
            or candidate_portfolio_completion > 0.0
            or candidate_preempt_recovery > 0.0
            or candidate_preempt_regret > 0.0
        )
        if portfolio_shadow_relevant and (
            candidate_shadow_portfolio_probe_count < max(0, int(settings.policy_shadow_min_portfolio_probe_count))
        ):
            passed = False
            reasons.append("candidate lacks enough shadow portfolio probes before promotion")
        if candidate_shadow_portfolio_probe_count > 0 and (
            candidate_shadow_portfolio_agreement < float(settings.policy_shadow_min_portfolio_agreement_rate)
        ):
            passed = False
            reasons.append("candidate shadow portfolio agreement is below the rollout floor")
        if candidate_shadow_portfolio_high_urgency_agreement and (
            candidate_shadow_portfolio_high_urgency_agreement
            < float(settings.policy_shadow_min_portfolio_agreement_rate)
        ):
            passed = False
            reasons.append("candidate shadow portfolio agreement is too low for urgent scheduling decisions")
        if portfolio_shadow_relevant and (
            candidate_shadow_portfolio_outcome_count < max(0, int(settings.policy_shadow_min_outcome_count))
        ):
            passed = False
            reasons.append("candidate lacks enough shadow portfolio outcomes before promotion")
        if candidate_shadow_portfolio_regret_signal_rate > float(settings.policy_shadow_max_regret_signal_rate):
            passed = False
            reasons.append("candidate shadow portfolio regret signal rate is too high")
    if passed:
        reasons.append("candidate policy met or exceeded the active policy on core safety and success metrics")

    return {
        "passed": passed,
        "reasons": reasons,
        "metrics": {
            "active_success_rate": active_success,
            "candidate_success_rate": candidate_success,
            "active_trace_coverage": active_trace,
            "candidate_trace_coverage": candidate_trace,
            "active_prompt_leak_count": active_prompt_leaks,
            "candidate_prompt_leak_count": candidate_prompt_leaks,
            "active_unauthorized_tool_calls": active_unauthorized,
            "candidate_unauthorized_tool_calls": candidate_unauthorized,
            "active_status_mismatches": active_mismatches,
            "candidate_status_mismatches": candidate_mismatches,
            "active_portfolio_goal_completion_rate": active_portfolio_completion,
            "candidate_portfolio_goal_completion_rate": candidate_portfolio_completion,
            "active_preempt_recovery_success_rate": active_preempt_recovery,
            "candidate_preempt_recovery_success_rate": candidate_preempt_recovery,
            "active_preempt_regret_rate": active_preempt_regret,
            "candidate_preempt_regret_rate": candidate_preempt_regret,
            "active_agenda_stability": active_agenda_stability,
            "candidate_agenda_stability": candidate_agenda_stability,
            "candidate_shadow_probe_count": candidate_shadow_probe_count,
            "candidate_shadow_action_agreement_rate": candidate_shadow_action_agreement,
            "candidate_shadow_high_risk_action_agreement_rate": candidate_shadow_high_risk_action_agreement,
            "candidate_shadow_regret_signal_rate": candidate_shadow_regret_signal_rate,
            "candidate_shadow_portfolio_probe_count": candidate_shadow_portfolio_probe_count,
            "candidate_shadow_portfolio_agreement_rate": candidate_shadow_portfolio_agreement,
            "candidate_shadow_portfolio_high_urgency_agreement_rate": candidate_shadow_portfolio_high_urgency_agreement,
            "candidate_shadow_portfolio_outcome_count": candidate_shadow_portfolio_outcome_count,
            "candidate_shadow_portfolio_regret_signal_rate": candidate_shadow_portfolio_regret_signal_rate,
        },
    }


def record_policy_eval(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    actor_user_id: str | None,
    candidate_version_id: str | None,
    summary: dict[str, Any],
    auto_promote: bool = True,
) -> dict[str, Any]:
    active = ensure_active_policy_version(repo=repo, tenant_id=tenant_id, actor_user_id=actor_user_id)
    candidate = None
    if repo is not None and candidate_version_id:
        candidate = repo.get_policy_version(tenant_id=tenant_id, version_id=candidate_version_id)
    if candidate is None:
        candidate = _candidate_from_feedback(
            repo=repo,
            tenant_id=tenant_id,
            active_version=active,
            actor_user_id=actor_user_id,
            source="eval_feedback",
        )

    payload = deepcopy(_as_dict(candidate.get("memory_payload"))) or _default_memory_payload()
    comparison_payload = deepcopy(_as_dict(candidate.get("comparison_payload")))
    merged_summary = derive_policy_eval_summary(
        memory_payload=payload,
        base_summary=summary,
    )
    merged_summary.update({key: value for key, value in _shadow_probe_summary(comparison_payload=comparison_payload).items() if key not in merged_summary})
    merged_summary.update({key: value for key, value in _shadow_outcome_summary(comparison_payload=comparison_payload).items() if key not in merged_summary})
    merged_summary.update({key: value for key, value in _shadow_portfolio_summary(comparison_payload=comparison_payload).items() if key not in merged_summary})
    verdict = compare_eval_summaries(
        active_summary=_as_dict(_as_dict(active.get("memory_payload")).get("eval_summary")),
        candidate_summary=merged_summary,
    )

    feedback_counts = _as_dict(payload.get("feedback_counts"))
    feedback_counts["eval_runs"] = int(feedback_counts.get("eval_runs") or 0) + 1
    payload["feedback_counts"] = feedback_counts
    payload = _apply_memory_hygiene(payload)
    merged_summary.update(
        {
            key: value
            for key, value in derive_policy_eval_summary(
                memory_payload=payload,
                base_summary=merged_summary,
            ).items()
            if key in {"memory_confidence", "policy_lesson_count", "policy_tool_memory_count", "feedback_eval_run_count"}
        }
    )
    payload["eval_summary"] = dict(merged_summary)

    comparison_payload["last_eval_verdict"] = verdict
    comparison_payload["last_eval_summary"] = dict(merged_summary)
    comparison_payload["rollback_target_version_id"] = str(active.get("version_id") or "")

    eval_run = {
        "eval_run_id": f"eval-{uuid.uuid4().hex[:16]}",
        "candidate_version_id": str(candidate.get("version_id") or ""),
        "baseline_version_id": str(active.get("version_id") or ""),
        "summary": dict(merged_summary),
        "verdict": verdict,
    }
    candidate_status = str(candidate.get("status") or "CANDIDATE")
    prior_status = candidate_status.upper()
    comparison_payload["last_eval_disposition"] = (
        "promoted"
        if verdict["passed"] and auto_promote
        else (
            "canary"
            if verdict["passed"] and bool(settings.policy_eval_canary_on_pass_without_promote)
            else (
                "rolled_back"
                if prior_status == "CANARY" and bool(settings.policy_canary_auto_rollback_on_failure)
                else "rejected"
            )
        )
    )

    if repo is not None:
        repo.update_policy_version(
            tenant_id=tenant_id,
            version_id=str(candidate.get("version_id") or ""),
            memory_payload=payload,
            comparison_payload=comparison_payload,
        )
        repo.create_eval_run(
            tenant_id=tenant_id,
            eval_run_id=eval_run["eval_run_id"],
            candidate_version_id=eval_run["candidate_version_id"],
            baseline_version_id=eval_run["baseline_version_id"],
            summary=eval_run["summary"],
            verdict=eval_run["verdict"],
            created_by=actor_user_id,
        )
        if verdict["passed"] and auto_promote:
            repo.activate_policy_version(
                tenant_id=tenant_id,
                version_id=str(candidate.get("version_id") or ""),
                actor_user_id=actor_user_id,
            )
            candidate_status = "ACTIVE"
        elif verdict["passed"] and bool(settings.policy_eval_canary_on_pass_without_promote):
            repo.mark_policy_version_status(
                tenant_id=tenant_id,
                version_id=str(candidate.get("version_id") or ""),
                status="CANARY",
            )
            candidate_status = "CANARY"
        elif not verdict["passed"]:
            failed_status = (
                "ROLLED_BACK"
                if prior_status == "CANARY" and bool(settings.policy_canary_auto_rollback_on_failure)
                else "REJECTED"
            )
            repo.mark_policy_version_status(
                tenant_id=tenant_id,
                version_id=str(candidate.get("version_id") or ""),
                status=failed_status,
            )
            candidate_status = failed_status
    elif verdict["passed"] and auto_promote:
        candidate_status = "ACTIVE"
    elif verdict["passed"] and bool(settings.policy_eval_canary_on_pass_without_promote):
        candidate_status = "CANARY"
    elif not verdict["passed"]:
        candidate_status = (
            "ROLLED_BACK"
            if prior_status == "CANARY" and bool(settings.policy_canary_auto_rollback_on_failure)
            else "REJECTED"
        )

    return {
        "active_version_id": str(active.get("version_id") or ""),
        "candidate_version_id": str(candidate.get("version_id") or ""),
        "eval_run_id": eval_run["eval_run_id"],
        "verdict": verdict,
        "promoted": bool(verdict["passed"] and auto_promote),
        "candidate_status": candidate_status,
        "rollback_target_version_id": str(active.get("version_id") or ""),
    }


def rollback_policy_version(
    *,
    repo: PolicyMemoryRepository | None,
    tenant_id: str,
    version_id: str,
    actor_user_id: str | None,
) -> dict[str, Any]:
    if repo is None:
        return {"rolled_back_to": version_id, "status": "ephemeral"}
    repo.activate_policy_version(
        tenant_id=tenant_id,
        version_id=version_id,
        actor_user_id=actor_user_id,
        rollback=True,
    )
    restored = repo.get_policy_version(tenant_id=tenant_id, version_id=version_id)
    return {
        "rolled_back_to": version_id,
        "status": str((restored or {}).get("status") or "ACTIVE"),
    }
