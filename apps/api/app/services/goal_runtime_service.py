from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import re
from typing import Any
import uuid

from ..config import settings
from ..repositories import GoalRepository


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _normalized_entity_refs(value: Any) -> list[str]:
    refs: list[str] = []
    for raw_item in _as_list(value):
        if isinstance(raw_item, dict):
            ref_value = str(raw_item.get("id") or raw_item.get("value") or "").strip()
        else:
            ref_value = str(raw_item or "").strip()
        if ref_value and ref_value not in refs:
            refs.append(ref_value)
    return refs


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalized_memory_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _external_source_reliability_lookup(
    policy_memory: dict[str, Any],
    *,
    source: str,
    event_topic: str,
) -> dict[str, Any]:
    reliability = _as_dict(policy_memory.get("external_source_reliability"))
    normalized_source = _normalized_memory_key(source) or "external_signal"
    normalized_topic = _normalized_memory_key(event_topic)
    candidate_keys: list[str] = []
    if normalized_topic:
        candidate_keys.append(f"{normalized_source}:topic:{normalized_topic}")
    candidate_keys.append(normalized_source)
    for candidate_key in candidate_keys:
        record = _as_dict(reliability.get(candidate_key))
        if record:
            return {
                "key": candidate_key,
                "score": _safe_float(record.get("score")),
                "confidence": _safe_float(record.get("confidence")),
                "evidence": int(record.get("evidence") or 0),
            }
    return {}


def _external_source_wait_strategy(
    runtime_state: dict[str, Any],
    *,
    source: str,
    event_topic: str,
) -> dict[str, Any]:
    policy = _as_dict(runtime_state.get("policy"))
    policy_memory = _as_dict(policy.get("policy_memory"))
    reliability = _external_source_reliability_lookup(policy_memory, source=source, event_topic=event_topic)
    score = _safe_float(reliability.get("score"))
    confidence = max(0.0, min(1.0, _safe_float(reliability.get("confidence"))))
    timeout_multiplier = 1.0
    tier = "neutral"
    if confidence >= float(settings.goal_external_source_confidence_floor):
        if score <= float(settings.goal_external_source_low_reliability_score):
            timeout_multiplier = max(0.1, float(settings.goal_external_source_low_timeout_multiplier))
            tier = "low_reliability"
        elif score >= float(settings.goal_external_source_high_reliability_score):
            timeout_multiplier = max(0.1, float(settings.goal_external_source_high_timeout_multiplier))
            tier = "high_reliability"
    return {
        "key": str(reliability.get("key") or ""),
        "score": round(max(-1.0, min(1.0, score)), 3),
        "confidence": round(confidence, 3),
        "evidence": int(reliability.get("evidence") or 0),
        "timeout_multiplier": round(timeout_multiplier, 3),
        "tier": tier,
    }


def _latest_result(runtime_state: dict[str, Any]) -> dict[str, Any]:
    task_state = _as_dict(runtime_state.get("task_state"))
    latest_result = _as_dict(task_state.get("latest_result"))
    if latest_result:
        return latest_result
    return _as_dict(runtime_state.get("latest_result"))


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _portfolio_state(runtime_state: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(runtime_state.get("portfolio"))


def _portfolio_hold_active(portfolio: dict[str, Any]) -> bool:
    if str(portfolio.get("hold_status") or "") not in {"HELD", "PREEMPTING"}:
        return False
    hold_until = _parse_datetime(portfolio.get("hold_until"))
    if hold_until is None:
        return True
    return hold_until > datetime.now(timezone.utc)


def _portfolio_preempted_for_task(portfolio: dict[str, Any], task_id: str | None) -> bool:
    if not _portfolio_hold_active(portfolio):
        return False
    expected_task_id = str(portfolio.get("preempted_task_id") or "").strip()
    if not expected_task_id:
        return False
    return expected_task_id == str(task_id or "").strip()


EVENT_DRIVEN_KINDS = {"user_message", "approval", "task_completion", "external_signal"}
FAILURE_OUTCOMES = {"failure", "timeout"}
DEFAULT_EXTERNAL_SUBSCRIPTION_OUTCOMES = ("success", "failure", "timeout")


def _slug_text(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return lowered.strip("-")[:40] or "item"


def _event_topic_value(item: dict[str, Any]) -> str:
    payload = _as_dict(item.get("payload"))
    return str(
        item.get("event_topic")
        or payload.get("event_topic")
        or payload.get("event_type")
        or payload.get("status")
        or payload.get("state")
        or ""
    ).strip()


def _expected_event_outcomes(value: Any, *, kind: str) -> list[str]:
    if kind != "external_signal":
        return []
    values: list[str] = []
    raw_items = value if isinstance(value, list) else ([value] if value not in (None, "") else [])
    for raw_item in raw_items:
        token = re.sub(r"[^a-z0-9]+", "_", str(raw_item or "").strip().lower()).strip("_")
        if token and token not in values:
            values.append(token)
    if values:
        return values
    return list(DEFAULT_EXTERNAL_SUBSCRIPTION_OUTCOMES)


def _event_entity_refs(item: dict[str, Any]) -> list[str]:
    payload = _as_dict(item.get("payload"))
    refs = _normalized_entity_refs(item.get("entity_refs")) + _normalized_entity_refs(payload.get("entity_refs"))
    for raw_value in (
        payload.get("artifact_id"),
        payload.get("object_key"),
        payload.get("file_path"),
        payload.get("path"),
        payload.get("filename"),
        payload.get("resource_id"),
        payload.get("external_id"),
        payload.get("job_id"),
        payload.get("task_id"),
        payload.get("run_id"),
        payload.get("approval_id"),
    ):
        ref_value = str(raw_value or "").strip()
        if ref_value and ref_value not in refs:
            refs.append(ref_value)
    return refs


def _derive_subscription_event_key(
    *,
    kind: str,
    event_key: str,
    source: str,
    event_topic: str,
    entity_refs: list[str],
    fallback: str,
) -> str:
    if event_key:
        return event_key
    if kind != "external_signal":
        return fallback
    if source and event_topic:
        return f"{source}:topic:{event_topic}"
    if source and entity_refs:
        return f"{source}:ref:{entity_refs[0]}"
    return fallback


def _append_unique_text(values: list[str], raw_value: Any) -> list[str]:
    value = str(raw_value or "").strip()
    if value and value not in values:
        values.append(value)
    return values


def _event_outcome(event_kind: str, event_payload: dict[str, Any] | None) -> str:
    payload = _as_dict(event_payload)
    if event_kind != "external_signal":
        return ""
    explicit = str(payload.get("adapter_outcome") or payload.get("outcome") or "").strip().lower()
    if explicit:
        return explicit
    for raw_value in (
        payload.get("source_status"),
        payload.get("status"),
        payload.get("state"),
        payload.get("event_topic"),
        payload.get("event_type"),
        payload.get("source_operation"),
        payload.get("operation"),
        payload.get("change_type"),
    ):
        token = re.sub(r"[^a-z0-9]+", "_", str(raw_value or "").strip().lower()).strip("_")
        if token in {"timeout", "timed_out", "expired"}:
            return "timeout"
        if token in {"failed", "failure", "error", "errored", "cancelled", "canceled", "rejected", "denied", "blocked"}:
            return "failure"
        if token in {"pending", "running", "processing", "queued", "in_progress", "started"}:
            return "progress"
        if token in {"completed", "complete", "succeeded", "success", "ready", "approved", "granted", "created", "uploaded", "available", "delivered", "artifact_ready", "modified"}:
            return "success"
    return ""


def _event_resume_directive(
    *,
    event_kind: str,
    event_payload: dict[str, Any] | None,
    default_action: str,
) -> dict[str, Any]:
    payload = _as_dict(event_payload)
    outcome = _event_outcome(event_kind, payload)
    requires_replan = bool(payload.get("requires_replan")) or outcome in FAILURE_OUTCOMES
    observation_summary = str(payload.get("observation_summary") or "").strip()
    if not observation_summary:
        if outcome and event_kind == "external_signal":
            source = str(payload.get("source") or "external_signal").strip() or "external_signal"
            topic = str(
                payload.get("event_topic")
                or payload.get("event_type")
                or payload.get("source_status")
                or payload.get("status")
                or payload.get("state")
                or "update"
            ).strip()
            observation_summary = f"Observed external {outcome} `{topic}` from `{source}`."
        else:
            observation_summary = f"Resumed after `{event_kind}` event."
    resume_action = "replan" if requires_replan else default_action
    return {
        "outcome": outcome,
        "requires_replan": requires_replan,
        "resume_action": resume_action,
        "summary": observation_summary,
        "clear_event_requirements": requires_replan and event_kind == "external_signal",
    }


def _event_matches_subscription(
    event: dict[str, Any],
    *,
    kind: str,
    event_key: str,
    source: str | None = None,
    event_topic: str | None = None,
    entity_refs: list[str] | None = None,
    expected_outcomes: list[str] | None = None,
) -> bool:
    if str(event.get("kind") or "").strip() != str(kind or "").strip():
        return False
    expected_key = str(event_key or "").strip()
    if expected_key and str(event.get("event_key") or "").strip() != expected_key:
        return False
    expected_source = str(source or "").strip()
    actual_source = str(event.get("source") or _as_dict(event.get("payload")).get("source") or "").strip()
    if expected_source and actual_source and actual_source != expected_source:
        return False
    expected_topic = str(event_topic or "").strip()
    actual_topic = _event_topic_value(event)
    if expected_topic and actual_topic != expected_topic:
        return False
    required_refs = _normalized_entity_refs(entity_refs)
    if required_refs:
        actual_refs = _event_entity_refs(event)
        if not any(ref in actual_refs for ref in required_refs):
            return False
    normalized_outcomes = _expected_event_outcomes(expected_outcomes, kind=kind) if expected_outcomes is not None else []
    if normalized_outcomes:
        actual_outcome = _event_outcome(str(kind or ""), _as_dict(event.get("payload")))
        if actual_outcome not in normalized_outcomes:
            return False
    return True


def _event_memory_entries(runtime_state: dict[str, Any], previous_goal_state: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str, str, tuple[str, ...]], dict[str, Any]] = {}
    for source in (
        _as_list(_as_dict(previous_goal_state).get("event_memory")),
        _as_list(runtime_state.get("event_memory")),
    ):
        for item in source:
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind") or "").strip()
            event_key = str(item.get("event_key") or "").strip()
            if not kind or not event_key:
                continue
            event_topic = _event_topic_value(item)
            entity_refs = _event_entity_refs(item)
            merged[(kind, event_key, str(item.get("source") or ""), event_topic, tuple(sorted(entity_refs)))] = {
                "kind": kind,
                "event_key": event_key,
                "source": str(item.get("source") or ""),
                "event_topic": event_topic,
                "entity_refs": entity_refs,
                "received_at": str(item.get("received_at") or ""),
                "payload": _as_dict(item.get("payload")),
            }
    return list(merged.values())


def _event_seen(
    event_memory: list[dict[str, Any]],
    *,
    kind: str,
    event_key: str,
    source: str | None = None,
    event_topic: str | None = None,
    entity_refs: list[str] | None = None,
    expected_outcomes: list[str] | None = None,
) -> bool:
    target_kind = str(kind or "").strip()
    target_key = str(event_key or "").strip()
    if not target_kind:
        return False
    for item in event_memory:
        if _event_matches_subscription(
            item,
            kind=target_kind,
            event_key=target_key,
            source=source,
            event_topic=event_topic,
            entity_refs=entity_refs,
            expected_outcomes=expected_outcomes,
        ):
            return True
    return False


def _subscription_lookup(previous_goal_state: dict[str, Any] | None) -> dict[tuple[str, str, str, str, str, tuple[str, ...], tuple[str, ...]], dict[str, Any]]:
    lookup: dict[tuple[str, str, str, str, str, tuple[str, ...], tuple[str, ...]], dict[str, Any]] = {}
    for item in _as_list(_as_dict(previous_goal_state).get("event_subscriptions")):
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip()
        event_key = str(item.get("event_key") or "").strip()
        subgoal_id = str(item.get("subgoal_id") or "").strip()
        if not kind:
            continue
        lookup[
            (
                kind,
                event_key,
                subgoal_id,
                str(item.get("source") or "").strip(),
                str(item.get("event_topic") or "").strip(),
                tuple(sorted(_normalized_entity_refs(item.get("entity_refs")))),
                tuple(sorted(_expected_event_outcomes(item.get("expected_outcomes"), kind=kind))),
            )
        ] = dict(item)
    return lookup


def _resolve_subscription_timing(
    *,
    item: dict[str, Any],
    previous_item: dict[str, Any] | None,
    now: datetime,
    timeout_multiplier: float = 1.0,
) -> tuple[str, str, int]:
    first_seen_at = str((previous_item or {}).get("first_seen_at") or item.get("first_seen_at") or now.isoformat())
    timeout_s = 0
    explicit_timeout = item.get("timeout_s")
    previous_timeout = (previous_item or {}).get("timeout_s")
    try:
        timeout_s = int(explicit_timeout if explicit_timeout not in (None, "") else previous_timeout or 0)
    except (TypeError, ValueError):
        timeout_s = 0
    if timeout_s <= 0 and bool(item.get("required")):
        base_timeout = max(0, int(settings.goal_event_subscription_default_timeout_s))
        timeout_s = max(0, int(round(base_timeout * max(0.1, float(timeout_multiplier or 1.0)))))

    explicit_expiry = str(item.get("expires_at") or "").strip()
    previous_expiry = str((previous_item or {}).get("expires_at") or "").strip()
    if explicit_expiry:
        expires_at = explicit_expiry
    elif previous_expiry:
        expires_at = previous_expiry
    elif timeout_s > 0:
        first_seen_dt = _parse_datetime(first_seen_at) or now
        expires_at = (first_seen_dt.timestamp() + timeout_s)
        expires_at = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
    else:
        expires_at = ""
    return first_seen_at, expires_at, timeout_s


def _normalize_event_subscriptions(
    *,
    goal_id: str,
    goal: dict[str, Any],
    runtime_state: dict[str, Any],
    active_subgoal: dict[str, Any] | None,
    wake_condition: dict[str, Any],
    wake_graph: dict[str, Any],
    event_memory: list[dict[str, Any]],
    previous_goal_state: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    subscriptions: list[dict[str, Any]] = []
    active_subgoal_id = str(_as_dict(active_subgoal).get("subgoal_id") or "")
    explicit_keys: set[tuple[str, str, str, str, str, tuple[str, ...], tuple[str, ...]]] = set()
    previous_lookup = _subscription_lookup(previous_goal_state)
    now = datetime.now(timezone.utc)
    explicit = [item for item in _as_list(runtime_state.get("event_requirements")) + _as_list(goal.get("wake_requirements")) if isinstance(item, dict)]
    if explicit:
        for index, item in enumerate(explicit, start=1):
            kind = str(item.get("kind") or "").strip()
            source = str(item.get("source") or wake_condition.get("source") or "subscription_graph")
            event_topic = str(item.get("event_topic") or "").strip()
            entity_refs = _normalized_entity_refs(item.get("entity_refs"))
            event_key = _derive_subscription_event_key(
                kind=kind,
                event_key=str(item.get("event_key") or "").strip(),
                source=source,
                event_topic=event_topic,
                entity_refs=entity_refs,
                fallback="",
            )
            if not kind or not event_key:
                continue
            subgoal_id = str(item.get("subgoal_id") or active_subgoal_id)
            expected_outcomes = _expected_event_outcomes(item.get("expected_outcomes"), kind=kind)
            dedupe_key = (
                kind,
                event_key,
                subgoal_id,
                source,
                event_topic,
                tuple(sorted(entity_refs)),
                tuple(sorted(expected_outcomes)),
            )
            if dedupe_key in explicit_keys:
                continue
            explicit_keys.add(dedupe_key)
            subscriptions.append(
                {
                    "subscription_id": str(item.get("subscription_id") or f"{goal_id}:sub:{index}:{kind}:{_slug_text(event_key)}"),
                    "kind": kind,
                    "event_key": event_key,
                    "source": source,
                    "event_topic": event_topic or None,
                    "entity_refs": entity_refs,
                    "expected_outcomes": expected_outcomes,
                    "resume_action": str(item.get("resume_action") or wake_condition.get("resume_action") or "workflow_call"),
                    "required": bool(item.get("required", True)),
                    "scope": str(item.get("scope") or "goal"),
                    "subgoal_id": subgoal_id,
                    "timeout_s": item.get("timeout_s"),
                    "expires_at": item.get("expires_at"),
                }
            )
    elif str(wake_condition.get("kind") or "") in EVENT_DRIVEN_KINDS:
        wake_source = str(wake_condition.get("source") or "runtime")
        wake_topic = str(wake_condition.get("event_topic") or "").strip()
        wake_refs = _normalized_entity_refs(wake_condition.get("entity_refs"))
        wake_key = _derive_subscription_event_key(
            kind=str(wake_condition.get("kind") or ""),
            event_key=str(wake_condition.get("event_key") or ""),
            source=wake_source,
            event_topic=wake_topic,
            entity_refs=wake_refs,
            fallback=goal_id,
        )
        subscriptions.append(
            {
                "subscription_id": f"{goal_id}:sub:primary:{str(wake_condition.get('kind') or '')}:{_slug_text(wake_key)}",
                "kind": str(wake_condition.get("kind") or ""),
                "event_key": wake_key,
                "source": wake_source,
                "event_topic": wake_topic or None,
                "entity_refs": wake_refs,
                "expected_outcomes": _expected_event_outcomes(
                    wake_condition.get("expected_outcomes"),
                    kind=str(wake_condition.get("kind") or ""),
                ),
                "resume_action": str(wake_condition.get("resume_action") or "workflow_call"),
                "required": True,
                "scope": "goal",
                "subgoal_id": active_subgoal_id,
                "timeout_s": None,
                "expires_at": None,
            }
        )

    existing_keys = {
        (
            str(item.get("kind") or ""),
            str(item.get("event_key") or ""),
            str(item.get("subgoal_id") or ""),
            str(item.get("source") or ""),
            str(item.get("event_topic") or ""),
            tuple(sorted(_normalized_entity_refs(item.get("entity_refs")))),
            tuple(sorted(_expected_event_outcomes(item.get("expected_outcomes"), kind=str(item.get("kind") or "")))),
        )
        for item in subscriptions
    }
    for index, item in enumerate(_as_list(wake_graph.get("waiting_events")), start=1):
        row = _as_dict(item)
        kind = str(row.get("kind") or "").strip()
        source = str(row.get("source") or "wake_graph")
        event_topic = str(row.get("event_topic") or "").strip()
        entity_refs = _normalized_entity_refs(row.get("entity_refs"))
        event_key = _derive_subscription_event_key(
            kind=kind,
            event_key=str(row.get("event_key") or "").strip(),
            source=source,
            event_topic=event_topic,
            entity_refs=entity_refs,
            fallback="",
        )
        subgoal_id = str(row.get("subgoal_id") or "").strip()
        if kind not in EVENT_DRIVEN_KINDS or not event_key:
            continue
        expected_outcomes = _expected_event_outcomes(row.get("expected_outcomes"), kind=kind)
        identity = (
            kind,
            event_key,
            subgoal_id,
            source,
            event_topic,
            tuple(sorted(entity_refs)),
            tuple(sorted(expected_outcomes)),
        )
        if identity in existing_keys:
            continue
        subscriptions.append(
            {
                "subscription_id": f"{goal_id}:sub:observe:{index}:{kind}:{_slug_text(event_key)}",
                "kind": kind,
                "event_key": event_key,
                "source": source,
                "event_topic": event_topic or None,
                "entity_refs": entity_refs,
                "expected_outcomes": expected_outcomes,
                "resume_action": str(row.get("resume_action") or wake_condition.get("resume_action") or "workflow_call"),
                "required": False,
                "scope": "subgoal",
                "subgoal_id": subgoal_id,
                "timeout_s": row.get("timeout_s"),
                "expires_at": row.get("expires_at"),
            }
        )
        existing_keys.add(identity)

    normalized: list[dict[str, Any]] = []
    for item in subscriptions:
        kind = str(item.get("kind") or "").strip()
        event_key = str(item.get("event_key") or "").strip()
        source = str(item.get("source") or "").strip()
        event_topic = str(item.get("event_topic") or "").strip()
        entity_refs = _normalized_entity_refs(item.get("entity_refs"))
        expected_outcomes = _expected_event_outcomes(item.get("expected_outcomes"), kind=kind)
        if not kind or not event_key:
            continue
        source_wait_strategy = (
            _external_source_wait_strategy(runtime_state, source=source, event_topic=event_topic)
            if kind == "external_signal"
            else {}
        )
        identity = (
            kind,
            event_key,
            str(item.get("subgoal_id") or ""),
            source,
            event_topic,
            tuple(sorted(entity_refs)),
            tuple(sorted(expected_outcomes)),
        )
        previous_item = previous_lookup.get(identity)
        first_seen_at, expires_at, timeout_s = _resolve_subscription_timing(
            item=item,
            previous_item=previous_item,
            now=now,
            timeout_multiplier=_safe_float(source_wait_strategy.get("timeout_multiplier") or 1.0),
        )
        satisfied = _event_seen(
            event_memory,
            kind=kind,
            event_key=event_key,
            source=source or None,
            event_topic=event_topic or None,
            entity_refs=entity_refs,
            expected_outcomes=expected_outcomes,
        )
        expired = False
        expires_at_dt = _parse_datetime(expires_at)
        if not satisfied and expires_at_dt is not None and expires_at_dt <= now:
            expired = True
        normalized.append(
            {
                **item,
                "first_seen_at": first_seen_at,
                "expires_at": expires_at or None,
                "timeout_s": timeout_s,
                "status": "satisfied" if satisfied else ("expired" if expired else "pending"),
                "source_reliability_key": str(source_wait_strategy.get("key") or ""),
                "source_reliability_score": _safe_float(source_wait_strategy.get("score")),
                "source_reliability_confidence": _safe_float(source_wait_strategy.get("confidence")),
                "source_timeout_multiplier": _safe_float(source_wait_strategy.get("timeout_multiplier") or 1.0),
                "source_strategy_tier": str(source_wait_strategy.get("tier") or ""),
            }
        )
    return normalized


def _pending_required_subscriptions(event_subscriptions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in event_subscriptions
        if bool(item.get("required"))
        and str(item.get("status") or "pending") != "satisfied"
    ]


def _expired_required_subscriptions(event_subscriptions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in event_subscriptions
        if bool(item.get("required"))
        and str(item.get("status") or "") == "expired"
    ]


def _goal_runtime_status(runtime_state: dict[str, Any]) -> str:
    runtime_status = str(runtime_state.get("status") or "").upper()
    current_action = str(_as_dict(runtime_state.get("current_action")).get("action_type") or "")
    portfolio = _portfolio_state(runtime_state)
    if runtime_status == "SUCCEEDED":
        return "COMPLETED"
    if runtime_status in {"FAILED_FINAL", "TIMED_OUT"}:
        return "FAILED"
    if runtime_status == "CANCELLED":
        if _portfolio_hold_active(portfolio):
            return "WAITING"
        return "CANCELLED"
    if runtime_status in {"WAITING_HUMAN", "WAITING_APPROVAL"} or current_action in {"wait", "ask_user", "approval_request"}:
        return "WAITING"
    return "ACTIVE"


def _wake_condition(runtime_state: dict[str, Any]) -> dict[str, Any]:
    current_action = str(_as_dict(runtime_state.get("current_action")).get("action_type") or "")
    reflection = _as_dict(runtime_state.get("reflection"))
    next_action = str(reflection.get("next_action") or "")
    runtime_status = str(runtime_state.get("status") or "").upper()
    goal_ref = _as_dict(runtime_state.get("goal_ref"))
    task_state = _as_dict(runtime_state.get("task_state"))
    latest_result = _latest_result(runtime_state)
    portfolio = _portfolio_state(runtime_state)
    pending_approvals = [str(item).strip() for item in _as_list(task_state.get("pending_approvals")) if str(item).strip()]
    conversation_id = str(runtime_state.get("conversation_id") or _as_dict(runtime_state.get("goal")).get("conversation_id") or "")
    if _portfolio_hold_active(portfolio):
        return {
            "kind": "scheduler_cooldown",
            "status": "held" if str(portfolio.get("hold_status") or "") == "HELD" else "preempting",
            "resume_action": next_action or "workflow_call",
            "event_key": str(goal_ref.get("goal_id") or _as_dict(runtime_state.get("goal")).get("goal_id") or ""),
            "source": "goal_portfolio",
        }
    if current_action == "ask_user":
        event_key = conversation_id or str(goal_ref.get("goal_id") or "")
        return {
            "kind": "user_message",
            "status": "pending",
            "resume_action": next_action or "retrieve",
            "event_key": event_key,
            "source": "conversation",
        }
    if current_action == "approval_request":
        approval_id = pending_approvals[0] if pending_approvals else str(latest_result.get("approval_id") or "")
        return {
            "kind": "approval",
            "status": "pending",
            "resume_action": next_action or "workflow_call",
            "event_key": approval_id or str(goal_ref.get("goal_id") or ""),
            "source": "approval_queue",
        }
    if current_action == "wait" and runtime_status in {"WAITING_HUMAN", "WAITING_APPROVAL"}:
        dependency_task_id = str(latest_result.get("awaiting_task_id") or latest_result.get("dependency_task_id") or "")
        if dependency_task_id:
            return {
                "kind": "task_completion",
                "status": "pending",
                "resume_action": next_action or "workflow_call",
                "event_key": dependency_task_id,
                "source": "task_runtime",
            }
        return {
            "kind": "external_signal",
            "status": "pending",
            "resume_action": next_action or "workflow_call",
            "event_key": str(goal_ref.get("goal_id") or ""),
            "source": "runtime_wait",
        }
    if next_action in {"workflow_call", "replan"} or current_action in {"workflow_call", "replan"}:
        return {
            "kind": "scheduler_cooldown",
            "status": "armed",
            "resume_action": next_action or current_action,
            "event_key": str(goal_ref.get("goal_id") or ""),
            "source": "goal_scheduler",
        }
    return {
        "kind": "none",
        "status": "idle",
        "resume_action": next_action or current_action or "respond",
        "event_key": str(goal_ref.get("goal_id") or ""),
        "source": "runtime",
    }


def _subgoal_blueprints(goal: dict[str, Any], runtime_state: dict[str, Any], goal_id: str) -> list[dict[str, Any]]:
    structured = [item for item in _as_list(goal.get("subgoals")) if isinstance(item, dict)]
    rows: list[dict[str, Any]] = []
    if structured:
        for index, item in enumerate(structured):
            title = str(item.get("title") or item.get("name") or "").strip()
            if not title:
                continue
            sequence_no = int(item.get("sequence_no") or index + 1)
            subgoal_id = str(item.get("subgoal_id") or f"{goal_id}:sg:{sequence_no}")
            depends_on = [str(dep).strip() for dep in _as_list(item.get("depends_on")) if str(dep).strip()]
            rows.append(
                {
                    "subgoal_id": subgoal_id,
                    "sequence_no": sequence_no,
                    "title": title,
                    "depends_on": depends_on,
                }
            )
    if rows:
        return sorted(rows, key=lambda item: (int(item.get("sequence_no") or 0), str(item.get("subgoal_id") or "")))

    criteria = [str(item).strip() for item in _as_list(goal.get("success_criteria")) if str(item).strip()]
    if not criteria:
        criteria = [str(goal.get("normalized_goal") or "Advance the goal").strip()]
    previous_subgoal_id = ""
    for index, title in enumerate(criteria, start=1):
        subgoal_id = f"{goal_id}:sg:{index}"
        depends_on = [previous_subgoal_id] if previous_subgoal_id else []
        rows.append(
            {
                "subgoal_id": subgoal_id,
                "sequence_no": index,
                "title": title,
                "depends_on": depends_on,
            }
        )
        previous_subgoal_id = subgoal_id
    if not rows:
        return rows

    existing_ids = {str(item.get("subgoal_id") or "") for item in rows}
    existing_titles = {str(item.get("title") or "").strip().lower() for item in rows}
    goal_ref = _as_dict(runtime_state.get("goal_ref"))
    task_state = _as_dict(runtime_state.get("task_state"))
    reflection = _as_dict(runtime_state.get("reflection"))
    latest_result = _latest_result(runtime_state)
    portfolio = _portfolio_state(runtime_state)
    active_index = min(max(int(goal_ref.get("active_subgoal_index") or 0), 0), max(len(rows) - 1, 0))
    completed_anchor_id = ""
    for item in rows:
        if int(item.get("sequence_no") or 0) <= active_index:
            completed_anchor_id = str(item.get("subgoal_id") or "")
    next_sequence = max(int(item.get("sequence_no") or 0) for item in rows) + 1
    latest_dynamic_id = completed_anchor_id
    dynamic_ids: list[str] = []

    def append_dynamic(kind: str, title: str) -> None:
        nonlocal next_sequence, latest_dynamic_id
        normalized_title = title.strip()
        if not normalized_title or normalized_title.lower() in existing_titles:
            return
        subgoal_id = f"{goal_id}:dyn:{kind}:{_slug_text(normalized_title)}"
        if subgoal_id in existing_ids:
            return
        depends_on = [latest_dynamic_id] if latest_dynamic_id else ([completed_anchor_id] if completed_anchor_id else [])
        rows.append(
            {
                "subgoal_id": subgoal_id,
                "sequence_no": next_sequence,
                "title": normalized_title,
                "depends_on": [dep for dep in depends_on if dep],
            }
        )
        existing_ids.add(subgoal_id)
        existing_titles.add(normalized_title.lower())
        dynamic_ids.append(subgoal_id)
        latest_dynamic_id = subgoal_id
        next_sequence += 1

    blockers = [str(item).strip() for item in _as_list(task_state.get("blockers")) if str(item).strip()]
    for blocker in blockers[:3]:
        append_dynamic("blocker", f"Resolve blocker: {blocker}")

    unknowns = [str(item).strip() for item in _as_list(task_state.get("unknowns")) if str(item).strip()]
    current_action = str(_as_dict(runtime_state.get("current_action")).get("action_type") or "")
    if unknowns and current_action in {"retrieve", "replan", "ask_user", "workflow_call"}:
        for unknown in unknowns[:3]:
            append_dynamic("clarify", f"Clarify unknown: {unknown}")

    latest_status = str(latest_result.get("status") or "").upper()
    reason_code = str(latest_result.get("reason_code") or latest_result.get("error_code") or "").strip()
    if bool(reflection.get("requires_replan")) or current_action == "replan" or latest_status in {"FAILED_RETRYABLE", "FAILED_FINAL", "TIMED_OUT"}:
        reason_text = reason_code or latest_status or str(reflection.get("summary") or "runtime drift")
        append_dynamic("repair", f"Recover and replan: {reason_text}")

    if str(portfolio.get("resume_strategy") or "") == "replan_after_preemption":
        held_by_goal_id = str(portfolio.get("last_held_by_goal_id") or portfolio.get("held_by_goal_id") or "").strip()
        hold_reason = str(portfolio.get("last_hold_reason") or portfolio.get("hold_reason") or "portfolio preemption").strip()
        resume_title = f"Reassess plan after preemption by {held_by_goal_id}" if held_by_goal_id else "Reassess plan after portfolio preemption"
        append_dynamic("resume", resume_title)
        append_dynamic("resume", f"Resume deferred work with updated priorities: {hold_reason}")

    if dynamic_ids:
        gate_id = dynamic_ids[-1]
        for item in rows:
            subgoal_id = str(item.get("subgoal_id") or "")
            if subgoal_id in dynamic_ids:
                continue
            if int(item.get("sequence_no") or 0) <= active_index:
                continue
            depends_on = [str(dep).strip() for dep in _as_list(item.get("depends_on")) if str(dep).strip()]
            if gate_id not in depends_on:
                depends_on.append(gate_id)
                item["depends_on"] = depends_on
    return sorted(rows, key=lambda item: (int(item.get("sequence_no") or 0), str(item.get("subgoal_id") or "")))


def _subgoal_rows(goal: dict[str, Any], runtime_state: dict[str, Any], goal_id: str) -> list[dict[str, Any]]:
    blueprints = _subgoal_blueprints(goal, runtime_state, goal_id)
    current_action = str(_as_dict(runtime_state.get("current_action")).get("action_type") or "")
    runtime_status = str(runtime_state.get("status") or "").upper()
    wake_condition = _wake_condition(runtime_state)
    portfolio = _portfolio_state(runtime_state)
    checkpoint_payload = {
        "status": runtime_status,
        "current_phase": str(runtime_state.get("current_phase") or ""),
        "current_action": current_action,
        "reflection": _as_dict(runtime_state.get("reflection")),
    }
    goal_ref = _as_dict(runtime_state.get("goal_ref"))
    active_index = int(goal_ref.get("active_subgoal_index") or 0)
    active_index = min(max(active_index, 0), max(len(blueprints) - 1, 0))
    active_subgoal_id = str(goal_ref.get("active_subgoal_id") or "")
    completed_ids: set[str] = set()
    if runtime_status == "SUCCEEDED":
        completed_ids = {str(item.get("subgoal_id") or "") for item in blueprints}
    else:
        for item in blueprints:
            sequence_no = int(item.get("sequence_no") or 0)
            subgoal_id = str(item.get("subgoal_id") or "")
            if subgoal_id and sequence_no <= active_index:
                completed_ids.add(subgoal_id)

    rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    for index, item in enumerate(blueprints):
        subgoal_id = str(item.get("subgoal_id") or f"{goal_id}:sg:{index + 1}")
        depends_on = [str(dep).strip() for dep in _as_list(item.get("depends_on")) if str(dep).strip()]
        missing_dependencies = [dep for dep in depends_on if dep not in completed_ids]
        is_target = bool(active_subgoal_id and active_subgoal_id == subgoal_id) or (not active_subgoal_id and index == active_index)
        status = "PENDING"
        subgoal_wake = {"kind": "none", "status": "idle"}
        dependency_status = {
            "satisfied": not missing_dependencies,
            "missing": missing_dependencies,
        }
        if subgoal_id in completed_ids:
            status = "COMPLETED"
        elif _portfolio_hold_active(portfolio) and is_target:
            status = "WAITING"
            subgoal_wake = dict(wake_condition)
        elif runtime_status in {"FAILED_FINAL", "TIMED_OUT", "CANCELLED"} and is_target:
            status = "BLOCKED"
        elif missing_dependencies:
            status = "BLOCKED"
            subgoal_wake = {
                "kind": "dependency",
                "status": "blocked",
                "resume_action": "workflow_call",
                "event_key": ",".join(missing_dependencies),
                "source": "subgoal_dependency",
            }
        elif is_target:
            if current_action in {"ask_user", "wait", "approval_request"} or wake_condition.get("kind") in {"user_message", "approval", "external_signal"}:
                status = "WAITING"
            else:
                status = "ACTIVE"
            subgoal_wake = dict(wake_condition)
        candidate_rows.append(
            {
                "subgoal_id": subgoal_id,
                "sequence_no": int(item.get("sequence_no") or index + 1),
                "title": str(item.get("title") or ""),
                "status": status,
                "depends_on": depends_on,
                "dependency_status": dependency_status,
                "kind": "dynamic" if ":dyn:" in subgoal_id else "planned",
                "checkpoint_payload": {
                    **checkpoint_payload,
                    "title": str(item.get("title") or ""),
                    "sequence_no": int(item.get("sequence_no") or index + 1),
                    "depends_on": depends_on,
                    "dependency_status": dependency_status,
                    "kind": "dynamic" if ":dyn:" in subgoal_id else "planned",
                },
                "wake_condition": subgoal_wake,
            }
        )

    if not any(str(row.get("status") or "") in {"ACTIVE", "WAITING"} for row in candidate_rows):
        for row in candidate_rows:
            dependency_status = _as_dict(row.get("dependency_status"))
            if str(row.get("status") or "") == "PENDING" and bool(dependency_status.get("satisfied", True)):
                row["status"] = "ACTIVE" if str(wake_condition.get("kind") or "") in {"scheduler_cooldown", "none"} else "WAITING"
                row["wake_condition"] = dict(wake_condition)
                checkpoint = _as_dict(row.get("checkpoint_payload"))
                checkpoint["dependency_status"] = dependency_status
                row["checkpoint_payload"] = checkpoint
                break

    for row in candidate_rows:
        rows.append(
            {
                "subgoal_id": str(row.get("subgoal_id") or ""),
                "sequence_no": int(row.get("sequence_no") or 0),
                "title": str(row.get("title") or ""),
                "status": str(row.get("status") or "PENDING"),
                "depends_on": list(row.get("depends_on") or []),
                "dependency_status": _as_dict(row.get("dependency_status")),
                "kind": str(row.get("kind") or "planned"),
                "checkpoint_payload": dict(row.get("checkpoint_payload") or {}),
                "wake_condition": dict(row.get("wake_condition") or {}),
            }
        )
    return rows


def build_preempted_goal_runtime(
    runtime_state: dict[str, Any],
    *,
    goal_row: dict[str, Any] | None,
    task_id: str | None,
) -> dict[str, Any]:
    portfolio = {
        **_as_dict(_as_dict(goal_row).get("goal_state")).get("portfolio", {}),
        **_portfolio_state(runtime_state),
    }
    if not _portfolio_preempted_for_task(portfolio, task_id):
        return runtime_state
    updated = deepcopy(runtime_state)
    task_state = _as_dict(updated.get("task_state"))
    latest_result = {
        **_latest_result(updated),
        "status": "CANCELLED",
        "reason_code": "goal_preempted",
        "held_by_goal_id": str(portfolio.get("held_by_goal_id") or ""),
        "hold_reason": str(portfolio.get("hold_reason") or "soft_preempted_by_urgent_goal"),
    }
    task_state["latest_result"] = latest_result
    updated["task_state"] = task_state
    updated["current_phase"] = "wait"
    updated["current_action"] = {
        **_as_dict(updated.get("current_action")),
        "action_type": "wait",
        "status": "paused",
        "rationale": "The goal was preempted by a higher-priority portfolio decision and will resume later.",
    }
    updated["policy"] = {
        **_as_dict(updated.get("policy")),
        "selected_action": "replan",
        "portfolio_control": "preempted",
        "held_by_goal_id": str(portfolio.get("held_by_goal_id") or ""),
    }
    updated["reflection"] = {
        **_as_dict(updated.get("reflection")),
        "summary": "The goal was preempted by a higher-priority goal and should resume after the hold window.",
        "requires_replan": True,
        "next_action": "replan",
    }
    updated["portfolio"] = {
        **portfolio,
        "resume_strategy": "replan_after_preemption",
        "last_preempted_at": datetime.now(timezone.utc).isoformat(),
        "last_held_by_goal_id": str(portfolio.get("held_by_goal_id") or ""),
        "last_hold_reason": str(portfolio.get("hold_reason") or "soft_preempted_by_urgent_goal"),
    }
    return updated


def _build_wake_graph(goal_id: str, subgoals: list[dict[str, Any]], active_subgoal: dict[str, Any] | None, wake_condition: dict[str, Any]) -> dict[str, Any]:
    waiting_events: list[dict[str, Any]] = []
    resume_candidates: list[str] = []
    blocked_by_dependency: list[dict[str, Any]] = []
    for row in subgoals:
        subgoal_id = str(row.get("subgoal_id") or "")
        row_wake = _as_dict(row.get("wake_condition"))
        row_status = str(row.get("status") or "")
        dependency_status = _as_dict(row.get("dependency_status"))
        if row_status in {"ACTIVE", "PENDING"} and bool(dependency_status.get("satisfied", True)):
            resume_candidates.append(subgoal_id)
        if row_status in {"WAITING", "BLOCKED"}:
            waiting_events.append(
                {
                    "subgoal_id": subgoal_id,
                    "kind": str(row_wake.get("kind") or ""),
                    "status": str(row_wake.get("status") or ""),
                    "event_key": str(row_wake.get("event_key") or ""),
                    "source": str(row_wake.get("source") or ""),
                    "resume_action": str(row_wake.get("resume_action") or ""),
                }
            )
        if not bool(dependency_status.get("satisfied", True)):
            blocked_by_dependency.append(
                {
                    "subgoal_id": subgoal_id,
                    "missing": list(dependency_status.get("missing") or []),
                }
            )
    return {
        "goal_id": goal_id,
        "active_subgoal_id": str(_as_dict(active_subgoal).get("subgoal_id") or ""),
        "goal_wake_condition": dict(wake_condition),
        "resume_candidates": resume_candidates,
        "waiting_events": waiting_events,
        "blocked_by_dependency": blocked_by_dependency,
    }


def _agenda_profile(
    goal: dict[str, Any],
    policy: dict[str, Any],
    wake_condition: dict[str, Any],
    active_subgoal: dict[str, Any] | None,
    ready_subgoals: list[dict[str, Any]],
    blocked_subgoals: list[dict[str, Any]],
    wake_graph: dict[str, Any],
) -> dict[str, Any]:
    risk_level = str(goal.get("risk_level") or policy.get("risk_level") or "low").lower()
    risk_score = {"low": 0.2, "medium": 0.5, "high": 0.8}.get(risk_level, 0.35)
    selected_action = str(policy.get("selected_action") or "")
    wake_kind = str(wake_condition.get("kind") or "")
    wake_status = str(wake_condition.get("status") or "")
    active = _as_dict(active_subgoal)
    active_kind = str(active.get("kind") or "planned")
    policy_memory = _as_dict(policy.get("policy_memory"))
    action_bias = _as_dict(policy_memory.get("action_bias"))
    critic_patterns = _as_dict(policy_memory.get("critic_patterns"))
    experience_profile = _as_dict(policy.get("experience_profile"))
    ready_count = len(ready_subgoals)
    blocked_count = len(blocked_subgoals)
    resume_candidate_count = len(_as_list(wake_graph.get("resume_candidates")))
    waiting_event_count = len(_as_list(wake_graph.get("waiting_events")))
    workflow_bias = int(action_bias.get("workflow_call") or 0)
    replan_bias = int(action_bias.get("replan") or 0)
    ask_user_bias = int(action_bias.get("ask_user") or 0)
    retry_failures = int(experience_profile.get("retryable_failures") or 0)
    critic_pressure = sum(int(value or 0) for key, value in critic_patterns.items() if str(key).upper() in {"FAILED_RETRYABLE", "TIMED_OUT"})
    source_reliability = _external_source_reliability_lookup(
        policy_memory,
        source=str(wake_condition.get("source") or ""),
        event_topic=str(wake_condition.get("event_topic") or ""),
    )
    source_reliability_score = _safe_float(
        wake_condition.get("source_reliability_score")
        if wake_condition.get("source_reliability_score") is not None
        else source_reliability.get("score")
    )
    source_reliability_confidence = _safe_float(
        wake_condition.get("source_reliability_confidence")
        if wake_condition.get("source_reliability_confidence") is not None
        else source_reliability.get("confidence")
    )

    score = risk_score
    score += min(0.25, ready_count * 0.08)
    score += min(0.12, resume_candidate_count * 0.04)
    score += min(0.18, blocked_count * 0.03)
    score += min(0.12, waiting_event_count * 0.03)
    score += min(0.15, workflow_bias * 0.01)
    score += min(0.12, replan_bias * 0.01)
    score += min(0.08, retry_failures * 0.02)
    score += min(0.08, critic_pressure * 0.01)
    if active_kind == "dynamic":
        score += 0.15
    if selected_action in {"workflow_call", "replan"}:
        score += 0.08
    if wake_kind == "scheduler_cooldown":
        score += 0.08
        if wake_status == "subscription_timeout":
            score += 0.12
    elif wake_kind in {"task_completion", "approval"}:
        score += 0.04
    elif wake_kind == "user_message":
        score -= min(0.05, ask_user_bias * 0.01)
    elif wake_kind == "external_signal" and source_reliability_confidence >= float(settings.goal_external_source_confidence_floor):
        if source_reliability_score <= float(settings.goal_external_source_low_reliability_score):
            score += 0.08
        elif source_reliability_score >= float(settings.goal_external_source_high_reliability_score):
            score -= 0.04
    score = max(0.0, min(1.0, round(score, 3)))

    priority_bucket = "background"
    if score >= 0.75:
        priority_bucket = "urgent"
    elif score >= 0.45:
        priority_bucket = "normal"

    rationale: list[str] = [f"risk={risk_level}", f"wake={wake_kind or 'none'}", f"ready={ready_count}", f"blocked={blocked_count}"]
    if wake_status:
        rationale.append(f"wake_status={wake_status}")
    if wake_kind == "external_signal" and source_reliability_confidence > 0.0:
        rationale.append(f"source_reliability={source_reliability_score:.2f}")
    if active_kind == "dynamic":
        rationale.append("dynamic_repair_subgoal")
    if selected_action:
        rationale.append(f"action={selected_action}")
    return {
        "priority_score": score,
        "priority_bucket": priority_bucket,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "resume_candidate_count": resume_candidate_count,
        "waiting_event_count": waiting_event_count,
        "active_subgoal_kind": active_kind,
        "selected_action": selected_action,
        "rationale": rationale,
    }


def resume_goal_from_event(
    *,
    repo: GoalRepository | None,
    tenant_id: str,
    goal_row: dict[str, Any],
    event_kind: str,
    event_key: str | None = None,
    event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    goal_state = _as_dict(goal_row.get("goal_state"))
    goal = _as_dict(goal_state.get("goal"))
    if not goal or repo is None:
        return goal_row
    wake_condition = _as_dict(goal_state.get("wake_condition"))
    event_subscriptions = [item for item in _as_list(goal_state.get("event_subscriptions")) if isinstance(item, dict)]
    resolved_event_key = str(
        event_key
        or (event_payload or {}).get("approval_id")
        or (event_payload or {}).get("task_id")
        or (event_payload or {}).get("conversation_id")
        or (event_payload or {}).get("event_key")
        or wake_condition.get("event_key")
        or ""
    )
    payload_source = str((event_payload or {}).get("source") or "").strip()
    payload_topic = str(
        (event_payload or {}).get("event_topic")
        or (event_payload or {}).get("event_type")
        or (event_payload or {}).get("status")
        or (event_payload or {}).get("state")
        or ""
    ).strip()
    payload_entity_refs = _event_entity_refs({"payload": event_payload or {}})
    event_envelope = {
        "kind": event_kind,
        "event_key": resolved_event_key,
        "source": payload_source,
        "event_topic": payload_topic,
        "entity_refs": payload_entity_refs,
        "payload": _as_dict(event_payload),
    }
    matching_subscriptions = [
        item
        for item in event_subscriptions
        if _event_matches_subscription(
            event_envelope,
            kind=str(item.get("kind") or ""),
            event_key=str(item.get("event_key") or ""),
            source=str(item.get("source") or "") or None,
            event_topic=str(item.get("event_topic") or "") or None,
            entity_refs=_normalized_entity_refs(item.get("entity_refs")),
            expected_outcomes=_expected_event_outcomes(
                item.get("expected_outcomes"),
                kind=str(item.get("kind") or ""),
            ),
        )
    ]
    structural_matching_subscriptions = [
        item
        for item in event_subscriptions
        if _event_matches_subscription(
            event_envelope,
            kind=str(item.get("kind") or ""),
            event_key=str(item.get("event_key") or ""),
            source=str(item.get("source") or "") or None,
            event_topic=str(item.get("event_topic") or "") or None,
            entity_refs=_normalized_entity_refs(item.get("entity_refs")),
        )
    ]
    primary_matches = _event_matches_subscription(
        event_envelope,
        kind=str(wake_condition.get("kind") or ""),
        event_key=str(wake_condition.get("event_key") or ""),
        source=str(wake_condition.get("source") or "") or None,
        event_topic=str(wake_condition.get("event_topic") or "") or None,
        entity_refs=_normalized_entity_refs(wake_condition.get("entity_refs")),
        expected_outcomes=_expected_event_outcomes(
            wake_condition.get("expected_outcomes"),
            kind=str(wake_condition.get("kind") or ""),
        ),
    )
    primary_structural_match = _event_matches_subscription(
        event_envelope,
        kind=str(wake_condition.get("kind") or ""),
        event_key=str(wake_condition.get("event_key") or ""),
        source=str(wake_condition.get("source") or "") or None,
        event_topic=str(wake_condition.get("event_topic") or "") or None,
        entity_refs=_normalized_entity_refs(wake_condition.get("entity_refs")),
    )
    if event_subscriptions:
        if not structural_matching_subscriptions:
            return goal_row
    elif not primary_structural_match:
        return goal_row
    active_subgoal = _as_dict(goal_state.get("active_subgoal"))
    goal_ref = _as_dict(goal_state.get("goal_ref"))
    task_state = _as_dict(goal_state.get("task_state"))
    latest_result = _latest_result({"task_state": task_state})
    event_memory = _event_memory_entries({"event_memory": goal_state.get("event_memory")}, None)
    if event_payload:
        latest_result = {
            **latest_result,
            "event_kind": event_kind,
            "event_payload": dict(event_payload),
        }
    observed_source = payload_source or str(
        _as_dict(matching_subscriptions[0] if matching_subscriptions else structural_matching_subscriptions[0] if structural_matching_subscriptions else wake_condition).get("source") or ""
    )
    if event_kind and resolved_event_key and not _event_seen(
        event_memory,
        kind=event_kind,
        event_key=resolved_event_key,
        source=observed_source or None,
        event_topic=payload_topic or None,
        entity_refs=payload_entity_refs,
    ):
        event_memory.append(
            {
                "kind": event_kind,
                "event_key": resolved_event_key,
                "source": observed_source,
                "event_topic": payload_topic,
                "entity_refs": payload_entity_refs,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "payload": _as_dict(event_payload),
            }
        )
    pending_required = [
        item
        for item in event_subscriptions
        if bool(item.get("required"))
        and not _event_seen(
            event_memory,
            kind=str(item.get("kind") or ""),
            event_key=str(item.get("event_key") or ""),
            source=str(item.get("source") or "") or None,
            event_topic=str(item.get("event_topic") or "") or None,
            entity_refs=_normalized_entity_refs(item.get("entity_refs")),
            expected_outcomes=_expected_event_outcomes(
                item.get("expected_outcomes"),
                kind=str(item.get("kind") or ""),
            ),
        )
    ]
    task_state["latest_result"] = latest_result
    default_resume_action = str(wake_condition.get("resume_action") or _as_dict(goal_state.get("current_action")).get("action_type") or "workflow_call")
    resume_directive = _event_resume_directive(
        event_kind=event_kind,
        event_payload=event_payload,
        default_action=default_resume_action,
    )
    resume_action = str(resume_directive.get("resume_action") or default_resume_action)
    if str(resume_directive.get("outcome") or ""):
        task_state["latest_result"] = {
            **task_state["latest_result"],
            "event_outcome": str(resume_directive.get("outcome") or ""),
            "resume_action": resume_action,
        }
    if bool(resume_directive.get("requires_replan")):
        blockers = [str(item).strip() for item in _as_list(task_state.get("blockers")) if str(item).strip()]
        task_state["blockers"] = _append_unique_text(blockers, resume_directive.get("summary"))
    serialized_event_requirements = [
        {
            "subscription_id": str(item.get("subscription_id") or ""),
            "kind": str(item.get("kind") or ""),
            "event_key": str(item.get("event_key") or ""),
            "source": str(item.get("source") or ""),
            "event_topic": str(item.get("event_topic") or ""),
            "entity_refs": _normalized_entity_refs(item.get("entity_refs")),
            "expected_outcomes": _expected_event_outcomes(
                item.get("expected_outcomes"),
                kind=str(item.get("kind") or ""),
            ),
            "resume_action": str(item.get("resume_action") or ""),
            "required": bool(item.get("required")),
            "scope": str(item.get("scope") or "goal"),
            "subgoal_id": str(item.get("subgoal_id") or ""),
        }
        for item in event_subscriptions
    ]
    primary_pending_requirement = (
        {
            "kind": str(wake_condition.get("kind") or ""),
            "event_key": str(wake_condition.get("event_key") or ""),
            "source": str(wake_condition.get("source") or ""),
            "event_topic": str(wake_condition.get("event_topic") or ""),
            "entity_refs": _normalized_entity_refs(wake_condition.get("entity_refs")),
            "expected_outcomes": _expected_event_outcomes(
                wake_condition.get("expected_outcomes"),
                kind=str(wake_condition.get("kind") or ""),
            ),
            "resume_action": str(wake_condition.get("resume_action") or resume_action),
        }
        if not event_subscriptions and primary_structural_match and not primary_matches
        else {}
    )
    if bool(resume_directive.get("clear_event_requirements")):
        serialized_event_requirements = []
    if (pending_required or primary_pending_requirement) and not bool(resume_directive.get("requires_replan")):
        waiting_action = _as_dict(goal_state.get("current_action"))
        waiting_type = str(waiting_action.get("action_type") or "")
        if waiting_type not in {"ask_user", "approval_request", "wait"}:
            waiting_action = {
                **waiting_action,
                "action_type": "wait",
                "status": "pending",
            }
        primary_pending = _as_dict(pending_required[0]) if pending_required else primary_pending_requirement
        pending_count = len(pending_required) if pending_required else 1
        return sync_goal_progress(
            repo=repo,
            tenant_id=tenant_id,
            user_id=str(goal_row.get("user_id") or ""),
            conversation_id=str(goal_row.get("conversation_id") or "") or None,
            goal={**goal, "goal_id": str(goal_row.get("goal_id") or goal.get("goal_id") or "")},
            runtime_state={
                "status": "WAITING_APPROVAL" if str(primary_pending.get("kind") or "") == "approval" else "WAITING_HUMAN",
                "current_phase": str(goal_state.get("current_phase") or "wait"),
                "conversation_id": str(goal_row.get("conversation_id") or ""),
                "goal": goal,
                "planner": _as_dict(goal_state.get("planner")),
                "unified_task": _as_dict(goal_state.get("unified_task")),
                "retrieval_hits": _as_list(goal_state.get("retrieval_hits")),
                "episodes": _as_list(goal_state.get("episodes")),
                "memory": _as_dict(goal_state.get("memory")),
                "goal_ref": {
                    **goal_ref,
                    "goal_id": str(goal_row.get("goal_id") or goal_ref.get("goal_id") or goal.get("goal_id") or ""),
                },
                "current_action": waiting_action,
                "policy": {
                    **_as_dict(goal_state.get("policy")),
                    "resume_trigger": event_kind,
                    "subscription_pending": True,
                    "resume_outcome": str(resume_directive.get("outcome") or ""),
                    "resume_requires_replan": bool(resume_directive.get("requires_replan")),
                },
                "task_state": task_state,
                "reflection": {
                    **_as_dict(goal_state.get("reflection")),
                    "summary": f"{resume_directive.get('summary')} Still waiting on {pending_count} subscription(s).",
                    "next_action": str(primary_pending.get("resume_action") or resume_action),
                    "requires_replan": False,
                },
                "final_output": _as_dict(goal_state.get("final_output")),
                "event_memory": event_memory,
                "event_requirements": serialized_event_requirements,
            },
            task_id=str(goal_row.get("current_task_id") or "") or None,
            turn_id=str(goal_row.get("last_turn_id") or "") or None,
            goal_id=str(goal_row.get("goal_id") or goal.get("goal_id") or "") or None,
        )
    runtime_state = {
        "status": "RUNNING",
        "current_phase": "reflect" if bool(resume_directive.get("requires_replan")) else (
            "plan" if resume_action in {"workflow_call", "replan", "retrieve", "tool_call"} else str(goal_state.get("current_phase") or "plan")
        ),
        "conversation_id": str(goal_row.get("conversation_id") or ""),
        "goal": goal,
        "planner": _as_dict(goal_state.get("planner")),
        "unified_task": _as_dict(goal_state.get("unified_task")),
        "retrieval_hits": _as_list(goal_state.get("retrieval_hits")),
        "episodes": _as_list(goal_state.get("episodes")),
        "memory": _as_dict(goal_state.get("memory")),
        "goal_ref": {
            **goal_ref,
            "goal_id": str(goal_row.get("goal_id") or goal_ref.get("goal_id") or goal.get("goal_id") or ""),
            "active_subgoal_id": str(active_subgoal.get("subgoal_id") or goal_ref.get("active_subgoal_id") or ""),
            "active_subgoal_index": max(0, int(active_subgoal.get("sequence_no") or goal_ref.get("active_subgoal_index") or 1) - 1),
        },
        "current_action": {
            **_as_dict(goal_state.get("current_action")),
            "action_type": resume_action,
            "status": "planned",
            "target": str(active_subgoal.get("title") or _as_dict(goal_state.get("current_action")).get("target") or ""),
        },
        "policy": {
            **_as_dict(goal_state.get("policy")),
            "resume_trigger": event_kind,
            "resume_source": payload_source,
            "resume_topic": payload_topic,
            "resume_outcome": str(resume_directive.get("outcome") or ""),
            "resume_requires_replan": bool(resume_directive.get("requires_replan")),
            "resume_observation": str(resume_directive.get("summary") or ""),
        },
        "task_state": task_state,
        "reflection": {
            **_as_dict(goal_state.get("reflection")),
            "summary": str(resume_directive.get("summary") or f"Resumed after `{event_kind}` event."),
            "next_action": resume_action,
            "requires_replan": bool(resume_directive.get("requires_replan")) or resume_action == "replan",
        },
        "final_output": _as_dict(goal_state.get("final_output")),
        "event_memory": event_memory,
        "event_requirements": serialized_event_requirements,
    }
    return sync_goal_progress(
        repo=repo,
        tenant_id=tenant_id,
        user_id=str(goal_row.get("user_id") or ""),
        conversation_id=str(goal_row.get("conversation_id") or "") or None,
        goal={**goal, "goal_id": str(goal_row.get("goal_id") or goal.get("goal_id") or "")},
        runtime_state=runtime_state,
        task_id=str(goal_row.get("current_task_id") or "") or None,
        turn_id=str(goal_row.get("last_turn_id") or "") or None,
        goal_id=str(goal_row.get("goal_id") or goal.get("goal_id") or "") or None,
    )


def resume_waiting_goals_for_event(
    *,
    repo: GoalRepository | None,
    tenant_id: str,
    event_kind: str,
    event_key: str,
    event_payload: dict[str, Any] | None = None,
    user_id: str | None = None,
    conversation_id: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if repo is None or not event_key:
        return []
    rows = repo.list_goals_waiting_on_event(
        tenant_id=tenant_id,
        event_kind=event_kind,
        event_key=event_key,
        user_id=user_id,
        conversation_id=conversation_id,
        limit=limit,
    )
    resumed: list[dict[str, Any]] = []
    for row in rows:
        resumed.append(
            resume_goal_from_event(
                repo=repo,
                tenant_id=tenant_id,
                goal_row=row,
                event_kind=event_kind,
                event_key=event_key,
                event_payload=event_payload,
            )
        )
    return resumed


def _goal_state_snapshot(
    runtime_state: dict[str, Any],
    *,
    goal_override: dict[str, Any] | None = None,
    goal_id_override: str | None = None,
    previous_goal_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    goal = {**_as_dict(runtime_state.get("goal")), **_as_dict(goal_override)}
    goal_ref = _as_dict(runtime_state.get("goal_ref"))
    goal_id = str(goal_id_override or goal.get("goal_id") or goal_ref.get("goal_id") or "")
    subgoals = _subgoal_rows(goal, runtime_state, goal_id) if goal_id or goal else []
    active_subgoal = next(
        (row for row in subgoals if str(row.get("status")) in {"ACTIVE", "WAITING"}),
        next(
            (
                row
                for row in subgoals
                if str(row.get("status")) == "PENDING"
                and bool(_as_dict(row.get("dependency_status")).get("satisfied", True))
            ),
            subgoals[0] if subgoals else None,
        ),
    )
    ready_subgoals = [
        row
        for row in subgoals
        if str(row.get("status")) in {"ACTIVE", "PENDING"}
        and bool(_as_dict(row.get("dependency_status")).get("satisfied", True))
    ]
    blocked_subgoals = [
        row
        for row in subgoals
        if str(row.get("status")) == "BLOCKED" or not bool(_as_dict(row.get("dependency_status")).get("satisfied", True))
    ]
    wake_condition = _wake_condition(runtime_state)
    wake_graph = _build_wake_graph(goal_id, subgoals, active_subgoal, wake_condition)
    event_memory = _event_memory_entries(runtime_state, previous_goal_state)
    event_subscriptions = _normalize_event_subscriptions(
        goal_id=goal_id,
        goal=goal,
        runtime_state=runtime_state,
        active_subgoal=active_subgoal,
        wake_condition=wake_condition,
        wake_graph=wake_graph,
        event_memory=event_memory,
        previous_goal_state=previous_goal_state,
    )
    pending_required = _pending_required_subscriptions(event_subscriptions)
    expired_required = _expired_required_subscriptions(event_subscriptions)
    if expired_required:
        primary = _as_dict(expired_required[0])
        wake_condition = {
            "kind": "scheduler_cooldown",
            "status": "subscription_timeout",
            "resume_action": "replan",
            "event_key": goal_id,
            "source": "subscription_timeout",
            "expired_count": len(expired_required),
            "primary_kind": str(primary.get("kind") or ""),
            "primary_event_key": str(primary.get("event_key") or ""),
        }
        wake_graph = _build_wake_graph(goal_id, subgoals, active_subgoal, wake_condition)
    elif pending_required:
        primary = _as_dict(pending_required[0])
        if len(pending_required) > 1:
            wake_condition = {
                "kind": "composite",
                "status": "pending",
                "resume_action": str(primary.get("resume_action") or wake_condition.get("resume_action") or "workflow_call"),
                "event_key": goal_id,
                "source": "subscription_graph",
                "pending_count": len(pending_required),
                "primary_kind": str(primary.get("kind") or ""),
                "primary_event_key": str(primary.get("event_key") or ""),
                "primary_source": str(primary.get("source") or ""),
                "primary_event_topic": str(primary.get("event_topic") or ""),
                "primary_entity_refs": _normalized_entity_refs(primary.get("entity_refs")),
                "primary_expected_outcomes": _expected_event_outcomes(
                    primary.get("expected_outcomes"),
                    kind=str(primary.get("kind") or ""),
                ),
                "primary_source_reliability_key": str(primary.get("source_reliability_key") or ""),
                "primary_source_reliability_score": _safe_float(primary.get("source_reliability_score")),
                "primary_source_reliability_confidence": _safe_float(primary.get("source_reliability_confidence")),
            }
        else:
            wake_condition = {
                "kind": str(primary.get("kind") or wake_condition.get("kind") or "external_signal"),
                "status": "pending",
                "resume_action": str(primary.get("resume_action") or wake_condition.get("resume_action") or "workflow_call"),
                "event_key": str(primary.get("event_key") or wake_condition.get("event_key") or goal_id),
                "source": str(primary.get("source") or "subscription_graph"),
                "event_topic": str(primary.get("event_topic") or ""),
                "entity_refs": _normalized_entity_refs(primary.get("entity_refs")),
                "expected_outcomes": _expected_event_outcomes(
                    primary.get("expected_outcomes"),
                    kind=str(primary.get("kind") or wake_condition.get("kind") or ""),
                ),
                "source_reliability_key": str(primary.get("source_reliability_key") or ""),
                "source_reliability_score": _safe_float(primary.get("source_reliability_score")),
                "source_reliability_confidence": _safe_float(primary.get("source_reliability_confidence")),
                "source_timeout_multiplier": _safe_float(primary.get("source_timeout_multiplier") or 1.0),
            }
        wake_graph = _build_wake_graph(goal_id, subgoals, active_subgoal, wake_condition)
    agenda = _agenda_profile(
        goal,
        _as_dict(runtime_state.get("policy")),
        wake_condition,
        active_subgoal,
        ready_subgoals,
        blocked_subgoals,
        wake_graph,
    )
    return {
        "status": str(runtime_state.get("status") or ""),
        "current_phase": str(runtime_state.get("current_phase") or ""),
        "goal": goal,
        "planner": _as_dict(runtime_state.get("planner")),
        "unified_task": _as_dict(runtime_state.get("unified_task")),
        "retrieval_hits": list(runtime_state.get("retrieval_hits") or []),
        "episodes": list(runtime_state.get("episodes") or []),
        "memory": _as_dict(runtime_state.get("memory")),
        "goal_ref": _as_dict(runtime_state.get("goal_ref")),
        "current_action": _as_dict(runtime_state.get("current_action")),
        "policy": _as_dict(runtime_state.get("policy")),
        "task_state": _as_dict(runtime_state.get("task_state")),
        "reflection": _as_dict(runtime_state.get("reflection")),
        "final_output": _as_dict(runtime_state.get("final_output")),
        "subgoals": subgoals,
        "active_subgoal": active_subgoal,
        "ready_subgoals": ready_subgoals,
        "blocked_subgoals": blocked_subgoals,
        "wake_condition": wake_condition,
        "wake_graph": wake_graph,
        "event_memory": event_memory,
        "event_subscriptions": event_subscriptions,
        "pending_event_subscriptions": pending_required,
        "expired_event_subscriptions": expired_required,
        "event_timeouts": {
            "expired_required_count": len(expired_required),
            "pending_required_count": len(pending_required),
        },
        "agenda": agenda,
    }


def sync_goal_progress(
    *,
    repo: GoalRepository | None,
    tenant_id: str,
    user_id: str,
    conversation_id: str | None,
    goal: dict[str, Any],
    runtime_state: dict[str, Any],
    task_id: str | None = None,
    turn_id: str | None = None,
    goal_id: str | None = None,
) -> dict[str, Any]:
    normalized_goal = str(goal.get("normalized_goal") or "").strip()
    if not normalized_goal:
        return {
            "goal_id": goal_id or f"goal-{uuid.uuid4().hex[:16]}",
            "status": _goal_runtime_status(runtime_state),
            "goal_state": _goal_state_snapshot(runtime_state),
            "continuation_count": 0,
        }
    if repo is None:
        return {
            "goal_id": goal_id or f"goal-{uuid.uuid4().hex[:16]}",
            "status": _goal_runtime_status(runtime_state),
            "goal_state": _goal_state_snapshot(runtime_state),
            "continuation_count": 0,
        }

    current = repo.get_goal(tenant_id=tenant_id, goal_id=goal_id) if goal_id else None
    if current is None:
        current = repo.find_open_goal(
            tenant_id=tenant_id,
            user_id=user_id,
            conversation_id=conversation_id,
            normalized_goal=normalized_goal,
        )

    if current is None:
        resolved_goal_id = goal_id or f"goal-{uuid.uuid4().hex[:16]}"
        snapshot = _goal_state_snapshot(
            runtime_state,
            goal_override=goal,
            goal_id_override=resolved_goal_id,
            previous_goal_state=None,
        )
        runtime_portfolio = _as_dict(runtime_state.get("portfolio"))
        if runtime_portfolio:
            snapshot["portfolio"] = runtime_portfolio
        current = repo.create_goal(
            tenant_id=tenant_id,
            goal_id=resolved_goal_id,
            user_id=user_id,
            conversation_id=conversation_id,
            normalized_goal=normalized_goal,
            status=_goal_runtime_status(runtime_state),
            goal_state=snapshot,
            current_task_id=task_id,
            last_turn_id=turn_id,
            policy_version_id=str(_as_dict(runtime_state.get("policy")).get("policy_version_id") or "") or None,
        )
        repo.replace_subgoals(
            tenant_id=tenant_id,
            goal_id=str(current.get("goal_id") or ""),
            subgoals=list(snapshot.get("subgoals") or []),
        )
        return current

    next_state = _goal_state_snapshot(
        runtime_state,
        goal_override=goal,
        previous_goal_state=_as_dict(current.get("goal_state")),
    )
    existing_portfolio = _as_dict(_as_dict(current.get("goal_state")).get("portfolio"))
    runtime_portfolio = _as_dict(runtime_state.get("portfolio"))
    merged_portfolio = {**existing_portfolio, **runtime_portfolio}
    if merged_portfolio:
        next_state["portfolio"] = merged_portfolio
    continuation_count = int(current.get("continuation_count") or 0)
    if task_id and str(current.get("current_task_id") or "") and str(current.get("current_task_id") or "") != str(task_id):
        continuation_count += 1
    elif turn_id and str(current.get("last_turn_id") or "") and str(current.get("last_turn_id") or "") != str(turn_id):
        continuation_count += 1

    repo.update_goal(
        tenant_id=tenant_id,
        goal_id=str(current.get("goal_id") or ""),
        status=_goal_runtime_status(runtime_state),
        goal_state=next_state,
        current_task_id=task_id or str(current.get("current_task_id") or "") or None,
        last_turn_id=turn_id or str(current.get("last_turn_id") or "") or None,
        continuation_count=continuation_count,
        policy_version_id=str(_as_dict(runtime_state.get("policy")).get("policy_version_id") or current.get("policy_version_id") or "") or None,
    )
    repo.replace_subgoals(
        tenant_id=tenant_id,
        goal_id=str(current.get("goal_id") or ""),
        subgoals=list(next_state.get("subgoals") or []),
    )
    updated = repo.get_goal(tenant_id=tenant_id, goal_id=str(current.get("goal_id") or ""))
    return updated or {
        **deepcopy(current),
        "goal_state": next_state,
        "status": _goal_runtime_status(runtime_state),
        "continuation_count": continuation_count,
    }
