from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any
import uuid

from ..repositories import GoalRepository, PolicyMemoryRepository, TaskRepository
from .policy_memory_service import record_external_signal_feedback, record_portfolio_feedback
from .goal_runtime_service import resume_waiting_goals_for_event

SOURCE_ALIAS_FIELDS: dict[str, tuple[str, ...]] = {
    "vendor_webhook": ("job_id", "external_id", "request_id", "ticket_id"),
    "artifact_store": ("artifact_id", "object_key", "file_path", "path", "filename"),
    "file_watch": ("file_path", "path", "filename"),
    "task_callback": ("task_id", "run_id", "status"),
    "webhook": ("resource_id", "external_id", "event_type", "status"),
    "github_check": ("check_run_id", "check_suite_id", "sha", "repository", "pull_request"),
    "email_inbox": ("message_id", "thread_id", "mailbox", "from"),
    "slack_thread": ("thread_ts", "channel_id", "message_ts", "user_id"),
    "calendar_watch": ("event_id", "calendar_id", "meeting_id", "resource_id"),
}

SOURCE_TOPIC_FIELDS: dict[str, tuple[str, ...]] = {
    "vendor_webhook": ("event_topic", "event_type", "status", "state"),
    "artifact_store": ("event_topic", "event_type", "operation", "status", "state"),
    "file_watch": ("event_topic", "event_type", "change_type", "operation"),
    "task_callback": ("event_topic", "status", "state"),
    "webhook": ("event_topic", "event_type", "status", "state"),
    "github_check": ("event_topic", "conclusion", "status", "check_name", "event_type"),
    "email_inbox": ("event_topic", "mailbox", "folder", "label", "status"),
    "slack_thread": ("event_topic", "channel_name", "status", "event_type"),
    "calendar_watch": ("event_topic", "status", "calendar_name", "event_type"),
}

SOURCE_ENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "vendor_webhook": ("job_id", "external_id", "request_id", "ticket_id"),
    "artifact_store": ("artifact_id", "object_key", "file_path", "path", "filename"),
    "file_watch": ("file_path", "path", "filename"),
    "task_callback": ("task_id", "run_id"),
    "webhook": ("resource_id", "external_id"),
    "github_check": ("check_run_id", "check_suite_id", "sha", "repository", "pull_request"),
    "email_inbox": ("message_id", "thread_id", "mailbox", "from"),
    "slack_thread": ("thread_ts", "channel_id", "message_ts", "user_id"),
    "calendar_watch": ("event_id", "calendar_id", "meeting_id", "resource_id"),
}

SOURCE_DEFAULT_TOPICS: dict[str, str] = {
    "vendor_webhook": "callback_received",
    "artifact_store": "artifact_update",
    "file_watch": "file_changed",
    "task_callback": "task_update",
    "webhook": "webhook_received",
    "github_check": "check_update",
    "email_inbox": "message_received",
    "slack_thread": "thread_update",
    "calendar_watch": "calendar_update",
}

SUPPORTED_EXTERNAL_ADAPTERS = frozenset(SOURCE_DEFAULT_TOPICS.keys())

SUCCESS_SIGNAL_TOKENS = {
    "completed",
    "complete",
    "succeeded",
    "success",
    "ready",
    "approved",
    "granted",
    "created",
    "uploaded",
    "available",
    "delivered",
    "artifact_ready",
    "modified",
}

FAILURE_SIGNAL_TOKENS = {
    "failed",
    "failure",
    "error",
    "errored",
    "cancelled",
    "canceled",
    "rejected",
    "denied",
    "blocked",
    "invalid",
    "missing",
}

TIMEOUT_SIGNAL_TOKENS = {"timeout", "timed_out", "expired"}

PROGRESS_SIGNAL_TOKENS = {"pending", "running", "processing", "queued", "in_progress", "started"}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _append_unique(values: list[str], raw_value: Any) -> None:
    value = str(raw_value or "").strip()
    if value and value not in values:
        values.append(value)


def _coalesce_str(*values: Any) -> str | None:
    for raw_value in values:
        value = str(raw_value or "").strip()
        if value:
            return value
    return None


def _normalized_signal_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _normalized_entity_refs(signal: dict[str, Any]) -> list[str]:
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    payload = dict(signal.get("payload") or {})
    refs: list[str] = []
    for raw_ref in _as_list(signal.get("entity_refs")):
        if isinstance(raw_ref, dict):
            _append_unique(refs, raw_ref.get("id") or raw_ref.get("value"))
        else:
            _append_unique(refs, raw_ref)
    for field_name in SOURCE_ENTITY_FIELDS.get(source, ()):
        _append_unique(refs, payload.get(field_name))
    for raw_ref in _as_list(payload.get("entity_refs")):
        if isinstance(raw_ref, dict):
            ref_type = str(raw_ref.get("type") or raw_ref.get("kind") or "ref").strip() or "ref"
            ref_id = str(raw_ref.get("id") or raw_ref.get("value") or "").strip()
            if ref_id:
                _append_unique(refs, ref_id)
                _append_unique(refs, f"{source}:{ref_type}:{ref_id}")
        else:
            _append_unique(refs, raw_ref)
    return refs


def _normalized_event_topic(signal: dict[str, Any]) -> str | None:
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    payload = dict(signal.get("payload") or {})
    explicit = _coalesce_str(signal.get("event_topic"))
    if explicit:
        return explicit
    for field_name in SOURCE_TOPIC_FIELDS.get(source, ()):
        topic = _coalesce_str(payload.get(field_name))
        if topic:
            return topic
    return SOURCE_DEFAULT_TOPICS.get(source)


def _source_operation(signal: dict[str, Any]) -> str | None:
    payload = dict(signal.get("payload") or {})
    return _coalesce_str(
        payload.get("operation"),
        payload.get("event_type"),
        payload.get("change_type"),
        signal.get("event_topic"),
    )


def _source_status(signal: dict[str, Any]) -> str | None:
    payload = dict(signal.get("payload") or {})
    return _coalesce_str(payload.get("status"), payload.get("state"))


def _source_signal_tokens(signal: dict[str, Any]) -> list[str]:
    payload = dict(signal.get("payload") or {})
    tokens: list[str] = []
    for raw_value in (
        signal.get("event_topic"),
        payload.get("event_topic"),
        payload.get("event_type"),
        payload.get("operation"),
        payload.get("change_type"),
        payload.get("status"),
        payload.get("state"),
    ):
        token = _normalized_signal_token(raw_value)
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def _adapter_outcome(signal: dict[str, Any]) -> str:
    tokens = _source_signal_tokens(signal)
    if any(token in TIMEOUT_SIGNAL_TOKENS for token in tokens):
        return "timeout"
    if any(token in FAILURE_SIGNAL_TOKENS for token in tokens):
        return "failure"
    if any(token in PROGRESS_SIGNAL_TOKENS for token in tokens):
        return "progress"
    if any(token in SUCCESS_SIGNAL_TOKENS for token in tokens):
        return "success"
    return "success"


def _adapter_requires_replan(signal: dict[str, Any], *, outcome: str) -> bool:
    payload = dict(signal.get("payload") or {})
    explicit = payload.get("requires_replan")
    if explicit is not None:
        return bool(explicit)
    return outcome in {"failure", "timeout"}


def _adapter_risk_level(*, outcome: str) -> str:
    if outcome in {"failure", "timeout"}:
        return "high"
    if outcome == "progress":
        return "medium"
    return "low"


def _observation_summary(signal: dict[str, Any], *, outcome: str) -> str:
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    topic = _normalized_event_topic(signal) or "update"
    entity_refs = _normalized_entity_refs(signal)
    focus = entity_refs[0] if entity_refs else source
    if outcome == "failure":
        return f"Observed external failure `{topic}` for `{focus}`."
    if outcome == "timeout":
        return f"Observed external timeout `{topic}` for `{focus}`."
    if outcome == "progress":
        return f"Observed in-flight external update `{topic}` for `{focus}`."
    if outcome == "success":
        return f"Observed external success `{topic}` for `{focus}`."
    return f"Observed external update `{topic}` for `{focus}`."


def _normalize_external_signal(signal: dict[str, Any]) -> dict[str, Any]:
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    payload = dict(signal.get("payload") or {})
    event_topic = _normalized_event_topic(signal)
    entity_refs = _normalized_entity_refs(signal)
    event_aliases = [str(item).strip() for item in _as_list(signal.get("event_aliases")) if str(item).strip()]
    source_status = _source_status(signal)
    source_operation = _source_operation(signal)
    adapter_outcome = _adapter_outcome({**signal, "event_topic": event_topic})
    requires_replan = _adapter_requires_replan(signal, outcome=adapter_outcome)
    risk_level = _adapter_risk_level(outcome=adapter_outcome)
    observation_summary = _observation_summary({**signal, "event_topic": event_topic, "entity_refs": entity_refs}, outcome=adapter_outcome)
    normalized_payload = {
        **payload,
        "source": source,
        "event_topic": event_topic,
        "entity_refs": entity_refs,
        "source_status": source_status,
        "source_operation": source_operation,
        "adapter_outcome": adapter_outcome,
        "requires_replan": requires_replan,
        "risk_level": risk_level,
        "observation_summary": observation_summary,
    }
    return {
        **signal,
        "source": source,
        "event_topic": event_topic,
        "entity_refs": entity_refs,
        "event_aliases": event_aliases,
        "payload": normalized_payload,
        "adapter": {
            "source": source,
            "topic": event_topic,
            "entity_refs": entity_refs,
            "source_status": source_status,
            "source_operation": source_operation,
            "outcome": adapter_outcome,
            "requires_replan": requires_replan,
            "risk_level": risk_level,
            "observation_summary": observation_summary,
        },
    }


def _source_aliases(signal: dict[str, Any]) -> list[str]:
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    payload = dict(signal.get("payload") or {})
    aliases: list[str] = []
    candidate_fields: list[str] = []
    for field_name in (
        *SOURCE_ALIAS_FIELDS.get(source, ()),
        "resource_id",
        "artifact_id",
        "object_key",
        "file_path",
        "path",
        "filename",
        "external_id",
        "job_id",
        "task_id",
        "run_id",
        "approval_id",
        "status",
        "state",
    ):
        if field_name not in candidate_fields:
            candidate_fields.append(field_name)
    for field_name in candidate_fields:
        field_value = payload.get(field_name)
        if field_value in (None, ""):
            continue
        _append_unique(aliases, f"{source}:{field_name}:{field_value}")
        if field_name not in {"status", "state", "event_type"}:
            _append_unique(aliases, f"{source}:ref:{field_value}")

    for topic_value in [
        signal.get("event_topic"),
        payload.get("event_topic"),
        payload.get("event_type"),
        payload.get("status"),
        payload.get("state"),
    ]:
        topic = str(topic_value or "").strip()
        if topic:
            _append_unique(aliases, f"{source}:topic:{topic}")

    for raw_ref in [*_as_list(signal.get("entity_refs")), *_as_list(payload.get("entity_refs"))]:
        if isinstance(raw_ref, dict):
            ref_type = str(raw_ref.get("type") or raw_ref.get("kind") or "ref").strip()
            ref_id = str(raw_ref.get("id") or raw_ref.get("value") or "").strip()
            if ref_id:
                _append_unique(aliases, ref_id)
                _append_unique(aliases, f"{source}:{ref_type}:{ref_id}")
            continue
        ref_value = str(raw_ref or "").strip()
        if ref_value:
            _append_unique(aliases, ref_value)
            _append_unique(aliases, f"{source}:ref:{ref_value}")
    return aliases


def _signal_event_keys(signal: dict[str, Any]) -> list[str]:
    event_keys: list[str] = []
    for raw_value in [signal.get("event_key"), *_as_list(signal.get("event_aliases")), *_source_aliases(signal)]:
        _append_unique(event_keys, raw_value)
    return event_keys


def _derive_primary_event_key(signal: dict[str, Any]) -> str:
    explicit = str(signal.get("event_key") or "").strip()
    if explicit:
        return explicit
    event_topic = str(_normalized_event_topic(signal) or "").strip()
    source = str(signal.get("source") or "external_signal").strip() or "external_signal"
    if event_topic:
        return f"{source}:topic:{event_topic}"
    aliases = _source_aliases(signal)
    if aliases:
        return str(aliases[0])
    entity_refs = _normalized_entity_refs(signal)
    if entity_refs:
        return f"{source}:ref:{entity_refs[0]}"
    return f"{source}:{SOURCE_DEFAULT_TOPICS.get(source, 'external_update')}"


def dispatch_external_signal(
    *,
    goal_repo: GoalRepository | None,
    policy_repo: PolicyMemoryRepository | None = None,
    task_repo: TaskRepository | None,
    tenant_id: str,
    worker_id: str,
    signal: dict[str, Any],
    trace_id: str,
) -> dict[str, Any]:
    normalized = _normalize_external_signal(signal)
    signal_id = str(normalized.get("signal_id") or f"signal-{uuid.uuid4().hex[:16]}")
    source = str(normalized.get("source") or "external_signal").strip() or "external_signal"
    payload = dict(normalized.get("payload") or {})
    event_keys = _signal_event_keys(normalized)
    payload["source"] = source
    payload["signal_id"] = signal_id
    payload["received_at"] = datetime.now(timezone.utc).isoformat()
    payload["matched_event_keys"] = list(event_keys)

    resumed_by_goal: dict[str, dict[str, Any]] = {}
    for event_key in event_keys:
        matches = resume_waiting_goals_for_event(
            repo=goal_repo,
            tenant_id=tenant_id,
            event_kind="external_signal",
            event_key=event_key,
            event_payload={
                **payload,
                "event_key": event_key,
            },
            user_id=str(signal.get("user_id") or "") or None,
            conversation_id=str(signal.get("conversation_id") or "") or None,
            limit=int(signal.get("limit") or 20),
        )
        for row in matches:
            goal_id = str(row.get("goal_id") or "").strip()
            if goal_id:
                resumed_by_goal[goal_id] = row

    matched_rows = list(resumed_by_goal.values())
    resumed_goal_ids = [
        str(row.get("goal_id") or "")
        for row in matched_rows
        if str(row.get("status") or "") == "ACTIVE"
    ]
    waiting_goal_ids = [
        str(row.get("goal_id") or "")
        for row in matched_rows
        if str(row.get("status") or "") == "WAITING"
    ]

    if policy_repo is not None and (matched_rows or bool(normalized.get("adapter", {}).get("requires_replan"))):
        record_external_signal_feedback(
            repo=policy_repo,
            tenant_id=tenant_id,
            actor_user_id=None,
            feedback={
                "source": source,
                "event_topic": str(normalized.get("payload", {}).get("event_topic") or ""),
                "adapter_outcome": str(normalized.get("payload", {}).get("adapter_outcome") or ""),
                "requires_replan": bool(normalized.get("payload", {}).get("requires_replan")),
                "matched_goal_count": len(matched_rows),
                "resumed_goal_count": len(resumed_goal_ids),
            },
        )
        adapter_outcome = str(normalized.get("payload", {}).get("adapter_outcome") or "")
        portfolio_event_kind = ""
        if adapter_outcome == "success" and resumed_goal_ids:
            portfolio_event_kind = "external_wait_success"
        elif adapter_outcome in {"failure", "timeout"} and matched_rows:
            portfolio_event_kind = "subscription_timeout" if adapter_outcome == "timeout" else "external_wait_failure"
        if portfolio_event_kind:
            for row in matched_rows:
                goal_state = dict(row.get("goal_state") or {})
                agenda = dict(goal_state.get("agenda") or {})
                record_portfolio_feedback(
                    repo=policy_repo,
                    tenant_id=tenant_id,
                    actor_user_id=None,
                    feedback={
                        "event_kind": portfolio_event_kind,
                        "goal_id": str(row.get("goal_id") or ""),
                        "urgency_score": float(agenda.get("priority_score") or 0.0),
                    },
                )

    if task_repo is not None:
        task_repo.insert_audit_log(
            tenant_id=tenant_id,
            actor_user_id=None,
            action="goal_external_signal_ingest",
            target_type="goal_signal",
            target_id=signal_id,
            detail_masked={
                "worker_id": worker_id,
                "source": source,
                "adapter": dict(normalized.get("adapter") or {}),
                "event_keys": event_keys,
                "event_topic": str(payload.get("event_topic") or ""),
                "entity_refs": list(payload.get("entity_refs") or []),
                "matched_goal_count": len(matched_rows),
                "resumed_goal_ids": resumed_goal_ids,
                "still_waiting_goal_ids": waiting_goal_ids,
                "payload_keys": sorted(str(key) for key in payload.keys()),
            },
            trace_id=trace_id,
        )

    return {
        "status": "ok",
        "signal_id": signal_id,
        "source": source,
        "event_key": str(normalized.get("event_key") or ""),
        "adapter": dict(normalized.get("adapter") or {}),
        "event_topic": str(payload.get("event_topic") or ""),
        "entity_refs": list(payload.get("entity_refs") or []),
        "event_keys": event_keys,
        "matched_goal_count": len(matched_rows),
        "resumed_goal_ids": resumed_goal_ids,
        "still_waiting_goal_ids": waiting_goal_ids,
    }


def dispatch_external_adapter_signal(
    *,
    goal_repo: GoalRepository | None,
    policy_repo: PolicyMemoryRepository | None = None,
    task_repo: TaskRepository | None,
    tenant_id: str,
    worker_id: str,
    source: str,
    signal: dict[str, Any],
    trace_id: str,
) -> dict[str, Any]:
    normalized_source = str(source or "").strip().lower().replace("-", "_")
    if normalized_source not in SUPPORTED_EXTERNAL_ADAPTERS:
        raise ValueError(f"unsupported external adapter source: {normalized_source}")
    adapter_signal = {
        **dict(signal or {}),
        "source": normalized_source,
    }
    adapter_signal["event_key"] = _derive_primary_event_key(adapter_signal)
    adapter_signal.setdefault("event_aliases", [])
    payload = dict(adapter_signal.get("payload") or {})
    payload["adapter_source"] = normalized_source
    adapter_signal["payload"] = payload
    return dispatch_external_signal(
        goal_repo=goal_repo,
        policy_repo=policy_repo,
        task_repo=task_repo,
        tenant_id=tenant_id,
        worker_id=worker_id,
        signal=adapter_signal,
        trace_id=trace_id,
    )
