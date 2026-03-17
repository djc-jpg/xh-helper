from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from ..config import settings
from ..masking import mask_payload
from ..repositories import GoalRepository
from ..repositories import TaskRepository
from .goal_runtime_service import resume_waiting_goals_for_event

logger = logging.getLogger(__name__)


def _retry_delay_s(attempt_count: int) -> int:
    base = max(1, int(settings.approval_signal_retry_base_delay_s))
    cap = max(base, int(settings.approval_signal_retry_max_delay_s))
    return min(cap, base * (2 ** max(0, attempt_count)))


async def dispatch_pending_approval_signals(
    *,
    task_repo: TaskRepository,
    signal_approval: Callable[[str, dict[str, Any]], Awaitable[None]],
    max_items: int | None = None,
) -> dict[str, int]:
    limit = max_items if max_items is not None else int(settings.approval_signal_dispatch_batch_size)
    processed = 0
    sent = 0
    failed = 0

    for _ in range(max(1, limit)):
        row = task_repo.claim_next_approval_signal_outbox()
        if not row:
            break
        processed += 1
        outbox_id = str(row["id"])
        workflow_id = str(row["workflow_id"])
        payload = row.get("signal_payload") or {}
        attempt_count = int(row.get("attempt_count") or 0)
        try:
            await signal_approval(workflow_id, payload)
            task_repo.mark_approval_signal_sent(outbox_id)
            sent += 1
        except Exception as exc:  # pragma: no cover - covered via service tests with mocks
            delay = _retry_delay_s(attempt_count + 1)
            state = task_repo.mark_approval_signal_failure(
                outbox_id=outbox_id,
                error_message=str(exc),
                retry_delay_s=delay,
                max_attempts=int(settings.approval_signal_retry_max_attempts),
            )
            failed += 1
            logger.warning(
                "approval_signal_dispatch_failed outbox_id=%s workflow_id=%s status=%s attempts=%s error=%s",
                outbox_id,
                workflow_id,
                state.get("status"),
                state.get("attempt_count"),
                exc,
            )

    return {"processed": processed, "sent": sent, "failed": failed}


async def run_approval_signal_dispatcher(
    *,
    task_repo: TaskRepository,
    signal_approval: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> None:
    interval = max(0.5, float(settings.approval_signal_dispatch_interval_s))
    while True:
        try:
            await dispatch_pending_approval_signals(task_repo=task_repo, signal_approval=signal_approval)
        except Exception as exc:  # pragma: no cover - defensive loop guard
            logger.exception("approval_signal_dispatcher_loop_error error=%s", exc)
        await asyncio.sleep(interval)


async def apply_approval_decision(
    *,
    action: str,
    approval_id: str,
    tenant_id: str,
    actor_user_id: str,
    reason: str | None,
    edited_output: str | None,
    trace_id: str,
    task_repo: TaskRepository,
    signal_approval: Callable[[str, dict[str, Any]], Awaitable[None]],
    goal_repo: GoalRepository | None = None,
) -> dict[str, Any]:
    decision_map = {
        "approve": ("APPROVED", "approval_approve"),
        "reject": ("REJECTED", "approval_reject"),
        "edit": ("EDITED", "approval_edit"),
    }
    if action not in decision_map:
        raise HTTPException(status_code=400, detail="unsupported action")
    status_text, audit_action = decision_map[action]

    signal_payload: dict[str, Any]
    if action == "reject":
        signal_payload = {"decision": "REJECTED", "approval_id": approval_id}
    elif action == "edit":
        signal_payload = {"decision": "APPROVED", "approval_id": approval_id, "edited_output": edited_output}
    else:
        signal_payload = {"decision": "APPROVED", "approval_id": approval_id, "edited_output": None}

    try:
        decision_result = task_repo.apply_approval_decision_with_outbox(
            tenant_id=tenant_id,
            approval_id=approval_id,
            status_text=status_text,
            decided_by=actor_user_id,
            reason=reason,
            edited_output=edited_output,
            signal_payload=signal_payload,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="approval not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="approval already decided") from exc

    dispatch_result = await dispatch_pending_approval_signals(
        task_repo=task_repo,
        signal_approval=signal_approval,
        max_items=1,
    )

    detail: dict[str, Any] = {"reason": reason}
    if action == "edit":
        detail["edited_output"] = mask_payload(edited_output)
    detail["idempotent"] = bool(decision_result.get("idempotent"))
    detail["outbox_status"] = decision_result.get("outbox_status")
    detail["signal_dispatch_sent"] = int(dispatch_result.get("sent") or 0)
    detail["signal_dispatch_failed"] = int(dispatch_result.get("failed") or 0)
    task_repo.insert_audit_log(
        tenant_id=tenant_id,
        actor_user_id=actor_user_id,
        action=audit_action,
        target_type="approval",
        target_id=approval_id,
        detail_masked=detail,
        trace_id=trace_id,
    )
    resume_waiting_goals_for_event(
        repo=goal_repo,
        tenant_id=tenant_id,
        event_kind="approval",
        event_key=approval_id,
        event_payload={
            "approval_id": approval_id,
            "decision": status_text,
            "reason": reason,
            "edited_output": edited_output,
        },
        limit=20,
    )
    return {
        "approval_id": approval_id,
        "status": status_text,
        "idempotent": bool(decision_result.get("idempotent")),
        "outbox_status": decision_result.get("outbox_status"),
    }
