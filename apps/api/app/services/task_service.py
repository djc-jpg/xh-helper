from __future__ import annotations

from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from ..config import settings
from ..input_crypto import decrypt_input_payload, encrypt_input_payload
from ..masking import mask_payload, summarize_payload
from ..metrics import task_budget_usd, task_cost_usd, task_failure_total, task_total
from ..replay_input import NON_REPLAYABLE_INPUT_SENTINEL
from ..repositories import TaskRepository, normalize_task_failure_fields
from ..state_machine import FINAL_STATES


def _is_unique_violation(exc: Exception) -> bool:
    current: Exception | None = exc
    while current is not None:
        sqlstate = getattr(current, "sqlstate", None)
        if str(sqlstate or "") == "23505":
            return True
        current = getattr(current, "__cause__", None)
    return "duplicate key value violates unique constraint" in str(exc).lower()


def _append_step(
    *,
    task_repo: TaskRepository,
    tenant_id: str,
    run_id: str,
    status_text: str,
    step_key: str,
    payload: dict[str, Any],
    trace_id: str,
    span_id: str | None = None,
    attempt: int = 1,
) -> None:
    task_repo.append_step(
        tenant_id=tenant_id,
        run_id=run_id,
        status_text=status_text,
        step_key=step_key,
        payload_masked=mask_payload(payload),
        trace_id=trace_id,
        span_id=span_id,
        attempt=attempt,
    )


async def create_task(
    *,
    task_repo: TaskRepository,
    req: Any,
    tenant_id: str,
    user: dict[str, Any],
    trace_id: str,
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> dict[str, Any]:
    task_total.inc()
    existing = task_repo.get_task_by_client_request_id(tenant_id, req.client_request_id)
    if existing:
        return {
            "task_id": str(existing["id"]),
            "status": existing["status"],
            "trace_id": existing["trace_id"],
            "idempotent": True,
        }

    requires_hitl = req.task_type == "ticket_email"
    try:
        task = task_repo.create_task(
            tenant_id=tenant_id,
            client_request_id=req.client_request_id,
            task_type=req.task_type,
            created_by=str(user["id"]),
            input_masked=mask_payload(req.input),
            input_raw_encrypted=encrypt_input_payload(req.input),
            trace_id=trace_id,
            budget=float(req.budget),
            requires_hitl=requires_hitl,
            conversation_id=getattr(req, "conversation_id", None),
            assistant_turn_id=getattr(req, "assistant_turn_id", None),
            goal_id=getattr(req, "goal_id", None),
            origin=str(getattr(req, "origin", "task_api") or "task_api"),
        )
    except Exception as exc:
        if not _is_unique_violation(exc):
            raise
        existing = task_repo.get_task_by_client_request_id(tenant_id, req.client_request_id)
        if existing:
            return {
                "task_id": str(existing["id"]),
                "status": existing["status"],
                "trace_id": existing["trace_id"],
                "idempotent": True,
            }
        raise HTTPException(status_code=409, detail="task creation conflict") from exc
    task_id = str(task["id"])
    task_budget_usd.labels(task_id=task_id).set(float(req.budget))
    task_cost_usd.labels(task_id=task_id).set(0.0)

    workflow_id = f"task-{task_id}-run-1"
    run = task_repo.create_run(
        tenant_id=tenant_id,
        task_id=task_id,
        run_no=1,
        workflow_id=workflow_id,
        trace_id=trace_id,
        assigned_worker=settings.default_worker_id,
    )
    run_id = str(run["id"])
    task_repo.update_task_status(tenant_id, task_id, "QUEUED")
    _append_step(
        task_repo=task_repo,
        tenant_id=tenant_id,
        run_id=run_id,
        status_text="QUEUED",
        step_key="task_create",
        payload={"task_type": req.task_type, "client_request_id": req.client_request_id},
        trace_id=trace_id,
    )

    payload = {
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "run_no": 1,
        "task_type": req.task_type,
        "input": req.input,
        "user_id": str(user["id"]),
        "trace_id": trace_id,
        "budget": req.budget,
        "requires_hitl": requires_hitl,
        "goal_id": getattr(req, "goal_id", None),
        "global_ttl_sec": 600,
    }
    try:
        await start_workflow(workflow_id, payload)
    except Exception as exc:
        error_code, error_message = normalize_task_failure_fields(
            status_text="FAILED_RETRYABLE",
            error_code="workflow_start_failed",
            error_message=str(exc),
        )
        task_repo.mark_task_failed(
            tenant_id=tenant_id,
            task_id=task_id,
            status_text="FAILED_RETRYABLE",
            error_code=error_code,
            error_message=error_message,
        )
        task_repo.update_run_status(tenant_id, run_id, "FAILED_RETRYABLE")
        _append_step(
            task_repo=task_repo,
            tenant_id=tenant_id,
            run_id=run_id,
            status_text="FAILED_RETRYABLE",
            step_key="workflow_start",
            payload={"error": str(exc)},
            trace_id=trace_id,
        )
        task_failure_total.inc()
        raise HTTPException(status_code=500, detail="failed to start workflow") from exc

    return {"task_id": task_id, "run_id": run_id, "status": "QUEUED", "trace_id": trace_id, "idempotent": False}


async def cancel_task(
    *,
    task_repo: TaskRepository,
    task_id: str,
    tenant_id: str,
    user: dict[str, Any],
    trace_id: str,
    ensure_task_access: Callable[[dict[str, Any], dict[str, Any]], None],
    cancel_workflow: Callable[[str], Awaitable[None]],
) -> dict[str, Any]:
    task = task_repo.get_task_by_id(tenant_id, task_id, include_sensitive=True)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    ensure_task_access(task, user)
    current_status = str(task.get("status") or "")
    if current_status in FINAL_STATES:
        raise HTTPException(status_code=409, detail=f"task already in terminal status {current_status}")

    run = task_repo.get_latest_run_for_task(tenant_id, task_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    run_id = str(run["id"])
    try:
        await cancel_workflow(run["workflow_id"])
    except Exception as exc:
        _append_step(
            task_repo=task_repo,
            tenant_id=tenant_id,
            run_id=run_id,
            status_text=current_status or "RUNNING",
            step_key="task_cancel_failed",
            payload={"requested_by": str(user["id"]), "error": str(exc)},
            trace_id=trace_id,
        )
        task_repo.insert_audit_log(
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            action="task_cancel_failed",
            target_type="task",
            target_id=task_id,
            detail_masked=mask_payload(
                {
                    "task_id": task_id,
                    "run_id": run_id,
                    "error": summarize_payload(str(exc)),
                }
            ),
            trace_id=trace_id,
        )
        raise HTTPException(status_code=502, detail="failed to cancel workflow") from exc

    task_repo.update_run_status(tenant_id, run_id, "CANCELLED")
    task_repo.update_task_status(tenant_id, task_id, "CANCELLED")
    _append_step(
        task_repo=task_repo,
        tenant_id=tenant_id,
        run_id=run_id,
        status_text="CANCELLED",
        step_key="task_cancel",
        payload={"requested_by": str(user["id"])},
        trace_id=trace_id,
    )
    task_repo.insert_audit_log(
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        action="task_cancel",
        target_type="task",
        target_id=task_id,
        detail_masked=mask_payload({"task_id": task_id, "run_id": run_id}),
        trace_id=trace_id,
    )
    return {"task_id": task_id, "status": "CANCELLED"}


async def rerun_task(
    *,
    task_repo: TaskRepository,
    task_id: str,
    tenant_id: str,
    user: dict[str, Any],
    trace_id: str,
    ensure_task_access: Callable[[dict[str, Any], dict[str, Any]], None],
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> dict[str, Any]:
    task = task_repo.get_task_by_id(tenant_id, task_id, include_sensitive=True)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    ensure_task_access(task, user)
    encrypted = str(task.get("input_raw_encrypted") or "")
    if encrypted == NON_REPLAYABLE_INPUT_SENTINEL:
        raise HTTPException(status_code=409, detail="task input is marked non-replayable")
    if not encrypted:
        raise HTTPException(status_code=409, detail="task has no replayable input")
    try:
        replay_input = decrypt_input_payload(encrypted)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="task has invalid replay input") from exc

    if settings.rerun_conflict_test_mode:
        latest_run = task_repo.get_latest_run_for_task(tenant_id, task_id)
        latest_status = str((latest_run or {}).get("status") or "")
        if latest_status and latest_status not in FINAL_STATES:
            raise HTTPException(status_code=409, detail="concurrent rerun conflict")

    run_insert_attempts = 5
    run = None
    run_no = 0
    workflow_id = ""
    for _ in range(run_insert_attempts):
        run_no = task_repo.get_max_run_no(tenant_id, task_id) + 1
        workflow_id = f"task-{task_id}-run-{run_no}"
        try:
            run = task_repo.create_run(
                tenant_id=tenant_id,
                task_id=task_id,
                run_no=run_no,
                workflow_id=workflow_id,
                trace_id=trace_id,
                assigned_worker=settings.default_worker_id,
            )
            break
        except Exception as exc:
            if not _is_unique_violation(exc):
                raise
            continue
    if run is None:
        raise HTTPException(status_code=409, detail="concurrent rerun conflict")
    run_id = str(run["id"])
    task_repo.update_task_status(tenant_id, task_id, "QUEUED")
    _append_step(
        task_repo=task_repo,
        tenant_id=tenant_id,
        run_id=run_id,
        status_text="QUEUED",
        step_key="task_rerun",
        payload={"requested_by": str(user["id"]), "run_no": run_no},
        trace_id=trace_id,
    )
    payload = {
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "run_no": run_no,
        "task_type": task["task_type"],
        "input": replay_input,
        "user_id": str(task["created_by"]),
        "trace_id": trace_id,
        "budget": float(task["budget"]),
        "requires_hitl": bool(task["requires_hitl"]),
        "goal_id": str(task.get("goal_id") or "") or None,
        "global_ttl_sec": 600,
    }
    try:
        await start_workflow(workflow_id, payload)
    except Exception as exc:
        error_code, error_message = normalize_task_failure_fields(
            status_text="FAILED_RETRYABLE",
            error_code="workflow_start_failed",
            error_message=str(exc),
        )
        task_repo.mark_task_failed(
            tenant_id=tenant_id,
            task_id=task_id,
            status_text="FAILED_RETRYABLE",
            error_code=error_code,
            error_message=error_message,
        )
        task_repo.update_run_status(tenant_id, run_id, "FAILED_RETRYABLE")
        _append_step(
            task_repo=task_repo,
            tenant_id=tenant_id,
            run_id=run_id,
            status_text="FAILED_RETRYABLE",
            step_key="workflow_start",
            payload={"error": str(exc), "rerun": True, "run_no": run_no},
            trace_id=trace_id,
        )
        task_repo.insert_audit_log(
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            action="task_rerun_failed",
            target_type="task",
            target_id=task_id,
            detail_masked=mask_payload(
                {
                    "task_id": task_id,
                    "run_id": run_id,
                    "run_no": run_no,
                    "error": summarize_payload(error_message),
                }
            ),
            trace_id=trace_id,
        )
        task_failure_total.inc()
        raise HTTPException(status_code=500, detail="failed to start workflow") from exc
    task_repo.insert_audit_log(
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        action="task_rerun",
        target_type="task",
        target_id=task_id,
        detail_masked=mask_payload(
            {
                "task_id": task_id,
                "run_id": run_id,
                "run_no": run_no,
                "requested_by": str(user["id"]),
            }
        ),
        trace_id=trace_id,
    )
    return {"task_id": task_id, "run_id": run_id, "status": "QUEUED"}
