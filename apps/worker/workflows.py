from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from runtime_backbone import apply_runtime_event

try:
    from .failure_classification import classify_failure_status
except ImportError:  # pragma: no cover - runtime module path fallback
    from failure_classification import classify_failure_status

with workflow.unsafe.imports_passed_through():
    from activities import (
        create_approval_activity,
        execute_tools_activity,
        mas_orchestrate_activity,
        plan_activity,
        review_activity,
        set_status_activity,
        shadow_compare_activity,
        validate_activity,
    )


def _runtime_patch(
    base_runtime: dict[str, Any],
    *,
    status: str,
    current_phase: str,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    reflection: dict[str, Any] | None = None,
    final_output: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return apply_runtime_event(
        base_runtime,
        event_type=f"workflow.{current_phase}",
        status=status,
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        final_output=final_output,
        summary=str((reflection or {}).get("summary") or ""),
    )


@workflow.defn(name="TaskWorkflow")
class TaskWorkflow:
    def __init__(self) -> None:
        self._approval: dict[str, Any] | None = None

    @workflow.signal(name="approval_signal")
    def approval_signal(self, payload: dict[str, Any]) -> None:
        incoming_key = (
            str(payload.get("approval_id") or ""),
            str(payload.get("decision") or ""),
            str(payload.get("edited_output") or ""),
        )
        if self._approval is not None:
            current_key = (
                str(self._approval.get("approval_id") or ""),
                str(self._approval.get("decision") or ""),
                str(self._approval.get("edited_output") or ""),
            )
            if incoming_key == current_key:
                return
            # Ignore conflicting duplicate signals to keep workflow state monotonic.
            return
        self._approval = payload

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = payload["tenant_id"]
        task_id = payload["task_id"]
        run_id = payload["run_id"]
        trace_id = payload["trace_id"]
        attempt = workflow.info().attempt

        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=3,
        )

        async def _record_shadow(actual_status: str) -> None:
            try:
                await workflow.execute_activity(
                    shadow_compare_activity,
                    {
                        "tenant_id": tenant_id,
                        "task_id": task_id,
                        "run_id": run_id,
                        "trace_id": trace_id,
                        "task_payload": payload,
                        "actual_status": actual_status,
                    },
                    start_to_close_timeout=timedelta(seconds=20),
                )
            except Exception:
                # Shadow mode must never impact primary workflow correctness.
                return

        try:
            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": "VALIDATING",
                    "step_key": "validate",
                    "payload": {
                        "attempt": attempt,
                        "agent_runtime": _runtime_patch(
                            dict(payload.get("input", {}).get("runtime_state") or {}),
                            status="VALIDATING",
                            current_phase="validate",
                        ),
                    },
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

            await workflow.execute_activity(
                validate_activity,
                payload,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=retry_policy,
            )

            mas_gate = await workflow.execute_activity(
                mas_orchestrate_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "task_type": payload["task_type"],
                    "budget": payload.get("budget", 1.0),
                    "input": payload.get("input", {}),
                    "trace_id": trace_id,
                },
                start_to_close_timeout=timedelta(seconds=20),
            )
            if bool(mas_gate.get("enabled")):
                mas_status = str(mas_gate.get("status") or "")
                mas_mode = str(mas_gate.get("mode") or "gate")
                if mas_mode == "closed_loop_primary":
                    mas_turn = int(mas_gate.get("turn") or 1)
                    mas_runtime = dict(mas_gate.get("agent_runtime") or {})
                    if mas_status == "SUCCEEDED":
                        mas_result = dict(mas_gate.get("result") or {})
                        await workflow.execute_activity(
                            set_status_activity,
                            {
                                "tenant_id": tenant_id,
                                "task_id": task_id,
                                "run_id": run_id,
                                "status": "PLANNING",
                                "step_key": "mas_planner",
                                "payload": {
                                    "mode": mas_mode,
                                    "turn": mas_turn,
                                    "agent_runtime": _runtime_patch(
                                        mas_runtime,
                                        status="PLANNING",
                                        current_phase="plan",
                                    ),
                                },
                                "trace_id": trace_id,
                                "attempt": attempt,
                            },
                            start_to_close_timeout=timedelta(seconds=5),
                        )
                        await workflow.execute_activity(
                            set_status_activity,
                            {
                                "tenant_id": tenant_id,
                                "task_id": task_id,
                                "run_id": run_id,
                                "status": "RUNNING",
                                "step_key": "mas_execution",
                                "payload": {
                                    "mode": mas_mode,
                                    "turn": mas_turn,
                                    "agent_runtime": _runtime_patch(
                                        mas_runtime,
                                        status="RUNNING",
                                        current_phase="act",
                                    ),
                                },
                                "trace_id": trace_id,
                                "attempt": attempt,
                            },
                            start_to_close_timeout=timedelta(seconds=5),
                        )
                        await workflow.execute_activity(
                            set_status_activity,
                            {
                                "tenant_id": tenant_id,
                                "task_id": task_id,
                                "run_id": run_id,
                                "status": "REVIEWING",
                                "step_key": "mas_critic",
                                "payload": {
                                    "mode": mas_mode,
                                    "turn": mas_turn,
                                    "failure_semantic": mas_gate.get("failure_semantic"),
                                    "agent_runtime": _runtime_patch(
                                        mas_runtime,
                                        status="REVIEWING",
                                        current_phase="reflect",
                                    ),
                                },
                                "trace_id": trace_id,
                                "attempt": attempt,
                            },
                            start_to_close_timeout=timedelta(seconds=5),
                        )
                        await workflow.execute_activity(
                            set_status_activity,
                            {
                                "tenant_id": tenant_id,
                                "task_id": task_id,
                                "run_id": run_id,
                                "status": "SUCCEEDED",
                                "step_key": "done",
                                "payload": {
                                    "output": mas_result.get("output"),
                                    "artifacts": mas_result.get("artifacts", []),
                                    "tool_logs": mas_result.get("tool_logs", []),
                                    "evidence": mas_result.get("evidence", []),
                                    "turn": mas_result.get("turn", mas_turn),
                                    "agent_runtime": _runtime_patch(
                                        mas_runtime,
                                        status="SUCCEEDED",
                                        current_phase="respond",
                                        latest_result={"status": "SUCCEEDED", "output": mas_result.get("output")},
                                        final_output={
                                            "message": mas_result.get("output"),
                                            "artifacts": mas_result.get("artifacts", []),
                                            "tool_logs": mas_result.get("tool_logs", []),
                                            "evidence": mas_result.get("evidence", []),
                                        },
                                    ),
                                },
                                "trace_id": trace_id,
                                "attempt": attempt,
                            },
                            start_to_close_timeout=timedelta(seconds=5),
                        )
                        await _record_shadow("SUCCEEDED")
                        return {"status": "SUCCEEDED", "result": mas_result}

                    fail_status = "FAILED_FINAL" if mas_status == "FAILED_FINAL" else "FAILED_RETRYABLE"
                    await workflow.execute_activity(
                        set_status_activity,
                        {
                            "tenant_id": tenant_id,
                            "task_id": task_id,
                            "run_id": run_id,
                            "status": fail_status,
                            "step_key": "mas_closed_loop_failure",
                            "payload": {
                                "mas_gate": mas_gate,
                                "mode": mas_mode,
                                "agent_runtime": _runtime_patch(
                                    mas_runtime,
                                    status=fail_status,
                                    current_phase="reflect",
                                    latest_result={
                                        "status": fail_status,
                                        "reason": mas_gate.get("reason"),
                                        "failure_type": mas_gate.get("failure_type"),
                                    },
                                    reflection={
                                        "summary": str(mas_gate.get("reason") or "MAS closed loop failed."),
                                        "requires_replan": fail_status == "FAILED_RETRYABLE",
                                        "next_action": "replan" if fail_status == "FAILED_RETRYABLE" else "stop",
                                    },
                                ),
                            },
                            "trace_id": trace_id,
                            "attempt": attempt,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await _record_shadow(fail_status)
                    return {
                        "status": fail_status,
                        "reason": "mas_closed_loop_failed",
                    }

                if mas_status == "REJECTED":
                    await workflow.execute_activity(
                        set_status_activity,
                        {
                            "tenant_id": tenant_id,
                            "task_id": task_id,
                            "run_id": run_id,
                            "status": "FAILED_FINAL",
                            "step_key": "mas_gate_rejected",
                            "payload": {
                                "mas_gate": mas_gate,
                                "agent_runtime": _runtime_patch(
                                    dict(mas_gate.get("agent_runtime") or {}),
                                    status="FAILED_FINAL",
                                    current_phase="reflect",
                                    latest_result={"status": "FAILED_FINAL", "reason": "mas_gate_rejected"},
                                    reflection={
                                        "summary": "MAS gate rejected the task before execution.",
                                        "requires_replan": False,
                                        "next_action": "stop",
                                    },
                                ),
                            },
                            "trace_id": trace_id,
                            "attempt": attempt,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await _record_shadow("FAILED_FINAL")
                    return {"status": "FAILED_FINAL", "reason": "mas_gate_rejected"}
                if mas_status != "SUCCEEDED":
                    await workflow.execute_activity(
                        set_status_activity,
                        {
                            "tenant_id": tenant_id,
                            "task_id": task_id,
                            "run_id": run_id,
                            "status": "FAILED_RETRYABLE",
                            "step_key": "mas_gate_error",
                            "payload": {
                                "mas_gate": mas_gate,
                                "agent_runtime": _runtime_patch(
                                    dict(mas_gate.get("agent_runtime") or {}),
                                    status="FAILED_RETRYABLE",
                                    current_phase="reflect",
                                    latest_result={"status": "FAILED_RETRYABLE", "reason": "mas_gate_error"},
                                    reflection={
                                        "summary": "MAS gate failed before durable execution could proceed.",
                                        "requires_replan": True,
                                        "next_action": "replan",
                                    },
                                ),
                            },
                            "trace_id": trace_id,
                            "attempt": attempt,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await _record_shadow("FAILED_RETRYABLE")
                    return {"status": "FAILED_RETRYABLE", "reason": "mas_gate_error"}

            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": "PLANNING",
                    "step_key": "planner",
                    "payload": {
                        "task_type": payload["task_type"],
                        "agent_runtime": _runtime_patch(
                            dict(payload.get("input", {}).get("runtime_state") or {}),
                            status="PLANNING",
                            current_phase="plan",
                        ),
                    },
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

            graph_result = await workflow.execute_activity(
                plan_activity,
                payload,
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=retry_policy,
            )

            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                        "status": "RUNNING",
                        "step_key": "running",
                        "payload": {
                            "stage": "execution",
                            "agent_runtime": _runtime_patch(
                                dict(graph_result.get("agent_runtime") or {}),
                                status="RUNNING",
                                current_phase="act",
                            ),
                        },
                        "trace_id": trace_id,
                        "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                        "status": "WAITING_TOOL",
                        "step_key": "tool_prepare",
                        "payload": {
                            "tool_plan_count": len(graph_result.get("tool_plans", [])),
                            "agent_runtime": _runtime_patch(
                                dict(graph_result.get("agent_runtime") or {}),
                                status="WAITING_TOOL",
                                current_phase="act",
                                latest_result={"status": "WAITING_TOOL", "planned_tool_calls": len(graph_result.get("tool_plans", []))},
                            ),
                        },
                        "trace_id": trace_id,
                        "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

            pre_tool = await workflow.execute_activity(
                execute_tools_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "task_type": payload["task_type"],
                    "user_id": payload["user_id"],
                    "trace_id": trace_id,
                    "tool_plans": graph_result.get("tool_plans", []),
                },
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy,
            )

            if graph_result.get("requires_hitl"):
                approval_meta = await workflow.execute_activity(
                    create_approval_activity,
                    {"tenant_id": tenant_id, "task_id": task_id, "run_id": run_id, "user_id": payload["user_id"]},
                    start_to_close_timeout=timedelta(seconds=5),
                )
                approval_id = approval_meta["approval_id"]
                await workflow.execute_activity(
                    set_status_activity,
                    {
                        "tenant_id": tenant_id,
                        "task_id": task_id,
                        "run_id": run_id,
                        "status": "WAITING_HUMAN",
                        "step_key": "approval_wait",
                        "payload": {
                            "approval_id": approval_id,
                            "agent_runtime": _runtime_patch(
                                dict(graph_result.get("agent_runtime") or {}),
                                status="WAITING_HUMAN",
                                current_phase="wait",
                                latest_result={"status": "WAITING_HUMAN", "approval_id": approval_id},
                                pending_approvals=[approval_id],
                                reflection={
                                    "summary": "Execution is paused pending explicit human approval.",
                                    "requires_replan": False,
                                    "next_action": "wait",
                                },
                            ),
                        },
                        "trace_id": trace_id,
                        "attempt": attempt,
                    },
                    start_to_close_timeout=timedelta(seconds=5),
                )
                ttl = int(payload.get("global_ttl_sec", 600))
                timed_out = False
                try:
                    await workflow.wait_condition(lambda: self._approval is not None, timeout=timedelta(seconds=ttl))
                except TimeoutError:
                    timed_out = True

                if timed_out or self._approval is None:
                    await workflow.execute_activity(
                        set_status_activity,
                        {
                            "tenant_id": tenant_id,
                            "task_id": task_id,
                            "run_id": run_id,
                            "status": "TIMED_OUT",
                            "step_key": "approval_timeout",
                            "payload": {
                                "ttl": ttl,
                                "agent_runtime": _runtime_patch(
                                    dict(graph_result.get("agent_runtime") or {}),
                                    status="TIMED_OUT",
                                    current_phase="wait",
                                    latest_result={"status": "TIMED_OUT", "ttl": ttl},
                                    pending_approvals=[approval_id],
                                    reflection={
                                        "summary": "Approval wait expired before a human decision arrived.",
                                        "requires_replan": False,
                                        "next_action": "stop",
                                    },
                                ),
                            },
                            "trace_id": trace_id,
                            "attempt": attempt,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await _record_shadow("TIMED_OUT")
                    return {"status": "TIMED_OUT"}
                if self._approval and self._approval.get("decision") == "REJECTED":
                    await workflow.execute_activity(
                        set_status_activity,
                        {
                            "tenant_id": tenant_id,
                            "task_id": task_id,
                            "run_id": run_id,
                            "status": "FAILED_FINAL",
                            "step_key": "approval_rejected",
                            "payload": {
                                "approval": self._approval,
                                "agent_runtime": _runtime_patch(
                                    dict(graph_result.get("agent_runtime") or {}),
                                    status="FAILED_FINAL",
                                    current_phase="reflect",
                                    latest_result={"status": "FAILED_FINAL", "approval": self._approval},
                                    pending_approvals=[],
                                    reflection={
                                        "summary": "Human approval rejected the gated action.",
                                        "requires_replan": False,
                                        "next_action": "stop",
                                    },
                                ),
                            },
                            "trace_id": trace_id,
                            "attempt": attempt,
                        },
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                    await _record_shadow("FAILED_FINAL")
                    return {"status": "FAILED_FINAL", "reason": "approval_rejected"}

            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": "REVIEWING",
                    "step_key": "review",
                    "payload": {
                        "agent_runtime": _runtime_patch(
                            dict(graph_result.get("agent_runtime") or {}),
                            status="REVIEWING",
                            current_phase="reflect",
                            latest_result={"status": "REVIEWING", "tool_results": len(pre_tool.get("tool_results", []))},
                        ),
                    },
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )

            reviewed = await workflow.execute_activity(
                review_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "task_type": payload["task_type"],
                    "user_id": payload["user_id"],
                    "trace_id": trace_id,
                    "graph_result": graph_result,
                    "tool_results": pre_tool.get("tool_results", []),
                    "approval": self._approval,
                },
                start_to_close_timeout=timedelta(seconds=150),
                retry_policy=retry_policy,
            )

            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": "SUCCEEDED",
                    "step_key": "done",
                    "payload": {
                        "output": reviewed["output"],
                        "plan_hash": reviewed.get("plan_hash"),
                        "citations": reviewed.get("citations", []),
                        "tool_results": reviewed.get("tool_results", []),
                        "agent_runtime": reviewed.get("agent_runtime", {}),
                    },
                    "cost": reviewed.get("cost", 0.0),
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )
            await _record_shadow("SUCCEEDED")
            return {"status": "SUCCEEDED", "result": reviewed}

        except asyncio.CancelledError:
            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": "CANCELLED",
                    "step_key": "cancelled",
                    "payload": {
                        "reason": "workflow cancelled",
                        "agent_runtime": _runtime_patch(
                            dict(payload.get("input", {}).get("runtime_state") or {}),
                            status="CANCELLED",
                            current_phase="observe",
                            latest_result={"status": "CANCELLED", "reason": "workflow cancelled"},
                            reflection={
                                "summary": "Workflow was cancelled before completion.",
                                "requires_replan": False,
                                "next_action": "stop",
                            },
                        ),
                    },
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )
            await _record_shadow("CANCELLED")
            raise
        except Exception as exc:
            fail_status = classify_failure_status(exc)
            await workflow.execute_activity(
                set_status_activity,
                {
                    "tenant_id": tenant_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": fail_status,
                    "step_key": "workflow_error",
                    "payload": {
                        "error": str(exc),
                        "agent_runtime": _runtime_patch(
                            dict(payload.get("input", {}).get("runtime_state") or {}),
                            status=fail_status,
                            current_phase="reflect",
                            latest_result={"status": fail_status, "error": str(exc)},
                            reflection={
                                "summary": f"Workflow raised an exception: {str(exc)}",
                                "requires_replan": fail_status == "FAILED_RETRYABLE",
                                "next_action": "replan" if fail_status == "FAILED_RETRYABLE" else "stop",
                            },
                        ),
                    },
                    "trace_id": trace_id,
                    "attempt": attempt,
                },
                start_to_close_timeout=timedelta(seconds=5),
            )
            await _record_shadow(fail_status)
            return {"status": fail_status, "error": str(exc)}
