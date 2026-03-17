from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import httpx
from opentelemetry import trace
from opentelemetry.propagate import inject
from prometheus_client import Counter, REGISTRY
from runtime_backbone import apply_runtime_event, merge_runtime_state, reduce_runtime_state
from temporalio import activity
from temporalio.exceptions import ApplicationError

from config import settings
from graph import run_langgraph
from idempotency import build_tool_call_id, plan_hash
from mas.closed_loop import ClosedLoopCoordinator
from mas.runtime import build_mas_runtime
from qwen_client import qwen_client
from mas.shadow import run_shadow_comparison
from repositories import worker_repo

PROMPT_LEAK_MARKERS = ["system prompt:", "developer message:", "hidden instruction:"]


def _merge_runtime(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    return merge_runtime_state(base, patch)


def _get_or_create_counter(name: str, documentation: str) -> Counter:
    # Re-imports can happen in tests/reload flows; reuse existing collector.
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Counter):
        return existing
    try:
        return Counter(name, documentation)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Counter):
            return existing
        raise


workflow_retries_total = _get_or_create_counter("workflow_retries_total", "Workflow retries")
shadow_runs_total = _get_or_create_counter("mas_shadow_runs_total", "MAS shadow comparisons")
shadow_matches_total = _get_or_create_counter("mas_shadow_matches_total", "MAS shadow exact matches")
shadow_mismatches_total = _get_or_create_counter("mas_shadow_mismatches_total", "MAS shadow mismatches")
tracer = trace.get_tracer("worker.activities")


def _headers() -> dict[str, str]:
    headers = {
        "X-Internal-Token": settings.internal_api_token,
        "X-Worker-Id": settings.worker_id,
        "X-Worker-Token": settings.worker_auth_token or settings.internal_api_token,
    }
    inject(headers)
    return headers


def _status_span_name(step_key: str) -> str:
    if step_key == "planner":
        return "planner"
    if step_key == "review":
        return "review"
    if step_key == "approval_wait":
        return "approval_wait"
    return "status_update"


def _status_event_id(*, run_id: str, status: str, step_key: str, attempt: int) -> str:
    raw = f"{run_id}:{status}:{step_key}:{attempt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _response_reason_code(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
    except Exception:
        return ""
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return str(detail.get("reason_code") or "")
    if isinstance(payload, dict):
        return str(payload.get("reason_code") or "")
    return ""


def _runtime_from_graph_result(
    graph_result: dict[str, Any],
    *,
    status: str,
    current_phase: str,
    latest_result: dict[str, Any] | None = None,
    pending_approvals: list[str] | None = None,
    reflection: dict[str, Any] | None = None,
    final_output: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return apply_runtime_event(
        dict(graph_result.get("agent_runtime") or {}),
        event_type=f"graph.{current_phase}",
        status=status,
        current_phase=current_phase,
        latest_result=latest_result,
        pending_approvals=pending_approvals,
        final_output=final_output,
        summary=str((reflection or {}).get("summary") or ""),
    )


def _agent_runtime_from_mas_result(payload: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    state = dict(result.get("state") or {})
    runtime = state.get("agent_runtime")
    if isinstance(runtime, dict) and runtime:
        return dict(runtime)

    goal_text = str(
        payload.get("input", {}).get("goal")
        or payload.get("input", {}).get("query")
        or payload.get("input", {}).get("message")
        or payload.get("task_type")
        or ""
    )
    status_text = str(result.get("status") or state.get("status") or "FAILED_RETRYABLE")
    return reduce_runtime_state(
        {
            "goal": {"normalized_goal": goal_text, "risk_level": str(state.get("risk_level") or "medium")},
            "unified_task": {"goal": goal_text, "task_type": str(payload.get("task_type") or "")},
            "task_state": {},
        },
        event_type="mas.activity_fallback",
        status=status_text,
        current_phase="respond" if status_text == "SUCCEEDED" else "replan",
        latest_result={
            "status": status_text,
            "reason": str(result.get("reason") or state.get("stop_reason") or ""),
            "failure_type": result.get("failure_type"),
        },
        reflection={
            "summary": str(result.get("reason") or state.get("stop_reason") or "MAS runtime completed."),
            "requires_replan": status_text == "FAILED_RETRYABLE",
            "next_action": "replan" if status_text == "FAILED_RETRYABLE" else "respond",
        },
        current_action={
            "action_type": "respond" if status_text == "SUCCEEDED" else "replan",
            "target": str(state.get("next_route") or "") or None,
            "input": {"turn": int(result.get("turn") or state.get("turn") or 1)},
            "rationale": str(state.get("stop_reason") or "MAS runtime fallback projection."),
            "requires_approval": False,
        },
        policy={
            "selected_action": "respond" if status_text == "SUCCEEDED" else "replan",
            "fallback_action": "respond",
            "approval_triggered": False,
            "planner_action": str(state.get("plan_state") or "mas_closed_loop"),
        },
        route="workflow_task",
    )


def _fallback_review_answer(
    *,
    task_type: str,
    graph_result: dict[str, Any],
    tool_results: list[dict[str, Any]],
    approval: dict[str, Any] | None,
) -> str:
    if task_type == "rag_qa":
        citations = graph_result.get("citations", [])
        return f"Answer based on local evidence. citations={citations}"
    if task_type == "tool_flow":
        return f"Tool flow done. tool_results={tool_results}"
    if task_type == "ticket_email":
        edited = approval.get("edited_output") if approval else None
        return edited or f"Draft approved and actions executed. tool_results={tool_results}"
    return f"Research summary complete. tool_results={tool_results}"


def _review_prompt(
    *,
    task_type: str,
    graph_result: dict[str, Any],
    tool_results: list[dict[str, Any]],
    approval: dict[str, Any] | None,
) -> str:
    citation_text = "; ".join(str(item.get("source") or "doc") for item in list(graph_result.get("citations") or [])[:3]) or "none"
    tool_text = "; ".join(
        str(((item.get("result") or {}).get("results") or [{}])[0].get("title") or item.get("status") or "tool")
        for item in tool_results[:3]
    ) or "none"
    return (
        f"task_type={task_type}\n"
        f"plan={list(graph_result.get('plan') or [])[:4]}\n"
        f"draft={str(graph_result.get('draft_output') or '')[:180]}\n"
        f"review_notes={str(graph_result.get('review_notes') or '')[:80]}\n"
        f"citations={citation_text}\n"
        f"tools={tool_text}\n"
        f"approval={approval or {}}\n"
        "Write one concise plain-text final result for the user. Stay grounded in the provided execution data."
    )


def _review_with_qwen(
    *,
    task_type: str,
    graph_result: dict[str, Any],
    tool_results: list[dict[str, Any]],
    approval: dict[str, Any] | None,
) -> str:
    edited = approval.get("edited_output") if approval else None
    if edited:
        return str(edited)
    fallback = _fallback_review_answer(
        task_type=task_type,
        graph_result=graph_result,
        tool_results=tool_results,
        approval=approval,
    )
    if not qwen_client.is_enabled():
        return fallback
    try:
        text = qwen_client.chat_text(
            system_prompt=(
                "You are preparing the final result for a governed orchestration system. "
                "Return plain text only. Keep the answer grounded in the provided execution data."
            ),
            user_prompt=_review_prompt(
                task_type=task_type,
                graph_result=graph_result,
                tool_results=tool_results,
                approval=approval,
            ),
            temperature=0.2,
            max_tokens=180,
            timeout_s=max(settings.qwen_timeout_s, 90.0),
        )
        return text or fallback
    except Exception:
        return fallback


async def _post_status(
    *,
    tenant_id: str,
    task_id: str,
    run_id: str,
    status: str,
    step_key: str,
    payload: dict[str, Any],
    trace_id: str,
    attempt: int = 1,
    cost: float | None = None,
) -> None:
    span_name = _status_span_name(step_key)
    with tracer.start_as_current_span(span_name):
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "run_id": run_id,
            "status": status,
            "step_key": step_key,
            "payload": payload,
            "trace_id": trace_id,
            "attempt": attempt,
            "status_event_id": _status_event_id(run_id=run_id, status=status, step_key=step_key, attempt=attempt),
        }
        if cost is not None:
            body["cost"] = cost
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{settings.api_base_url}/internal/tasks/{task_id}/status",
                headers=_headers(),
                json=body,
            )
            resp.raise_for_status()


async def _execute_tool_plans(
    *,
    tenant_id: str,
    task_id: str,
    run_id: str,
    task_type: str,
    user_id: str,
    trace_id: str,
    tool_plans: list[dict[str, Any]],
    approval_id: str | None = None,
    step_key: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=20) as client:
        for idx, plan in enumerate(tool_plans, start=1):
            with tracer.start_as_current_span("tool_call") as span:
                tool_call_id = build_tool_call_id(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    step_key=step_key,
                    tool_id=plan["tool_id"],
                    call_seq=idx,
                    plan_payload=plan["payload"],
                )
                span.set_attribute("tool.id", plan["tool_id"])
                span.set_attribute("tool.call_id", tool_call_id)
                body = {
                    "tenant_id": tenant_id,
                    "tool_call_id": tool_call_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "task_type": task_type,
                    "tool_id": plan["tool_id"],
                    "payload": plan["payload"],
                    "caller_user_id": user_id,
                    "approval_id": approval_id,
                    "trace_id": trace_id,
                }
                resp = await client.post(
                    f"{settings.api_base_url}/internal/tool-gateway/execute",
                    headers=_headers(),
                    json=body,
                )
                if resp.status_code >= 400:
                    reason_code = _response_reason_code(resp)
                    retryable = resp.status_code >= 500 or resp.status_code in {408, 429}
                    raise ApplicationError(
                        f"tool_call_http_error status={resp.status_code} reason={reason_code}",
                        type="ToolCallRetryableError" if retryable else "ToolCallNonRetryableError",
                        non_retryable=not retryable,
                    )
                data = resp.json()
                if data["status"] != "SUCCEEDED":
                    reason_code = str(data.get("reason_code") or "")
                    retryable = reason_code in {
                        "adapter_http_408",
                        "adapter_http_429",
                        "adapter_http_5xx",
                        "timeout",
                        "adapter_network_error",
                    }
                    raise ApplicationError(
                        f"tool_denied:{reason_code}",
                        type="ToolCallRetryableError" if retryable else "ToolCallNonRetryableError",
                        non_retryable=not retryable,
                    )
                out.append(data)
                worker_repo.insert_cost(tenant_id=tenant_id, task_id=task_id, run_id=run_id, category="tool", amount=0.001)
    return out


@activity.defn(name="set_status_activity")
async def set_status_activity(payload: dict[str, Any]) -> dict[str, Any]:
    if payload["status"] == "VALIDATING" and int(payload.get("attempt", 1)) > 1:
        workflow_retries_total.inc()
    await _post_status(
        tenant_id=payload["tenant_id"],
        task_id=payload["task_id"],
        run_id=payload["run_id"],
        status=payload["status"],
        step_key=payload["step_key"],
        payload=payload.get("payload", {}),
        trace_id=payload["trace_id"],
        attempt=int(payload.get("attempt", 1)),
        cost=payload.get("cost"),
    )
    return {"ok": True}


@activity.defn(name="validate_activity")
def validate_activity(payload: dict[str, Any]) -> dict[str, Any]:
    task_type = payload["task_type"]
    if task_type not in {"rag_qa", "tool_flow", "ticket_email", "research_summary"}:
        raise ValueError(f"unsupported task_type={task_type}")
    if not isinstance(payload.get("input"), dict):
        raise ValueError("input must be object")
    return {"validated": True}


@activity.defn(name="plan_activity")
def plan_activity(payload: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span("planner"):
        errors: list[str] = []
        models = ["mock-primary", "mock-backup"]
        if settings.qwen_api_key.strip():
            models = [settings.qwen_model, "mock-primary", "mock-backup"]
        for model in models:
            try:
                with tracer.start_as_current_span("llm_call") as llm_span:
                    llm_span.set_attribute("model", model)
                result = run_langgraph(
                    task_type=payload["task_type"],
                    input_payload=payload["input"],
                    thread_id=payload["run_id"],
                    model_hint=model,
                )
                result["model_used"] = model
                result["agent_runtime"] = {
                    "goal": payload["input"].get("goal", {}),
                    "unified_task": payload["input"].get("unified_task", {}),
                    "task_state": result.get("task_state", payload["input"].get("task_state", {})),
                    "current_action": payload["input"].get("current_action", {}),
                    "policy": payload["input"].get("policy", {}),
                    "episodes": payload["input"].get("episodes", []),
                    "planner": payload["input"].get("planner", {}),
                    "plan": result.get("plan", []),
                    "retrieval_hits": result.get("citations", []),
                    "observations": result.get("observations", []),
                    "decision": result.get("decision", {}),
                    "reflection": result.get("reflection", {}),
                    "steps": result.get("agent_steps", []),
                    "current_phase": "plan",
                    "status": "RUNNING",
                }
                result["plan_hash"] = plan_hash(
                    {
                        "task_type": payload["task_type"],
                        "input": payload["input"],
                        "plan": result.get("plan", []),
                        "tool_plans": result.get("tool_plans", []),
                        "pending_tool_plans": result.get("pending_tool_plans", []),
                    }
                )
                return result
            except Exception as exc:
                errors.append(f"{model}:{exc}")
        raise RuntimeError("all planning models failed -> " + " | ".join(errors))


@activity.defn(name="execute_tools_activity")
async def execute_tools_activity(payload: dict[str, Any]) -> dict[str, Any]:
    results = await _execute_tool_plans(
        tenant_id=payload["tenant_id"],
        task_id=payload["task_id"],
        run_id=payload["run_id"],
        task_type=payload["task_type"],
        user_id=payload["user_id"],
        trace_id=payload["trace_id"],
        tool_plans=payload.get("tool_plans", []),
        approval_id=payload.get("approval_id"),
        step_key="execute_tools_activity",
    )
    return {"tool_results": results}


@activity.defn(name="create_approval_activity")
def create_approval_activity(payload: dict[str, Any]) -> dict[str, Any]:
    approval_id = worker_repo.insert_approval(
        tenant_id=payload["tenant_id"],
        task_id=payload["task_id"],
        run_id=payload["run_id"],
        requested_by=payload["user_id"],
        reason="ticket_email requires human approval",
    )
    return {"approval_id": approval_id}


@activity.defn(name="review_activity")
async def review_activity(payload: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span("review"):
        task_id = payload["task_id"]
        tenant_id = payload["tenant_id"]
        run_id = payload["run_id"]
        task_type = payload["task_type"]
        trace_id = payload["trace_id"]
        graph_result = payload["graph_result"]
        tool_results = list(payload.get("tool_results", []))
        approval = payload.get("approval")

        pending = graph_result.get("pending_tool_plans", [])
        if pending:
            if not approval or approval.get("decision") != "APPROVED":
                raise RuntimeError("approval required for pending tools")
            extra = await _execute_tool_plans(
                tenant_id=tenant_id,
                task_id=task_id,
                run_id=run_id,
                task_type=task_type,
                user_id=payload["user_id"],
                trace_id=trace_id,
                tool_plans=pending,
                approval_id=approval.get("approval_id"),
                step_key="review_activity_pending_tools",
            )
            tool_results.extend(extra)

        answer = _review_with_qwen(
            task_type=task_type,
            graph_result=graph_result,
            tool_results=tool_results,
            approval=approval,
        )

        lowered = answer.lower()
        for marker in PROMPT_LEAK_MARKERS:
            if marker in lowered:
                answer = answer.replace(marker, "[redacted]")

        token_in = max(1, len(str(graph_result)) // 4)
        token_out = max(1, len(answer) // 4)
        llm_cost = round((token_in + token_out) * 0.000002, 6)
        total_cost = llm_cost + (len(tool_results) * 0.001)

        worker_repo.insert_cost(
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=run_id,
            category="llm",
            amount=llm_cost,
            token_in=token_in,
            token_out=token_out,
        )

        artifact_dir = Path(settings.artifact_dir) / "runs" / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_file = artifact_dir / "result.json"
        artifact_payload = {
            "task_id": task_id,
            "run_id": run_id,
            "answer": answer,
            "plan_hash": graph_result.get("plan_hash"),
            "citations": graph_result.get("citations", []),
            "tool_results": tool_results,
        }
        artifact_file.write_text(json.dumps(artifact_payload, ensure_ascii=True, indent=2), encoding="utf-8")
        worker_repo.insert_artifact(
            tenant_id=tenant_id,
            task_id=task_id,
            run_id=run_id,
            artifact_type="run_result",
            uri=str(artifact_file),
            metadata={"trace_id": trace_id, "citations": graph_result.get("citations", [])},
        )

        return {
            "output": answer,
            "plan_hash": graph_result.get("plan_hash"),
            "citations": graph_result.get("citations", []),
            "tool_results": tool_results,
            "token_in": token_in,
            "token_out": token_out,
            "cost": total_cost,
            "agent_runtime": {
                **dict(graph_result.get("agent_runtime") or {}),
                "status": "SUCCEEDED",
                "current_phase": "reflect",
                "task_state": {
                    **dict((graph_result.get("agent_runtime") or {}).get("task_state") or {}),
                    "current_phase": "reflect",
                    "latest_result": {"status": "SUCCEEDED", "output": answer},
                },
                "reflection": dict(graph_result.get("reflection") or {}),
                "final_output": {
                    "message": answer,
                    "citations": graph_result.get("citations", []),
                    "tool_results": tool_results,
                },
            },
        }


async def _mas_execution_placeholder(task: dict[str, Any]) -> dict[str, Any]:
    # Main task execution remains in Temporal workflow activities.
    return {"accepted": True, "task_id": str(task.get("task_id") or "")}


def _build_mas_gate_task(payload: dict[str, Any]) -> dict[str, Any]:
    raw_input = payload.get("input", {})
    input_payload = raw_input if isinstance(raw_input, dict) else {}
    budget = float(payload.get("budget") or 1.0)
    estimated_cost = input_payload.get("estimated_cost", budget)
    estimated_minutes = input_payload.get("estimated_minutes", 0)
    deadline_minutes = input_payload.get("deadline_minutes", 0)
    priority = input_payload.get("priority", 0)
    try:
        estimated_cost_value = float(estimated_cost)
    except Exception:
        estimated_cost_value = budget
    try:
        estimated_minutes_value = int(estimated_minutes)
    except Exception:
        estimated_minutes_value = 0
    try:
        deadline_minutes_value = int(deadline_minutes)
    except Exception:
        deadline_minutes_value = 0
    try:
        priority_value = int(priority)
    except Exception:
        priority_value = 0
    return {
        "task_id": str(payload.get("task_id") or ""),
        "run_id": str(payload.get("run_id") or ""),
        "task_type": str(payload.get("task_type") or ""),
        "status": "QUEUED",
        "budget": budget,
        "estimated_cost": estimated_cost_value,
        "estimated_minutes": estimated_minutes_value,
        "deadline_minutes": deadline_minutes_value,
        "priority": priority_value,
    }


@activity.defn(name="mas_orchestrate_activity")
async def mas_orchestrate_activity(payload: dict[str, Any]) -> dict[str, Any]:
    if not settings.mas_enabled:
        return {"enabled": False, "status": "SKIPPED"}

    mode = str(getattr(settings, "mas_orchestration_mode", "closed_loop") or "closed_loop").strip().lower()

    if mode in {"gate", "legacy_gate"}:
        task = _build_mas_gate_task(payload)
        try:
            coordinator = await build_mas_runtime(settings=settings, task_handler=_mas_execution_placeholder)
            coordinator.submit_task(task)
            summary = await coordinator.run_until_idle(max_cycles=20)
        except Exception as exc:
            return {
                "enabled": True,
                "mode": "gate",
                "status": "FAILED_RETRYABLE",
                "error": str(exc),
                "task_id": task["task_id"],
                "run_id": task["run_id"],
            }

        final_task = (summary.get("tasks") or {}).get(task["task_id"], task)
        return {
            "enabled": True,
            "mode": "gate",
            "status": str(final_task.get("status") or "FAILED_RETRYABLE"),
            "task_id": task["task_id"],
            "run_id": task["run_id"],
            "cycles": int(summary.get("cycles") or 0),
            "pending": int(summary.get("pending") or 0),
        }

    task_id = str(payload.get("task_id") or "")
    run_id = str(payload.get("run_id") or "")
    try:
        coordinator = ClosedLoopCoordinator()
        result = await coordinator.run(payload)
        return {
            "enabled": True,
            "mode": "closed_loop_primary",
            "agent_runtime": _agent_runtime_from_mas_result(payload, result),
            **result,
        }
    except Exception as exc:
        return {
            "enabled": True,
            "mode": "closed_loop_primary",
            "status": "FAILED_RETRYABLE",
            "error": str(exc),
            "task_id": task_id,
            "run_id": run_id,
            "state": {
                "task_state": "FAILED_RETRYABLE",
                "plan_state": "UNPLANNED",
                "evidence": [],
                "risk_level": "unknown",
                "retry_budget": 0,
                "latency_budget": 0,
            },
            "protocol_messages": [],
            "failure_type": "ORCHESTRATION_RUNTIME_ERROR",
            "failure_semantic": "FAIL_RETRYABLE",
            "result": None,
        }


@activity.defn(name="shadow_compare_activity")
async def shadow_compare_activity(payload: dict[str, Any]) -> dict[str, Any]:
    if not settings.mas_shadow_mode:
        return {"enabled": False}

    task_payload = dict(payload.get("task_payload") or {})
    actual_status = str(payload.get("actual_status") or "")
    run_id = str(payload.get("run_id") or task_payload.get("run_id") or "")
    task_id = str(payload.get("task_id") or task_payload.get("task_id") or "")
    trace_id = str(payload.get("trace_id") or task_payload.get("trace_id") or "")

    comparison = await run_shadow_comparison(task_payload=task_payload, actual_status=actual_status)
    now_ts = time.time()

    shadow_runs_total.inc()
    if comparison["comparable"]:
        if comparison["consistent"]:
            shadow_matches_total.inc()
        else:
            shadow_mismatches_total.inc()

    out_dir = Path(settings.artifact_dir) / "shadow_mode"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{run_id or 'unknown-run'}.json"
    out_payload = {
        "task_id": task_id,
        "run_id": run_id,
        "trace_id": trace_id,
        "task_type": str(task_payload.get("task_type") or ""),
        "timestamp": now_ts,
        "actual_status": comparison["actual_status"],
        "predicted_status": comparison["predicted_status"],
        "comparable": comparison["comparable"],
        "consistent": comparison["consistent"],
        "path": comparison["path"],
        "approval_status": comparison["approval_status"],
        "execution_status": comparison["execution_status"],
    }
    out_file.write_text(json.dumps(out_payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return {"enabled": True, **out_payload}
