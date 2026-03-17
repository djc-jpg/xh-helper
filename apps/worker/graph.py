from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from opentelemetry import trace
from prometheus_client import Counter, Gauge, REGISTRY
from runtime_backbone import runtime_requires_approval, should_prepare_tools

from config import settings
from qwen_client import qwen_client

GraphApp = Any
_GRAPH: GraphApp | None = None
_CHECKPOINTER: Any | None = None
_CHECKPOINTER_CONTEXT: Any | None = None
_GRAPH_LOCK = threading.RLock()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer("worker.graph")


def _get_or_create_counter(name: str, documentation: str) -> Counter:
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


def _get_or_create_gauge(name: str, documentation: str) -> Gauge:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Gauge):
        return existing
    try:
        return Gauge(name, documentation)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Gauge):
            return existing
        raise


checkpoint_error_total = _get_or_create_counter(
    "worker_langgraph_checkpoint_error_total",
    "Total LangGraph checkpoint initialization/runtime errors.",
)
checkpoint_degraded = _get_or_create_gauge(
    "worker_langgraph_checkpoint_degraded",
    "1 when worker checkpoint backend is degraded to in-memory fallback due to errors.",
)


class AgentState(TypedDict, total=False):
    task_type: str
    input: dict[str, Any]
    model_hint: str
    goal: dict[str, Any]
    unified_task: dict[str, Any]
    task_state: dict[str, Any]
    current_action: dict[str, Any]
    policy: dict[str, Any]
    episodes: list[dict[str, Any]]
    plan: list[str]
    retrievals: list[dict[str, Any]]
    citations: list[dict[str, Any]]
    tool_plans: list[dict[str, Any]]
    pending_tool_plans: list[dict[str, Any]]
    draft_output: str
    final_output: str
    review_notes: str
    requires_hitl: bool
    agent_steps: list[dict[str, Any]]
    observations: list[dict[str, Any]]
    decision: dict[str, Any]
    reflection: dict[str, Any]


def _runtime_seed(payload: dict[str, Any]) -> dict[str, Any]:
    seeded = dict(payload.get("runtime_state") or {}) if isinstance(payload.get("runtime_state"), dict) else {}
    return {
        "goal": dict(payload.get("goal") or seeded.get("goal") or {}),
        "unified_task": dict(payload.get("unified_task") or seeded.get("unified_task") or {}),
        "task_state": dict(payload.get("task_state") or seeded.get("task_state") or {}),
        "current_action": dict(payload.get("current_action") or seeded.get("current_action") or {}),
        "policy": dict(payload.get("policy") or seeded.get("policy") or {}),
        "episodes": list(payload.get("episodes") or seeded.get("episodes") or []),
    }


def _default_plan(task_type: str) -> list[str]:
    if task_type == "rag_qa":
        return ["retrieve_evidence", "compose_answer_with_citations"]
    if task_type == "tool_flow":
        return ["interpret_intent", "call_internal_api", "summarize_result"]
    if task_type == "ticket_email":
        return ["classify_mail", "extract_entities", "draft_reply", "wait_approval"]
    return ["research", "summarize"]


def _normalize_plan_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in text.splitlines():
        item = raw.strip().lstrip("-*").strip()
        if not item:
            continue
        if "." in item[:3]:
            _, _, item = item.partition(".")
            item = item.strip()
        if item:
            lines.append(item[:120])
    seen: set[str] = set()
    ordered: list[str] = []
    for item in lines:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered[:5]


def _agent_step(phase: str, title: str, summary: str) -> dict[str, Any]:
    return {
        "phase": phase,
        "title": title,
        "summary": summary,
    }


def _decision_from_input(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    planner_snapshot = payload.get("planner", {}) if isinstance(payload.get("planner"), dict) else {}
    current_action = payload.get("current_action", {}) if isinstance(payload.get("current_action"), dict) else {}
    policy = payload.get("policy", {}) if isinstance(payload.get("policy"), dict) else {}
    selected_tool = str(
        current_action.get("target")
        or payload.get("selected_tool")
        or planner_snapshot.get("selected_tool")
        or ""
    )
    return {
        "action": str(current_action.get("action_type") or policy.get("selected_action") or planner_snapshot.get("action") or task_type),
        "intent": str(planner_snapshot.get("intent") or task_type),
        "selected_tool": selected_tool or None,
        "route": str(planner_snapshot.get("route") or payload.get("origin") or "workflow_task"),
        "confidence": planner_snapshot.get("confidence"),
    }


def _query_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("question") or payload.get("query") or payload.get("message") or "").strip()


def _runtime_action(state: AgentState) -> dict[str, Any]:
    current_action = state.get("current_action")
    if isinstance(current_action, dict):
        return dict(current_action)
    return {}


def _runtime_policy(state: AgentState) -> dict[str, Any]:
    policy = state.get("policy")
    if isinstance(policy, dict):
        return dict(policy)
    return {}


def _build_runtime_directed_tool_plan(state: AgentState) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    inp = state["input"]
    current_action = _runtime_action(state)
    policy = _runtime_policy(state)
    action_type = str(current_action.get("action_type") or policy.get("selected_action") or "")
    target = str(
        current_action.get("target")
        or inp.get("selected_tool")
        or state.get("decision", {}).get("selected_tool")
        or ""
    )
    requires_approval = bool(current_action.get("requires_approval")) or bool(policy.get("approval_triggered"))
    action_input = current_action.get("input") if isinstance(current_action.get("input"), dict) else {}
    metadata = inp.get("metadata") if isinstance(inp.get("metadata"), dict) else {}

    if not should_prepare_tools(action_type) or not target:
        return [], [], ""

    immediate: list[dict[str, Any]] = []
    gated: list[dict[str, Any]] = []
    draft = f"Prepared runtime-directed action `{action_type}` with tool `{target}`."

    if target == "web_search":
        plan = {
            "tool_id": target,
            "payload": {
                "query": inp.get("query") or inp.get("message") or action_input.get("goal") or _query_from_payload(inp) or "multi-agent orchestration",
                "domain": inp.get("domain") or metadata.get("domain") or "example.com",
                "top_k": int(inp.get("top_k", 3)),
            },
        }
        (gated if requires_approval or action_type == "approval_request" else immediate).append(plan)
        return immediate, gated, draft

    if target == "internal_rest_api":
        method = str(action_input.get("method") or inp.get("method") or "GET").upper()
        path = str(action_input.get("path") or inp.get("path") or "/records")
        payload: dict[str, Any] = {"method": method, "path": path}
        params = action_input.get("params") if isinstance(action_input.get("params"), dict) else inp.get("params")
        body = action_input.get("body") if isinstance(action_input.get("body"), dict) else inp.get("body")
        if isinstance(params, dict):
            payload["params"] = params
        if isinstance(body, dict):
            payload["body"] = body
        if method != "GET" and "body" not in payload:
            payload["body"] = {"value": inp.get("value", "updated")}
        plan = {"tool_id": target, "payload": payload}
        (gated if requires_approval or method != "GET" or action_type == "approval_request" else immediate).append(plan)
        return immediate, gated, draft

    if target == "email_ticketing":
        plan = {
            "tool_id": target,
            "payload": {
                "action": action_input.get("action") or inp.get("action") or "send_email",
                "target": action_input.get("target") or inp.get("target") or "ops@example.com",
                "subject": action_input.get("subject") or inp.get("subject") or "Agent follow-up",
                "body": action_input.get("body") or inp.get("reply_draft") or inp.get("content") or "Prepared by runtime policy.",
            },
        }
        gated.append(plan)
        return immediate, gated, draft

    if target == "object_storage":
        plan = {
            "tool_id": target,
            "payload": {
                "bucket": action_input.get("bucket") or inp.get("bucket") or "artifacts",
                "key": action_input.get("key") or inp.get("key") or "result.json",
                "content": action_input.get("content") or inp.get("content") or inp.get("message") or "",
            },
        }
        (gated if requires_approval or action_type == "approval_request" else immediate).append(plan)
        return immediate, gated, draft

    return [], [], ""


def _qwen_plan(task_type: str, payload: dict[str, Any], model: str) -> list[str]:
    if not qwen_client.is_enabled():
        return []
    system_prompt = (
        "You are planning short execution steps for an orchestration backend. "
        "Return 2 to 5 concise plan steps, one per line, no markdown, no explanations."
    )
    user_prompt = (
        f"Task type: {task_type}\n"
        f"Input payload: {payload}\n"
        "Focus on actionable orchestration steps that match the task type and available backend workflow."
    )
    try:
        text = qwen_client.chat_text(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.1,
            max_tokens=240,
            model=model,
        )
    except Exception:
        return []
    return _normalize_plan_lines(text)


def planner_node(state: AgentState) -> AgentState:
    model = state.get("model_hint", settings.qwen_model if qwen_client.is_enabled() else "mock-primary")
    payload = state["input"]
    if model == "mock-primary" and payload.get("force_model_fail"):
        raise RuntimeError("mock-primary planning failed")

    task_type = state["task_type"]
    plan = _default_plan(task_type)
    if str(model).startswith("qwen"):
        qwen_plan = _qwen_plan(task_type, payload, str(model))
        if qwen_plan:
            plan = qwen_plan
    decision = _decision_from_input(task_type, payload)
    runtime_seed = _runtime_seed(payload)
    runtime_seed["task_state"] = {
        **dict(runtime_seed.get("task_state") or {}),
        "current_phase": "plan",
        "available_actions": list((runtime_seed.get("task_state") or {}).get("available_actions") or []),
    }
    agent_steps = list(state.get("agent_steps", [])) + [
        _agent_step("understand", "Understand request", "Captured the goal, prior state, and runtime policy seed."),
        _agent_step("plan", "Plan execution", f"Prepared {len(plan)} execution step(s) with model `{model}`."),
    ]
    return {**runtime_seed, "plan": plan, "decision": decision, "agent_steps": agent_steps}


def retrieval_node(state: AgentState) -> AgentState:
    with tracer.start_as_current_span("retrieval"):
        task_type = state["task_type"]
        if task_type not in {"rag_qa", "research_summary"}:
            return {"retrievals": [], "citations": []}

        payload = state["input"]
        existing_hits = payload.get("retrieval_hits")
        if isinstance(existing_hits, list) and existing_hits:
            normalized_hits = []
            for hit in existing_hits:
                if isinstance(hit, dict):
                    normalized_hits.append(
                        {
                            "doc": str(hit.get("title") or hit.get("source") or "doc"),
                            "snippet": str(hit.get("snippet") or ""),
                        }
                    )
            observations = list(state.get("observations", [])) + [
                {
                    "kind": "retrieval",
                    "summary": f"Reused {len(normalized_hits)} retrieval hit(s) from assistant planning.",
                    "source": "assistant_retrieval",
                }
            ]
            agent_steps = list(state.get("agent_steps", [])) + [
                _agent_step("observe", "Observe retrieval context", f"Loaded {len(normalized_hits)} retrieval hit(s) from the assistant layer."),
            ]
            citations = [{"source": item["doc"], "snippet": item["snippet"]} for item in normalized_hits]
            return {"retrievals": normalized_hits, "citations": citations, "observations": observations, "agent_steps": agent_steps}

        query = _query_from_payload(payload).lower()
        docs_dir = Path(settings.docs_dir)
        matches: list[dict[str, Any]] = []
        citations: list[dict[str, Any]] = []
        for p in sorted(docs_dir.glob("*.md")):
            text = p.read_text(encoding="utf-8")
            if query and query not in text.lower():
                continue
            snippet = text.replace("\n", " ")[:220]
            matches.append({"doc": p.name, "snippet": snippet})
            citations.append({"source": p.name, "snippet": snippet})
        if not matches:
            matches.append({"doc": "none", "snippet": "No exact local match, fallback summary path."})
        observations = list(state.get("observations", [])) + [
            {
                "kind": "retrieval",
                "summary": f"Collected {len(matches)} retrieval hit(s) using query `{query or 'fallback'}`.",
                "source": "local_docs",
            }
        ]
        agent_steps = list(state.get("agent_steps", [])) + [
            _agent_step("observe", "Observe retrieval context", f"Collected {len(matches)} retrieval hit(s) from local docs."),
        ]
        return {"retrievals": matches, "citations": citations, "observations": observations, "agent_steps": agent_steps}


def tool_node(state: AgentState) -> AgentState:
    task_type = state["task_type"]
    inp = state["input"]
    current_action = dict(state.get("current_action") or {})
    tool_plans: list[dict[str, Any]] = []
    pending_tool_plans: list[dict[str, Any]] = []
    draft = ""
    runtime_tool_plans, runtime_pending_tool_plans, runtime_draft = _build_runtime_directed_tool_plan(state)
    if runtime_tool_plans or runtime_pending_tool_plans:
        tool_plans.extend(runtime_tool_plans)
        pending_tool_plans.extend(runtime_pending_tool_plans)
        draft = runtime_draft

    if not tool_plans and not pending_tool_plans and task_type == "tool_flow":
        action = str(inp.get("action", "query"))
        if action == "query":
            tool_plans.append(
                {
                    "tool_id": "internal_rest_api",
                    "payload": {
                        "method": "GET",
                        "path": "/records",
                        "params": {"q": inp.get("query", "all")},
                    },
                }
            )
        elif action == "create":
            pending_tool_plans.append(
                {
                    "tool_id": "internal_rest_api",
                    "payload": {
                        "method": "POST",
                        "path": "/records",
                        "body": {"name": inp.get("name", "new-record"), "value": inp.get("value", "default")},
                        "idempotency_key": str(inp.get("idempotency_key", "local-create")),
                    },
                }
            )
        else:
            pending_tool_plans.append(
                {
                    "tool_id": "internal_rest_api",
                    "payload": {
                        "method": "PUT",
                        "path": f"/records/{inp.get('record_id', '1')}",
                        "body": {"value": inp.get("value", "updated")},
                    },
                }
            )
        draft = f"Prepared tool flow action={action}"

    elif not tool_plans and not pending_tool_plans and task_type == "ticket_email":
        content = str(inp.get("content", ""))
        lower = content.lower()
        label = "incident" if "error" in lower or "incident" in lower else "general"
        draft = f"Classified as {label}. Draft reply prepared."
        pending_tool_plans.append(
            {
                "tool_id": "email_ticketing",
                "payload": {
                    "action": "create_ticket" if label == "incident" else "send_email",
                    "target": inp.get("target", "ops@example.com"),
                    "subject": inp.get("subject", f"Re: {label} follow-up"),
                    "body": inp.get("reply_draft", f"Received your request. Category={label}."),
                },
            }
        )
    elif not tool_plans and not pending_tool_plans and task_type == "research_summary":
        selected_tool = str(
            current_action.get("target")
            or inp.get("selected_tool")
            or state.get("decision", {}).get("selected_tool")
            or "web_search"
        )
        metadata = inp.get("metadata") if isinstance(inp.get("metadata"), dict) else {}
        tool_plans.append(
            {
                "tool_id": selected_tool or "web_search",
                "payload": {
                    "query": inp.get("query") or inp.get("message") or current_action.get("input", {}).get("goal") or "multi-agent orchestration",
                    "domain": inp.get("domain") or metadata.get("domain") or "example.com",
                    "top_k": int(inp.get("top_k", 3)),
                },
            }
        )
        draft = "Prepared controlled web search plan."
    elif not draft:
        draft = "Prepared evidence-based response draft."
    observations = list(state.get("observations", []))
    observations.append(
        {
            "kind": "act",
            "summary": f"Prepared {len(tool_plans)} immediate tool call(s) and {len(pending_tool_plans)} gated tool call(s).",
            "source": "tool_planner",
        }
    )
    agent_steps = list(state.get("agent_steps", [])) + [
        _agent_step("act", "Prepare tool execution", draft or "Prepared next action for tool execution."),
    ]
    return {
        "tool_plans": tool_plans,
        "pending_tool_plans": pending_tool_plans,
        "draft_output": draft,
        "observations": observations,
        "agent_steps": agent_steps,
    }


def review_node(state: AgentState) -> AgentState:
    task_type = state["task_type"]
    citations = state.get("citations", [])
    tools = state.get("tool_plans", [])
    draft = state.get("draft_output", "")
    final = {
        "task_type": task_type,
        "draft": draft,
        "citations": citations,
        "planned_tool_calls": len(tools),
    }
    reflection = {
        "summary": "Reviewed the collected evidence and prepared the final response payload.",
        "requires_replan": False,
    }
    task_state = dict(state.get("task_state") or {})
    if task_state:
        task_state["current_phase"] = "reflect"
        task_state["latest_result"] = {"status": "review_ready", "planned_tool_calls": len(tools)}
    agent_steps = list(state.get("agent_steps", [])) + [
        _agent_step("reflect", "Reflect on execution", reflection["summary"]),
    ]
    return {
        "task_state": task_state,
        "final_output": str(final),
        "review_notes": "review_passed",
        "reflection": reflection,
        "agent_steps": agent_steps,
    }


def hitl_node(state: AgentState) -> AgentState:
    current_action = _runtime_action(state)
    policy = _runtime_policy(state)
    requires_hitl = runtime_requires_approval(
        task_type=state["task_type"],
        current_action=current_action,
        policy=policy,
        pending_tool_plans=list(state.get("pending_tool_plans", [])),
    )
    if not requires_hitl:
        return {"requires_hitl": False}
    agent_steps = list(state.get("agent_steps", [])) + [
        _agent_step("replan", "Prepare approval handoff", "Shifted execution into a gated approval path before continuing."),
    ]
    return {"requires_hitl": True, "agent_steps": agent_steps}


def _get_checkpointer():
    global _CHECKPOINTER
    global _CHECKPOINTER_CONTEXT
    if _CHECKPOINTER is not None:
        return _CHECKPOINTER

    degraded = False
    if settings.langgraph_postgres_dsn:
        try:
            from langgraph.checkpoint.postgres import PostgresSaver

            maybe_context = PostgresSaver.from_conn_string(settings.langgraph_postgres_dsn)
            if hasattr(maybe_context, "__enter__") and hasattr(maybe_context, "__exit__"):
                _CHECKPOINTER_CONTEXT = maybe_context
                saver = maybe_context.__enter__()
            else:
                saver = maybe_context
            if hasattr(saver, "setup"):
                saver.setup()
            _CHECKPOINTER = saver
            checkpoint_degraded.set(0)
            return _CHECKPOINTER
        except Exception as exc:
            degraded = True
            checkpoint_error_total.inc()
            checkpoint_degraded.set(1)
            logger.error(
                "langgraph_checkpoint_init_failed backend=postgres fail_fast=%s dsn=%s error=%s",
                bool(settings.langgraph_checkpoint_fail_fast),
                settings.langgraph_postgres_dsn,
                exc,
                exc_info=True,
            )
            if settings.langgraph_checkpoint_fail_fast:
                raise RuntimeError("langgraph checkpoint initialization failed") from exc

    from langgraph.checkpoint.memory import MemorySaver

    if not degraded:
        checkpoint_degraded.set(0)
    _CHECKPOINTER = MemorySaver()
    return _CHECKPOINTER


def _close_checkpointer_context() -> None:
    global _CHECKPOINTER_CONTEXT
    if _CHECKPOINTER_CONTEXT is not None:
        try:
            _CHECKPOINTER_CONTEXT.__exit__(None, None, None)
        except Exception:
            pass
        _CHECKPOINTER_CONTEXT = None


def _reset_graph_cache() -> None:
    global _GRAPH
    global _CHECKPOINTER
    _close_checkpointer_context()
    _GRAPH = None
    _CHECKPOINTER = None


def get_graph():
    global _GRAPH
    with _GRAPH_LOCK:
        if _GRAPH is not None:
            return _GRAPH

        graph = StateGraph(AgentState)
        graph.add_node("planner", planner_node)
        graph.add_node("retrieval", retrieval_node)
        graph.add_node("tool", tool_node)
        graph.add_node("review", review_node)
        graph.add_node("hitl", hitl_node)

        graph.add_edge(START, "planner")
        graph.add_edge("planner", "retrieval")
        graph.add_edge("retrieval", "tool")
        graph.add_edge("tool", "review")
        graph.add_edge("review", "hitl")
        graph.add_edge("hitl", END)

        _GRAPH = graph.compile(checkpointer=_get_checkpointer())
        return _GRAPH


def _is_recoverable_graph_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "connection is closed" in msg or "closed the connection" in msg


def _invoke_graph(task_type: str, input_payload: dict[str, Any], thread_id: str, model_hint: str) -> dict[str, Any]:
    app = get_graph()
    initial: AgentState = {
        "task_type": task_type,
        "input": input_payload,
        "model_hint": model_hint,
        **_runtime_seed(input_payload),
    }
    out = app.invoke(initial, config={"configurable": {"thread_id": thread_id}})
    return dict(out)


def run_langgraph(task_type: str, input_payload: dict[str, Any], thread_id: str, model_hint: str = "mock-primary") -> dict[str, Any]:
    try:
        return _invoke_graph(task_type, input_payload, thread_id, model_hint)
    except Exception as exc:
        if not _is_recoverable_graph_error(exc):
            raise
        checkpoint_error_total.inc()
        checkpoint_degraded.set(1)
        logger.error(
            "langgraph_checkpoint_runtime_error fail_fast=%s error=%s",
            bool(settings.langgraph_checkpoint_fail_fast),
            exc,
            exc_info=True,
        )
        if settings.langgraph_checkpoint_fail_fast:
            raise RuntimeError("langgraph checkpoint runtime error") from exc
        with _GRAPH_LOCK:
            _reset_graph_cache()
        return _invoke_graph(task_type, input_payload, thread_id, model_hint)


def close_graph_resources() -> None:
    with _GRAPH_LOCK:
        _reset_graph_cache()
