from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .schemas import (
    AssistantChatRequest,
    AssistantChatResponse,
    AssistantConversationDetail,
    AssistantConversationSummary,
    AssistantConversationUpdateRequest,
    AssistantTaskTraceResponse,
    AssistantTurnSummary,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
)
from .security import (
    create_access_token,
    create_refresh_token,
    create_task_event_token,
    decode_task_event_token,
    decode_token,
    hash_password,
    verify_password,
)
from .services import assistant_chat as service_assistant_chat
from .services.assistant_runtime_service import build_turn_summary
from .services.retrieval_service import RetrievalService
from .tool_gateway import ToolGateway
from .config import settings
import app.services.assistant_orchestration_service as assistant_orchestration_service

logger = logging.getLogger(__name__)
FINAL_TASK_STATES = {"SUCCEEDED", "FAILED_FINAL", "FAILED_RETRYABLE", "CANCELLED", "TIMED_OUT"}
STATUS_LABELS = {
    "QUEUED": "排队中",
    "RUNNING": "处理中",
    "WAITING_HUMAN": "等待确认",
    "SUCCEEDED": "已完成",
    "FAILED_FINAL": "失败",
    "FAILED_RETRYABLE": "可重试",
    "CANCELLED": "已取消",
    "TIMED_OUT": "已超时",
}
DEMO_DOCS_DIR = Path(__file__).resolve().parents[3] / "docs"
settings.docs_dir = str(DEMO_DOCS_DIR)
CURATED_DOCS: list[dict[str, str]] = [
    {
        "title": "workflow runtime overview",
        "source": str(DEMO_DOCS_DIR / "campus_job_pitch.md"),
        "snippet": "This workflow runtime turns each request into goals, actions, and state transitions. The API decides whether to answer directly, call a tool, or hand work to a longer-running worker. Temporal handles durable execution and recovery, while LangGraph tracks planning state.",
        "keywords": "workflow runtime temporal langgraph goal action state execution recovery",
    },
    {
        "title": "tool gateway governance",
        "source": str(DEMO_DOCS_DIR / "audit" / "tool-gateway-governance.md"),
        "snippet": "Tool Gateway provides controlled access to external tools, blocks risky actions when approval is needed, and writes tool results back into tasks and conversations.",
        "keywords": "tool gateway tool governance approval risk permission",
    },
    {
        "title": "assistant architecture",
        "source": str(DEMO_DOCS_DIR / "mas_architecture.md"),
        "snippet": "The frontend owns the chat experience, the API owns routing and memory, and the worker owns durable execution and recovery. That lets the product feel like a normal assistant while still supporting longer-running tasks.",
        "keywords": "assistant architecture frontend api worker chat long running memory orchestration",
    },
]


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _trace_id() -> str:
    return uuid.uuid4().hex


def _status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def _task_chat_state(status: str) -> str:
    if status == "WAITING_HUMAN":
        return "等待确认"
    if status in {"QUEUED", "RUNNING"}:
        return "处理中"
    if status == "SUCCEEDED":
        return "已完成"
    if status in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
        return "需要处理"
    return "处理中"


def _task_progress_message(task: dict[str, Any]) -> str:
    status = str(task.get("status") or "")
    if status == "SUCCEEDED":
        return "这项任务已经完成。"
    if status == "WAITING_HUMAN":
        return "这一步正在等待确认。"
    if status in {"FAILED_FINAL", "FAILED_RETRYABLE", "TIMED_OUT"}:
        return "这项任务暂时没有顺利完成。"
    if status == "QUEUED":
        return "任务已经创建，正在准备执行。"
    return "任务仍在处理中。"


def _task_summary(task: dict[str, Any]) -> str:
    result_preview = str(task.get("result_preview") or "").strip()
    if result_preview:
        return result_preview
    failure_reason = str(task.get("failure_reason") or "").strip()
    if failure_reason:
        return failure_reason
    return _task_progress_message(task)


class DemoConversationRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def get_or_create_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str):
        row = self.rows.get(conversation_id)
        now = _utcnow()
        if row:
            row["updated_at"] = now
            return row
        row = {
            "conversation_id": conversation_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "message_history": [],
            "last_task_result": {},
            "last_tool_result": {},
            "user_preferences": {},
            "title": None,
            "created_at": now,
            "updated_at": now,
        }
        self.rows[conversation_id] = row
        return row

    def append_message(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        role: str,
        message: str,
        route: str,
        metadata: dict | None,
        created_at: str,
        max_messages: int,
    ):
        row = self.get_or_create_conversation(tenant_id=tenant_id, user_id=user_id, conversation_id=conversation_id)
        history = list(row.get("message_history") or [])
        item = {"role": role, "message": message, "route": route, "created_at": created_at}
        if metadata:
            item["metadata"] = metadata
        history.append(item)
        row["message_history"] = history[-max_messages:]
        row["updated_at"] = _utcnow()
        return row["message_history"]

    def update_memory(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        last_task_result: dict | None = None,
        last_tool_result: dict | None = None,
        user_preferences: dict | None = None,
    ):
        row = self.get_or_create_conversation(tenant_id=tenant_id, user_id=user_id, conversation_id=conversation_id)
        if last_task_result is not None:
            row["last_task_result"] = dict(last_task_result)
        if last_tool_result is not None:
            row["last_tool_result"] = dict(last_tool_result)
        if user_preferences is not None:
            row["user_preferences"] = dict(user_preferences)
        row["updated_at"] = _utcnow()
        return {
            "last_task_result": row["last_task_result"],
            "last_tool_result": row["last_tool_result"],
            "user_preferences": row["user_preferences"],
        }

    def list_conversations_for_user(self, *, tenant_id: str, user_id: str, limit: int = 30):
        rows = [
            row for row in self.rows.values() if str(row.get("tenant_id")) == tenant_id and str(row.get("user_id")) == user_id
        ]
        rows.sort(key=lambda item: item.get("updated_at") or item.get("created_at") or _utcnow(), reverse=True)
        return rows[:limit]

    def get_conversation(self, *, tenant_id: str, conversation_id: str):
        row = self.rows.get(conversation_id)
        if not row or str(row.get("tenant_id")) != tenant_id:
            return None
        return row

    def update_title(self, *, tenant_id: str, user_id: str, conversation_id: str, title: str | None):
        row = self.get_conversation(tenant_id=tenant_id, conversation_id=conversation_id)
        if row is None:
            raise LookupError("conversation not found")
        if str(row.get("user_id")) != user_id:
            raise PermissionError("conversation ownership mismatch")
        row["title"] = title
        return row

    def delete_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str):
        row = self.get_conversation(tenant_id=tenant_id, conversation_id=conversation_id)
        if row is None:
            raise LookupError("conversation not found")
        if str(row.get("user_id")) != user_id:
            raise PermissionError("conversation ownership mismatch")
        del self.rows[conversation_id]


class DemoEpisodeRepo:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def list_recent_episodes_for_user(self, *, tenant_id: str, user_id: str, limit: int = 30):
        return [row for row in self.rows if row.get("tenant_id") == tenant_id and row.get("user_id") == user_id][:limit]

    def upsert_episode(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str | None,
        turn_id: str | None,
        task_id: str | None,
        episode: dict,
    ):
        row = dict(episode)
        row.update(
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "conversation_id": conversation_id,
                "turn_id": turn_id,
                "task_id": task_id,
            }
        )
        self.rows = [item for item in self.rows if item.get("episode_id") != row.get("episode_id")]
        self.rows.insert(0, row)
        return row


class DemoTurnRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def create_turn(self, **kwargs):
        row = dict(kwargs)
        now = _utcnow()
        row.setdefault("created_at", now)
        row["updated_at"] = now
        self.rows[str(row["turn_id"])] = row
        return row

    def list_turns_for_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str, limit: int = 30):
        rows = [
            row
            for row in self.rows.values()
            if str(row.get("tenant_id")) == tenant_id
            and str(row.get("user_id")) == user_id
            and str(row.get("conversation_id")) == conversation_id
        ]
        rows.sort(key=lambda item: item.get("created_at") or _utcnow())
        return rows[-limit:]


class DemoTaskRepo:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.runs: dict[str, list[dict[str, Any]]] = {}
        self.steps: dict[str, list[dict[str, Any]]] = {}
        self.approvals: dict[str, list[dict[str, Any]]] = {}

    def create_task(self, **kwargs):
        task_id = _id("task")
        now = _utcnow()
        row = {
            "id": task_id,
            "task_id": task_id,
            "tenant_id": kwargs["tenant_id"],
            "client_request_id": kwargs.get("client_request_id"),
            "task_type": kwargs.get("task_type"),
            "created_by": kwargs.get("created_by"),
            "input_masked": dict(kwargs.get("input_masked") or {}),
            "trace_id": kwargs.get("trace_id") or _trace_id(),
            "conversation_id": kwargs.get("conversation_id"),
            "assistant_turn_id": kwargs.get("assistant_turn_id"),
            "goal_id": kwargs.get("goal_id"),
            "origin": kwargs.get("origin", "assistant_chat"),
            "status": "QUEUED",
            "latest_step_key": "task_create",
            "tool_call_count": 0,
            "waiting_approval_count": 0,
            "created_at": now,
            "updated_at": now,
            "output_masked": {},
            "error_code": None,
            "error_message": None,
            "result_preview": None,
            "failure_reason": None,
        }
        self.tasks[task_id] = row
        return {"id": task_id, "trace_id": row["trace_id"], "budget": kwargs.get("budget", 1.0)}

    def create_run(self, **kwargs):
        run_id = _id("run")
        row = {
            "id": run_id,
            "task_id": kwargs.get("task_id"),
            "trace_id": kwargs.get("trace_id"),
            "status": "QUEUED",
            "created_at": _utcnow(),
            "updated_at": _utcnow(),
        }
        self.runs.setdefault(str(kwargs.get("task_id")), []).append(row)
        self.steps.setdefault(run_id, [])
        return {"id": run_id}

    def update_task_status(self, tenant_id: str, task_id: str, status_text: str) -> None:
        del tenant_id
        task = self.tasks[str(task_id)]
        task["status"] = status_text
        task["updated_at"] = _utcnow()

    def append_step(self, **kwargs) -> bool:
        run_id = str(kwargs["run_id"])
        payload = {
            "id": _id("step"),
            "run_id": run_id,
            "status": kwargs.get("status_text"),
            "step_key": kwargs.get("step_key"),
            "payload_masked": dict(kwargs.get("payload_masked") or {}),
            "created_at": _utcnow(),
        }
        self.steps.setdefault(run_id, []).append(payload)
        task_id = self._task_id_from_run(run_id)
        if task_id and task_id in self.tasks:
            self.tasks[task_id]["latest_step_key"] = str(kwargs.get("step_key") or "")
            self.tasks[task_id]["updated_at"] = _utcnow()
        return True

    def update_run_status(self, tenant_id: str, run_id: str, status_text: str) -> None:
        del tenant_id
        task_id = self._task_id_from_run(str(run_id))
        if not task_id:
            return
        for run in self.runs.get(task_id, []):
            if str(run.get("id")) == str(run_id):
                run["status"] = status_text
                run["updated_at"] = _utcnow()

    def mark_task_succeeded(self, tenant_id: str, task_id: str, payload_masked: dict) -> None:
        del tenant_id
        task = self.tasks[str(task_id)]
        task["status"] = "SUCCEEDED"
        task["output_masked"] = dict(payload_masked or {})
        task["result_preview"] = "任务已经完成。"
        task["updated_at"] = _utcnow()

    def mark_task_failed(
        self,
        tenant_id: str,
        task_id: str,
        status_text: str,
        error_code: str | None,
        error_message=None,
    ) -> None:
        del tenant_id
        task = self.tasks[str(task_id)]
        task["status"] = status_text
        task["error_code"] = error_code
        task["error_message"] = error_message
        task["failure_reason"] = str(error_code or "执行失败")
        task["updated_at"] = _utcnow()

    def insert_audit_log(self, **kwargs) -> None:
        del kwargs

    def list_assistant_tasks_for_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str, limit: int = 30):
        rows = [
            row
            for row in self.tasks.values()
            if str(row.get("tenant_id")) == tenant_id
            and str(row.get("created_by")) == user_id
            and str(row.get("conversation_id") or "") == str(conversation_id)
        ]
        rows.sort(key=lambda item: item.get("updated_at") or item.get("created_at") or _utcnow(), reverse=True)
        return rows[:limit]

    def get_task_by_id(self, *, tenant_id: str, task_id: str, include_sensitive: bool = False):
        del include_sensitive
        task = self.tasks.get(str(task_id))
        if task and str(task.get("tenant_id")) == tenant_id:
            return task
        return None

    def list_runs_for_task(self, tenant_id: str, task_id: str):
        del tenant_id
        return list(self.runs.get(str(task_id), []))

    def list_steps_for_run_ids(self, tenant_id: str, run_ids: list[str]):
        del tenant_id
        rows: list[dict[str, Any]] = []
        for run_id in run_ids:
            rows.extend(self.steps.get(str(run_id), []))
        rows.sort(key=lambda item: item.get("created_at") or _utcnow())
        return rows

    def list_tool_calls_for_task(self, tenant_id: str, task_id: str):
        del tenant_id, task_id
        return []

    def list_approvals_for_task(self, tenant_id: str, task_id: str):
        del tenant_id
        return list(self.approvals.get(str(task_id), []))

    def _task_id_from_run(self, run_id: str) -> str | None:
        for task_id, runs in self.runs.items():
            if any(str(run.get("id")) == run_id for run in runs):
                return task_id
        return None


class DemoToolRepo:
    def __init__(self) -> None:
        self.tools = [
            {
                "tool_name": "web_search",
                "version": "v1",
                "description": "Search docs",
                "input_schema": {"type": "object"},
                "risk_level": "low",
                "requires_approval": False,
                "supported_use_cases": ["knowledge_lookup", "docs_search"],
                "enabled": True,
            },
            {
                "tool_name": "email_ticketing",
                "version": "v1",
                "description": "Send tickets",
                "input_schema": {"type": "object"},
                "risk_level": "high",
                "requires_approval": True,
                "supported_use_cases": ["ticket_action"],
                "enabled": True,
            },
        ]

    def list_assistant_registry(self, *, tenant_id: str, enabled_only: bool = True, use_case: str | None = None):
        del tenant_id
        rows = list(self.tools)
        if enabled_only:
            rows = [row for row in rows if bool(row.get("enabled"))]
        if use_case:
            rows = [row for row in rows if use_case in list(row.get("supported_use_cases") or [])]
        return rows


class DemoGateway(ToolGateway):
    async def execute(self, req: dict):
        payload = dict(req.get("payload") or {})
        query = str(payload.get("query") or "").strip()
        lowered = query.lower()
        if "澶辫触" in query or "fail" in lowered:
            return {
                "status": "FAILED",
                "tool_call_id": _id("tool-call"),
                "reason_code": "tool_denied",
                "result": {"error": "permission denied"},
                "idempotent_hit": False,
            }
        if "闄愭祦" in query or "429" in lowered or "busy" in lowered:
            return {
                "status": "FAILED",
                "tool_call_id": _id("tool-call"),
                "reason_code": "adapter_http_429",
                "result": {"error": "rate limited"},
                "idempotent_hit": False,
            }
        return {
            "status": "SUCCEEDED",
            "tool_call_id": _id("tool-call"),
            "reason_code": None,
            "result": {
                "results": [
                    {
                        "title": "workspace docs",
                        "url": "https://docs.python.org/3/library/asyncio.html",
                        "snippet": "Temporal coordinates durable execution, LangGraph manages stateful planning, and Tool Gateway guards tool execution.",
                    }
                ]
            },
            "idempotent_hit": False,
        }


class DemoRetrievalService(RetrievalService):
    def retrieve(self, *, query: str, top_k: int = 3) -> list[dict[str, Any]]:
        lowered = str(query or "").lower()
        seeded: list[dict[str, Any]] = []
        for item in CURATED_DOCS:
            keywords = str(item.get("keywords") or "").split()
            if any(keyword.lower() in lowered for keyword in keywords):
                seeded.append(
                    {
                        "title": item["title"],
                        "source": item["source"],
                        "score": 0.8,
                        "snippet": item["snippet"],
                        "matched_terms": keywords[:3],
                    }
                )
        hits = super().retrieve(query=query, top_k=top_k)
        combined = seeded + [hit for hit in hits if str(hit.get("source") or "") not in {item["source"] for item in seeded}]
        return combined[: max(1, int(top_k))]


conversation_repo = DemoConversationRepo()
episode_repo = DemoEpisodeRepo()
turn_repo = DemoTurnRepo()
task_repo = DemoTaskRepo()
tool_repo = DemoToolRepo()
gateway = DemoGateway()
users_by_id: dict[str, dict[str, Any]] = {}
users_by_email: dict[str, dict[str, Any]] = {}
refresh_tokens: dict[str, str] = {}


def _user_public_record(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(user["id"]),
        "email": str(user["email"]),
        "role": str(user["role"]),
        "tenant_id": str(user["tenant_id"]),
    }


def _issue_tokens(user: dict[str, Any]) -> TokenResponse:
    public_user = _user_public_record(user)
    access_token = create_access_token(public_user)
    refresh_token, _ = create_refresh_token(public_user)
    refresh_tokens[refresh_token] = public_user["id"]
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


def _current_user(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing auth")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_token(token)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token") from exc
    if str(payload.get("type") or "") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token type")
    user = users_by_id.get(str(payload.get("sub") or ""))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="user not found")
    return user


def _conversation_summary(row: dict[str, Any]) -> dict[str, Any]:
    history = list(row.get("message_history") or [])
    last_user = None
    last_assistant = None
    last_route = None
    for item in reversed(history):
        role = str(item.get("role") or "")
        if role == "assistant" and last_assistant is None:
            last_assistant = str(item.get("message") or "") or None
            last_route = str(item.get("route") or "") or None
        if role == "user" and last_user is None:
            last_user = str(item.get("message") or "") or None
        if last_user is not None and last_assistant is not None:
            break
    tasks = task_repo.list_assistant_tasks_for_conversation(
        tenant_id=str(row["tenant_id"]),
        user_id=str(row["user_id"]),
        conversation_id=str(row["conversation_id"]),
        limit=100,
    )
    running_count = sum(1 for task in tasks if str(task.get("status") or "") not in FINAL_TASK_STATES)
    title = str(row.get("title") or "").strip() or (last_user or "新的对话")
    preview = last_assistant or last_user or "从这里继续刚才的对话。"
    return {
        "conversation_id": str(row["conversation_id"]),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "last_user_message": last_user,
        "last_assistant_message": last_assistant,
        "title": title,
        "preview": preview[:120],
        "last_route": last_route,
        "task_count": len(tasks),
        "running_task_count": running_count,
        "waiting_approval_count": 0,
    }


def _task_card(task: dict[str, Any]) -> dict[str, Any]:
    status = str(task.get("status") or "QUEUED")
    route = "workflow_task" if str(task.get("task_type") or "") != "tool_flow" else "tool_task"
    return {
        "task_id": str(task["id"]),
        "task_type": str(task.get("task_type") or "research_summary"),
        "task_kind": "持续执行任务" if route == "workflow_task" else "工具任务",
        "route": route,
        "status": status,
        "status_label": _status_label(status),
        "progress_message": _task_progress_message(task),
        "current_step": str(task.get("latest_step_key") or "") or None,
        "waiting_for": None,
        "next_action": None,
        "tool_call_count": int(task.get("tool_call_count") or 0),
        "waiting_approval_count": int(task.get("waiting_approval_count") or 0),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "trace_id": str(task.get("trace_id") or ""),
        "result_preview": task.get("result_preview"),
        "failure_reason": task.get("failure_reason"),
        "chat_state": _task_chat_state(status),
        "assistant_summary": _task_summary(task),
    }


async def _complete_demo_task(task_id: str) -> None:
    await asyncio.sleep(2.0)
    task = task_repo.tasks.get(task_id)
    if not task or str(task.get("status") or "") in FINAL_TASK_STATES:
        return
    task["status"] = "SUCCEEDED"
    task["latest_step_key"] = "workflow_start"
    task["result_preview"] = "任务已经完成当前这一轮处理，你可以继续追问，或发起下一步。"
    task["updated_at"] = _utcnow()


async def _demo_service_create_task(*, task_repo: DemoTaskRepo, req, tenant_id: str, user: dict[str, Any], trace_id: str, start_workflow):
    del start_workflow
    task = task_repo.create_task(
        tenant_id=tenant_id,
        client_request_id=req.client_request_id,
        task_type=req.task_type,
        created_by=str(user["id"]),
        input_masked=req.input,
        trace_id=trace_id,
        budget=req.budget,
        requires_hitl=False,
        conversation_id=req.conversation_id,
        assistant_turn_id=req.assistant_turn_id,
        goal_id=req.goal_id,
        origin=req.origin,
    )
    task_id = str(task["id"])
    task_repo.create_run(
        tenant_id=tenant_id,
        task_id=task_id,
        run_no=1,
        workflow_id=f"demo-workflow-{task_id}",
        trace_id=trace_id,
        assigned_worker="demo-worker",
    )
    task_repo.update_task_status(tenant_id, task_id, "QUEUED")
    task_repo.append_step(
        tenant_id=tenant_id,
        run_id=task_repo.runs[task_id][0]["id"],
        status_text="QUEUED",
        step_key="workflow_start",
        payload_masked={"summary": "Workflow task created in demo mode."},
        trace_id=trace_id,
        status_event_id=_id("status"),
    )
    asyncio.create_task(_complete_demo_task(task_id))
    return {"task_id": task_id, "run_id": task_repo.runs[task_id][0]["id"], "status": "QUEUED", "idempotent": False}


assistant_orchestration_service.service_create_task = _demo_service_create_task
assistant_orchestration_service.RetrievalService = DemoRetrievalService

app = FastAPI(title="XH Demo API", version="0.1.0-demo")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_origin_regex=r"^http://(localhost|127\.0\.0\.1):\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "demo"}


@app.get("/tasks")
def list_tasks(
    from_ts: str | None = Query(default=None),
    to_ts: str | None = Query(default=None),
    user: dict[str, Any] = Depends(_current_user),
):
    del from_ts, to_ts
    rows = [
        task
        for task in task_repo.tasks.values()
        if str(task.get("tenant_id")) == str(user["tenant_id"]) and str(task.get("created_by")) == str(user["id"])
    ]
    rows.sort(key=lambda item: item.get("updated_at") or item.get("created_at") or _utcnow(), reverse=True)
    return [
        {
            "id": str(task["id"]),
            "task_type": str(task.get("task_type") or "research_summary"),
            "status": str(task.get("status") or "QUEUED"),
            "trace_id": str(task.get("trace_id") or ""),
            "cost_total": 0,
            "created_at": task.get("created_at"),
            "updated_at": task.get("updated_at"),
            "requires_hitl": False,
        }
        for task in rows
    ]


@app.post("/auth/register", response_model=TokenResponse)
def register(req: RegisterRequest) -> TokenResponse:
    email = str(req.email).strip().lower()
    if email in users_by_email:
        raise HTTPException(status_code=409, detail="email already registered")
    user = {
        "id": str(uuid.uuid4()),
        "email": email,
        "role": "user",
        "tenant_id": "default",
        "password_hash": hash_password(req.password),
        "created_at": _utcnow(),
    }
    users_by_id[user["id"]] = user
    users_by_email[email] = user
    return _issue_tokens(user)


@app.post("/auth/login", response_model=TokenResponse)
def login(req: LoginRequest) -> TokenResponse:
    email = str(req.email).strip().lower()
    user = users_by_email.get(email)
    if not user or not verify_password(req.password, str(user["password_hash"])):
        raise HTTPException(status_code=401, detail="invalid credentials")
    return _issue_tokens(user)


@app.post("/auth/refresh", response_model=TokenResponse)
def refresh(req: RefreshRequest) -> TokenResponse:
    token = str(req.refresh_token).strip()
    user_id = refresh_tokens.get(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="invalid refresh token")
    user = users_by_id.get(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="user not found")
    return _issue_tokens(user)


@app.post("/auth/logout")
def logout(req: LogoutRequest, user: dict[str, Any] = Depends(_current_user)) -> dict[str, str]:
    del user
    refresh_tokens.pop(str(req.refresh_token).strip(), None)
    return {"status": "ok"}


def _stream_event(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")


def _stream_chunks(message: str) -> list[str]:
    text = str(message or "")
    if not text:
        return []
    size = 12 if len(text) <= 180 else 20
    return [text[i : i + size] for i in range(0, len(text), size)]


@app.post("/assistant/chat", response_model=AssistantChatResponse)
async def assistant_chat_endpoint(req: AssistantChatRequest, request: Request, user: dict[str, Any] = Depends(_current_user)):
    trace_id = request.headers.get("x-trace-id", _trace_id())
    conversation_id = req.conversation_id or _id("conv")
    req = AssistantChatRequest(
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        message=req.message,
        mode=req.mode,
        metadata=req.metadata,
    )
    result = await service_assistant_chat(
        conversation_repo=conversation_repo,
        episode_repo=episode_repo,
        turn_repo=turn_repo,
        task_repo=task_repo,
        tool_repo=tool_repo,
        gateway=gateway,
        req=req,
        tenant_id=str(user["tenant_id"]),
        user=_user_public_record(user),
        trace_id=trace_id,
        start_workflow=lambda workflow_id, payload: asyncio.sleep(0),
        policy_repo=None,
        goal_repo=None,
    )
    return AssistantChatResponse(**result)


@app.post("/assistant/chat/stream")
async def assistant_chat_stream(req: AssistantChatRequest, request: Request, user: dict[str, Any] = Depends(_current_user)):
    async def generator():
        try:
            result = await assistant_chat_endpoint(req, request, user)
            for chunk in _stream_chunks(str(result.message or "")):
                yield _stream_event({"type": "delta", "delta": chunk})
                await asyncio.sleep(0.01)
            yield _stream_event({"type": "complete", "response": jsonable_encoder(result)})
        except HTTPException as exc:
            yield _stream_event({"type": "error", "detail": str(exc.detail)})
        except Exception as exc:
            logger.exception("demo_assistant_chat_stream_failed error=%s", exc)
            yield _stream_event({"type": "error", "detail": "assistant_stream_failed"})

    return StreamingResponse(generator(), media_type="application/x-ndjson")


@app.get("/assistant/conversations", response_model=list[AssistantConversationSummary])
def list_conversations(limit: int = Query(default=30, ge=1, le=200), user: dict[str, Any] = Depends(_current_user)):
    rows = conversation_repo.list_conversations_for_user(tenant_id=str(user["tenant_id"]), user_id=str(user["id"]), limit=limit)
    return [AssistantConversationSummary(**_conversation_summary(row)) for row in rows]


@app.get("/assistant/conversations/{conversation_id}", response_model=AssistantConversationDetail)
def get_conversation(conversation_id: str, task_limit: int = Query(default=30, ge=1, le=200), user: dict[str, Any] = Depends(_current_user)):
    row = conversation_repo.get_conversation(tenant_id=str(user["tenant_id"]), conversation_id=conversation_id)
    if row is None or str(row.get("user_id")) != str(user["id"]):
        raise HTTPException(status_code=404, detail="conversation not found")
    tasks = task_repo.list_assistant_tasks_for_conversation(
        tenant_id=str(user["tenant_id"]),
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=task_limit,
    )
    task_cards = [_task_card(task) for task in tasks]
    task_card_by_turn_id = {
        str(task.get("assistant_turn_id") or ""): card
        for task, card in zip(tasks, task_cards)
        if str(task.get("assistant_turn_id") or "")
    }
    turns = turn_repo.list_turns_for_conversation(
        tenant_id=str(user["tenant_id"]),
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=task_limit,
    )
    turn_history = [AssistantTurnSummary(**build_turn_summary(turn, task_card_by_turn_id.get(str(turn.get("turn_id") or "")))) for turn in turns]
    summary = _conversation_summary(row)
    return AssistantConversationDetail(
        conversation_id=conversation_id,
        user_id=str(user["id"]),
        title=summary["title"],
        preview=summary["preview"],
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        context_window=len(list(row.get("message_history") or [])),
        message_history=list(row.get("message_history") or []),
        memory={
            "last_task_result": dict(row.get("last_task_result") or {}),
            "last_tool_result": dict(row.get("last_tool_result") or {}),
            "user_preferences": dict(row.get("user_preferences") or {}),
        },
        turn_history=turn_history,
        task_history=task_cards,
    )


@app.patch("/assistant/conversations/{conversation_id}", response_model=AssistantConversationSummary)
def update_conversation(conversation_id: str, req: AssistantConversationUpdateRequest, user: dict[str, Any] = Depends(_current_user)):
    try:
        row = conversation_repo.update_title(
            tenant_id=str(user["tenant_id"]),
            user_id=str(user["id"]),
            conversation_id=conversation_id,
            title=" ".join(str(req.title or "").split()) or None,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="conversation not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail="conversation ownership mismatch") from exc
    return AssistantConversationSummary(**_conversation_summary(row))


@app.delete("/assistant/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_conversation(conversation_id: str, user: dict[str, Any] = Depends(_current_user)):
    tasks = task_repo.list_assistant_tasks_for_conversation(
        tenant_id=str(user["tenant_id"]),
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=100,
    )
    if any(str(task.get("status") or "") not in FINAL_TASK_STATES for task in tasks):
        raise HTTPException(status_code=409, detail="conversation still has active tasks")
    conversation_repo.delete_conversation(
        tenant_id=str(user["tenant_id"]),
        user_id=str(user["id"]),
        conversation_id=conversation_id,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/assistant/tasks/{task_id}/trace", response_model=AssistantTaskTraceResponse)
def get_task_trace(task_id: str, user: dict[str, Any] = Depends(_current_user)):
    task = task_repo.get_task_by_id(tenant_id=str(user["tenant_id"]), task_id=task_id, include_sensitive=True)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    if str(task.get("created_by")) != str(user["id"]):
        raise HTTPException(status_code=403, detail="task ownership mismatch")
    card = _task_card(task)
    task_summary = _task_summary(task)
    status_text = str(task.get("status") or "")
    run_ids = [str(run.get("id")) for run in task_repo.list_runs_for_task(str(user["tenant_id"]), task_id)]
    trace_steps = [
        {
            "step_key": str(step.get("step_key") or ""),
            "title": str(step.get("step_key") or ""),
            "status": str(step.get("status") or ""),
            "status_label": _status_label(str(step.get("status") or "")),
            "created_at": step.get("created_at"),
            "detail": None,
        }
        for step in task_repo.list_steps_for_run_ids(str(user["tenant_id"]), run_ids)
    ]
    return AssistantTaskTraceResponse(
        task=card,
        task_summary=task_summary,
        assistant_status=_task_chat_state(status_text),
        assistant_summary=task_summary,
        next_step_hint="你可以继续追问，或发起下一步。",
        planner={},
        retrieval_hits=[],
        episodes=[],
        trace_steps=trace_steps,
        runtime_steps=[],
        runtime_debugger={},
        tool_calls=[],
        approvals=[],
        run_history=task_repo.list_runs_for_task(str(user["tenant_id"]), task_id),
        final_output={"preview": task.get("result_preview")} if task.get("result_preview") else {},
        failure_reason=task.get("failure_reason"),
        is_final=status_text in FINAL_TASK_STATES,
    )


@app.get("/events/token")
@app.post("/events/token")
def create_events_token(task_id: str = Query(...), user: dict[str, Any] = Depends(_current_user)):
    task = task_repo.get_task_by_id(tenant_id=str(user["tenant_id"]), task_id=task_id, include_sensitive=True)
    if task is None or str(task.get("created_by")) != str(user["id"]):
        raise HTTPException(status_code=404, detail="task not found")
    token = create_task_event_token(user_id=str(user["id"]), tenant_id=str(user["tenant_id"]), task_id=task_id, ttl_seconds=300)
    return {"token": token, "expires_in_sec": 300}


def _sse_event(event: str, payload: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


@app.get("/events")
async def stream_events(task_id: str, sse_token: str):
    payload = decode_task_event_token(sse_token)
    if str(payload.get("task_id") or "") != str(task_id):
        raise HTTPException(status_code=403, detail="event token mismatch")

    async def generator():
        last_status = None
        deadline = _utcnow().timestamp() + 20
        while _utcnow().timestamp() < deadline:
            task = task_repo.tasks.get(str(task_id))
            if task is None:
                break
            status_text = str(task.get("status") or "")
            if status_text != last_status:
                last_status = status_text
                yield _sse_event("status", {"status": status_text})
            if status_text in FINAL_TASK_STATES:
                yield _sse_event("done", {"status": status_text})
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(generator(), media_type="text/event-stream")


@app.post("/approvals/{approval_id}/approve")
@app.post("/approvals/{approval_id}/reject")
def approval_placeholder(approval_id: str, user: dict[str, Any] = Depends(_current_user)):
    del approval_id, user
    return {"status": "ok"}


if not users_by_email:
    seeded = {
        "id": str(uuid.uuid4()),
        "email": "demo@example.com",
        "role": "user",
        "tenant_id": "default",
        "password_hash": hash_password("password123"),
        "created_at": _utcnow(),
    }
    users_by_id[seeded["id"]] = seeded
    users_by_email[seeded["email"]] = seeded

