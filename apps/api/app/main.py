from __future__ import annotations

import asyncio
import os
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from opentelemetry import trace
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from .config import settings
from .db import close_pool, ensure_schema_compat, init_pool
from .deps import get_current_user, get_optional_user, require_role
from .otel import setup_otel
from .policy_middleware import PolicyCheckMiddleware
from .repositories import (
    AssistantConversationRepository,
    AssistantEpisodeRepository,
    AssistantTurnRepository,
    AuthRepository,
    GoalRepository,
    PolicyMemoryRepository,
    TaskRepository,
    ToolRepository,
)
from .schemas import (
    AssistantChatRequest,
    AssistantChatResponse,
    AssistantConversationDetail,
    AssistantConversationSummary,
    AssistantTaskCard,
    AssistantTaskTraceResponse,
    AssistantTurnSummary,
    AssistantToolRegistryItem,
    AssistantToolRegistryUpsertRequest,
    ApprovalActionRequest,
    ApprovalEditRequest,
    InternalGoalExternalAdapterRequest,
    InternalGoalExternalSignalRequest,
    InternalTaskStatusRequest,
    InternalToolExecuteRequest,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    RegisterRequest,
    TaskCreateRequest,
    TokenResponse,
    ToolManifestUpsert,
)
from .security import (
    create_access_token,
    create_refresh_token,
    create_task_event_token,
    decode_task_event_token,
    decode_token,
    hash_password,
    hash_token,
    verify_password,
)
from .services import (
    apply_approval_decision,
    assistant_chat as service_assistant_chat,
    cancel_task as service_cancel_task,
    dispatch_external_adapter_signal,
    create_task as service_create_task,
    dispatch_external_signal,
    execute_internal_tool,
    rerun_task as service_rerun_task,
    run_approval_signal_dispatcher,
    update_internal_task_status,
)
from .services.goal_scheduler_service import run_goal_scheduler
from .state_machine import FINAL_STATES
from .temporal_client import cancel_workflow, signal_approval, start_task_workflow
from .tenant import resolve_tenant_id
from .tool_gateway import ToolGateway
from .services.assistant_experience_service import (
    build_conversation_summary,
    build_memory_snapshot,
    build_task_card,
    build_task_trace_view,
)
from .services.assistant_runtime_service import build_turn_summary
from .services.tool_registry_service import ToolRegistryService

logger = logging.getLogger(__name__)
gateway = ToolGateway()

auth_repo = AuthRepository()
task_repo = TaskRepository()
tool_repo = ToolRepository()
conversation_repo = AssistantConversationRepository()
episode_repo = AssistantEpisodeRepository()
turn_repo = AssistantTurnRepository()
goal_repo = GoalRepository()
policy_repo = PolicyMemoryRepository()
tool_registry_service = ToolRegistryService(tool_repo)
instrumentator = Instrumentator()
TENANT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_dispatcher_lifecycle_lock = asyncio.Lock()
_goal_scheduler_lifecycle_lock = asyncio.Lock()


def _setup_observability_once(application: FastAPI) -> None:
    if getattr(application.state, "observability_initialized", False):
        return
    strict = bool(getattr(settings, "otel_startup_strict", False))
    try:
        setup_otel(application)
        instrumentator.instrument(application)
        application.state.observability_ready = True
    except Exception as exc:
        application.state.observability_ready = False
        logger.warning("observability_setup_failed strict=%s error=%s", strict, exc)
        if strict:
            raise
    finally:
        application.state.observability_initialized = True


def _dispatcher_enabled() -> bool:
    return settings.approval_signal_dispatcher_enabled and "PYTEST_CURRENT_TEST" not in os.environ


async def _start_dispatcher_if_needed(application: FastAPI) -> None:
    if not _dispatcher_enabled():
        application.state.approval_dispatcher_task = None
        return
    existing: asyncio.Task | None = getattr(application.state, "approval_dispatcher_task", None)
    if existing is not None and not existing.done():
        return
    application.state.approval_dispatcher_task = asyncio.create_task(
        run_approval_signal_dispatcher(task_repo=task_repo, signal_approval=signal_approval)
    )


def _goal_scheduler_enabled() -> bool:
    return settings.goal_scheduler_enabled and "PYTEST_CURRENT_TEST" not in os.environ


async def _start_goal_scheduler_if_needed(application: FastAPI) -> None:
    if not _goal_scheduler_enabled():
        application.state.goal_scheduler_task = None
        return
    existing: asyncio.Task | None = getattr(application.state, "goal_scheduler_task", None)
    if existing is not None and not existing.done():
        return
    application.state.goal_scheduler_task = asyncio.create_task(
        run_goal_scheduler(
            goal_repo=goal_repo,
            task_repo=task_repo,
            start_workflow=start_task_workflow,
            cancel_workflow=cancel_workflow,
            policy_repo=policy_repo,
        )
    )


async def _stop_dispatcher_if_running(application: FastAPI) -> None:
    dispatcher_task: asyncio.Task | None = getattr(application.state, "approval_dispatcher_task", None)
    if dispatcher_task is None:
        return
    if not dispatcher_task.done():
        dispatcher_task.cancel()
        with suppress(asyncio.CancelledError):
            await dispatcher_task
    application.state.approval_dispatcher_task = None


async def _stop_goal_scheduler_if_running(application: FastAPI) -> None:
    scheduler_task: asyncio.Task | None = getattr(application.state, "goal_scheduler_task", None)
    if scheduler_task is None:
        return
    if not scheduler_task.done():
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
    application.state.goal_scheduler_task = None


@asynccontextmanager
async def app_lifespan(application: FastAPI):
    _setup_observability_once(application)
    init_pool()
    ensure_schema_compat()
    async with _dispatcher_lifecycle_lock:
        await _start_dispatcher_if_needed(application)
    async with _goal_scheduler_lifecycle_lock:
        await _start_goal_scheduler_if_needed(application)
    try:
        yield
    finally:
        async with _dispatcher_lifecycle_lock:
            await _stop_dispatcher_if_running(application)
        async with _goal_scheduler_lifecycle_lock:
            await _stop_goal_scheduler_if_running(application)
        close_pool()


app = FastAPI(title="XH Task Orchestrator API", version="0.1.0", lifespan=app_lifespan)
tracer = trace.get_tracer("api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(PolicyCheckMiddleware)


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    trace_id = request.headers.get("x-trace-id", str(uuid.uuid4()))
    request.state.trace_id = trace_id
    response = await call_next(request)
    response.headers["x-trace-id"] = trace_id
    return response


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _resolve_auth_tenant(header_tenant_id: str | None) -> str:
    tenant_id = str(header_tenant_id or settings.default_tenant_id)
    if not TENANT_ID_PATTERN.match(tenant_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid tenant id")
    return tenant_id


def _internal_auth(
    x_internal_token: str = Header(default="", alias="X-Internal-Token"),
    x_worker_id: str = Header(default="", alias="X-Worker-Id"),
    x_worker_token: str = Header(default="", alias="X-Worker-Token"),
) -> str:
    if x_internal_token != settings.internal_api_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid internal token")
    if not x_worker_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing worker identity")
    allowed = set(settings.allowed_worker_ids or [])
    if x_worker_id not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="worker identity not allowed")
    expected_worker_token = (settings.worker_auth_tokens or {}).get(x_worker_id)
    if expected_worker_token:
        if x_worker_token != expected_worker_token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid worker credential")
    elif x_worker_token != settings.internal_api_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid worker credential")
    return x_worker_id


def _metrics_auth(
    x_internal_token: str = Header(default="", alias="X-Internal-Token"),
    authorization: str = Header(default="", alias="Authorization"),
) -> None:
    if x_internal_token == settings.internal_api_token:
        return
    if authorization.lower().startswith("bearer "):
        if authorization.split(" ", 1)[1].strip() == settings.internal_api_token:
            return
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized metrics access")


def _issue_tokens(user: dict[str, Any]) -> TokenResponse:
    access_token = create_access_token(user)
    refresh_token, refresh_exp = create_refresh_token(user)
    auth_repo.store_refresh_token(
        tenant_id=str(user["tenant_id"]),
        user_id=str(user["id"]),
        token_hash=hash_token(refresh_token),
        expires_at=refresh_exp,
    )
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


def _ensure_task_access(task: dict[str, Any], user: dict[str, Any]) -> None:
    if str(task["tenant_id"]) != str(user["tenant_id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="cross-tenant access denied")
    if user["role"] in {"owner", "operator"}:
        return
    if str(task["created_by"]) != str(user["id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="task not visible")


def _load_task_or_404(task_id: str, tenant_id: str) -> dict[str, Any]:
    task = task_repo.get_task_by_id(tenant_id=tenant_id, task_id=task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return task


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics(_: None = Depends(_metrics_auth)) -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/auth/register", response_model=TokenResponse)
def register(req: RegisterRequest, x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id")) -> TokenResponse:
    with tracer.start_as_current_span("auth_register"):
        tenant_id = _resolve_auth_tenant(x_tenant_id)
        if auth_repo.user_exists(tenant_id, req.email):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="email already exists")
        role = "owner" if auth_repo.count_users(tenant_id) == 0 else "user"
        user = auth_repo.create_user(tenant_id, req.email, hash_password(req.password), role)
        return _issue_tokens(user)


@app.post("/auth/login", response_model=TokenResponse)
def login(req: LoginRequest, x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id")) -> TokenResponse:
    with tracer.start_as_current_span("auth_login"):
        tenant_id = _resolve_auth_tenant(x_tenant_id)
        user = auth_repo.get_user_by_email(tenant_id, req.email)
        if not user or not user["is_active"] or not verify_password(req.password, user["password_hash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid credentials")
        return _issue_tokens(user)


@app.post("/auth/refresh", response_model=TokenResponse)
def refresh(req: RefreshRequest) -> TokenResponse:
    with tracer.start_as_current_span("auth_refresh"):
        try:
            payload = decode_token(req.refresh_token)
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid refresh token") from exc
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token type")
        tenant_id = str(payload.get("tenant_id") or settings.default_tenant_id)
        token_row = auth_repo.consume_refresh_token(tenant_id=tenant_id, token_hash=hash_token(req.refresh_token))
        if not token_row:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid refresh token")
        user = auth_repo.get_user_by_id(tenant_id, str(token_row["user_id"]))
        if not user or not user["is_active"]:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="inactive user")
        return _issue_tokens(user)


@app.post("/auth/logout")
def logout(req: LogoutRequest, user: dict[str, Any] = Depends(get_current_user)) -> dict[str, str]:
    auth_repo.revoke_refresh_token_for_user(
        tenant_id=str(user["tenant_id"]),
        token_hash=hash_token(req.refresh_token),
        user_id=str(user["id"]),
    )
    return {"status": "ok"}


@app.post("/assistant/chat", response_model=AssistantChatResponse)
async def assistant_chat(
    req: AssistantChatRequest,
    request: Request,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> AssistantChatResponse:
    with tracer.start_as_current_span("assistant_chat"):
        if str(req.user_id) != str(user["id"]):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user mismatch")
        tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
        result = await service_assistant_chat(
            conversation_repo=conversation_repo,
            episode_repo=episode_repo,
            turn_repo=turn_repo,
            task_repo=task_repo,
            tool_repo=tool_repo,
            policy_repo=policy_repo,
            goal_repo=goal_repo,
            gateway=gateway,
            req=req,
            tenant_id=tenant_id,
            user=user,
            trace_id=request.state.trace_id,
            start_workflow=start_task_workflow,
        )
        return AssistantChatResponse(**result)


@app.get("/assistant/conversations", response_model=list[AssistantConversationSummary])
def list_assistant_conversations(
    limit: int = Query(default=30, ge=1, le=200),
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> list[AssistantConversationSummary]:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    rows = conversation_repo.list_conversations_for_user(
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        limit=limit,
    )
    return [AssistantConversationSummary(**build_conversation_summary(row)) for row in rows]


@app.get("/assistant/conversations/{conversation_id}", response_model=AssistantConversationDetail)
def get_assistant_conversation(
    conversation_id: str,
    task_limit: int = Query(default=30, ge=1, le=200),
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> AssistantConversationDetail:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    conversation = conversation_repo.get_conversation(tenant_id=tenant_id, conversation_id=conversation_id)
    if not conversation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="conversation not found")
    if str(conversation["user_id"]) != str(user["id"]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="conversation ownership mismatch")

    tasks = task_repo.list_assistant_tasks_for_conversation(
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=task_limit,
    )
    task_card_dicts = [build_task_card(row) for row in tasks]
    task_cards = [AssistantTaskCard(**row) for row in task_card_dicts]
    task_card_by_turn_id = {
        str(task.get("assistant_turn_id") or ""): card
        for task, card in zip(tasks, task_card_dicts)
        if str(task.get("assistant_turn_id") or "")
    }
    turns = turn_repo.list_turns_for_conversation(
        tenant_id=tenant_id,
        user_id=str(user["id"]),
        conversation_id=conversation_id,
        limit=task_limit,
    )
    turn_history = [AssistantTurnSummary(**build_turn_summary(row, task_card_by_turn_id.get(str(row.get("turn_id") or "")))) for row in turns]
    history = list(conversation.get("message_history") or [])
    return AssistantConversationDetail(
        conversation_id=conversation_id,
        user_id=str(conversation["user_id"]),
        created_at=conversation.get("created_at"),
        updated_at=conversation.get("updated_at"),
        context_window=len(history),
        message_history=history,
        memory=build_memory_snapshot(conversation),
        turn_history=turn_history,
        task_history=task_cards,
    )


@app.get("/assistant/tasks/{task_id}/trace", response_model=AssistantTaskTraceResponse)
def get_assistant_task_trace(
    task_id: str,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> AssistantTaskTraceResponse:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    task = task_repo.get_task_by_id(tenant_id=tenant_id, task_id=task_id, include_sensitive=True)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    _ensure_task_access(task, user)
    runs = task_repo.list_runs_for_task(tenant_id=tenant_id, task_id=task_id)
    run_ids = [row["id"] for row in runs]
    steps = task_repo.list_steps_for_run_ids(tenant_id=tenant_id, run_ids=run_ids)
    tool_calls = task_repo.list_tool_calls_for_task(tenant_id=tenant_id, task_id=task_id)
    approvals = task_repo.list_approvals_for_task(tenant_id=tenant_id, task_id=task_id)
    trace_view = build_task_trace_view(
        task=task,
        runs=runs,
        steps=steps,
        tool_calls=tool_calls,
        approvals=approvals,
    )
    return AssistantTaskTraceResponse(**trace_view)


@app.get("/assistant/tools", response_model=list[AssistantToolRegistryItem])
def list_assistant_tools(
    use_case: str = Query(default=""),
    enabled_only: bool = Query(default=True),
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> list[AssistantToolRegistryItem]:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    rows = tool_registry_service.list_tools(
        tenant_id=tenant_id,
        enabled_only=enabled_only,
        use_case=use_case or None,
    )
    return [AssistantToolRegistryItem(**row) for row in rows]


@app.post("/assistant/tools", response_model=AssistantToolRegistryItem)
def upsert_assistant_tool(
    req: AssistantToolRegistryUpsertRequest,
    request: Request,
    user: dict[str, Any] = Depends(require_role("owner")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> AssistantToolRegistryItem:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    try:
        row = tool_registry_service.upsert_tool(
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            payload=req.model_dump(),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="tool not found") from exc
    task_repo.insert_audit_log(
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        action="assistant_tool_registry_upsert",
        target_type="tool",
        target_id=f"{req.tool_name}:{req.version}",
        detail_masked={"tool_name": req.tool_name, "version": req.version, "risk_level": req.risk_level},
        trace_id=request.state.trace_id,
    )
    return AssistantToolRegistryItem(**row)


@app.post("/tasks")
async def create_task(
    req: TaskCreateRequest,
    request: Request,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    with tracer.start_as_current_span("task_create"):
        tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
        return await service_create_task(
            task_repo=task_repo,
            req=req,
            tenant_id=tenant_id,
            user=user,
            trace_id=request.state.trace_id,
            start_workflow=start_task_workflow,
        )

@app.get("/tasks")
def list_tasks(
    status_filter: str = Query(default="", alias="status"),
    task_type: str = Query(default=""),
    from_ts: str = Query(default=""),
    to_ts: str = Query(default=""),
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    created_by = str(user["id"]) if user["role"] == "user" else None
    return task_repo.list_tasks(
        tenant_id=tenant_id,
        status_filter=status_filter,
        task_type=task_type,
        from_ts=from_ts,
        to_ts=to_ts,
        created_by=created_by,
    )


@app.get("/tasks/{task_id}")
def get_task(
    task_id: str,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    task = _load_task_or_404(task_id, tenant_id)
    _ensure_task_access(task, user)
    runs = task_repo.list_runs_for_task(tenant_id, task_id)
    run_ids = [r["id"] for r in runs]
    return {
        "task": task,
        "runs": runs,
        "steps": task_repo.list_steps_for_run_ids(tenant_id, run_ids),
        "tool_calls": task_repo.list_tool_calls_for_task(tenant_id, task_id),
        "approvals": task_repo.list_approvals_for_task(tenant_id, task_id),
        "artifacts": task_repo.list_artifacts_for_task(tenant_id, task_id),
        "cost_ledger": task_repo.list_cost_for_task(tenant_id, task_id),
    }


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    request: Request,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return await service_cancel_task(
        task_repo=task_repo,
        task_id=task_id,
        tenant_id=tenant_id,
        user=user,
        trace_id=request.state.trace_id,
        ensure_task_access=_ensure_task_access,
        cancel_workflow=cancel_workflow,
    )


@app.post("/tasks/{task_id}/rerun")
async def rerun_task(
    task_id: str,
    request: Request,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return await service_rerun_task(
        task_repo=task_repo,
        task_id=task_id,
        tenant_id=tenant_id,
        user=user,
        trace_id=request.state.trace_id,
        ensure_task_access=_ensure_task_access,
        start_workflow=start_task_workflow,
    )


@app.get("/runs/{run_id}")
def get_run(
    run_id: str,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    run = task_repo.get_run_by_id(tenant_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    task = _load_task_or_404(str(run["task_id"]), tenant_id)
    _ensure_task_access(task, user)
    return {
        "run": run,
        "steps": task_repo.list_steps_for_run(tenant_id, run_id),
        "tool_calls": task_repo.list_tool_calls_for_run(tenant_id, run_id),
        "cost_ledger": task_repo.list_cost_for_run(tenant_id, run_id),
    }


@app.get("/steps")
def get_steps(
    run_id: str,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    run = task_repo.get_run_by_id(tenant_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    task = _load_task_or_404(str(run["task_id"]), tenant_id)
    _ensure_task_access(task, user)
    return task_repo.list_steps_for_run(tenant_id, run_id)


@app.get("/approvals")
def list_approvals(
    status_filter: str = Query(default="", alias="status"),
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return task_repo.list_approvals(tenant_id, status_filter)


@app.post("/approvals/{approval_id}/approve")
async def approve(
    approval_id: str,
    req: ApprovalActionRequest,
    request: Request,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    with tracer.start_as_current_span("approval_action") as span:
        span.set_attribute("approval.id", approval_id)
        span.set_attribute("approval.action", "approve")
        tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
        return await apply_approval_decision(
            action="approve",
            approval_id=approval_id,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            reason=req.reason,
            edited_output=None,
            trace_id=request.state.trace_id,
            task_repo=task_repo,
            signal_approval=signal_approval,
            goal_repo=goal_repo,
        )


@app.post("/approvals/{approval_id}/reject")
async def reject(
    approval_id: str,
    req: ApprovalActionRequest,
    request: Request,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    with tracer.start_as_current_span("approval_action") as span:
        span.set_attribute("approval.id", approval_id)
        span.set_attribute("approval.action", "reject")
        tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
        return await apply_approval_decision(
            action="reject",
            approval_id=approval_id,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            reason=req.reason,
            edited_output=None,
            trace_id=request.state.trace_id,
            task_repo=task_repo,
            signal_approval=signal_approval,
            goal_repo=goal_repo,
        )


@app.post("/approvals/{approval_id}/edit")
async def edit_approval(
    approval_id: str,
    req: ApprovalEditRequest,
    request: Request,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    with tracer.start_as_current_span("approval_action") as span:
        span.set_attribute("approval.id", approval_id)
        span.set_attribute("approval.action", "edit")
        tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
        return await apply_approval_decision(
            action="edit",
            approval_id=approval_id,
            tenant_id=tenant_id,
            actor_user_id=str(user["id"]),
            reason=req.reason,
            edited_output=req.edited_output,
            trace_id=request.state.trace_id,
            task_repo=task_repo,
            signal_approval=signal_approval,
            goal_repo=goal_repo,
        )


@app.get("/tools")
def list_tools(
    enabled_only: bool = Query(default=False),
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return tool_repo.list_tools(tenant_id, enabled_only)


@app.post("/tools")
def create_tool(
    manifest: ToolManifestUpsert,
    request: Request,
    user: dict[str, Any] = Depends(require_role("owner")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    tool_repo.upsert_tool(tenant_id, str(user["id"]), manifest.model_dump())
    task_repo.insert_audit_log(
        tenant_id=tenant_id,
        actor_user_id=str(user["id"]),
        action="tool_upsert",
        target_type="tool",
        target_id=f"{manifest.tool_id}:{manifest.version}",
        detail_masked={"tool_id": manifest.tool_id, "version": manifest.version},
        trace_id=request.state.trace_id,
    )
    return {"status": "ok"}


@app.put("/tools/{tool_id}/{version}")
def update_tool(
    tool_id: str,
    version: str,
    manifest: ToolManifestUpsert,
    request: Request,
    user: dict[str, Any] = Depends(require_role("owner")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    if tool_id != manifest.tool_id or version != manifest.version:
        raise HTTPException(status_code=400, detail="path/body mismatch")
    return create_tool(manifest, request, user, x_tenant_id)


@app.get("/audit/logs")
def get_audit_logs(
    limit: int = 200,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return task_repo.list_audit_logs(tenant_id, limit)


@app.get("/audit/tool-calls")
def get_audit_tool_calls(
    limit: int = 200,
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return task_repo.list_audit_tool_calls(tenant_id, limit)


@app.get("/metrics/summary")
def metrics_summary(
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return task_repo.metrics_summary(tenant_id)


@app.get("/metrics/cost")
def metrics_cost(
    user: dict[str, Any] = Depends(require_role("operator")),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
):
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    return task_repo.metrics_cost_rows(tenant_id)

@app.post("/events/token")
def create_events_token(
    task_id: str,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(str(user["tenant_id"]), x_tenant_id)
    task = _load_task_or_404(task_id, tenant_id)
    _ensure_task_access(task, user)
    token = create_task_event_token(
        user_id=str(user["id"]),
        tenant_id=tenant_id,
        task_id=task_id,
        ttl_seconds=60,
    )
    return {"token": token, "expires_in_sec": 60}


@app.get("/events")
async def stream_events(
    task_id: str,
    sse_token: str = Query(default=""),
    user: dict[str, Any] | None = Depends(get_optional_user),
):
    if user is None:
        if not sse_token:
            raise HTTPException(status_code=401, detail="missing auth")
        try:
            payload = decode_task_event_token(sse_token)
        except Exception as exc:
            raise HTTPException(status_code=401, detail="invalid sse token") from exc
        if str(payload.get("task_id")) != task_id:
            raise HTTPException(status_code=403, detail="task scope mismatch")
        user = auth_repo.get_user_by_id(str(payload.get("tenant_id")), str(payload["sub"]))
        if not user or not user["is_active"]:
            raise HTTPException(status_code=401, detail="inactive user")

    tenant_id = str(user["tenant_id"])
    task = _load_task_or_404(task_id, tenant_id)
    _ensure_task_access(task, user)

    async def event_generator():
        last_status = ""
        last_step_id = 0
        while True:
            current = _load_task_or_404(task_id, tenant_id)
            if current["status"] != last_status:
                data = {
                    "task_id": task_id,
                    "status": current["status"],
                    "trace_id": current["trace_id"],
                    "updated_at": current["updated_at"].isoformat() if current["updated_at"] else None,
                }
                yield f"event: status\\ndata: {json.dumps(data)}\\n\\n"
                last_status = current["status"]
            for st in task_repo.list_new_steps_for_sse(tenant_id, task_id, last_step_id):
                last_step_id = int(st["id"])
                step_data = {
                    "id": int(st["id"]),
                    "run_id": str(st["run_id"]),
                    "step_key": st["step_key"],
                    "status": st["status"],
                    "created_at": st["created_at"].isoformat() if st["created_at"] else None,
                }
                yield f"event: step\\ndata: {json.dumps(step_data)}\\n\\n"
            if current["status"] in FINAL_STATES:
                yield 'event: done\\ndata: {"done": true}\\n\\n'
                break
            await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/internal/tool-gateway/execute")
async def internal_tool_execute(
    req: InternalToolExecuteRequest,
    worker_id: str = Depends(_internal_auth),
):
    return await execute_internal_tool(
        task_repo=task_repo,
        gateway=gateway,
        req=req,
        worker_id=worker_id,
    )


@app.post("/internal/tasks/{task_id}/status")
def internal_task_status(
    task_id: str,
    body: InternalTaskStatusRequest,
    worker_id: str = Depends(_internal_auth),
):
    return update_internal_task_status(
        task_repo=task_repo,
        conversation_repo=conversation_repo,
        turn_repo=turn_repo,
        episode_repo=episode_repo,
        policy_repo=policy_repo,
        goal_repo=goal_repo,
        task_id=task_id,
        body=body.model_dump(exclude_none=True),
        worker_id=worker_id,
    )


@app.post("/internal/goals/external-signal")
def internal_goal_external_signal(
    body: InternalGoalExternalSignalRequest,
    request: Request,
    worker_id: str = Depends(_internal_auth),
):
    tenant_id = _resolve_auth_tenant(body.tenant_id)
    return dispatch_external_signal(
        goal_repo=goal_repo,
        policy_repo=policy_repo,
        task_repo=task_repo,
        tenant_id=tenant_id,
        worker_id=worker_id,
        signal=body.model_dump(exclude_none=True),
        trace_id=request.state.trace_id,
    )


@app.post("/internal/goals/external-signal/{source}")
def internal_goal_external_adapter_signal(
    source: str,
    body: InternalGoalExternalAdapterRequest,
    request: Request,
    worker_id: str = Depends(_internal_auth),
):
    tenant_id = _resolve_auth_tenant(body.tenant_id)
    try:
        return dispatch_external_adapter_signal(
            goal_repo=goal_repo,
            policy_repo=policy_repo,
            task_repo=task_repo,
            tenant_id=tenant_id,
            worker_id=worker_id,
            source=source,
            signal=body.model_dump(exclude_none=True),
            trace_id=request.state.trace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
