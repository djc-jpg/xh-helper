from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TaskCreateRequest(BaseModel):
    client_request_id: str = Field(min_length=3, max_length=128)
    task_type: str = Field(pattern="^(rag_qa|tool_flow|ticket_email|research_summary)$")
    input: dict[str, Any]
    budget: float = Field(default=1.0, gt=0)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=128)
    assistant_turn_id: str | None = Field(default=None, min_length=1, max_length=128)
    goal_id: str | None = Field(default=None, min_length=1, max_length=128)
    origin: str = Field(default="task_api", min_length=1, max_length=32)


class ApprovalActionRequest(BaseModel):
    reason: str | None = None


class ApprovalEditRequest(BaseModel):
    edited_output: str = Field(min_length=1)
    reason: str | None = None


class ToolManifestUpsert(BaseModel):
    tool_id: str
    version: str
    description: str
    required_scopes: list[str] = Field(default_factory=list)
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    auth_type: str = "none"
    rate_limit_rpm: int = 60
    run_limit: int = 20
    timeout_connect_s: int = 5
    timeout_read_s: int = 10
    timeout_overall_s: int = 15
    idempotency_strategy: str = "tool_call_id"
    audit_fields: list[str] = Field(default_factory=list)
    masking_rules: dict[str, Any] = Field(default_factory=dict)
    egress_policy: dict[str, Any] = Field(default_factory=dict)
    risk_level: str = Field(default="low", pattern="^(low|medium|high)$")
    requires_approval: bool = False
    supported_use_cases: list[str] = Field(default_factory=list)
    enabled: bool = True


class InternalToolExecuteRequest(BaseModel):
    tenant_id: str
    tool_call_id: str
    task_id: str
    run_id: str
    task_type: str
    tool_id: str
    version: str | None = None
    payload: dict[str, Any]
    caller_user_id: str
    approval_id: str | None = None
    trace_id: str


class InternalTaskStatusRequest(BaseModel):
    tenant_id: str | None = None
    run_id: str
    status: str
    step_key: str = "worker"
    payload: dict[str, Any] = Field(default_factory=dict)
    trace_id: str | None = None
    traceId: str | None = None
    span_id: str | None = None
    attempt: int = 1
    cost: float | None = None
    status_event_id: str = Field(min_length=8, max_length=128)


class InternalGoalExternalSignalRequest(BaseModel):
    tenant_id: str | None = None
    signal_id: str | None = Field(default=None, min_length=8, max_length=128)
    source: str = Field(min_length=1, max_length=64)
    event_key: str = Field(min_length=1, max_length=256)
    event_topic: str | None = Field(default=None, min_length=1, max_length=128)
    event_aliases: list[str] = Field(default_factory=list, max_length=8)
    entity_refs: list[str] = Field(default_factory=list, max_length=12)
    payload: dict[str, Any] = Field(default_factory=dict)
    user_id: str | None = Field(default=None, min_length=1, max_length=128)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=128)
    limit: int = Field(default=20, ge=1, le=100)


class InternalGoalExternalAdapterRequest(BaseModel):
    tenant_id: str | None = None
    signal_id: str | None = Field(default=None, min_length=8, max_length=128)
    event_key: str | None = Field(default=None, min_length=1, max_length=256)
    event_topic: str | None = Field(default=None, min_length=1, max_length=128)
    event_aliases: list[str] = Field(default_factory=list, max_length=12)
    entity_refs: list[str] = Field(default_factory=list, max_length=16)
    payload: dict[str, Any] = Field(default_factory=dict)
    user_id: str | None = Field(default=None, min_length=1, max_length=128)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=128)
    limit: int = Field(default=20, ge=1, le=100)


class AssistantChatRequest(BaseModel):
    user_id: str = Field(min_length=1)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4000)
    mode: str | None = Field(default=None, pattern="^(auto|direct_answer|tool_task|workflow_task)$")
    metadata: dict[str, Any] = Field(default_factory=dict)


class AssistantConversationUpdateRequest(BaseModel):
    title: str | None = Field(default=None, max_length=120)


class AssistantTaskRef(BaseModel):
    task_id: str
    run_id: str | None = None
    status: str


class AgentObservation(BaseModel):
    kind: str
    summary: str
    source: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentDecision(BaseModel):
    action: str
    intent: str | None = None
    route: str | None = None
    selected_tool: str | None = None
    confidence: float | None = None
    need_confirmation: bool = False
    summary: str | None = None
    candidate_actions: list[dict[str, Any]] = Field(default_factory=list)
    why_not: dict[str, str] = Field(default_factory=dict)


class AgentReflection(BaseModel):
    summary: str
    requires_replan: bool = False
    next_action: str | None = None


class AgentGoal(BaseModel):
    goal_id: str | None = None
    parent_goal_id: str | None = None
    lifecycle_state: str | None = None
    user_intent: str | None = None
    normalized_goal: str
    constraints: list[str] = Field(default_factory=list)
    success_criteria: list[str] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    risk_level: str = Field(default="low", pattern="^(low|medium|high)$")


class AgentAction(BaseModel):
    action_type: str
    rationale: str | None = None
    target: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    expected_result: str | None = None
    success_conditions: list[str] = Field(default_factory=list)
    fallback: str | None = None
    stop_conditions: list[str] = Field(default_factory=list)
    requires_approval: bool = False
    status: str = "planned"


class AgentPolicy(BaseModel):
    selected_action: str
    reasoning: list[str] = Field(default_factory=list)
    fallback_action: str | None = None
    replan_triggers: list[str] = Field(default_factory=list)
    approval_triggered: bool = False
    ask_user_triggered: bool = False
    episode_retrieval_triggered: bool = False
    similar_episode_ids: list[str] = Field(default_factory=list)
    planner_action: str | None = None
    planner_signal_action: str | None = None
    risk_level: str | None = None
    policy_version_id: str | None = None
    policy_memory: dict[str, Any] = Field(default_factory=dict)


class AgentEpisode(BaseModel):
    episode_id: str
    task_summary: str
    chosen_strategy: str
    steps_taken: list[str] = Field(default_factory=list)
    tool_usage: list[str] = Field(default_factory=list)
    final_outcome: str
    useful_lessons: list[str] = Field(default_factory=list)
    similarity: float | None = None
    outcome_status: str | None = None


class AgentTaskState(BaseModel):
    current_goal: AgentGoal
    current_subgoals: list[str] = Field(default_factory=list)
    current_phase: str
    current_action_candidate: AgentAction | None = None
    observations: list["AgentObservation"] = Field(default_factory=list)
    beliefs: list[str] = Field(default_factory=list)
    known_facts: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    pending_approvals: list[str] = Field(default_factory=list)
    fallback_state: str | None = None
    latest_result: dict[str, Any] = Field(default_factory=dict)
    available_actions: list[str] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    policy_memory: dict[str, Any] = Field(default_factory=dict)


class UnifiedTaskModel(BaseModel):
    goal: AgentGoal
    available_actions: list[str] = Field(default_factory=list)
    current_beliefs: list[str] = Field(default_factory=list)
    current_facts: list[str] = Field(default_factory=list)
    planner_signal: dict[str, Any] = Field(default_factory=dict)
    episode_context: list[AgentEpisode] = Field(default_factory=list)
    policy_memory: dict[str, Any] = Field(default_factory=dict)


class AgentRuntimeStep(BaseModel):
    key: str
    phase: str
    title: str
    status: str
    summary: str
    created_at: datetime | None = None
    observation: AgentObservation | None = None
    decision: AgentDecision | None = None
    reflection: AgentReflection | None = None
    state_before: dict[str, Any] = Field(default_factory=dict)
    state_after: dict[str, Any] = Field(default_factory=dict)


class AgentRun(BaseModel):
    turn_id: str
    route: str
    status: str
    current_phase: str
    task_id: str | None = None
    trace_id: str
    planner: dict[str, Any] = Field(default_factory=dict)
    retrieval_hits: list[dict[str, Any]] = Field(default_factory=list)
    memory: dict[str, Any] = Field(default_factory=dict)
    goal_ref: dict[str, Any] = Field(default_factory=dict)
    goal: AgentGoal | None = None
    unified_task: UnifiedTaskModel | None = None
    task_state: AgentTaskState | None = None
    current_action: AgentAction | None = None
    policy: AgentPolicy | None = None
    episodes: list[AgentEpisode] = Field(default_factory=list)
    observations: list[AgentObservation] = Field(default_factory=list)
    decision: AgentDecision | None = None
    reflection: AgentReflection | None = None
    steps: list[AgentRuntimeStep] = Field(default_factory=list)
    final_output: dict[str, Any] = Field(default_factory=dict)


class AssistantTurnSummary(BaseModel):
    turn_id: str
    route: str = Field(pattern="^(direct_answer|tool_task|workflow_task)$")
    status: str
    current_phase: str
    display_state: str | None = None
    display_summary: str | None = None
    response_type: str
    user_message: str
    assistant_message: str | None = None
    task_id: str | None = None
    trace_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    agent_run: AgentRun


class AssistantChatResponse(BaseModel):
    conversation_id: str
    route: str = Field(pattern="^(direct_answer|tool_task|workflow_task)$")
    response_type: str = Field(pattern="^(direct_answer|task_created)$")
    message: str
    task: AssistantTaskRef | None = None
    tool_result: dict[str, Any] | None = None
    planner: dict[str, Any] | None = None
    retrieval_hits: list[dict[str, Any]] = Field(default_factory=list)
    memory: dict[str, Any] | None = None
    need_confirmation: bool = False
    trace_id: str
    turn: AssistantTurnSummary | None = None


class AssistantToolRegistryItem(BaseModel):
    tool_name: str
    version: str
    description: str
    input_schema: dict[str, Any]
    risk_level: str = Field(pattern="^(low|medium|high)$")
    requires_approval: bool
    supported_use_cases: list[str] = Field(default_factory=list)
    enabled: bool = True


class AssistantToolRegistryUpsertRequest(BaseModel):
    tool_name: str = Field(min_length=1, max_length=128)
    version: str = Field(default="v1", min_length=1, max_length=32)
    description: str = Field(min_length=1, max_length=4000)
    input_schema: dict[str, Any]
    risk_level: str = Field(default="low", pattern="^(low|medium|high)$")
    requires_approval: bool = False
    supported_use_cases: list[str] = Field(default_factory=list)
    enabled: bool = True


class AssistantConversationSummary(BaseModel):
    conversation_id: str
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_user_message: str | None = None
    last_assistant_message: str | None = None
    title: str | None = None
    preview: str | None = None
    last_route: str | None = None
    task_count: int = 0
    running_task_count: int = 0
    waiting_approval_count: int = 0


class AssistantTaskCard(BaseModel):
    task_id: str
    task_type: str
    task_kind: str
    route: str = Field(pattern="^(direct_answer|tool_task|workflow_task)$")
    status: str
    status_label: str
    progress_message: str
    current_step: str | None = None
    waiting_for: str | None = None
    next_action: str | None = None
    tool_call_count: int = 0
    waiting_approval_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    trace_id: str
    result_preview: str | None = None
    failure_reason: str | None = None
    chat_state: str | None = None
    assistant_summary: str | None = None


class AssistantConversationDetail(BaseModel):
    conversation_id: str
    user_id: str
    title: str | None = None
    preview: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    context_window: int = 0
    message_history: list[dict[str, Any]] = Field(default_factory=list)
    memory: dict[str, Any] = Field(default_factory=dict)
    turn_history: list[AssistantTurnSummary] = Field(default_factory=list)
    task_history: list[AssistantTaskCard] = Field(default_factory=list)


class AssistantTraceStep(BaseModel):
    step_key: str
    title: str
    status: str
    status_label: str
    created_at: datetime | None = None
    detail: str | None = None


class AssistantTraceToolCall(BaseModel):
    tool_call_id: str
    tool_name: str
    status: str
    status_label: str
    reason_code: str | None = None
    duration_ms: int = 0
    why_this_tool: str | None = None
    request_summary: str | None = None
    response_summary: str | None = None
    created_at: datetime | None = None


class AssistantTraceApproval(BaseModel):
    approval_id: str
    status: str
    status_label: str
    reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    action_hint: str | None = None


class AssistantTaskTraceResponse(BaseModel):
    task: AssistantTaskCard
    task_summary: str
    assistant_status: str | None = None
    assistant_summary: str | None = None
    next_step_hint: str | None = None
    planner: dict[str, Any] = Field(default_factory=dict)
    retrieval_hits: list[dict[str, Any]] = Field(default_factory=list)
    goal: AgentGoal | None = None
    unified_task: UnifiedTaskModel | None = None
    task_state: AgentTaskState | None = None
    current_action: AgentAction | None = None
    policy: AgentPolicy | None = None
    episodes: list[AgentEpisode] = Field(default_factory=list)
    reflection: AgentReflection | None = None
    trace_steps: list[AssistantTraceStep] = Field(default_factory=list)
    runtime_steps: list[AgentRuntimeStep] = Field(default_factory=list)
    runtime_debugger: dict[str, Any] = Field(default_factory=dict)
    tool_calls: list[AssistantTraceToolCall] = Field(default_factory=list)
    approvals: list[AssistantTraceApproval] = Field(default_factory=list)
    run_history: list[dict[str, Any]] = Field(default_factory=list)
    final_output: dict[str, Any] = Field(default_factory=dict)
    failure_reason: str | None = None
    is_final: bool = False
