export type ThemeMode = "light" | "dark";

export type AuthStorageMode = "memory" | "sessionStorage" | "localStorage";

export const FINAL_STATES = [
  "SUCCEEDED",
  "FAILED_RETRYABLE",
  "FAILED_FINAL",
  "CANCELLED",
  "TIMED_OUT"
] as const;

export type FinalState = (typeof FINAL_STATES)[number];

export type TaskStatus =
  | "RECEIVED"
  | "QUEUED"
  | "VALIDATING"
  | "PLANNING"
  | "RUNNING"
  | "WAITING_TOOL"
  | "WAITING_HUMAN"
  | "REVIEWING"
  | "SUCCEEDED"
  | "FAILED_RETRYABLE"
  | "FAILED_FINAL"
  | "CANCELLED"
  | "TIMED_OUT";

export interface ProtocolMessage {
  id?: string;
  ts?: string;
  turn?: number;
  phase?: string;
  agent?: string;
  type?: string;
  status?: string;
  verdict?: string;
  content?: string;
  payload?: Record<string, unknown>;
  raw?: Record<string, unknown>;
}

export interface EvidenceItem {
  id?: string;
  source?: string;
  title?: string;
  snippet?: string;
  confidence?: number;
  conflict?: boolean;
  tags?: string[];
  raw?: Record<string, unknown>;
}

export interface CriticVerdict {
  verdict: "PASS" | "FAIL" | "PENDING" | "NEED_INFO";
  failureType?: string;
  failureSemantic?: string;
  stopReason?: string;
  fixInstructions: string[];
}

export interface Metrics {
  messageTotal?: number;
  tokenIn?: number;
  tokenOut?: number;
  totalCost?: number;
  elapsedMs?: number;
  raw: Record<string, unknown>;
}

export interface MASState {
  task_state?: string;
  plan_state?: string;
  evidence?: EvidenceItem[];
  risk_level?: string;
  retry_budget?: {
    remaining?: number;
    max?: number;
  };
  latency_budget?: {
    remaining_ms?: number;
    max_ms?: number;
  };
  turn?: number;
  phase?: string;
  status?: string;
  verdict?: string;
  stop_reason?: string;
  trace_id?: string;
  metrics?: Record<string, unknown>;
  msgs?: ProtocolMessage[];
  fix_instructions?: string[];
}

export interface RunSummary {
  taskId: string;
  runId: string;
  runNo: number;
  taskType: string;
  status: TaskStatus | string;
  phase?: string;
  turn?: number;
  retryRemaining?: number;
  latencyRemainingMs?: number;
  verdict?: string;
  riskLevel?: string;
  traceId?: string;
  createdAt?: string;
  endedAt?: string | null;
  costUsd?: number;
  mode?: string;
  shadowEnabled?: boolean;
}

export interface StepRecord {
  id: number;
  run_id: string;
  step_key: string;
  status: string;
  payload_masked: Record<string, unknown>;
  trace_id?: string;
  attempt?: number;
  created_at?: string;
}

export interface TaskRecord {
  id: string;
  task_type: string;
  status: string;
  trace_id: string;
  cost_total: number;
  created_at: string;
  updated_at?: string;
  requires_hitl?: boolean;
}

export interface RunDetailResponse {
  run: Record<string, unknown>;
  steps: StepRecord[];
  tool_calls: Array<Record<string, unknown>>;
  cost_ledger: Array<Record<string, unknown>>;
}

export interface TaskDetailResponse {
  task: Record<string, unknown>;
  runs: Array<Record<string, unknown>>;
  steps: StepRecord[];
  tool_calls: Array<Record<string, unknown>>;
  approvals: Array<Record<string, unknown>>;
  artifacts: Array<Record<string, unknown>>;
  cost_ledger: Array<Record<string, unknown>>;
}

export interface TimelineEvent {
  id: string;
  ts?: string;
  turn?: number;
  phase?: string;
  agent: string;
  type: string;
  status?: string;
  verdict?: string;
  title: string;
  payload: Record<string, unknown>;
  fallbackHighlighted?: boolean;
}

export interface AssistantTaskRef {
  task_id: string;
  run_id?: string | null;
  status: string;
}

export interface AgentObservation {
  kind: string;
  summary: string;
  source?: string | null;
  payload: Record<string, unknown>;
}

export interface AgentDecision {
  action: string;
  intent?: string | null;
  route?: string | null;
  selected_tool?: string | null;
  confidence?: number | null;
  need_confirmation: boolean;
  summary?: string | null;
}

export interface AgentReflection {
  summary: string;
  requires_replan: boolean;
  next_action?: string | null;
}

export interface AgentGoal {
  user_intent?: string | null;
  normalized_goal: string;
  constraints: string[];
  success_criteria: string[];
  unknowns: string[];
  risk_level: string;
}

export interface AgentAction {
  action_type: string;
  rationale?: string | null;
  target?: string | null;
  input: Record<string, unknown>;
  requires_approval: boolean;
  status: string;
}

export interface AgentPolicy {
  selected_action: string;
  reasoning: string[];
  fallback_action?: string | null;
  replan_triggers: string[];
  approval_triggered: boolean;
  ask_user_triggered: boolean;
  episode_retrieval_triggered: boolean;
  similar_episode_ids: string[];
  planner_action?: string | null;
  risk_level?: string | null;
}

export interface AgentEpisode {
  episode_id: string;
  task_summary: string;
  chosen_strategy: string;
  steps_taken: string[];
  tool_usage: string[];
  final_outcome: string;
  useful_lessons: string[];
  similarity?: number | null;
  outcome_status?: string | null;
}

export interface AgentTaskState {
  current_goal: AgentGoal;
  current_subgoals: string[];
  current_phase: string;
  current_action_candidate?: AgentAction | null;
  observations: AgentObservation[];
  beliefs: string[];
  known_facts: string[];
  blockers: string[];
  pending_approvals: string[];
  fallback_state?: string | null;
  latest_result: Record<string, unknown>;
  available_actions: string[];
  unknowns: string[];
}

export interface UnifiedTaskModel {
  goal: AgentGoal;
  available_actions: string[];
  current_beliefs: string[];
  current_facts: string[];
  planner_signal: Record<string, unknown>;
  episode_context: AgentEpisode[];
}

export interface AgentRuntimeStep {
  key: string;
  phase: string;
  title: string;
  status: string;
  summary: string;
  created_at?: string;
  observation?: AgentObservation | null;
  decision?: AgentDecision | null;
  reflection?: AgentReflection | null;
}

export interface AgentRun {
  turn_id: string;
  route: string;
  status: string;
  current_phase: string;
  task_id?: string | null;
  trace_id: string;
  planner: Record<string, unknown>;
  retrieval_hits: Array<Record<string, unknown>>;
  memory: Record<string, unknown>;
  goal?: AgentGoal | null;
  unified_task?: UnifiedTaskModel | null;
  task_state?: AgentTaskState | null;
  current_action?: AgentAction | null;
  policy?: AgentPolicy | null;
  episodes: AgentEpisode[];
  observations: AgentObservation[];
  decision?: AgentDecision | null;
  reflection?: AgentReflection | null;
  steps: AgentRuntimeStep[];
  final_output: Record<string, unknown>;
}

export interface AssistantTurnSummary {
  turn_id: string;
  route: "direct_answer" | "tool_task" | "workflow_task";
  status: string;
  current_phase: string;
  response_type: string;
  user_message: string;
  assistant_message?: string | null;
  task_id?: string | null;
  trace_id: string;
  created_at?: string;
  updated_at?: string;
  agent_run: AgentRun;
}

export interface AssistantChatResponse {
  conversation_id: string;
  route: "direct_answer" | "tool_task" | "workflow_task";
  response_type: "direct_answer" | "task_created";
  message: string;
  task: AssistantTaskRef | null;
  tool_result: Record<string, unknown> | null;
  planner?: Record<string, unknown> | null;
  retrieval_hits: Array<Record<string, unknown>>;
  memory?: Record<string, unknown> | null;
  need_confirmation: boolean;
  trace_id: string;
  turn?: AssistantTurnSummary | null;
}

export interface AssistantConversationSummary {
  conversation_id: string;
  created_at?: string;
  updated_at?: string;
  last_user_message?: string | null;
  last_assistant_message?: string | null;
  last_route?: string | null;
  task_count: number;
  running_task_count: number;
  waiting_approval_count: number;
}

export interface AssistantTaskCard {
  task_id: string;
  task_type: string;
  task_kind: string;
  route: "direct_answer" | "tool_task" | "workflow_task";
  status: string;
  status_label: string;
  progress_message: string;
  current_step?: string | null;
  waiting_for?: string | null;
  next_action?: string | null;
  tool_call_count: number;
  waiting_approval_count: number;
  created_at?: string;
  updated_at?: string;
  trace_id: string;
  result_preview?: string | null;
  failure_reason?: string | null;
}

export interface AssistantConversationDetail {
  conversation_id: string;
  user_id: string;
  created_at?: string;
  updated_at?: string;
  context_window: number;
  message_history: Array<Record<string, unknown>>;
  memory: Record<string, unknown>;
  turn_history: AssistantTurnSummary[];
  task_history: AssistantTaskCard[];
}

export interface AssistantTraceStep {
  step_key: string;
  title: string;
  status: string;
  status_label: string;
  created_at?: string;
  detail?: string | null;
}

export interface AssistantTraceToolCall {
  tool_call_id: string;
  tool_name: string;
  status: string;
  status_label: string;
  reason_code?: string | null;
  duration_ms: number;
  why_this_tool?: string | null;
  request_summary?: string | null;
  response_summary?: string | null;
  created_at?: string;
}

export interface AssistantTraceApproval {
  approval_id: string;
  status: string;
  status_label: string;
  reason?: string | null;
  created_at?: string;
  updated_at?: string;
  action_hint?: string | null;
}

export interface AssistantTaskTrace {
  task: AssistantTaskCard;
  task_summary: string;
  planner: Record<string, unknown>;
  retrieval_hits: Array<Record<string, unknown>>;
  goal?: AgentGoal | null;
  unified_task?: UnifiedTaskModel | null;
  task_state?: AgentTaskState | null;
  current_action?: AgentAction | null;
  policy?: AgentPolicy | null;
  episodes: AgentEpisode[];
  reflection?: AgentReflection | null;
  trace_steps: AssistantTraceStep[];
  tool_calls: AssistantTraceToolCall[];
  approvals: AssistantTraceApproval[];
  run_history: Array<Record<string, unknown>>;
  final_output: Record<string, unknown>;
  failure_reason?: string | null;
  is_final: boolean;
}
