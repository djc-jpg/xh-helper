import {
  AssistantChatResponse,
  AssistantConversationDetail,
  AssistantConversationSummary,
  AssistantTaskTrace,
  RunDetailResponse,
  RunSummary,
  TaskDetailResponse,
  TaskRecord,
  type AuthStorageMode,
  type StepRecord
} from "./mas-types";
import { extractMasEnvelope, extractMasState } from "./mas-utils";

export function resolveApiBase(): string {
  if (process.env.NEXT_PUBLIC_API_BASE_URL) {
    return process.env.NEXT_PUBLIC_API_BASE_URL;
  }
  if (typeof window !== "undefined" && window.location?.hostname) {
    return `${window.location.protocol}//${window.location.hostname}:18000`;
  }
  return "http://localhost:18000";
}

export const API_BASE = resolveApiBase();

export interface AuthTokens {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

export interface UserClaims {
  sub: string;
  email: string;
  role: string;
  tenant_id: string;
  exp: number;
  iat: number;
}

export class ApiError extends Error {
  status: number;
  detail: string;
  requestId: string;

  constructor({ status, detail, requestId }: { status: number; detail: string; requestId: string }) {
    super(`${status}: ${detail}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
    this.requestId = requestId;
  }
}

let unauthorizedHandler: ((error: ApiError) => void) | null = null;
const UNAUTHORIZED_THROTTLE_MS = 1000;
let lastUnauthorizedAtMs = 0;

export function setUnauthorizedHandler(handler: ((error: ApiError) => void) | null): void {
  unauthorizedHandler = handler;
  lastUnauthorizedAtMs = 0;
}

function notifyUnauthorized(error: ApiError, allowAnonymous: boolean): void {
  if (allowAnonymous || !unauthorizedHandler) {
    return;
  }
  const now = Date.now();
  if (now - lastUnauthorizedAtMs < UNAUTHORIZED_THROTTLE_MS) {
    return;
  }
  lastUnauthorizedAtMs = now;
  unauthorizedHandler(error);
}

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `rid-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function parseJwt(token: string): UserClaims | null {
  try {
    const payload = token.split(".")[1];
    if (!payload) {
      return null;
    }
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
    const decoded = atob(padded);
    const json = JSON.parse(decoded) as UserClaims;
    return json;
  } catch {
    return null;
  }
}

async function parseErrorDetail(resp: Response): Promise<string> {
  try {
    const data = await resp.json();
    if (typeof data?.detail === "string") {
      return data.detail;
    }
    if (typeof data?.detail === "object" && data?.detail) {
      return JSON.stringify(data.detail);
    }
    return JSON.stringify(data);
  } catch {
    try {
      return await resp.text();
    } catch {
      return "request_failed";
    }
  }
}

const DETAIL_LABELS: Array<[string, string]> = [
  ["backend_unreachable", "服务暂时不可用，请稍后再试。"],
  ["assistant_stream_failed", "这次回复在生成途中中断了，请稍后再试。"],
  ["stream_failed", "这次回复在生成途中中断了，请稍后再试。"],
  ["missing_token", "登录状态已失效，请重新登录。"],
  ["missing auth", "登录状态已失效，请重新登录。"],
  ["user mismatch", "当前登录身份和会话用户不一致，请刷新后重试。"],
  ["conversation ownership mismatch", "你没有权限访问这条会话。"],
  ["workflow_start_failed", "任务启动失败了，请稍后再试。"],
  ["tool_denied", "这一步需要更高权限或人工确认。"],
  ["approval_not_approved", "因为没有通过确认，这次操作没有继续执行。"],
  ["approval_invalid", "这次确认已经失效，请重新发起。"],
  ["approval_context_invalid", "确认上下文已经变化，请重新发起这次操作。"],
  ["qwen_not_configured", "当前模型服务还没有配置完成。"],
  ["qwen_empty_response", "模型这次没有返回内容，请再试一次。"],
  ["idempotency_in_progress", "同一个请求还在处理中，稍后就会同步结果。"],
  ["adapter_http_408", "外部服务响应超时了，请稍后重试。"],
  ["adapter_http_429", "外部服务当前较忙，请稍后重试。"],
  ["adapter_http_5xx", "外部服务暂时不可用，请稍后重试。"],
  ["adapter_network_error", "连接外部服务时出了问题，请稍后重试。"],
  ["timed_out", "处理超时了，请缩小范围后再试一次。"],
  ["timeout", "处理超时了，请缩小范围后再试一次。"],
  ["request_failed", "请求失败了，请稍后再试。"]
];

export function humanizeErrorDetail(detail: string): string {
  const normalized = String(detail || "").trim();
  if (!normalized) {
    return "请求失败了，请稍后再试。";
  }
  const lowered = normalized.toLowerCase();
  for (const [token, label] of DETAIL_LABELS) {
    if (lowered === token || lowered.includes(token)) {
      return label;
    }
  }
  return normalized;
}

export function getDisplayErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return humanizeErrorDetail(error.detail);
  }
  if (error instanceof TypeError) {
    return "服务暂时不可用，请稍后再试。";
  }
  if (error instanceof Error) {
    return humanizeErrorDetail(error.message);
  }
  return "请求失败了，请稍后再试。";
}

interface RequestOptions {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  token?: string;
  allowAnonymous?: boolean;
  body?: unknown;
  signal?: AbortSignal;
  headers?: HeadersInit;
}

export async function requestJson<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const requestId = createRequestId();
  const traceId = createRequestId();
  const {
    method = "GET",
    token,
    allowAnonymous = false,
    body,
    signal,
    headers: extraHeaders
  } = options;

  if (!allowAnonymous && !token) {
    const error = new ApiError({ status: 401, detail: "missing_token", requestId });
    notifyUnauthorized(error, allowAnonymous);
    throw error;
  }

  const headers = new Headers(extraHeaders || {});
  headers.set("X-Request-Id", requestId);
  headers.set("X-Trace-Id", traceId);
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  if (body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let resp: Response;
  try {
    resp = await fetch(`${API_BASE}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
      cache: "no-store"
    });
  } catch {
    throw new ApiError({ status: 0, detail: "backend_unreachable", requestId });
  }

  if (!resp.ok) {
    const detail = await parseErrorDetail(resp);
    const error = new ApiError({ status: resp.status, detail, requestId });
    if (resp.status === 401) {
      notifyUnauthorized(error, allowAnonymous);
    }
    throw error;
  }

  if (resp.status === 204) {
    return {} as T;
  }

  return (await resp.json()) as T;
}

export function decodeUserClaims(accessToken: string): UserClaims | null {
  return parseJwt(accessToken);
}

export async function authLogin(email: string, password: string): Promise<AuthTokens> {
  return requestJson<AuthTokens>("/auth/login", {
    method: "POST",
    allowAnonymous: true,
    body: { email, password }
  });
}

export async function authRegister(email: string, password: string): Promise<AuthTokens> {
  return requestJson<AuthTokens>("/auth/register", {
    method: "POST",
    allowAnonymous: true,
    body: { email, password }
  });
}

export async function authRefresh(refreshToken: string): Promise<AuthTokens> {
  return requestJson<AuthTokens>("/auth/refresh", {
    method: "POST",
    allowAnonymous: true,
    body: { refresh_token: refreshToken }
  });
}

export async function authLogout(accessToken: string, refreshToken: string): Promise<{ status: string }> {
  return requestJson<{ status: string }>("/auth/logout", {
    method: "POST",
    token: accessToken,
    body: { refresh_token: refreshToken }
  });
}

export interface AssistantChatInput {
  userId: string;
  conversationId?: string;
  message: string;
  mode?: "auto" | "direct_answer" | "tool_task" | "workflow_task";
  metadata?: Record<string, unknown>;
}

export interface AssistantChatStreamEvent {
  type: "start" | "delta" | "complete" | "error";
  delta?: string;
  detail?: string;
  response?: AssistantChatResponse;
}

export async function assistantChat(token: string, input: AssistantChatInput): Promise<AssistantChatResponse> {
  return requestJson<AssistantChatResponse>("/assistant/chat", {
    method: "POST",
    token,
    body: {
      user_id: input.userId,
      conversation_id: input.conversationId,
      message: input.message,
      mode: input.mode || "auto",
      metadata: input.metadata || {}
    }
  });
}

export async function streamAssistantChat(
  token: string,
  input: AssistantChatInput,
  handlers: {
    signal?: AbortSignal;
    onEvent?: (event: AssistantChatStreamEvent) => void;
  } = {}
): Promise<AssistantChatResponse | null> {
  const requestId = createRequestId();
  const traceId = createRequestId();
  const headers = new Headers();
  headers.set("Authorization", `Bearer ${token}`);
  headers.set("Content-Type", "application/json");
  headers.set("X-Request-Id", requestId);
  headers.set("X-Trace-Id", traceId);

  let resp: Response;
  try {
    resp = await fetch(`${API_BASE}/assistant/chat/stream`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        user_id: input.userId,
        conversation_id: input.conversationId,
        message: input.message,
        mode: input.mode || "auto",
        metadata: input.metadata || {}
      }),
      signal: handlers.signal,
      cache: "no-store"
    });
  } catch {
    throw new ApiError({ status: 0, detail: "backend_unreachable", requestId });
  }

  if (!resp.ok) {
    const detail = await parseErrorDetail(resp);
    const error = new ApiError({ status: resp.status, detail, requestId });
    if (resp.status === 401) {
      notifyUnauthorized(error, false);
    }
    throw error;
  }

  const reader = resp.body?.getReader();
  if (!reader) {
    return null;
  }

  const decoder = new TextDecoder();
  let buffer = "";
  let completed: AssistantChatResponse | null = null;

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;
      let event: AssistantChatStreamEvent;
      try {
        event = JSON.parse(line) as AssistantChatStreamEvent;
      } catch {
        continue;
      }
      handlers.onEvent?.(event);
      if (event.type === "error") {
        throw new Error(event.detail || "stream_failed");
      }
      if (event.type === "complete" && event.response) {
        completed = event.response;
      }
    }
  }

  if (buffer.trim()) {
    try {
      const event = JSON.parse(buffer.trim()) as AssistantChatStreamEvent;
      handlers.onEvent?.(event);
      if (event.type === "error") {
        throw new Error(event.detail || "stream_failed");
      }
      if (event.type === "complete" && event.response) {
        completed = event.response;
      }
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
    }
  }

  return completed;
}

export async function listAssistantConversations(token: string, limit = 30): Promise<AssistantConversationSummary[]> {
  return requestJson<AssistantConversationSummary[]>(`/assistant/conversations?limit=${Math.max(1, limit)}`, { token });
}

export async function getAssistantConversation(
  token: string,
  conversationId: string,
  taskLimit = 30
): Promise<AssistantConversationDetail> {
  return requestJson<AssistantConversationDetail>(
    `/assistant/conversations/${encodeURIComponent(conversationId)}?task_limit=${Math.max(1, taskLimit)}`,
    { token }
  );
}

export async function updateAssistantConversation(
  token: string,
  conversationId: string,
  input: { title?: string | null }
): Promise<AssistantConversationSummary> {
  return requestJson<AssistantConversationSummary>(`/assistant/conversations/${encodeURIComponent(conversationId)}`, {
    method: "PATCH",
    token,
    body: {
      title: input.title ?? null
    }
  });
}

export async function deleteAssistantConversation(token: string, conversationId: string): Promise<void> {
  await requestJson(`/assistant/conversations/${encodeURIComponent(conversationId)}`, {
    method: "DELETE",
    token
  });
}

export async function getAssistantTaskTrace(token: string, taskId: string): Promise<AssistantTaskTrace> {
  return requestJson<AssistantTaskTrace>(`/assistant/tasks/${encodeURIComponent(taskId)}/trace`, { token });
}

export interface TaskListFilter {
  status?: string;
  taskType?: string;
  fromTs?: string;
  toTs?: string;
}

export async function listTasks(token: string, filter: TaskListFilter = {}): Promise<TaskRecord[]> {
  const params = new URLSearchParams();
  if (filter.status) params.set("status", filter.status);
  if (filter.taskType) params.set("task_type", filter.taskType);
  if (filter.fromTs) params.set("from_ts", filter.fromTs);
  if (filter.toTs) params.set("to_ts", filter.toTs);
  const query = params.toString();
  return requestJson<TaskRecord[]>(`/tasks${query ? `?${query}` : ""}`, { token });
}

export async function getTaskDetail(token: string, taskId: string): Promise<TaskDetailResponse> {
  return requestJson<TaskDetailResponse>(`/tasks/${taskId}`, { token });
}

export async function createRun(input: {
  token: string;
  taskType: string;
  payload: Record<string, unknown>;
  budget: number;
}): Promise<{ task_id: string; run_id: string; status: string; trace_id: string }> {
  return requestJson<{ task_id: string; run_id: string; status: string; trace_id: string }>("/tasks", {
    method: "POST",
    token: input.token,
    body: {
      client_request_id: `ui-${Date.now()}`,
      task_type: input.taskType,
      input: input.payload,
      budget: input.budget
    }
  });
}

export async function rerunTask(token: string, taskId: string): Promise<{ task_id: string; run_id: string; status: string }> {
  return requestJson<{ task_id: string; run_id: string; status: string }>(`/tasks/${taskId}/rerun`, {
    method: "POST",
    token
  });
}

export async function getRun(token: string, runId: string): Promise<RunDetailResponse> {
  return requestJson<RunDetailResponse>(`/runs/${runId}`, { token });
}

export async function listApprovals(token: string, status = ""): Promise<Array<Record<string, unknown>>> {
  const query = status ? `?status=${encodeURIComponent(status)}` : "";
  return requestJson<Array<Record<string, unknown>>>(`/approvals${query}`, { token });
}

export async function approvalAction(
  token: string,
  approvalId: string,
  action: "approve" | "reject" | "edit",
  payload: Record<string, unknown>
): Promise<Record<string, unknown>> {
  return requestJson<Record<string, unknown>>(`/approvals/${approvalId}/${action}`, {
    method: "POST",
    token,
    body: payload
  });
}

export async function getSummaryMetrics(token: string): Promise<Record<string, unknown>> {
  return requestJson<Record<string, unknown>>("/metrics/summary", { token });
}

export async function getCostMetrics(token: string): Promise<Array<Record<string, unknown>>> {
  return requestJson<Array<Record<string, unknown>>>("/metrics/cost", { token });
}

export async function createEventsToken(token: string, taskId: string): Promise<{ token: string; expires_in_sec: number }> {
  return requestJson<{ token: string; expires_in_sec: number }>(`/events/token?task_id=${encodeURIComponent(taskId)}`, {
    method: "POST",
    token
  });
}

function latestRunFromTask(detail: TaskDetailResponse): {
  runId: string;
  runNo: number;
  status: string;
  traceId: string;
  createdAt?: string;
  endedAt?: string | null;
  costUsd?: number;
  steps: StepRecord[];
} | null {
  const runs = (detail.runs || []) as Array<Record<string, unknown>>;
  if (runs.length === 0) {
    return null;
  }
  const sorted = [...runs].sort((a, b) => Number(a.run_no || 0) - Number(b.run_no || 0));
  const latest = sorted[sorted.length - 1];
  const runId = String(latest.id || "");
  const steps = (detail.steps || []).filter((step) => String(step.run_id) === runId);
  return {
    runId,
    runNo: Number(latest.run_no || 0),
    status: String(latest.status || detail.task.status || ""),
    traceId: String(latest.trace_id || detail.task.trace_id || ""),
    createdAt: String(latest.created_at || detail.task.created_at || ""),
    endedAt: (latest.ended_at as string | null) || null,
    costUsd: Number(latest.run_cost_usd || 0),
    steps
  };
}

export interface ListRunsInput {
  token: string;
  status?: string;
  taskType?: string;
  risk?: string;
  fromTs?: string;
  toTs?: string;
  page: number;
  pageSize: number;
}

export interface ListRunsOutput {
  items: RunSummary[];
  total: number;
  totalPages: number;
}

const MAX_LIST_RUN_DETAIL_CONCURRENCY = 8;

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  mapper: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  if (items.length === 0) {
    return [];
  }
  const workerCount = Math.max(1, Math.min(limit, items.length));
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  async function worker(): Promise<void> {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

export async function listRunSummaries(input: ListRunsInput): Promise<ListRunsOutput> {
  const tasks = await listTasks(input.token, {
    status: input.status,
    taskType: input.taskType,
    fromTs: input.fromTs,
    toTs: input.toTs
  });

  const details = await mapWithConcurrency(tasks, MAX_LIST_RUN_DETAIL_CONCURRENCY, async (task) => {
    try {
      return await getTaskDetail(input.token, task.id);
    } catch {
      return null;
    }
  });

  const summaries: RunSummary[] = [];
  for (let i = 0; i < tasks.length; i++) {
    const task = tasks[i];
    const detail = details[i];
    if (!detail) continue;
    const latest = latestRunFromTask(detail);
    if (!latest) continue;

    const snapshot = extractMasState(latest.steps);
    const envelope = extractMasEnvelope(latest.steps);
    const riskLevel = String(snapshot.risk_level || "unknown");
    if (input.risk && input.risk !== "all" && riskLevel !== input.risk) {
      continue;
    }

    summaries.push({
      taskId: task.id,
      runId: latest.runId,
      runNo: latest.runNo,
      taskType: task.task_type,
      status: latest.status,
      phase: String(snapshot.phase || ""),
      turn: typeof snapshot.turn === "number" ? snapshot.turn : undefined,
      retryRemaining:
        typeof snapshot.retry_budget?.remaining === "number" ? snapshot.retry_budget.remaining : undefined,
      latencyRemainingMs:
        typeof snapshot.latency_budget?.remaining_ms === "number" ? snapshot.latency_budget.remaining_ms : undefined,
      verdict: String(snapshot.verdict || envelope?.failure_semantic || ""),
      riskLevel,
      traceId: latest.traceId,
      createdAt: latest.createdAt,
      endedAt: latest.endedAt,
      costUsd: latest.costUsd,
      mode: envelope?.mode || "workflow",
      shadowEnabled: asBoolean((envelope?.result || {}) as Record<string, unknown>, "shadow_enabled")
    });
  }

  summaries.sort((a, b) => {
    const left = new Date(b.createdAt || "").getTime();
    const right = new Date(a.createdAt || "").getTime();
    return left - right;
  });

  const total = summaries.length;
  const totalPages = Math.max(1, Math.ceil(total / input.pageSize));
  const safePage = Math.min(Math.max(input.page, 1), totalPages);
  const start = (safePage - 1) * input.pageSize;
  const items = summaries.slice(start, start + input.pageSize);

  return {
    items,
    total,
    totalPages
  };
}

function asBoolean(record: Record<string, unknown>, key: string): boolean {
  const value = record[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    return value === "true";
  }
  return false;
}

export function storageModeLabel(mode: AuthStorageMode): string {
  if (mode === "memory") return "内存";
  if (mode === "sessionStorage") return "sessionStorage";
  return "localStorage";
}



