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

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:18000";

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

  const resp = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
    cache: "no-store"
  });

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



