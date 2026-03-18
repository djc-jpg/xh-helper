"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { AuthGate } from "../../components/auth-gate";
import { EmptyState } from "../../components/empty-state";
import { ErrorBanner } from "../../components/error-banner";
import { JsonViewer } from "../../components/json-viewer";
import { SectionCard } from "../../components/section-card";
import { StatusBadge } from "../../components/status-badge";
import { useRunStream } from "../../hooks/use-run-stream";
import { useAuth } from "../../lib/auth-context";
import {
  approvalAction,
  assistantChat,
  getAssistantConversation,
  getAssistantTaskTrace,
  listAssistantConversations,
  type AssistantChatInput
} from "../../lib/api";
import { formatDateTime, formatRelativeTime, truncate } from "../../lib/format";
import type {
  AgentRuntimeStep,
  AssistantChatResponse,
  AssistantConversationSummary
} from "../../lib/mas-types";
import { useToast } from "../../lib/toast-context";

const FINAL_STATES = new Set(["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED", "TIMED_OUT"]);

function normalizeText(value: unknown, maxLen = 140): string {
  const text = String(value || "").trim();
  if (!text) return "-";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 3)}...`;
}

function routeLabel(route: string): string {
  if (route === "direct_answer") return "直接回答";
  if (route === "tool_task") return "工具任务";
  return "工作流任务";
}

function testIdSafe(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function phaseLabel(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (s) => s.toUpperCase());
}

function humanizeKey(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (s) => s.toUpperCase());
}

type ThreadItem =
  | { id: string; type: "user"; message: string; createdAt?: string; turnId?: string; taskId?: string | null }
  | {
      id: string;
      type: "assistant";
      message: string;
      createdAt?: string;
      turnId?: string;
      route?: string;
      status?: string;
      taskId?: string | null;
    };

export default function AssistantPage() {
  const auth = useAuth();
  const queryClient = useQueryClient();
  const { pushToast } = useToast();
  const canQuery = auth.ready && auth.isAuthenticated;
  const userId = String(auth.user?.sub || "");
  const isOperator = auth.user?.role === "owner" || auth.user?.role === "operator";

  const [selectedConversationId, setSelectedConversationId] = useState("");
  const [selectedTurnId, setSelectedTurnId] = useState("");
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [inputMessage, setInputMessage] = useState("");
  const [mode, setMode] = useState<AssistantChatInput["mode"]>("auto");
  const [lastResponse, setLastResponse] = useState<AssistantChatResponse | null>(null);
  const [pendingConfirmMessage, setPendingConfirmMessage] = useState<string | null>(null);
  const [creatingNewConversation, setCreatingNewConversation] = useState(false);

  const conversationsQuery = useQuery({
    queryKey: ["assistant", "conversations"],
    enabled: canQuery,
    refetchInterval: 7000,
    queryFn: () => listAssistantConversations(auth.accessToken, 50)
  });

  useEffect(() => {
    if (!conversationsQuery.data?.length || creatingNewConversation || selectedConversationId) return;
    setSelectedConversationId(conversationsQuery.data[0].conversation_id);
  }, [conversationsQuery.data, creatingNewConversation, selectedConversationId]);

  const conversationQuery = useQuery({
    queryKey: ["assistant", "conversation", selectedConversationId],
    enabled: canQuery && Boolean(selectedConversationId),
    refetchInterval: 7000,
    queryFn: () => getAssistantConversation(auth.accessToken, selectedConversationId, 50)
  });

  useEffect(() => {
    if (!conversationQuery.data) return;
    const turns = conversationQuery.data.turn_history || [];
    if (turns.length === 0) {
      if (!selectedTaskId && conversationQuery.data.task_history[0]) {
        setSelectedTaskId(conversationQuery.data.task_history[0].task_id);
      }
      return;
    }
    if (selectedTurnId && turns.some((turn) => turn.turn_id === selectedTurnId)) {
      return;
    }
    setSelectedTurnId(turns[0].turn_id);
    if (turns[0].task_id) setSelectedTaskId(turns[0].task_id);
  }, [conversationQuery.data, selectedTaskId, selectedTurnId]);

  const selectedTurn = useMemo(() => {
    if (!conversationQuery.data?.turn_history?.length || !selectedTurnId) return null;
    return conversationQuery.data.turn_history.find((turn) => turn.turn_id === selectedTurnId) || null;
  }, [conversationQuery.data?.turn_history, selectedTurnId]);

  const selectedTaskIdForTrace = selectedTurn?.task_id || selectedTaskId || "";

  const traceQuery = useQuery({
    queryKey: ["assistant", "trace", selectedTaskIdForTrace],
    enabled: canQuery && Boolean(selectedTaskIdForTrace),
    refetchInterval: 7000,
    queryFn: () => getAssistantTaskTrace(auth.accessToken, selectedTaskIdForTrace)
  });

  const sendMutation = useMutation({
    mutationFn: async (params: { message: string; confirmed: boolean }) => {
      return assistantChat(auth.accessToken, {
        userId,
        conversationId: selectedConversationId || undefined,
        message: params.message,
        mode: mode || "auto",
        metadata: params.confirmed ? { confirmed: true } : {}
      });
    },
    onSuccess(data, variables) {
      setLastResponse(data);
      setSelectedConversationId(data.conversation_id);
      setSelectedTurnId(data.turn?.turn_id || "");
      setSelectedTaskId(data.task?.task_id || "");
      setCreatingNewConversation(false);
      setPendingConfirmMessage(data.need_confirmation ? variables.message : null);
      setInputMessage("");
      pushToast({ kind: "success", title: "xh-helper 已响应", description: routeLabel(data.route) });
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", data.conversation_id] }).catch(() => undefined);
      if (data.task?.task_id) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", data.task.task_id] }).catch(() => undefined);
      }
    },
    onError(error) {
      pushToast({ kind: "error", title: "发送失败", description: error instanceof Error ? error.message : "unknown_error" });
    }
  });

  const approvalMutation = useMutation({
    mutationFn: async (payload: { approvalId: string; action: "approve" | "reject" }) => {
      return approvalAction(auth.accessToken, payload.approvalId, payload.action, {
        reason: payload.action === "approve" ? "approved in assistant workspace" : "rejected in assistant workspace"
      });
    },
    onSuccess() {
      pushToast({ kind: "success", title: "审批已更新" });
      if (selectedTaskIdForTrace) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", selectedTaskIdForTrace] }).catch(() => undefined);
      }
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    },
    onError(error) {
      pushToast({ kind: "error", title: "审批失败", description: error instanceof Error ? error.message : "unknown_error" });
    }
  });

  useRunStream({
    accessToken: auth.accessToken,
    taskId: selectedTaskIdForTrace,
    enabled: canQuery && Boolean(selectedTaskIdForTrace),
    shouldStop: !selectedTaskIdForTrace || Boolean(traceQuery.data?.is_final),
    onStatus: () => {
      if (selectedTaskIdForTrace) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", selectedTaskIdForTrace] }).catch(() => undefined);
      }
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    },
    onStep: () => {
      if (selectedTaskIdForTrace) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", selectedTaskIdForTrace] }).catch(() => undefined);
      }
    },
    onDone: () => {
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    }
  });

  const runningTasks = useMemo(() => {
    const rows = conversationQuery.data?.task_history || [];
    return rows.filter((item) => !FINAL_STATES.has(item.status));
  }, [conversationQuery.data?.task_history]);

  const waitingApprovals = useMemo(() => {
    const approvals = traceQuery.data?.approvals || [];
    return approvals.filter((item) => item.status === "WAITING_HUMAN");
  }, [traceQuery.data?.approvals]);

  const activeConversationSummary: AssistantConversationSummary | null = useMemo(() => {
    if (!conversationsQuery.data || !selectedConversationId) return null;
    return conversationsQuery.data.find((item) => item.conversation_id === selectedConversationId) || null;
  }, [conversationsQuery.data, selectedConversationId]);

  const selectedTaskCard = useMemo(() => {
    if (!conversationQuery.data?.task_history?.length || !selectedTaskIdForTrace) return null;
    return conversationQuery.data.task_history.find((task) => task.task_id === selectedTaskIdForTrace) || null;
  }, [conversationQuery.data?.task_history, selectedTaskIdForTrace]);

  const threadItems = useMemo<ThreadItem[]>(() => {
    if (conversationQuery.data?.turn_history?.length) {
      const turns = [...conversationQuery.data.turn_history].sort((a, b) =>
        String(a.created_at || "").localeCompare(String(b.created_at || ""))
      );
      const items: ThreadItem[] = [];
      turns.forEach((turn) => {
        items.push({
          id: `${turn.turn_id}-user`,
          type: "user",
          message: turn.user_message,
          createdAt: turn.created_at,
          turnId: turn.turn_id
        });
        if (turn.assistant_message) {
          items.push({
            id: `${turn.turn_id}-assistant`,
            type: "assistant",
            message: turn.assistant_message,
            createdAt: turn.updated_at,
            turnId: turn.turn_id,
            route: turn.route,
            status: turn.status,
            taskId: turn.task_id
          });
        }
      });
      return items;
    }
    return (conversationQuery.data?.message_history || []).map((item, index) => ({
      id: `${String(item.created_at || index)}-${index}`,
      type: String(item.role || "user") === "assistant" ? "assistant" : "user",
      message: String(item.message || ""),
      createdAt: String(item.created_at || ""),
      turnId: typeof item.metadata === "object" && item.metadata ? String((item.metadata as Record<string, unknown>).turn_id || "") : "",
      route: String(item.route || ""),
      status: undefined,
      taskId: undefined
    })) as ThreadItem[];
  }, [conversationQuery.data]);

  const runtimeSteps: AgentRuntimeStep[] = selectedTurn?.agent_run.steps || [];
  const runtimeDecision = selectedTurn?.agent_run.decision;
  const runtimeReflection = selectedTurn?.agent_run.reflection;
  const runtimeObservations = selectedTurn?.agent_run.observations || [];
  const plannerView = traceQuery.data?.planner || selectedTurn?.agent_run.planner || {};
  const retrievalView = traceQuery.data?.retrieval_hits || selectedTurn?.agent_run.retrieval_hits || [];
  const finalOutputView = traceQuery.data?.final_output || selectedTurn?.agent_run.final_output || {};
  const goalView = traceQuery.data?.goal || selectedTurn?.agent_run.goal || null;
  const taskStateView = traceQuery.data?.task_state || selectedTurn?.agent_run.task_state || null;
  const currentActionView = traceQuery.data?.current_action || selectedTurn?.agent_run.current_action || null;
  const policyView = traceQuery.data?.policy || selectedTurn?.agent_run.policy || null;
  const episodeView = traceQuery.data?.episodes || selectedTurn?.agent_run.episodes || [];
  const runtimeDebugger = traceQuery.data?.runtime_debugger || null;
  const runtimeStepsView = traceQuery.data?.runtime_steps?.length ? traceQuery.data.runtime_steps : runtimeSteps;
  const debuggerStateBefore: Record<string, unknown> =
    runtimeDebugger && typeof runtimeDebugger.state_before === "object" ? runtimeDebugger.state_before || {} : {};
  const debuggerStateAfter: Record<string, unknown> =
    runtimeDebugger && typeof runtimeDebugger.state_after === "object" ? runtimeDebugger.state_after || {} : {};
  const debuggerLatestObservation: Record<string, unknown> =
    runtimeDebugger && typeof runtimeDebugger.latest_observation === "object"
      ? runtimeDebugger.latest_observation || {}
      : {};
  const wakeConditionView =
    debuggerStateAfter.wake_condition && typeof debuggerStateAfter.wake_condition === "object"
      ? (debuggerStateAfter.wake_condition as Record<string, unknown>)
      : null;
  const agendaView =
    debuggerStateAfter.agenda && typeof debuggerStateAfter.agenda === "object"
      ? (debuggerStateAfter.agenda as Record<string, unknown>)
      : null;
  const currentSubgoals = Array.isArray(taskStateView?.current_subgoals) ? taskStateView.current_subgoals : [];
  const unknownsView = Array.isArray(goalView?.unknowns) ? goalView.unknowns : Array.isArray(taskStateView?.unknowns) ? taskStateView.unknowns : [];
  const constraintsView = Array.isArray(goalView?.constraints) ? goalView.constraints : [];
  const beliefsView = Array.isArray(taskStateView?.beliefs) ? taskStateView.beliefs : [];
  const availableActionsView = Array.isArray(taskStateView?.available_actions) ? taskStateView.available_actions : [];
  const pendingApprovalsView = Array.isArray(taskStateView?.pending_approvals) ? taskStateView.pending_approvals : [];
  const whyNotEntries = Object.entries(runtimeDebugger?.why_not || {});
  const candidateActions = Array.isArray(runtimeDebugger?.candidate_actions) ? runtimeDebugger?.candidate_actions || [] : [];
  const actionContract = runtimeDebugger?.action_contract || null;
  const goalMeta = goalView ? ((goalView as unknown) as Record<string, unknown>) : {};
  const conversationHeroTitle = normalizeText(
    activeConversationSummary?.last_user_message ||
      goalView?.normalized_goal ||
      selectedTurn?.user_message ||
      selectedTurn?.assistant_message ||
      "开始新的对话",
    88
  );
  const conversationHeroSummary = normalizeText(
    activeConversationSummary?.last_assistant_message ||
      selectedTurn?.assistant_message ||
      selectedTaskCard?.progress_message ||
      "输入目标，让运行时开始规划、执行与恢复。",
    180
  );
  const selectedRouteLabel = selectedTurn?.route ? routeLabel(selectedTurn.route) : lastResponse?.route ? routeLabel(lastResponse.route) : "智能体对话";

  function submitMessage(confirmed = false): void {
    const message = confirmed ? pendingConfirmMessage || "" : inputMessage;
    if (!message.trim()) return;
    if (!userId) {
      pushToast({ kind: "warning", title: "缺少用户上下文", description: "请重新登录后再试。" });
      return;
    }
    sendMutation.mutate({ message: message.trim(), confirmed });
  }

  return (
    <AuthGate>
      <div className="assistant-workspace-page assistant-chat-page" data-testid="assistant-page">
        {conversationsQuery.error ? <ErrorBanner error={conversationsQuery.error} onRetry={() => conversationsQuery.refetch()} /> : null}
        {conversationQuery.error ? <ErrorBanner error={conversationQuery.error} onRetry={() => conversationQuery.refetch()} /> : null}
        {traceQuery.error ? <ErrorBanner error={traceQuery.error} onRetry={() => traceQuery.refetch()} /> : null}

        <div className="assistant-workspace assistant-chat-layout">
          <aside className="assistant-sidebar-column">
            <SectionCard
              className="assistant-shell-card assistant-sidebar-card"
              title="xh-helper"
              subtitle="面向长期目标、审批与运行时控制的中文工作区。"
              actions={
                <div className="inline-actions">
                  <button
                    type="button"
                    className="btn btn-ghost"
                    data-testid="assistant-new-conversation"
                    onClick={() => {
                      setCreatingNewConversation(true);
                      setSelectedConversationId("");
                      setSelectedTurnId("");
                      setSelectedTaskId("");
                      setLastResponse(null);
                      setPendingConfirmMessage(null);
                    }}
                  >
                    新建
                  </button>
                  <button
                    type="button"
                    className="btn btn-ghost"
                    data-testid="assistant-refresh"
                    onClick={() => {
                      conversationsQuery.refetch().catch(() => undefined);
                      if (selectedConversationId) conversationQuery.refetch().catch(() => undefined);
                      if (selectedTaskIdForTrace) traceQuery.refetch().catch(() => undefined);
                    }}
                  >
                    刷新
                  </button>
                </div>
              }
            >
              <div className="assistant-sidebar-stats">
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">当前对话</p>
                  <p className="metric-value" data-testid="assistant-current-conversation">
                    {selectedConversationId ? truncate(selectedConversationId, 16) : "新对话"}
                  </p>
                </div>
                <div className="grid cols-2">
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">运行中</p>
                    <p className="metric-value" data-testid="assistant-running-count">{runningTasks.length}</p>
                  </div>
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">待审批</p>
                    <p className="metric-value" data-testid="assistant-approval-count">{waitingApprovals.length}</p>
                  </div>
                </div>
                <p className="muted-text">
                  {activeConversationSummary?.updated_at ? `更新于 ${formatRelativeTime(activeConversationSummary.updated_at)}` : "暂无活动"}
                </p>
              </div>
            </SectionCard>

            <SectionCard
              className="assistant-shell-card assistant-sidebar-card"
              title="对话线程"
              subtitle="切换上下文，并跟随目标的持续执行过程。"
            >
              {!conversationsQuery.data || conversationsQuery.data.length === 0 ? (
                <EmptyState title="还没有对话" description="发送第一条消息后，这里会生成对话线程。" />
              ) : (
                <div className="assistant-conversation-list" data-testid="assistant-conversation-list">
                  {conversationsQuery.data.map((item) => {
                    const active = item.conversation_id === selectedConversationId;
                    return (
                      <button
                        key={item.conversation_id}
                        type="button"
                        data-testid={`assistant-conversation-item-${testIdSafe(item.conversation_id)}`}
                        className={active ? "assistant-conversation-item active" : "assistant-conversation-item"}
                        onClick={() => {
                          setCreatingNewConversation(false);
                          setSelectedConversationId(item.conversation_id);
                          setSelectedTurnId("");
                          setSelectedTaskId("");
                        }}
                      >
                        <div className="assistant-conversation-head">
                          <span className="mono">{truncate(item.conversation_id, 14)}</span>
                          <span className="muted-text">{formatRelativeTime(item.updated_at)}</span>
                        </div>
                        <p>{normalizeText(item.last_user_message || item.last_assistant_message || "新对话")}</p>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">任务 {item.task_count}</span>
                          <span className="tag tag-neutral">运行中 {item.running_task_count}</span>
                          <span className="tag tag-warning">等待 {item.waiting_approval_count}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}
            </SectionCard>
            <SectionCard
              className="assistant-shell-card assistant-sidebar-card"
              title="持久工作"
              subtitle="这条对话里的工作流、快速路径与运行时交接都在这里。"
            >
              {!conversationQuery.data || conversationQuery.data.task_history.length === 0 ? (
                <EmptyState title="还没有任务" description="当 xh-helper 调用工具或进入工作流时，这里会出现任务卡片。" />
              ) : (
                <div className="assistant-task-table" data-testid="assistant-task-history">
                  {conversationQuery.data.task_history.map((task) => (
                    <button
                      key={task.task_id}
                      type="button"
                      className={task.task_id === selectedTaskIdForTrace ? "assistant-task-card active" : "assistant-task-card"}
                      data-testid={`assistant-task-row-${testIdSafe(task.task_id)}`}
                      onClick={() => {
                        const turn = conversationQuery.data?.turn_history.find((item) => item.task_id === task.task_id);
                        if (turn) setSelectedTurnId(turn.turn_id);
                        setSelectedTaskId(task.task_id);
                      }}
                    >
                      <div className="assistant-task-card-head">
                        <span className="mono">{truncate(task.task_id, 14)}</span>
                        <StatusBadge status={task.status} />
                      </div>
                      <p>{normalizeText(task.progress_message, 120)}</p>
                      <div className="assistant-meta-row">
                        <span className="tag tag-neutral">{task.task_kind}</span>
                        <span className="muted-text">{formatRelativeTime(task.updated_at)}</span>
                      </div>
                      <span className="assistant-task-open-link" data-testid={`assistant-task-open-${testIdSafe(task.task_id)}`}>
                        打开追踪
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </SectionCard>
          </aside>

          <main className="assistant-thread-column">
            <SectionCard
              className="assistant-shell-card assistant-composer-card"
              title="给 xh-helper 发消息"
              subtitle="描述你想要的结果，让运行时为你选择合适路径。"
            >
              <div className="stack-gap-sm">
                <div className="assistant-composer-toolbar">
                  <select
                    value={mode || "auto"}
                    data-testid="assistant-mode-select"
                    onChange={(event) => setMode(event.target.value as AssistantChatInput["mode"])}
                    aria-label="assistant mode"
                  >
                    <option value="auto">自动</option>
                    <option value="direct_answer">直接回答</option>
                    <option value="tool_task">工具任务</option>
                    <option value="workflow_task">工作流任务</option>
                  </select>
                  {selectedTaskIdForTrace ? (
                    <div className="assistant-composer-context">
                      <span className="tag tag-neutral">跟踪中 {truncate(selectedTaskIdForTrace, 14)}</span>
                    </div>
                  ) : null}
                </div>
                <textarea
                  placeholder="告诉 xh-helper 你想达成什么，它会规划、执行、等待、恢复并协同完成。"
                  data-testid="assistant-input-message"
                  value={inputMessage}
                  onChange={(event) => setInputMessage(event.target.value)}
                />
                {pendingConfirmMessage ? (
                  <div className="assistant-confirm-banner" data-testid="assistant-pending-confirmation">
                    <div className="stack-gap-xs">
                      <p className="panel-subtitle">需要确认</p>
                      <p>{normalizeText(pendingConfirmMessage, 280)}</p>
                    </div>
                    <button
                      type="button"
                      className="btn btn-danger"
                      data-testid="assistant-confirm-button"
                      onClick={() => submitMessage(true)}
                      disabled={sendMutation.isPending}
                    >
                      确认并继续
                    </button>
                  </div>
                ) : null}
                <div className="assistant-composer-actions">
                  <button
                    type="button"
                    className="btn btn-primary"
                    data-testid="assistant-send-button"
                    onClick={() => submitMessage(false)}
                    disabled={sendMutation.isPending}
                  >
                    {sendMutation.isPending ? "发送中..." : "发送消息"}
                  </button>
                </div>
              </div>
            </SectionCard>

            <SectionCard
              className="assistant-shell-card assistant-thread-card"
              title={conversationHeroTitle}
              subtitle={conversationHeroSummary}
            >
              <div className="assistant-chat-hero">
                <div className="assistant-chat-hero-copy">
                  <span className="tag tag-neutral">{selectedRouteLabel}</span>
                  {selectedTaskCard?.status ? <StatusBadge status={selectedTaskCard.status} /> : null}
                  {activeConversationSummary?.updated_at ? (
                    <span className="muted-text">更新于 {formatRelativeTime(activeConversationSummary.updated_at)}</span>
                  ) : null}
                </div>
                <div className="assistant-chat-hero-copy">
                  <span className="muted-text">持久工作</span>
                  <span className="tag tag-neutral">{conversationQuery.data?.task_history.length || 0} 个任务</span>
                  <span className="tag tag-warning">{waitingApprovals.length} 个审批</span>
                </div>
              </div>
              {!conversationQuery.data ? (
                <EmptyState title="请选择一个对话" description="开始新对话，或者从左侧选择一个已有线程。" />
              ) : (
                <div className="assistant-thread-list" data-testid="assistant-message-history">
                  {threadItems.length === 0 ? (
                    <EmptyState title="还没有消息" description="发送一条消息后，这里会开始展示智能体循环。" />
                  ) : (
                    threadItems.map((item) => (
                      <button
                        key={item.id}
                        type="button"
                        className={
                          item.type === "assistant"
                            ? `assistant-message-item assistant-message-assistant${item.turnId === selectedTurnId ? " active" : ""}`
                            : `assistant-message-item assistant-message-user${item.turnId === selectedTurnId ? " active" : ""}`
                        }
                        onClick={() => {
                          if (item.turnId) setSelectedTurnId(item.turnId);
                          if (item.taskId) setSelectedTaskId(item.taskId);
                        }}
                      >
                        <div className="assistant-message-head">
                          <span className="tag tag-neutral">{item.type === "assistant" ? "xh-helper" : "你"}</span>
                          {"route" in item && item.route ? <span className="tag tag-neutral">{routeLabel(item.route)}</span> : null}
                          {"status" in item && item.status ? <StatusBadge status={item.status} /> : null}
                          <span className="muted-text">{item.createdAt ? formatDateTime(item.createdAt) : "-"}</span>
                        </div>
                        <p>{item.message}</p>
                      </button>
                    ))
                  )}
                </div>
              )}
            </SectionCard>

            <SectionCard
              className="assistant-shell-card assistant-result-card"
              title="最新结果"
              subtitle="不离开聊天界面，也能快速查看最近一次输出。"
            >
              {!lastResponse ? (
                <EmptyState title="还没有最新结果" description="每次发送后，最新回复会固定显示在这里。" />
              ) : (
                <div className="stack-gap-sm" data-testid="assistant-last-response">
                  <div className="assistant-meta-row">
                    <span className="tag tag-neutral" data-testid="assistant-last-route">{lastResponse.route}</span>
                    <span className="tag tag-neutral" data-testid="assistant-last-response-type">{lastResponse.response_type}</span>
                    <span className="mono">{truncate(lastResponse.trace_id, 18)}</span>
                  </div>
                  <p data-testid="assistant-last-message">{lastResponse.message}</p>
                  {lastResponse.task ? (
                    <div className="sub-panel stack-gap-xs" data-testid="assistant-last-task-card">
                      <p className="panel-subtitle">Task</p>
                      <div className="assistant-meta-row">
                        <span className="mono">{lastResponse.task.task_id}</span>
                        <StatusBadge status={lastResponse.task.status} />
                      </div>
                    </div>
                  ) : null}
                </div>
              )}
            </SectionCard>
          </main>

          <aside className="assistant-inspector-column">
            <SectionCard
              className="assistant-shell-card assistant-inspector-card"
              title="智能体上下文"
              subtitle="查看目标状态、控制策略、调试器和持久运行细节。"
            >
              {!selectedTurn ? (
                <EmptyState title="尚未选择轮次" description="请选择一条消息或任务，查看当前智能体运行时。" />
              ) : (
                <div className="stack-gap-md" data-testid="assistant-trace-panel">
                  <div className="grid cols-2">
                    <div className="sub-panel stack-gap-xs">
                      <p className="panel-subtitle">轮次</p>
                      <div data-testid="assistant-trace-status">
                        <StatusBadge status={traceQuery.data?.task.status || selectedTurn.status} />
                      </div>
                      <p>{routeLabel(selectedTurn.route)}</p>
                      <p className="muted-text">{phaseLabel(selectedTurn.current_phase)}</p>
                    </div>
                    <div className="sub-panel stack-gap-xs">
                      <p className="panel-subtitle">任务</p>
                      <p className="mono">{selectedTurn.task_id || "-"}</p>
                      <p className="muted-text">{selectedTaskCard?.progress_message || runtimeReflection?.next_action || "-"}</p>
                    </div>
                  </div>

                  {traceQuery.data?.approvals?.length ? (
                    <div className="sub-panel stack-gap-xs" data-testid="assistant-approvals-panel">
                      <p className="panel-subtitle">审批</p>
                      {traceQuery.data.approvals.map((item) => (
                        <div
                          key={item.approval_id}
                          className="assistant-approval-item"
                          data-testid={`assistant-approval-item-${testIdSafe(item.approval_id)}`}
                        >
                          <div className="assistant-approval-head">
                            <span className="mono">{truncate(item.approval_id, 16)}</span>
                            <span className="tag tag-warning">{item.status_label}</span>
                          </div>
                          <p className="muted-text">{item.action_hint || "-"}</p>
                          {item.status === "WAITING_HUMAN" ? (
                            <div className="inline-actions">
                              {isOperator ? (
                                <>
                                  <button
                                    type="button"
                                    className="btn btn-primary"
                                    data-testid={`assistant-approval-approve-${testIdSafe(item.approval_id)}`}
                                    onClick={() => approvalMutation.mutate({ approvalId: item.approval_id, action: "approve" })}
                                    disabled={approvalMutation.isPending}
                                  >
                                    通过
                                  </button>
                                  <button
                                    type="button"
                                    className="btn btn-danger"
                                    data-testid={`assistant-approval-reject-${testIdSafe(item.approval_id)}`}
                                    onClick={() => approvalMutation.mutate({ approvalId: item.approval_id, action: "reject" })}
                                    disabled={approvalMutation.isPending}
                                  >
                                    拒绝
                                  </button>
                                </>
                              ) : (
                                <p className="muted-text">请联系操作员或管理员处理这条审批。</p>
                              )}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-plan-section">
                    <p className="panel-subtitle">计划</p>
                    {runtimeDecision ? (
                      <div className="assistant-plan-summary">
                        <span className="tag tag-neutral">{runtimeDecision.action}</span>
                        {runtimeDecision.selected_tool ? <span className="tag tag-neutral">{runtimeDecision.selected_tool}</span> : null}
                        {runtimeDecision.need_confirmation ? <span className="tag tag-warning">需要确认</span> : null}
                      </div>
                    ) : null}
                    <JsonViewer value={plannerView} defaultExpanded title="规划器" />
                    {retrievalView.length > 0 ? (
                      <ul className="plain-list">
                        {retrievalView.map((item, idx) => (
                          <li key={`${String(item.source || item.title || idx)}-${idx}`}>
                            <strong>{String(item.title || item.source || "doc")}</strong>: {normalizeText(item.snippet, 180)}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted-text">这一轮没有检索上下文。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-goal-section">
                    <p className="panel-subtitle">目标控制台</p>
                    {goalView ? (
                      <>
                        <p>{normalizeText(goalView.normalized_goal, 220)}</p>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">风险 {String(goalView.risk_level || "low")}</span>
                          {goalView.user_intent ? <span className="tag tag-neutral">{String(goalView.user_intent)}</span> : null}
                          {"lifecycle_state" in goalMeta && goalMeta.lifecycle_state ? (
                            <span className="tag tag-neutral">{String(goalMeta.lifecycle_state)}</span>
                          ) : null}
                        </div>
                        {constraintsView.length ? (
                          <p className="muted-text">约束：{constraintsView.map((item) => String(item)).join(", ")}</p>
                        ) : null}
                        {unknownsView.length ? (
                          <p className="muted-text">未知项：{unknownsView.map((item) => String(item)).join(", ")}</p>
                        ) : (
                          <p className="muted-text">当前没有未解决的未知项。</p>
                        )}
                        {currentSubgoals.length ? (
                          <>
                            <p className="panel-subtitle">活动子目标</p>
                            <ul className="plain-list">
                              {currentSubgoals.map((item, idx) => (
                                <li key={`${String(item)}-${idx}`}>{String(item)}</li>
                              ))}
                            </ul>
                          </>
                        ) : null}
                        {Array.isArray(goalView.success_criteria) && goalView.success_criteria.length ? (
                          <ul className="plain-list">
                            {goalView.success_criteria.map((item, idx) => (
                              <li key={`${String(item)}-${idx}`}>{String(item)}</li>
                            ))}
                          </ul>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted-text">当前还没有归一化目标。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-state-section">
                    <p className="panel-subtitle">运行时状态</p>
                    {taskStateView ? (
                      <>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">{phaseLabel(String(taskStateView.current_phase || "unknown"))}</span>
                          {taskStateView.fallback_state ? <span className="tag tag-warning">{String(taskStateView.fallback_state)}</span> : null}
                        </div>
                        {Array.isArray(taskStateView.blockers) && taskStateView.blockers.length ? (
                          <p className="muted-text">阻塞项：{taskStateView.blockers.map((item) => String(item)).join(", ")}</p>
                        ) : (
                          <p className="muted-text">当前没有阻塞项。</p>
                        )}
                        {Array.isArray(taskStateView.known_facts) && taskStateView.known_facts.length ? (
                          <p className="muted-text">已知事实：{taskStateView.known_facts.map((item) => String(item)).join(", ")}</p>
                        ) : null}
                        {beliefsView.length ? <p className="muted-text">当前判断：{beliefsView.map((item) => String(item)).join(", ")}</p> : null}
                        {availableActionsView.length ? (
                          <div className="assistant-meta-row">
                            {availableActionsView.map((item) => (
                              <span key={String(item)} className="tag tag-neutral">
                                {String(item)}
                              </span>
                            ))}
                          </div>
                        ) : null}
                        {pendingApprovalsView.length ? (
                          <p className="muted-text">待审批项：{pendingApprovalsView.map((item) => String(item)).join(", ")}</p>
                        ) : null}
                        {wakeConditionView ? (
                          <details className="assistant-debug-details" open>
                            <summary>唤醒条件</summary>
                            <div className="assistant-meta-row">
                              {"kind" in wakeConditionView ? <span className="tag tag-warning">{String(wakeConditionView.kind)}</span> : null}
                              {"status" in wakeConditionView ? <span className="tag tag-neutral">{String(wakeConditionView.status)}</span> : null}
                              {"resume_action" in wakeConditionView ? <span className="tag tag-neutral">恢复 {String(wakeConditionView.resume_action)}</span> : null}
                            </div>
                            <JsonViewer value={wakeConditionView} title="唤醒条件" />
                          </details>
                        ) : null}
                        {agendaView ? (
                          <details className="assistant-debug-details" open>
                            <summary>调度议程</summary>
                            <div className="assistant-meta-row">
                              {"priority_score" in agendaView ? (
                                <span className="tag tag-neutral">优先级 {String(agendaView.priority_score)}</span>
                              ) : null}
                              {"dispatch_decision" in agendaView ? (
                                <span className="tag tag-neutral">{String(agendaView.dispatch_decision)}</span>
                              ) : null}
                              {"dispatch_band" in agendaView ? (
                                <span className="tag tag-neutral">{String(agendaView.dispatch_band)}</span>
                              ) : null}
                            </div>
                            <JsonViewer value={agendaView} title="调度议程" />
                          </details>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted-text">这一轮还没有可用的运行时状态。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-policy-section">
                    <p className="panel-subtitle">下一动作与策略</p>
                    {currentActionView ? (
                      <>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">{String(currentActionView.action_type || "unknown")}</span>
                          {currentActionView.target ? <span className="tag tag-neutral">{String(currentActionView.target)}</span> : null}
                          {currentActionView.requires_approval ? <span className="tag tag-warning">需要审批</span> : null}
                        </div>
                        <p className="muted-text">{normalizeText(currentActionView.rationale, 220)}</p>
                        {actionContract ? (
                          <>
                            {actionContract.expected_result ? <p className="muted-text">预期结果：{String(actionContract.expected_result)}</p> : null}
                            {Array.isArray(actionContract.success_conditions) && actionContract.success_conditions.length ? (
                              <p className="muted-text">
                                成功条件：{actionContract.success_conditions.map((item) => String(item)).join(", ")}
                              </p>
                            ) : null}
                            {actionContract.fallback ? <p className="muted-text">回退策略：{String(actionContract.fallback)}</p> : null}
                            {Array.isArray(actionContract.stop_conditions) && actionContract.stop_conditions.length ? (
                              <p className="muted-text">
                                停止条件：{actionContract.stop_conditions.map((item) => String(item)).join(", ")}
                              </p>
                            ) : null}
                          </>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted-text">当前还没有选定动作。</p>
                    )}
                    {policyView ? (
                      <>
                        <p className="muted-text">
                          已选择策略 {String(policyView.selected_action || "-")}
                          {policyView.fallback_action ? `，回退到 ${String(policyView.fallback_action)}` : ""}
                        </p>
                        {Array.isArray(policyView.reasoning) && policyView.reasoning.length ? (
                          <ul className="plain-list">
                            {policyView.reasoning.map((item, idx) => (
                              <li key={`${String(item)}-${idx}`}>{String(item)}</li>
                            ))}
                          </ul>
                        ) : null}
                      </>
                    ) : null}
                    {whyNotEntries.length ? (
                      <details className="assistant-debug-details" open>
                        <summary>为什么不是其他动作</summary>
                        <ul className="plain-list">
                          {whyNotEntries.map(([key, value]) => (
                            <li key={key}>
                              <strong>{humanizeKey(key)}</strong>: {String(value)}
                            </li>
                          ))}
                        </ul>
                      </details>
                    ) : null}
                    {candidateActions.length ? (
                      <details className="assistant-debug-details">
                        <summary>候选动作</summary>
                        <JsonViewer value={candidateActions} title="候选动作" />
                      </details>
                    ) : null}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-tools-section">
                    <p className="panel-subtitle">工具</p>
                    {traceQuery.data?.tool_calls?.length ? (
                      traceQuery.data.tool_calls.map((item) => (
                        <div key={item.tool_call_id} className="assistant-tool-item">
                          <div className="assistant-tool-head">
                            <span className="tag tag-neutral">{item.tool_name}</span>
                            <span className="tag tag-neutral">{item.status_label}</span>
                            <span className="muted-text">{item.duration_ms}ms</span>
                          </div>
                          <p className="muted-text">{item.why_this_tool || "由规划器和运行时上下文共同选择。"}</p>
                          {item.response_summary ? <p>{normalizeText(item.response_summary, 220)}</p> : null}
                        </div>
                      ))
                    ) : selectedTurn.agent_run.decision?.selected_tool ? (
                      <div className="assistant-tool-item">
                        <div className="assistant-tool-head">
                          <span className="tag tag-neutral">{selectedTurn.agent_run.decision.selected_tool}</span>
                          <span className="muted-text">已规划</span>
                        </div>
                        <p className="muted-text">{selectedTurn.agent_run.decision.summary || "在规划阶段被选中。"}</p>
                      </div>
                    ) : (
                      <p className="muted-text">这一轮没有工具活动。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-steps-section">
                    <p className="panel-subtitle">循环步骤</p>
                    {(traceQuery.data?.trace_steps?.length || runtimeStepsView.length) ? (
                      <ul className="assistant-trace-list">
                        {runtimeStepsView.map((step) => (
                          <li key={step.key} className="assistant-trace-item">
                            <div className="assistant-trace-head">
                              <span>{step.title}</span>
                              <span className="tag tag-neutral">{phaseLabel(step.phase)}</span>
                            </div>
                            <p className="muted-text">{step.summary}</p>
                            {step.decision?.action ? (
                              <p className="muted-text">决策：{String(step.decision.action)}</p>
                            ) : null}
                            {step.reflection?.summary ? (
                              <p className="muted-text">反思：{String(step.reflection.summary)}</p>
                            ) : null}
                          </li>
                        ))}
                        {(traceQuery.data?.trace_steps || []).map((step, idx) => (
                          <li key={`${step.step_key}-${idx}`} className="assistant-trace-item">
                            <div className="assistant-trace-head">
                              <span>{step.title}</span>
                              <span className="tag tag-neutral">{step.status_label}</span>
                            </div>
                            {step.detail ? <p className="muted-text">{normalizeText(step.detail, 220)}</p> : null}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted-text">还没有捕获到运行时步骤。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-debugger-section">
                    <p className="panel-subtitle">运行时调试器</p>
                    {runtimeDebugger ? (
                      <>
                        {Object.keys(debuggerLatestObservation).length ? (
                          <JsonViewer value={debuggerLatestObservation} title="最新观察" />
                        ) : (
                          <p className="muted-text">当前没有最新观察。</p>
                        )}
                        <details className="assistant-debug-details">
                          <summary>状态变化</summary>
                          <JsonViewer value={debuggerStateBefore} title="变化前" />
                          <JsonViewer value={debuggerStateAfter} title="变化后" />
                        </details>
                      </>
                    ) : (
                      <p className="muted-text">这一轮暂时没有可用的运行时调试信息。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-episodes-section">
                    <p className="panel-subtitle">经验复用</p>
                    {episodeView.length ? (
                      <ul className="assistant-trace-list">
                        {episodeView.map((episode, idx) => (
                          <li key={`${String(episode.episode_id || idx)}-${idx}`} className="assistant-trace-item">
                            <div className="assistant-trace-head">
                              <span>{normalizeText(episode.task_summary, 120)}</span>
                              <span className="tag tag-neutral">{String(episode.chosen_strategy || "strategy")}</span>
                            </div>
                            <p className="muted-text">{normalizeText(episode.final_outcome, 180)}</p>
                            {Array.isArray(episode.useful_lessons) && episode.useful_lessons.length ? (
                              <p className="muted-text">经验要点：{episode.useful_lessons.map((item) => String(item)).join("; ")}</p>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted-text">这一轮没有复用相似经验。</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-output-section">
                    <p className="panel-subtitle">结果</p>
                    <p className="muted-text">{traceQuery.data?.task_summary || runtimeReflection?.summary || selectedTaskCard?.progress_message || "-"}</p>
                    <JsonViewer value={finalOutputView} defaultExpanded title="final output" />
                    <details className="assistant-debug-details">
                      <summary>调试细节</summary>
                      <JsonViewer value={selectedTurn.agent_run.memory} title="记忆" />
                      <JsonViewer value={runtimeObservations} title="观察" />
                      <JsonViewer value={runtimeDebugger?.decision || {}} title="决策" />
                      <JsonViewer value={runtimeDebugger?.reflection || {}} title="反思" />
                    </details>
                  </div>
                </div>
              )}
            </SectionCard>
          </aside>
        </div>
      </div>
    </AuthGate>
  );
}
