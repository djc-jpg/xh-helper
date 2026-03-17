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
  if (route === "direct_answer") return "Direct Answer";
  if (route === "tool_task") return "Tool Task";
  return "Workflow Task";
}

function testIdSafe(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function phaseLabel(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (s) => s.toUpperCase());
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
      pushToast({ kind: "success", title: "Assistant responded", description: routeLabel(data.route) });
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", data.conversation_id] }).catch(() => undefined);
      if (data.task?.task_id) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", data.task.task_id] }).catch(() => undefined);
      }
    },
    onError(error) {
      pushToast({ kind: "error", title: "Send failed", description: error instanceof Error ? error.message : "unknown_error" });
    }
  });

  const approvalMutation = useMutation({
    mutationFn: async (payload: { approvalId: string; action: "approve" | "reject" }) => {
      return approvalAction(auth.accessToken, payload.approvalId, payload.action, {
        reason: payload.action === "approve" ? "approved in assistant workspace" : "rejected in assistant workspace"
      });
    },
    onSuccess() {
      pushToast({ kind: "success", title: "Approval updated" });
      if (selectedTaskIdForTrace) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", selectedTaskIdForTrace] }).catch(() => undefined);
      }
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    },
    onError(error) {
      pushToast({ kind: "error", title: "Approval failed", description: error instanceof Error ? error.message : "unknown_error" });
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

  function submitMessage(confirmed = false): void {
    const message = confirmed ? pendingConfirmMessage || "" : inputMessage;
    if (!message.trim()) return;
    if (!userId) {
      pushToast({ kind: "warning", title: "Missing user context", description: "Please sign in again." });
      return;
    }
    sendMutation.mutate({ message: message.trim(), confirmed });
  }

  return (
    <AuthGate>
      <div className="assistant-workspace-page" data-testid="assistant-page">
        {conversationsQuery.error ? <ErrorBanner error={conversationsQuery.error} onRetry={() => conversationsQuery.refetch()} /> : null}
        {conversationQuery.error ? <ErrorBanner error={conversationQuery.error} onRetry={() => conversationQuery.refetch()} /> : null}
        {traceQuery.error ? <ErrorBanner error={traceQuery.error} onRetry={() => traceQuery.refetch()} /> : null}

        <div className="assistant-workspace">
          <aside className="assistant-sidebar-column">
            <SectionCard
              title="Assistant Workspace"
              subtitle="Conversations, active work, and fast controls."
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
                    New
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
                    Refresh
                  </button>
                </div>
              }
            >
              <div className="assistant-sidebar-stats">
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">Conversation</p>
                  <p className="metric-value" data-testid="assistant-current-conversation">
                    {selectedConversationId ? truncate(selectedConversationId, 16) : "New thread"}
                  </p>
                </div>
                <div className="grid cols-2">
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">Running</p>
                    <p className="metric-value" data-testid="assistant-running-count">{runningTasks.length}</p>
                  </div>
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">Approvals</p>
                    <p className="metric-value" data-testid="assistant-approval-count">{waitingApprovals.length}</p>
                  </div>
                </div>
                <p className="muted-text">
                  {activeConversationSummary?.updated_at ? `Updated ${formatRelativeTime(activeConversationSummary.updated_at)}` : "No activity yet"}
                </p>
              </div>
            </SectionCard>

            <SectionCard title="Conversations" subtitle="Switch threads and follow live work.">
              {!conversationsQuery.data || conversationsQuery.data.length === 0 ? (
                <EmptyState title="No conversations yet" description="Send the first message to create one." />
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
                        <p>{normalizeText(item.last_user_message || item.last_assistant_message || "New conversation")}</p>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">Tasks {item.task_count}</span>
                          <span className="tag tag-neutral">Active {item.running_task_count}</span>
                          <span className="tag tag-warning">Waiting {item.waiting_approval_count}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}
            </SectionCard>
            <SectionCard title="Task History" subtitle="Every workflow and fast-path task for this conversation.">
              {!conversationQuery.data || conversationQuery.data.task_history.length === 0 ? (
                <EmptyState title="No tasks yet" description="Tasks appear here when the assistant executes tools or workflows." />
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
                        Open trace
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </SectionCard>
          </aside>

          <main className="assistant-thread-column">
            <SectionCard title="Composer" subtitle="Natural language in, governed execution out.">
              <div className="stack-gap-sm">
                <div className="grid cols-2">
                  <select
                    value={mode || "auto"}
                    data-testid="assistant-mode-select"
                    onChange={(event) => setMode(event.target.value as AssistantChatInput["mode"])}
                    aria-label="assistant mode"
                  >
                    <option value="auto">Auto</option>
                    <option value="direct_answer">Direct answer</option>
                    <option value="tool_task">Tool task</option>
                    <option value="workflow_task">Workflow task</option>
                  </select>
                  <button
                    type="button"
                    className="btn btn-primary"
                    data-testid="assistant-send-button"
                    onClick={() => submitMessage(false)}
                    disabled={sendMutation.isPending}
                  >
                    {sendMutation.isPending ? "Sending..." : "Send"}
                  </button>
                </div>
                <textarea
                  placeholder="Describe what you want the assistant to do..."
                  data-testid="assistant-input-message"
                  value={inputMessage}
                  onChange={(event) => setInputMessage(event.target.value)}
                />
                {pendingConfirmMessage ? (
                  <div className="assistant-confirm-banner" data-testid="assistant-pending-confirmation">
                    <div className="stack-gap-xs">
                      <p className="panel-subtitle">Confirmation needed</p>
                      <p>{normalizeText(pendingConfirmMessage, 280)}</p>
                    </div>
                    <button
                      type="button"
                      className="btn btn-danger"
                      data-testid="assistant-confirm-button"
                      onClick={() => submitMessage(true)}
                      disabled={sendMutation.isPending}
                    >
                      Confirm and continue
                    </button>
                  </div>
                ) : null}
              </div>
            </SectionCard>

            <SectionCard title="Conversation Thread" subtitle="Messages plus structured runtime cards in one place.">
              {!conversationQuery.data ? (
                <EmptyState title="Choose a conversation" description="Start a new thread or pick one from the left." />
              ) : (
                <div className="assistant-thread-list" data-testid="assistant-message-history">
                  {threadItems.length === 0 ? (
                    <EmptyState title="No messages yet" description="Send a message to start the agent loop." />
                  ) : (
                    threadItems.map((item) => (
                      <button
                        key={item.id}
                        type="button"
                        className={item.type === "assistant" ? "assistant-message-item assistant-message-assistant" : "assistant-message-item assistant-message-user"}
                        onClick={() => {
                          if (item.turnId) setSelectedTurnId(item.turnId);
                          if (item.taskId) setSelectedTaskId(item.taskId);
                        }}
                      >
                        <div className="assistant-message-head">
                          <span className="tag tag-neutral">{item.type === "assistant" ? "Assistant" : "User"}</span>
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

            <SectionCard title="Latest Response" subtitle="Immediate output for quick checking and demos.">
              {!lastResponse ? (
                <EmptyState title="No fresh response yet" description="The latest assistant reply will be pinned here after each send." />
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
            <SectionCard title="Runtime Inspector" subtitle="Selected turn, plan, tools, memory, and final output.">
              {!selectedTurn ? (
                <EmptyState title="No turn selected" description="Choose a message or task to inspect the agent runtime." />
              ) : (
                <div className="stack-gap-md" data-testid="assistant-trace-panel">
                  <div className="grid cols-2">
                    <div className="sub-panel stack-gap-xs">
                      <p className="panel-subtitle">Turn</p>
                      <div data-testid="assistant-trace-status">
                        <StatusBadge status={traceQuery.data?.task.status || selectedTurn.status} />
                      </div>
                      <p>{routeLabel(selectedTurn.route)}</p>
                      <p className="muted-text">{phaseLabel(selectedTurn.current_phase)}</p>
                    </div>
                    <div className="sub-panel stack-gap-xs">
                      <p className="panel-subtitle">Task</p>
                      <p className="mono">{selectedTurn.task_id || "-"}</p>
                      <p className="muted-text">{selectedTaskCard?.progress_message || runtimeReflection?.next_action || "-"}</p>
                    </div>
                  </div>

                  {traceQuery.data?.approvals?.length ? (
                    <div className="sub-panel stack-gap-xs" data-testid="assistant-approvals-panel">
                      <p className="panel-subtitle">Approvals</p>
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
                                    Approve
                                  </button>
                                  <button
                                    type="button"
                                    className="btn btn-danger"
                                    data-testid={`assistant-approval-reject-${testIdSafe(item.approval_id)}`}
                                    onClick={() => approvalMutation.mutate({ approvalId: item.approval_id, action: "reject" })}
                                    disabled={approvalMutation.isPending}
                                  >
                                    Reject
                                  </button>
                                </>
                              ) : (
                                <p className="muted-text">Ask an operator or owner to resolve this approval.</p>
                              )}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-plan-section">
                    <p className="panel-subtitle">Plan</p>
                    {runtimeDecision ? (
                      <div className="assistant-plan-summary">
                        <span className="tag tag-neutral">{runtimeDecision.action}</span>
                        {runtimeDecision.selected_tool ? <span className="tag tag-neutral">{runtimeDecision.selected_tool}</span> : null}
                        {runtimeDecision.need_confirmation ? <span className="tag tag-warning">Needs confirmation</span> : null}
                      </div>
                    ) : null}
                    <JsonViewer value={plannerView} defaultExpanded title="planner" />
                    {retrievalView.length > 0 ? (
                      <ul className="plain-list">
                        {retrievalView.map((item, idx) => (
                          <li key={`${String(item.source || item.title || idx)}-${idx}`}>
                            <strong>{String(item.title || item.source || "doc")}</strong>: {normalizeText(item.snippet, 180)}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted-text">No retrieval context for this turn.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-goal-section">
                    <p className="panel-subtitle">Goal</p>
                    {goalView ? (
                      <>
                        <p>{normalizeText(goalView.normalized_goal, 220)}</p>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">Risk {String(goalView.risk_level || "low")}</span>
                          {goalView.user_intent ? <span className="tag tag-neutral">{String(goalView.user_intent)}</span> : null}
                        </div>
                        {Array.isArray(goalView.success_criteria) && goalView.success_criteria.length ? (
                          <ul className="plain-list">
                            {goalView.success_criteria.map((item, idx) => (
                              <li key={`${String(item)}-${idx}`}>{String(item)}</li>
                            ))}
                          </ul>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted-text">No normalized goal captured yet.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-state-section">
                    <p className="panel-subtitle">Runtime State</p>
                    {taskStateView ? (
                      <>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">{phaseLabel(String(taskStateView.current_phase || "unknown"))}</span>
                          {taskStateView.fallback_state ? <span className="tag tag-warning">{String(taskStateView.fallback_state)}</span> : null}
                        </div>
                        {Array.isArray(taskStateView.blockers) && taskStateView.blockers.length ? (
                          <p className="muted-text">Blockers: {taskStateView.blockers.map((item) => String(item)).join(", ")}</p>
                        ) : (
                          <p className="muted-text">No active blockers.</p>
                        )}
                        {Array.isArray(taskStateView.known_facts) && taskStateView.known_facts.length ? (
                          <p className="muted-text">Known facts: {taskStateView.known_facts.map((item) => String(item)).join(", ")}</p>
                        ) : null}
                      </>
                    ) : (
                      <p className="muted-text">Runtime state is not available for this turn.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-policy-section">
                    <p className="panel-subtitle">Next Action & Policy</p>
                    {currentActionView ? (
                      <>
                        <div className="assistant-meta-row">
                          <span className="tag tag-neutral">{String(currentActionView.action_type || "unknown")}</span>
                          {currentActionView.target ? <span className="tag tag-neutral">{String(currentActionView.target)}</span> : null}
                          {currentActionView.requires_approval ? <span className="tag tag-warning">Approval gated</span> : null}
                        </div>
                        <p className="muted-text">{normalizeText(currentActionView.rationale, 220)}</p>
                      </>
                    ) : (
                      <p className="muted-text">No current action selected.</p>
                    )}
                    {policyView ? (
                      <>
                        <p className="muted-text">
                          Policy selected {String(policyView.selected_action || "-")}
                          {policyView.fallback_action ? `, fallback ${String(policyView.fallback_action)}` : ""}
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
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-tools-section">
                    <p className="panel-subtitle">Tools</p>
                    {traceQuery.data?.tool_calls?.length ? (
                      traceQuery.data.tool_calls.map((item) => (
                        <div key={item.tool_call_id} className="assistant-tool-item">
                          <div className="assistant-tool-head">
                            <span className="tag tag-neutral">{item.tool_name}</span>
                            <span className="tag tag-neutral">{item.status_label}</span>
                            <span className="muted-text">{item.duration_ms}ms</span>
                          </div>
                          <p className="muted-text">{item.why_this_tool || "Chosen by planner and runtime context."}</p>
                          {item.response_summary ? <p>{normalizeText(item.response_summary, 220)}</p> : null}
                        </div>
                      ))
                    ) : selectedTurn.agent_run.decision?.selected_tool ? (
                      <div className="assistant-tool-item">
                        <div className="assistant-tool-head">
                          <span className="tag tag-neutral">{selectedTurn.agent_run.decision.selected_tool}</span>
                          <span className="muted-text">planned</span>
                        </div>
                        <p className="muted-text">{selectedTurn.agent_run.decision.summary || "Selected during planning."}</p>
                      </div>
                    ) : (
                      <p className="muted-text">No tool activity for this turn.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-steps-section">
                    <p className="panel-subtitle">Loop Steps</p>
                    {(traceQuery.data?.trace_steps?.length || runtimeSteps.length) ? (
                      <ul className="assistant-trace-list">
                        {runtimeSteps.map((step) => (
                          <li key={step.key} className="assistant-trace-item">
                            <div className="assistant-trace-head">
                              <span>{step.title}</span>
                              <span className="tag tag-neutral">{phaseLabel(step.phase)}</span>
                            </div>
                            <p className="muted-text">{step.summary}</p>
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
                      <p className="muted-text">No runtime steps captured yet.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-runtime-episodes-section">
                    <p className="panel-subtitle">Episodes & Reuse</p>
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
                              <p className="muted-text">Lessons: {episode.useful_lessons.map((item) => String(item)).join("; ")}</p>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted-text">No similar episodes were reused for this turn.</p>
                    )}
                  </div>

                  <div className="sub-panel stack-gap-xs" data-testid="assistant-trace-output-section">
                    <p className="panel-subtitle">Result</p>
                    <p className="muted-text">{traceQuery.data?.task_summary || runtimeReflection?.summary || selectedTaskCard?.progress_message || "-"}</p>
                    <JsonViewer value={finalOutputView} defaultExpanded title="final output" />
                    <details className="assistant-debug-details">
                      <summary>Debug details</summary>
                      <JsonViewer value={selectedTurn.agent_run.memory} title="memory" />
                      <JsonViewer value={runtimeObservations} title="observations" />
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
