"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { AuthGate } from "../../components/auth-gate";
import { EmptyState } from "../../components/empty-state";
import { ErrorBanner } from "../../components/error-banner";
import { JsonViewer } from "../../components/json-viewer";
import { StatusBadge } from "../../components/status-badge";
import { useRunStream } from "../../hooks/use-run-stream";
import { useAuth } from "../../lib/auth-context";
import {
  approvalAction,
  assistantChat,
  deleteAssistantConversation,
  getDisplayErrorMessage,
  getAssistantConversation,
  getAssistantTaskTrace,
  listAssistantConversations,
  streamAssistantChat,
  updateAssistantConversation,
  type AssistantChatInput
} from "../../lib/api";
import { formatDateTime, formatRelativeTime } from "../../lib/format";
import type {
  AgentRuntimeStep,
  AssistantConversationSummary,
  AssistantTaskCard,
  AssistantTaskTrace,
  AssistantTurnSummary
} from "../../lib/mas-types";
import { useToast } from "../../lib/toast-context";

const FINAL_STATES = new Set(["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED", "TIMED_OUT"]);

const MODE_OPTIONS: Array<{ value: AssistantChatInput["mode"]; label: string }> = [
  { value: "auto", label: "\u81ea\u52a8" },
  { value: "direct_answer", label: "\u76f4\u7b54" },
  { value: "tool_task", label: "\u5de5\u5177" },
  { value: "workflow_task", label: "\u6267\u884c" }
];

const STARTER_PROMPTS = [
  {
    title: "\u62c6\u89e3\u9700\u6c42",
    prompt: "\u5e2e\u6211\u628a\u8fd9\u4e2a\u9700\u6c42\u62c6\u6210\u6e05\u6670\u7684\u6267\u884c\u6b65\u9aa4\uff0c\u5e76\u6307\u51fa\u4e3b\u8981\u98ce\u9669\u3002"
  },
  {
    title: "\u4ee3\u7801\u6392\u67e5",
    prompt: "\u5e2e\u6211\u5b9a\u4f4d\u8fd9\u4e2a\u4ed3\u5e93\u91cc\u6700\u503c\u5f97\u5148\u770b\u7684\u5173\u952e\u6a21\u5757\uff0c\u5e76\u89e3\u91ca\u5b83\u4eec\u4e4b\u95f4\u7684\u5173\u7cfb\u3002"
  },
  {
    title: "\u751f\u6210\u65b9\u6848",
    prompt: "\u57fa\u4e8e\u5f53\u524d\u9879\u76ee\u72b6\u6001\uff0c\u7ed9\u6211\u4e00\u4e2a\u53ef\u843d\u5730\u7684\u4f18\u5316\u65b9\u6848\uff0c\u4f18\u5148\u6309\u6536\u76ca\u6392\u5e8f\u3002"
  },
  {
    title: "\u7ee7\u7eed\u6267\u884c",
    prompt: "\u7ee7\u7eed\u63a8\u8fdb\u5f53\u524d\u4efb\u52a1\uff0c\u544a\u8bc9\u6211\u4e0b\u4e00\u6b65\u6700\u503c\u5f97\u5148\u505a\u4ec0\u4e48\u3002"
  }
] as const;

type StreamingDraft = {
  conversationId: string;
  turnId: string;
  userMessage: string;
  assistantMessage: string;
  taskId?: string | null;
  route: AssistantTurnSummary["route"];
  createdAt: string;
};

type ThreadItem =
  | {
      id: string;
      kind: "user";
      message: string;
      createdAt?: string;
      turn: AssistantTurnSummary;
    }
  | {
      id: string;
      kind: "assistant";
      message: string;
      createdAt?: string;
      turn: AssistantTurnSummary;
      taskCard?: AssistantTaskCard | null;
    };

type MessageSegment =
  | {
      kind: "text";
      value: string;
    }
  | {
      kind: "code";
      language: string;
      value: string;
    };

function normalizeText(value: unknown, maxLen = 180): string {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 3)}...`;
}

function routeLabel(route: string): string {
  if (route === "direct_answer") return "\u76f4\u63a5\u56de\u7b54";
  if (route === "tool_task") return "\u5de5\u5177\u6267\u884c";
  return "\u6301\u7eed\u6267\u884c";
}

function routeBadgeLabel(route: string): string {
  if (route === "tool_task") return "\u5de5\u5177";
  if (route === "workflow_task") return "\u6267\u884c\u4e2d";
  return "";
}

function phaseLabel(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (segment) => segment.toUpperCase());
}

function extractFinalText(value: Record<string, unknown> | undefined | null): string {
  if (!value || typeof value !== "object") return "";
  const candidates = ["message", "preview", "summary", "result"];
  for (const key of candidates) {
    const candidate = value[key];
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }
  return "";
}

function conversationTitle(
  detailTitle: string | null | undefined,
  summary: AssistantConversationSummary | null,
  fallback = "\u65b0\u7684\u5bf9\u8bdd"
): string {
  return (
    normalizeText(detailTitle || summary?.title || summary?.last_user_message || summary?.last_assistant_message, 34) ||
    fallback
  );
}

function conversationPreview(summary: AssistantConversationSummary | null): string {
  return (
    normalizeText(summary?.preview || summary?.last_assistant_message || summary?.last_user_message, 96) ||
    "\u4ece\u8fd9\u91cc\u7ee7\u7eed\u521a\u624d\u7684\u5bf9\u8bdd\u3002"
  );
}

function bubbleMessage(turn: AssistantTurnSummary, trace: AssistantTaskTrace | null): string {
  const directMessage = normalizeText(turn.assistant_message, 4000);
  if (directMessage) return directMessage;
  const summary = normalizeText(trace?.assistant_summary || turn.display_summary, 4000);
  if (summary) return summary;
  const finalText = extractFinalText(turn.agent_run.final_output);
  if (finalText) return finalText;
  return "\u6211\u5df2\u7ecf\u6536\u5230\u8fd9\u6761\u8bf7\u6c42\uff0c\u6b63\u5728\u7ee7\u7eed\u5904\u7406\u3002";
}

function splitMessageSegments(message: string): MessageSegment[] {
  const source = String(message || "");
  if (!source.trim()) return [];
  const segments: MessageSegment[] = [];
  const pattern = /```([\w-]+)?\n?([\s\S]*?)```/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(source))) {
    if (match.index > lastIndex) {
      segments.push({ kind: "text", value: source.slice(lastIndex, match.index) });
    }
    segments.push({
      kind: "code",
      language: String(match[1] || "").trim(),
      value: String(match[2] || "").replace(/\n$/, "")
    });
    lastIndex = pattern.lastIndex;
  }
  if (lastIndex < source.length) {
    segments.push({ kind: "text", value: source.slice(lastIndex) });
  }
  return segments.filter((segment) => segment.value.trim());
}

function renderInlineRichText(text: string, keyPrefix: string): ReactNode[] {
  return text.split(/(`[^`]+`)/g).filter(Boolean).map((part, index) => {
    if (part.startsWith("`") && part.endsWith("`") && part.length >= 2) {
      return (
        <code key={`${keyPrefix}-code-${index}`} className="assistant-chat-inline-code">
          {part.slice(1, -1)}
        </code>
      );
    }
    return <span key={`${keyPrefix}-text-${index}`}>{part}</span>;
  });
}

function renderTextSection(section: string, keyPrefix: string): ReactNode {
  const trimmed = section.trim();
  if (!trimmed) return null;
  const lines = trimmed.split("\n").map((line) => line.trimEnd());

  if (/^#{1,3}\s+/.test(trimmed) && lines.length === 1) {
    const level = Math.min(3, (trimmed.match(/^#+/)?.[0].length || 1));
    const content = trimmed.replace(/^#{1,3}\s+/, "");
    if (level === 1) return <h3 key={`${keyPrefix}-h1`}>{renderInlineRichText(content, `${keyPrefix}-h1`)}</h3>;
    if (level === 2) return <h4 key={`${keyPrefix}-h2`}>{renderInlineRichText(content, `${keyPrefix}-h2`)}</h4>;
    return <h5 key={`${keyPrefix}-h3`}>{renderInlineRichText(content, `${keyPrefix}-h3`)}</h5>;
  }

  if (lines.every((line) => /^[-*]\s+/.test(line.trim()))) {
    return (
      <ul key={`${keyPrefix}-ul`} className="assistant-chat-rich-list">
        {lines.map((line, index) => (
          <li key={`${keyPrefix}-li-${index}`}>{renderInlineRichText(line.trim().replace(/^[-*]\s+/, ""), `${keyPrefix}-li-${index}`)}</li>
        ))}
      </ul>
    );
  }

  if (lines.every((line) => /^\d+\.\s+/.test(line.trim()))) {
    return (
      <ol key={`${keyPrefix}-ol`} className="assistant-chat-rich-list">
        {lines.map((line, index) => (
          <li key={`${keyPrefix}-li-${index}`}>{renderInlineRichText(line.trim().replace(/^\d+\.\s+/, ""), `${keyPrefix}-li-${index}`)}</li>
        ))}
      </ol>
    );
  }

  return (
    <p key={`${keyPrefix}-p`}>
      {lines.map((line, index) => (
        <span key={`${keyPrefix}-line-${index}`}>
          {index > 0 ? <br /> : null}
          {renderInlineRichText(line, `${keyPrefix}-line-${index}`)}
        </span>
      ))}
    </p>
  );
}

function renderMessageContent(message: string, keyPrefix: string): ReactNode {
  const segments = splitMessageSegments(message);
  if (!segments.length) return message;
  return segments.map((segment, index) => {
    if (segment.kind === "code") {
      return (
        <pre key={`${keyPrefix}-codeblock-${index}`} className="assistant-chat-code-block">
          {segment.language ? <span className="assistant-chat-code-lang">{segment.language}</span> : null}
          <code>{segment.value}</code>
        </pre>
      );
    }
    const sections = segment.value.split(/\n{2,}/).map((section) => section.trim()).filter(Boolean);
    return (
      <div key={`${keyPrefix}-segment-${index}`} className="assistant-chat-rich-text">
        {sections.map((section, sectionIndex) => renderTextSection(section, `${keyPrefix}-${index}-${sectionIndex}`))}
      </div>
    );
  });
}

function sortTurns(turns: AssistantTurnSummary[]): AssistantTurnSummary[] {
  return [...turns].sort((a, b) => String(a.created_at || "").localeCompare(String(b.created_at || "")));
}

function buildThreadItems(turns: AssistantTurnSummary[], tasksById: Map<string, AssistantTaskCard>): ThreadItem[] {
  const items: ThreadItem[] = [];
  for (const turn of turns) {
    items.push({
      id: `${turn.turn_id}-user`,
      kind: "user",
      message: turn.user_message,
      createdAt: turn.created_at,
      turn
    });
    items.push({
      id: `${turn.turn_id}-assistant`,
      kind: "assistant",
      message: turn.assistant_message || turn.display_summary || extractFinalText(turn.agent_run.final_output),
      createdAt: turn.updated_at || turn.created_at,
      turn,
      taskCard: turn.task_id ? tasksById.get(turn.task_id) || null : null
    });
  }
  return items;
}

function detailSummary(trace: AssistantTaskTrace | null, taskCard: AssistantTaskCard | null): string {
  return (
    normalizeText(trace?.assistant_summary || trace?.task_summary || taskCard?.assistant_summary || taskCard?.progress_message, 220) ||
    "\u8fd9\u4e00\u8f6e\u6682\u65f6\u8fd8\u6ca1\u6709\u66f4\u591a\u8fd0\u884c\u6458\u8981\u3002"
  );
}

function routeToMode(route: AssistantTurnSummary["route"]): AssistantChatInput["mode"] {
  if (route === "tool_task") return "tool_task";
  if (route === "workflow_task") return "workflow_task";
  return "auto";
}

export default function AssistantPage() {
  const auth = useAuth();
  const queryClient = useQueryClient();
  const { pushToast } = useToast();
  const canQuery = auth.ready && auth.isAuthenticated;
  const userId = String(auth.user?.sub || "");
  const isOperator = auth.user?.role === "owner" || auth.user?.role === "operator";
  const threadViewportRef = useRef<HTMLDivElement | null>(null);
  const composerRef = useRef<HTMLTextAreaElement | null>(null);
  const renameInputRef = useRef<HTMLInputElement | null>(null);
  const streamAbortRef = useRef<AbortController | null>(null);

  const [selectedConversationId, setSelectedConversationId] = useState("");
  const [selectedTurnId, setSelectedTurnId] = useState("");
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [inputMessage, setInputMessage] = useState("");
  const [mode, setMode] = useState<AssistantChatInput["mode"]>("auto");
  const [pendingConfirmMessage, setPendingConfirmMessage] = useState<string | null>(null);
  const [creatingNewConversation, setCreatingNewConversation] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const [showModeControls, setShowModeControls] = useState(false);
  const [streamingDraft, setStreamingDraft] = useState<StreamingDraft | null>(null);
  const [streamingText, setStreamingText] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [isRenamingConversation, setIsRenamingConversation] = useState(false);
  const [renameConversationTitle, setRenameConversationTitle] = useState("");

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

  const sortedTurns = useMemo(() => sortTurns(conversationQuery.data?.turn_history || []), [conversationQuery.data?.turn_history]);

  useEffect(() => {
    if (!streamingDraft) return;
    if (sortedTurns.some((turn) => turn.turn_id === streamingDraft.turnId) && streamingText === streamingDraft.assistantMessage) {
      setStreamingDraft(null);
      setStreamingText("");
    }
  }, [sortedTurns, streamingDraft, streamingText]);

  useEffect(() => {
    if (!streamingDraft || !selectedConversationId) return;
    if (!streamingDraft.conversationId.startsWith("draft-conversation-")) return;
    if (!conversationQuery.data) return;
    setStreamingDraft((current) => {
      if (!current || !current.conversationId.startsWith("draft-conversation-")) {
        return current;
      }
      return {
        ...current,
        conversationId: selectedConversationId
      };
    });
  }, [conversationQuery.data, selectedConversationId, streamingDraft]);

  useEffect(() => {
    return () => {
      streamAbortRef.current?.abort();
      streamAbortRef.current = null;
    };
  }, []);

  useEffect(() => {
    setIsRenamingConversation(false);
    setRenameConversationTitle("");
  }, [creatingNewConversation, selectedConversationId]);

  useEffect(() => {
    if (!isRenamingConversation) return;
    renameInputRef.current?.focus();
    renameInputRef.current?.select();
  }, [isRenamingConversation]);

  useEffect(() => {
    if (!conversationQuery.data) return;
    if (!sortedTurns.length) {
      const latestTask = conversationQuery.data.task_history[0];
      if (!selectedTaskId && latestTask?.task_id) {
        setSelectedTaskId(latestTask.task_id);
      }
      return;
    }
    if (selectedTurnId && sortedTurns.some((turn) => turn.turn_id === selectedTurnId)) {
      return;
    }
    const latestTurn = sortedTurns[sortedTurns.length - 1];
    setSelectedTurnId(latestTurn.turn_id);
    if (latestTurn.task_id) {
      setSelectedTaskId(latestTurn.task_id);
    }
  }, [conversationQuery.data, selectedTaskId, selectedTurnId, sortedTurns]);

  const selectedTurn = useMemo(() => {
    if (!sortedTurns.length) return null;
    return sortedTurns.find((turn) => turn.turn_id === selectedTurnId) || sortedTurns[sortedTurns.length - 1] || null;
  }, [selectedTurnId, sortedTurns]);

  const selectedTaskIdForTrace = selectedTurn?.task_id || selectedTaskId || "";

  const traceQuery = useQuery({
    queryKey: ["assistant", "trace", selectedTaskIdForTrace],
    enabled: canQuery && Boolean(selectedTaskIdForTrace),
    refetchInterval: 7000,
    queryFn: () => getAssistantTaskTrace(auth.accessToken, selectedTaskIdForTrace)
  });

  const applyAssistantResponse = (data: Awaited<ReturnType<typeof assistantChat>>, variables: { message: string; confirmed: boolean }) => {
    setSelectedConversationId(data.conversation_id);
    setSelectedTurnId(data.turn?.turn_id || "");
    setSelectedTaskId(data.task?.task_id || "");
    setPendingConfirmMessage(data.need_confirmation ? variables.message : null);
    setCreatingNewConversation(false);
    setInputMessage("");
    if (data.turn?.turn_id) {
      setStreamingDraft({
        conversationId: data.conversation_id,
        turnId: data.turn.turn_id,
        userMessage: variables.message,
        assistantMessage: data.message,
        taskId: data.task?.task_id || data.turn.task_id || null,
        route: data.turn.route,
        createdAt: new Date().toISOString()
      });
      setStreamingText(data.message);
    } else {
      setStreamingDraft(null);
      setStreamingText("");
    }
    if (data.task?.task_id) {
      setShowDetails(true);
    }
    pushToast({
      kind: "success",
      title: "xh-helper \u5df2\u54cd\u5e94",
      description: `${routeLabel(data.route)}${data.need_confirmation ? "\uff0c\u7b49\u5f85\u4f60\u786e\u8ba4" : ""}`
    });
    queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
    queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", data.conversation_id] }).catch(() => undefined);
    if (data.task?.task_id) {
      queryClient.invalidateQueries({ queryKey: ["assistant", "trace", data.task.task_id] }).catch(() => undefined);
    }
  };

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
      applyAssistantResponse(data, variables);
    },
    onError(error) {
      pushToast({ kind: "error", title: "\u53d1\u9001\u5931\u8d25", description: getDisplayErrorMessage(error) });
    }
  });

  const approvalMutation = useMutation({
    mutationFn: async (payload: { approvalId: string; action: "approve" | "reject" }) => {
      return approvalAction(auth.accessToken, payload.approvalId, payload.action, {
        reason: payload.action === "approve" ? "approved in assistant workspace" : "rejected in assistant workspace"
      });
    },
    onSuccess() {
      pushToast({ kind: "success", title: "\u5ba1\u6279\u5df2\u66f4\u65b0" });
      if (selectedTaskIdForTrace) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "trace", selectedTaskIdForTrace] }).catch(() => undefined);
      }
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    },
    onError(error) {
      pushToast({ kind: "error", title: "\u5ba1\u6279\u5931\u8d25", description: getDisplayErrorMessage(error) });
    }
  });

  const renameConversationMutation = useMutation({
    mutationFn: async (payload: { conversationId: string; title: string | null }) => {
      return updateAssistantConversation(auth.accessToken, payload.conversationId, {
        title: payload.title
      });
    },
    onSuccess(data, variables) {
      queryClient.setQueryData<AssistantConversationSummary[] | undefined>(["assistant", "conversations"], (current) =>
        (current || []).map((item) =>
          item.conversation_id === variables.conversationId
            ? {
                ...item,
                title: data.title,
                preview: data.preview,
                updated_at: data.updated_at
              }
            : item
        )
      );
      queryClient.setQueryData(["assistant", "conversation", variables.conversationId], (current: unknown) => {
        if (!current || typeof current !== "object") return current;
        return {
          ...(current as Record<string, unknown>),
          title: data.title,
          preview: data.preview,
          updated_at: data.updated_at
        };
      });
      setIsRenamingConversation(false);
      setRenameConversationTitle("");
      pushToast({
        kind: "success",
        title: variables.title ? "\u4f1a\u8bdd\u5df2\u91cd\u547d\u540d" : "\u5df2\u6062\u590d\u81ea\u52a8\u6807\u9898"
      });
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", variables.conversationId] }).catch(() => undefined);
    },
    onError(error) {
      pushToast({ kind: "error", title: "\u91cd\u547d\u540d\u5931\u8d25", description: getDisplayErrorMessage(error) });
    }
  });

  const deleteConversationMutation = useMutation({
    mutationFn: async (conversationId: string) => {
      await deleteAssistantConversation(auth.accessToken, conversationId);
      return conversationId;
    },
    onSuccess(deletedConversationId) {
      const currentConversations = queryClient.getQueryData<AssistantConversationSummary[]>(["assistant", "conversations"]) || [];
      const remainingConversations = currentConversations.filter((item) => item.conversation_id !== deletedConversationId);
      queryClient.setQueryData(["assistant", "conversations"], remainingConversations);
      queryClient.removeQueries({ queryKey: ["assistant", "conversation", deletedConversationId] });
      if (selectedConversationId === deletedConversationId) {
        setSelectedConversationId(remainingConversations[0]?.conversation_id || "");
        setSelectedTurnId("");
        setSelectedTaskId("");
        setPendingConfirmMessage(null);
        setShowDetails(false);
        setCreatingNewConversation(false);
        setInputMessage("");
        setStreamingDraft(null);
        setStreamingText("");
      }
      pushToast({ kind: "success", title: "\u4f1a\u8bdd\u5df2\u5220\u9664" });
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
    },
    onError(error) {
      pushToast({ kind: "error", title: "\u5220\u9664\u5931\u8d25", description: getDisplayErrorMessage(error) });
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

  const activeConversationSummary = useMemo<AssistantConversationSummary | null>(() => {
    if (!conversationsQuery.data || !selectedConversationId) return null;
    return conversationsQuery.data.find((item) => item.conversation_id === selectedConversationId) || null;
  }, [conversationsQuery.data, selectedConversationId]);

  const tasksById = useMemo(() => {
    const map = new Map<string, AssistantTaskCard>();
    for (const task of conversationQuery.data?.task_history || []) {
      map.set(task.task_id, task);
    }
    return map;
  }, [conversationQuery.data?.task_history]);

  const selectedTaskCard = selectedTaskIdForTrace ? tasksById.get(selectedTaskIdForTrace) || null : null;
  const currentModeLabel = MODE_OPTIONS.find((option) => option.value === mode)?.label || "\u81ea\u52a8";
  const threadItems = useMemo(() => {
    const items = buildThreadItems(sortedTurns, tasksById);
    if (
      streamingDraft &&
      (selectedConversationId ? streamingDraft.conversationId === selectedConversationId : true) &&
      !sortedTurns.some((turn) => turn.turn_id === streamingDraft.turnId)
    ) {
      const syntheticTurn: AssistantTurnSummary = {
        turn_id: streamingDraft.turnId,
        route: streamingDraft.route,
        status: "RUNNING",
        current_phase: "respond",
        display_state: "\u6b63\u5728\u8f93\u51fa",
        display_summary: streamingDraft.assistantMessage,
        response_type: "direct_answer",
        user_message: streamingDraft.userMessage,
        assistant_message: streamingDraft.assistantMessage,
        task_id: streamingDraft.taskId || null,
        trace_id: "",
        created_at: streamingDraft.createdAt,
        updated_at: streamingDraft.createdAt,
        agent_run: {
          turn_id: streamingDraft.turnId,
          route: streamingDraft.route,
          status: "RUNNING",
          current_phase: "respond",
          task_id: streamingDraft.taskId || null,
          trace_id: "",
          planner: {},
          retrieval_hits: [],
          memory: {},
          episodes: [],
          observations: [],
          steps: [],
          final_output: { message: streamingDraft.assistantMessage }
        }
      };
      items.push({
        id: `${syntheticTurn.turn_id}-user`,
        kind: "user",
        message: syntheticTurn.user_message,
        createdAt: syntheticTurn.created_at,
        turn: syntheticTurn
      });
      items.push({
        id: `${syntheticTurn.turn_id}-assistant`,
        kind: "assistant",
        message: syntheticTurn.assistant_message || streamingDraft.assistantMessage,
        createdAt: syntheticTurn.updated_at,
        turn: syntheticTurn,
        taskCard: syntheticTurn.task_id ? tasksById.get(syntheticTurn.task_id) || null : null
      });
    }
    return items;
  }, [selectedConversationId, sortedTurns, streamingDraft, tasksById]);
  const waitingApprovals = traceQuery.data?.approvals?.filter((item) => item.status === "WAITING_HUMAN") || [];
  const runtimeSteps: AgentRuntimeStep[] = traceQuery.data?.runtime_steps || selectedTurn?.agent_run.steps || [];

  const runningConversationCount = useMemo(() => {
    return (conversationsQuery.data || []).filter((item) => item.running_task_count > 0).length;
  }, [conversationsQuery.data]);

  useEffect(() => {
    if (!threadViewportRef.current) return;
    threadViewportRef.current.scrollTo({
      top: threadViewportRef.current.scrollHeight,
      behavior: "smooth"
    });
  }, [threadItems.length, selectedConversationId]);

  const startAssistantStream = async (message: string, confirmed: boolean) => {
    const controller = new AbortController();
    const draftTurnId = `draft-${Date.now()}`;
    let streamed = "";
    let receivedStreamData = false;
    const syncConversationState = () => {
      queryClient.invalidateQueries({ queryKey: ["assistant", "conversations"] }).catch(() => undefined);
      if (selectedConversationId) {
        queryClient.invalidateQueries({ queryKey: ["assistant", "conversation", selectedConversationId] }).catch(() => undefined);
      }
    };
    streamAbortRef.current = controller;
    setIsStreaming(true);
    setCreatingNewConversation(false);
    setStreamingDraft({
      conversationId: selectedConversationId || `draft-conversation-${Date.now()}`,
      turnId: draftTurnId,
      userMessage: message,
      assistantMessage: "",
      taskId: null,
      route: mode === "workflow_task" ? "workflow_task" : mode === "tool_task" ? "tool_task" : "direct_answer",
      createdAt: new Date().toISOString()
    });
    setStreamingText("");
    setInputMessage("");
    setPendingConfirmMessage(confirmed ? message : null);

    try {
      const response = await streamAssistantChat(
        auth.accessToken,
        {
          userId,
          conversationId: selectedConversationId || undefined,
          message,
          mode: mode || "auto",
          metadata: confirmed ? { confirmed: true } : {}
        },
        {
          signal: controller.signal,
          onEvent: (event) => {
            if (event.type === "delta" && event.delta) {
              receivedStreamData = true;
              streamed += event.delta;
              setStreamingText(streamed);
              setStreamingDraft((current) => (current ? { ...current, assistantMessage: streamed } : current));
            }
          }
        }
      );

      if (response) {
        applyAssistantResponse(response, { message, confirmed });
        return;
      }

      if (receivedStreamData) {
        syncConversationState();
        pushToast({
          kind: "success",
          title: "\u56de\u590d\u5df2\u6536\u5230",
          description: "\u6d88\u606f\u6b63\u5728\u540c\u6b65\u5230\u4f1a\u8bdd\u5217\u8868\u3002"
        });
        return;
      }
    } catch (error) {
      if (controller.signal.aborted) {
        setStreamingDraft((current) => (current ? { ...current, assistantMessage: streamed } : current));
        setStreamingText(streamed);
        syncConversationState();
        return;
      }
      if (!receivedStreamData) {
        setStreamingDraft(null);
        setStreamingText("");
        sendMutation.mutate({ message, confirmed });
        return;
      }
      setStreamingDraft((current) => (current ? { ...current, assistantMessage: streamed } : current));
      setStreamingText(streamed);
      syncConversationState();
      pushToast({
        kind: "error",
        title: "\u56de\u590d\u4e2d\u65ad",
        description: `${getDisplayErrorMessage(error)} \u5df2\u4fdd\u7559\u521a\u624d\u6536\u5230\u7684\u5185\u5bb9\u3002`
      });
    } finally {
      if (streamAbortRef.current === controller) {
        streamAbortRef.current = null;
      }
      setIsStreaming(false);
    }
  };

  const handleSend = (confirmed: boolean) => {
    const message = (confirmed ? pendingConfirmMessage || "" : inputMessage).trim();
    if (!message || sendMutation.isPending || isStreaming) return;
    startAssistantStream(message, confirmed).catch(() => undefined);
  };

  const handleStopStreaming = () => {
    streamAbortRef.current?.abort();
    streamAbortRef.current = null;
    setIsStreaming(false);
    setPendingConfirmMessage(null);
  };

  const handleEditPrompt = (message: string, route?: AssistantTurnSummary["route"]) => {
    setInputMessage(message);
    setPendingConfirmMessage(null);
    if (route) {
      setMode(routeToMode(route));
    }
    composerRef.current?.focus();
  };

  const handleRegenerate = (turn: AssistantTurnSummary) => {
    if (sendMutation.isPending || isStreaming) return;
    setMode(routeToMode(turn.route));
    setPendingConfirmMessage(null);
    startAssistantStream(turn.user_message, false).catch(() => undefined);
  };

  const selectedConversationTitle = conversationTitle(conversationQuery.data?.title, activeConversationSummary);
  const selectedConversationPreview = conversationQuery.data?.preview || conversationPreview(activeConversationSummary);

  const startRenamingConversation = () => {
    if (!selectedConversationId || creatingNewConversation || isStreaming) return;
    setRenameConversationTitle(selectedConversationTitle);
    setIsRenamingConversation(true);
  };

  const submitConversationRename = () => {
    if (!selectedConversationId || renameConversationMutation.isPending) return;
    renameConversationMutation.mutate({
      conversationId: selectedConversationId,
      title: renameConversationTitle.trim() || null
    });
  };

  const cancelConversationRename = () => {
    if (renameConversationMutation.isPending) return;
    setIsRenamingConversation(false);
    setRenameConversationTitle("");
  };

  const requestDeleteConversation = (conversationId: string) => {
    if (deleteConversationMutation.isPending || isStreaming) return;
    const matchedConversation = (conversationsQuery.data || []).find((item) => item.conversation_id === conversationId) || null;
    const label = conversationTitle(undefined, matchedConversation);
    const shouldDelete = window.confirm(
      `\u786e\u8ba4\u5220\u9664\u201c${label}\u201d\u5417\uff1f\u8fd9\u4f1a\u628a\u5b83\u4ece\u804a\u5929\u4fa7\u680f\u79fb\u9664\uff0c\u4f46\u4e0d\u4f1a\u5220\u9664\u5df2\u7ecf\u5b8c\u6210\u7684\u4efb\u52a1\u8fd0\u884c\u8bb0\u5f55\u3002`
    );
    if (!shouldDelete) return;
    deleteConversationMutation.mutate(conversationId);
  };

  const selectedBubbleSummary = detailSummary(traceQuery.data || null, selectedTaskCard);

  return (
    <AuthGate>
      <div className="assistant-chat-product-page" data-testid="assistant-page">
        {conversationsQuery.error ? <ErrorBanner error={conversationsQuery.error} onRetry={() => conversationsQuery.refetch()} /> : null}
        {conversationQuery.error ? <ErrorBanner error={conversationQuery.error} onRetry={() => conversationQuery.refetch()} /> : null}
        {traceQuery.error ? <ErrorBanner error={traceQuery.error} onRetry={() => traceQuery.refetch()} /> : null}

        <div className="assistant-chat-product-shell">
          <aside className="assistant-chat-sidebar panel">
            <div className="assistant-chat-sidebar-top">
              <div className="assistant-chat-brand">
                <p className="assistant-chat-brand-kicker">{"\u0041\u0049 \u52a9\u624b"}</p>
                <h1>{"\u6e05\u6670\u804a\u5929\uff0c\u9700\u8981\u65f6\u518d\u6267\u884c"}</h1>
                <p className="assistant-chat-brand-copy">{"\u76f4\u63a5\u8f93\u5165\u95ee\u9898\u6216\u4efb\u52a1\uff0c\u6211\u4f1a\u5148\u76f4\u63a5\u56de\u7b54\uff0c\u9700\u8981\u65f6\u518d\u8c03\u7528\u5de5\u5177\u6216\u8fdb\u5165\u6267\u884c\u6d41\u7a0b\u3002"}</p>
              </div>
              <div className="assistant-chat-sidebar-actions">
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={() => {
                    setCreatingNewConversation(true);
                    setSelectedConversationId("");
                    setSelectedTurnId("");
                    setSelectedTaskId("");
                    setPendingConfirmMessage(null);
                    setShowDetails(false);
                    setInputMessage("");
                  }}
                >
                  {"\u65b0\u5bf9\u8bdd"}
                </button>
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={() => {
                    conversationsQuery.refetch().catch(() => undefined);
                    if (selectedConversationId) conversationQuery.refetch().catch(() => undefined);
                    if (selectedTaskIdForTrace) traceQuery.refetch().catch(() => undefined);
                  }}
                >
                  {"\u5237\u65b0"}
                </button>
              </div>
            </div>

            <div className="assistant-chat-sidebar-stats">
              <div className="assistant-chat-stat"><span>{"\u5bf9\u8bdd"}</span><strong>{conversationsQuery.data?.length || 0}</strong></div>
              <div className="assistant-chat-stat"><span>{"\u8fdb\u884c\u4e2d"}</span><strong>{runningConversationCount}</strong></div>
              <div className="assistant-chat-stat"><span>{"\u5f85\u5ba1\u6279"}</span><strong>{waitingApprovals.length}</strong></div>
            </div>

            <div className="assistant-chat-conversation-list">
              {(conversationsQuery.data || []).length === 0 ? (
                <EmptyState
                  title="\u8fd8\u6ca1\u6709\u4f1a\u8bdd"
                  description="\u53d1\u51fa\u7b2c\u4e00\u6761\u6d88\u606f\u540e\uff0c\u8fd9\u91cc\u4f1a\u4fdd\u5b58\u4f60\u7684\u5bf9\u8bdd\u5386\u53f2\u3002"
                />
              ) : (
                (conversationsQuery.data || []).map((item) => {
                  const active = item.conversation_id === selectedConversationId;
                  return (
                    <div
                      key={item.conversation_id}
                      role="button"
                      tabIndex={0}
                      className={active ? "assistant-chat-conversation active" : "assistant-chat-conversation"}
                      onClick={() => {
                        setCreatingNewConversation(false);
                        setSelectedConversationId(item.conversation_id);
                        setSelectedTurnId("");
                        setSelectedTaskId("");
                        setPendingConfirmMessage(null);
                        setShowDetails(false);
                      }}
                      onKeyDown={(event) => {
                        if (event.key !== "Enter" && event.key !== " ") return;
                        event.preventDefault();
                        setCreatingNewConversation(false);
                        setSelectedConversationId(item.conversation_id);
                        setSelectedTurnId("");
                        setSelectedTaskId("");
                        setPendingConfirmMessage(null);
                        setShowDetails(false);
                      }}
                    >
                      <div className="assistant-chat-conversation-head">
                        <strong>{conversationTitle(undefined, item)}</strong>
                        {item.running_task_count > 0 ? <span className="assistant-chip warning">{"\u6267\u884c\u4e2d"}</span> : null}
                      </div>
                      <p>{conversationPreview(item)}</p>
                      <div className="assistant-chat-conversation-meta">
                        <span>{formatRelativeTime(item.updated_at || item.created_at)}</span>
                        <span>{`${item.task_count} \u4e2a\u4efb\u52a1`}</span>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </aside>

          <main className="assistant-chat-main panel">
            <header className="assistant-chat-main-header">
              <div className="assistant-chat-main-title">
                {creatingNewConversation ? (
                  <>
                    <h2>{"\u65b0\u7684\u5bf9\u8bdd"}</h2>
                    <p>{"\u76f4\u63a5\u8bf4\u51fa\u4f60\u7684\u76ee\u6807\uff0c\u6211\u4f1a\u5148\u7ed9\u51fa\u56de\u590d\uff0c\u518d\u5728\u9700\u8981\u65f6\u7ee7\u7eed\u6267\u884c\u3002"}</p>
                  </>
                ) : isRenamingConversation ? (
                  <div className="assistant-chat-title-editor">
                    <div className="assistant-chat-title-row">
                      <input
                        ref={renameInputRef}
                        type="text"
                        value={renameConversationTitle}
                        maxLength={120}
                        className="assistant-chat-title-input"
                        placeholder="\u7ed9\u8fd9\u6761\u5bf9\u8bdd\u8d77\u4e2a\u540d\u5b57"
                        disabled={renameConversationMutation.isPending}
                        onChange={(event) => setRenameConversationTitle(event.target.value)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter") { event.preventDefault(); submitConversationRename(); }
                          if (event.key === "Escape") { event.preventDefault(); cancelConversationRename(); }
                        }}
                      />
                      <div className="assistant-chat-title-editor-actions">
                        <button type="button" className="btn btn-primary" disabled={renameConversationMutation.isPending} onClick={submitConversationRename}>{"\u4fdd\u5b58"}</button>
                        <button type="button" className="btn btn-ghost" disabled={renameConversationMutation.isPending} onClick={cancelConversationRename}>{"\u53d6\u6d88"}</button>
                      </div>
                    </div>
                    <p>{"\u7559\u7a7a\u4fdd\u5b58\u4f1a\u6062\u590d\u81ea\u52a8\u547d\u540d\u3002"}</p>
                  </div>
                ) : (
                  <>
                    <div className="assistant-chat-title-row">
                      <h2>{selectedConversationTitle}</h2>
                      <button type="button" className="assistant-link-button" disabled={!selectedConversationId || isStreaming} onClick={startRenamingConversation}>{"\u91cd\u547d\u540d"}</button>
                    </div>
                    <p>{threadItems.length > 0 ? selectedConversationPreview : "\u5728\u4e0b\u65b9\u76f4\u63a5\u8f93\u5165\u95ee\u9898\uff0c\u6216\u7ee7\u7eed\u5f53\u524d\u5bf9\u8bdd\u3002"}</p>
                  </>
                )}
              </div>
              <div className="assistant-chat-main-actions">
                {selectedTaskCard ? <StatusBadge status={selectedTaskCard.status} /> : null}
                {selectedTaskCard?.chat_state ? <span className="assistant-chip">{selectedTaskCard.chat_state}</span> : null}
                {!creatingNewConversation && selectedConversationId ? <button type="button" className="btn btn-ghost" disabled={deleteConversationMutation.isPending || isStreaming} onClick={() => requestDeleteConversation(selectedConversationId)}>{"\u5220\u9664\u4f1a\u8bdd"}</button> : null}
                <button type="button" className="btn btn-ghost" onClick={() => setShowDetails((value) => !value)}>{showDetails ? "\u9690\u85cf\u8fd0\u884c\u7ec6\u8282" : "\u67e5\u770b\u8fd0\u884c\u7ec6\u8282"}</button>
              </div>
            </header>

            {waitingApprovals.length > 0 ? (
              <div className="assistant-chat-banner">
                <div>
                  <strong>{"\u8fd9\u8f6e\u6267\u884c\u6b63\u5728\u7b49\u5f85\u786e\u8ba4"}</strong>
                  <p>{waitingApprovals[0].action_hint || "\u786e\u8ba4\u540e\u6211\u4f1a\u7ee7\u7eed\u5b8c\u6210\u5269\u4f59\u6b65\u9aa4\u3002"}</p>
                </div>
                {isOperator ? (
                  <div className="inline-actions">
                    <button type="button" className="btn btn-primary" disabled={approvalMutation.isPending || isStreaming} onClick={() => approvalMutation.mutate({ approvalId: waitingApprovals[0].approval_id, action: "approve" })}>{"\u7ee7\u7eed\u6267\u884c"}</button>
                    <button type="button" className="btn btn-ghost" disabled={approvalMutation.isPending || isStreaming} onClick={() => approvalMutation.mutate({ approvalId: waitingApprovals[0].approval_id, action: "reject" })}>{"\u62d2\u7edd"}</button>
                  </div>
                ) : null}
              </div>
            ) : null}

            <div ref={threadViewportRef} className="assistant-chat-thread" data-testid="assistant-message-history">
              {threadItems.length === 0 ? (
                <div className="assistant-chat-empty-shell">
                  <div className="assistant-chat-welcome">
                    <EmptyState
                      title={creatingNewConversation ? "\u8bf4\u51fa\u4f60\u7684\u95ee\u9898\u6216\u76ee\u6807" : "\u4ece\u4e00\u6761\u65b0\u6d88\u606f\u5f00\u59cb"}
                      description={creatingNewConversation ? "\u6211\u4f1a\u5148\u56de\u7b54\uff0c\u518d\u5728\u9700\u8981\u65f6\u8c03\u7528\u5de5\u5177\u6216\u8fdb\u5165\u6267\u884c\u6a21\u5f0f\u3002" : "\u53ef\u4ee5\u9009\u62e9\u5de6\u4fa7\u5386\u53f2\u4f1a\u8bdd\uff0c\u6216\u76f4\u63a5\u53d1\u4e00\u6761\u65b0\u6d88\u606f\u5f00\u59cb\u3002"}
                    />
                    <div className="assistant-chat-starter-grid">
                      {STARTER_PROMPTS.map((item) => (
                        <button key={item.title} type="button" className="assistant-chat-starter-card" onClick={() => { setCreatingNewConversation(true); handleEditPrompt(item.prompt, "direct_answer"); }}>
                          <strong>{item.title}</strong>
                          <span>{item.prompt}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              ) : (
                threadItems.map((item) => {
                  const isAssistant = item.kind === "assistant";
                  const isSelected = isAssistant && item.turn.turn_id === selectedTurn?.turn_id;
                  const taskCard = isAssistant ? item.taskCard : null;
                  const trace = isAssistant && item.turn.task_id === selectedTaskIdForTrace ? traceQuery.data || null : null;
                  const message = isAssistant && item.turn.turn_id === streamingDraft?.turnId && streamingText ? streamingText : isAssistant ? bubbleMessage(item.turn, trace) : item.message;

                  return (
                    <div key={item.id} className={isAssistant ? "assistant-chat-message-row assistant" : "assistant-chat-message-row user"}>
                      <div
                        role={isAssistant ? "button" : undefined}
                        tabIndex={isAssistant ? 0 : undefined}
                        className={isAssistant ? (isSelected ? "assistant-chat-bubble assistant selected" : "assistant-chat-bubble assistant") : "assistant-chat-bubble user"}
                        onClick={() => {
                          if (!isAssistant) return;
                          setSelectedTurnId(item.turn.turn_id);
                          if (item.turn.task_id) { setSelectedTaskId(item.turn.task_id); setShowDetails(true); }
                        }}
                        onKeyDown={(event) => {
                          if (!isAssistant) return;
                          if (event.key !== "Enter" && event.key !== " ") return;
                          event.preventDefault();
                          setSelectedTurnId(item.turn.turn_id);
                          if (item.turn.task_id) { setSelectedTaskId(item.turn.task_id); setShowDetails(true); }
                        }}
                      >
                        <div className="assistant-chat-bubble-head">
                          <span>{isAssistant ? "xh-helper" : "\u4f60"}</span>
                          <span>{formatRelativeTime(item.createdAt)}</span>
                          {isAssistant && routeBadgeLabel(item.turn.route) ? <span>{routeBadgeLabel(item.turn.route)}</span> : null}
                        </div>
                        <div className="assistant-chat-bubble-body">
                          {renderMessageContent(message, item.id)}
                          {isAssistant && item.turn.turn_id === streamingDraft?.turnId && streamingText !== (streamingDraft?.assistantMessage || "") ? <span className="assistant-chat-streaming-cursor" aria-hidden="true" /> : null}
                        </div>
                        {isAssistant && taskCard ? (
                          <div className="assistant-chat-inline-task">
                            <div className="assistant-chat-inline-task-head">
                              <span>{taskCard.chat_state || taskCard.status_label}</span>
                              <span>{taskCard.current_step || phaseLabel(item.turn.current_phase)}</span>
                            </div>
                            <p>{detailSummary(trace, taskCard)}</p>
                            {trace?.next_step_hint || taskCard.next_action ? <span className="assistant-chip subtle">{trace?.next_step_hint || taskCard.next_action}</span> : null}
                          </div>
                        ) : null}
                        {!isAssistant ? (
                          <div className="assistant-chat-bubble-actions">
                            <button type="button" className="assistant-link-button" disabled={isStreaming} onClick={(event) => { event.stopPropagation(); handleEditPrompt(item.turn.user_message, item.turn.route); }}>{"\u7f16\u8f91"}</button>
                            <button type="button" className="assistant-link-button" disabled={sendMutation.isPending || isStreaming} onClick={(event) => { event.stopPropagation(); handleRegenerate(item.turn); }}>{"\u91cd\u65b0\u751f\u6210"}</button>
                          </div>
                        ) : null}
                      </div>
                    </div>
                  );
                })
              )}
            </div>

            {showDetails && selectedTurn ? (
              <section className="assistant-chat-details">
                <div className="assistant-chat-details-grid">
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">{"\u5f53\u524d\u8fd9\u8f6e"}</p>
                    <div className="assistant-chat-detail-row"><span>{routeLabel(selectedTurn.route)}</span><span>{selectedTurn.display_state || selectedTaskCard?.chat_state || "\u5df2\u54cd\u5e94"}</span></div>
                    <p>{selectedBubbleSummary}</p>
                    <p className="muted-text">{selectedTaskCard?.current_step ? `\u5f53\u524d\u6b65\u9aa4\uff1a${selectedTaskCard.current_step}` : `\u5f53\u524d\u9636\u6bb5\uff1a${phaseLabel(selectedTurn.current_phase)}`}</p>
                    <p className="muted-text">{`\u6700\u540e\u66f4\u65b0\u65f6\u95f4\uff1a${formatDateTime(selectedTurn.updated_at || selectedTurn.created_at)}`}</p>
                  </div>
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">{"\u5de5\u5177\u4e0e\u5ba1\u6279"}</p>
                    {traceQuery.data?.tool_calls?.length ? traceQuery.data.tool_calls.slice(-3).map((item) => (
                      <div key={item.tool_call_id} className="assistant-chat-detail-card">
                        <div className="assistant-chat-detail-row"><strong>{item.tool_name}</strong><span>{item.status_label}</span></div>
                        {item.response_summary ? <p>{normalizeText(item.response_summary, 140)}</p> : null}
                      </div>
                    )) : <p className="muted-text">{"\u8fd9\u4e00\u8f6e\u6ca1\u6709\u660e\u663e\u7684\u5de5\u5177\u6d3b\u52a8\u3002"}</p>}
                    {waitingApprovals.length ? <div className="assistant-chat-detail-card"><strong>{"\u5f85\u786e\u8ba4"}</strong><p>{waitingApprovals[0].reason || waitingApprovals[0].action_hint || "\u6267\u884c\u88ab\u6682\u505c\uff0c\u7b49\u5f85\u4eba\u5de5\u786e\u8ba4\u3002"}</p></div> : null}
                  </div>
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">{"\u6267\u884c\u6b65\u9aa4"}</p>
                    {runtimeSteps.length ? runtimeSteps.slice(-5).map((step) => (
                      <div key={step.key} className="assistant-chat-detail-card">
                        <div className="assistant-chat-detail-row"><strong>{step.title}</strong><span>{phaseLabel(step.phase)}</span></div>
                        <p>{normalizeText(step.summary, 160)}</p>
                      </div>
                    )) : <p className="muted-text">{"\u5f53\u524d\u8fd8\u6ca1\u6709\u53ef\u5c55\u793a\u7684\u8fd0\u884c\u6b65\u9aa4\u3002"}</p>}
                  </div>
                  <div className="sub-panel stack-gap-xs">
                    <p className="panel-subtitle">{"\u539f\u59cb\u8c03\u8bd5\u89c6\u56fe"}</p>
                    <details className="assistant-chat-debug-details">
                      <summary>{"\u67e5\u770b planner / final output / debugger"}</summary>
                      <JsonViewer value={traceQuery.data?.planner || selectedTurn.agent_run.planner || {}} title="planner" />
                      <JsonViewer value={traceQuery.data?.final_output || selectedTurn.agent_run.final_output || {}} title="final output" />
                      <JsonViewer value={traceQuery.data?.runtime_debugger || {}} title="runtime debugger" />
                    </details>
                  </div>
                </div>
              </section>
            ) : null}

            <section className="assistant-chat-composer">
              <div className="assistant-chat-composer-topline">
                <div className="assistant-chat-composer-mode-summary">
                  <span className="assistant-chat-composer-section-label">{"\u56de\u7b54\u65b9\u5f0f"}</span>
                  <strong>{currentModeLabel}</strong>
                </div>
                <button type="button" className="assistant-link-button" onClick={() => setShowModeControls((value) => !value)}>{showModeControls ? "\u6536\u8d77\u9009\u9879" : "\u8c03\u6574"}</button>
              </div>
              {showModeControls ? (
                <div className="assistant-chat-mode-row">
                  {MODE_OPTIONS.map((option) => (
                    <button key={option.value} type="button" className={mode === option.value ? "assistant-mode-pill active" : "assistant-mode-pill"} onClick={() => setMode(option.value)}>{option.label}</button>
                  ))}
                </div>
              ) : null}
              <div className="assistant-chat-composer-box">
                <textarea
                  ref={composerRef}
                  value={inputMessage}
                  placeholder="\u76f4\u63a5\u8f93\u5165\u95ee\u9898\u6216\u4efb\u52a1\uff0c\u4f8b\u5982\uff1a\u5e2e\u6211\u5206\u6790\u8fd9\u4e2a\u9700\u6c42\uff0c\u5e76\u5217\u4e00\u4e2a\u6267\u884c\u8ba1\u5212\u3002"
                  onChange={(event) => setInputMessage(event.target.value)}
                  onKeyDown={(event) => { if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); handleSend(false); } }}
                />
                <div className="assistant-chat-composer-footer">
                  <div className="assistant-chat-composer-hint">
                    {pendingConfirmMessage ? "\u4e0a\u4e00\u6761\u8bf7\u6c42\u9700\u8981\u4f60\u786e\u8ba4\u540e\u518d\u7ee7\u7eed\u3002" : "\u9ed8\u8ba4\u4f1a\u5148\u76f4\u63a5\u56de\u7b54\uff1b\u5982\u679c\u4efb\u52a1\u66f4\u590d\u6742\uff0c\u7cfb\u7edf\u4f1a\u81ea\u52a8\u8f6c\u6210\u5de5\u5177\u8c03\u7528\u6216\u6301\u7eed\u6267\u884c\u3002"}
                  </div>
                  <div className="inline-actions">
                    {pendingConfirmMessage ? <button type="button" className="btn btn-ghost" disabled={sendMutation.isPending || isStreaming} onClick={() => handleSend(true)}>{"\u786e\u8ba4\u5e76\u7ee7\u7eed"}</button> : null}
                    {isStreaming ? (
                      <button type="button" className="btn btn-danger" onClick={handleStopStreaming}>{"\u505c\u6b62\u751f\u6210"}</button>
                    ) : (
                      <button type="button" className="btn btn-primary" disabled={sendMutation.isPending || !inputMessage.trim()} onClick={() => handleSend(false)}>{sendMutation.isPending ? "\u53d1\u9001\u4e2d..." : "\u53d1\u9001"}</button>
                    )}
                  </div>
                </div>
              </div>
            </section>
          </main>
        </div>
      </div>
    </AuthGate>
  );
}
