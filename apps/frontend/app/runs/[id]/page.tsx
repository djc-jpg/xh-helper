"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "next/navigation";
import { useMemo, useState } from "react";

import { AgentTimeline } from "../../../components/agent-timeline";
import { AuthGate } from "../../../components/auth-gate";
import { CriticPanel } from "../../../components/critic-panel";
import { EmptyState } from "../../../components/empty-state";
import { ErrorBanner } from "../../../components/error-banner";
import { EvidencePanel } from "../../../components/evidence-panel";
import { JsonViewer } from "../../../components/json-viewer";
import { MetricsPanel } from "../../../components/metrics-panel";
import { PhaseStepper } from "../../../components/phase-stepper";
import { SectionCard } from "../../../components/section-card";
import { PageSkeleton } from "../../../components/skeleton";
import { StatusBadge } from "../../../components/status-badge";
import { useRunStream } from "../../../hooks/use-run-stream";
import { useAuth } from "../../../lib/auth-context";
import { getRun, getTaskDetail } from "../../../lib/api";
import { formatCurrency, formatDateTime } from "../../../lib/format";
import {
  buildTimeline,
  extractCriticVerdict,
  extractEvidence,
  extractMasEnvelope,
  extractMasState,
  extractMetrics,
  extractOutputPayload,
  inferGraphEngine,
  isFinalState,
  safeStringify
} from "../../../lib/mas-utils";
import { TimelineEvent } from "../../../lib/mas-types";
import { useToast } from "../../../lib/toast-context";

function downloadJson(filename: string, value: unknown): void {
  const blob = new Blob([safeStringify(value)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

export default function RunDetailPage() {
  const params = useParams();
  const runId = String(params.id || "");
  const auth = useAuth();
  const queryClient = useQueryClient();
  const { pushToast } = useToast();
  const [streamEvents, setStreamEvents] = useState<TimelineEvent[]>([]);
  const canQuery = auth.ready && auth.isAuthenticated;

  const runQuery = useQuery({
    queryKey: ["run", runId],
    enabled: canQuery && Boolean(runId),
    queryFn: () => getRun(auth.accessToken, runId)
  });

  const taskId = useMemo(() => String(runQuery.data?.run?.task_id || ""), [runQuery.data]);

  const taskQuery = useQuery({
    queryKey: ["task", taskId],
    enabled: canQuery && Boolean(taskId),
    queryFn: () => getTaskDetail(auth.accessToken, taskId)
  });

  const steps = useMemo(() => {
    const base = runQuery.data?.steps || [];
    if (base.length > 0) return base;
    if (!taskQuery.data) return [];
    return taskQuery.data.steps.filter((step) => String(step.run_id) === runId);
  }, [runId, runQuery.data?.steps, taskQuery.data]);

  const snapshot = useMemo(() => extractMasState(steps), [steps]);
  const envelope = useMemo(() => extractMasEnvelope(steps), [steps]);
  const timeline = useMemo(() => buildTimeline(steps), [steps]);
  const evidence = useMemo(() => extractEvidence(steps), [steps]);
  const critic = useMemo(() => extractCriticVerdict(steps), [steps]);
  const metrics = useMemo(() => extractMetrics(steps), [steps]);
  const outputPayload = useMemo(() => extractOutputPayload(steps), [steps]);
  const graphEngine = useMemo(() => inferGraphEngine(steps), [steps]);

  const runStatus = String(runQuery.data?.run?.status || "");
  const shouldStop = isFinalState(runStatus);

  useRunStream({
    accessToken: auth.accessToken,
    taskId,
    enabled: canQuery && Boolean(taskId),
    shouldStop,
    onStatus(payload) {
      const event: TimelineEvent = {
        id: `stream-status-${Date.now()}`,
        ts: String(payload.updated_at || ""),
        agent: "sse.status",
        type: "status",
        status: String(payload.status || ""),
        title: `任务状态更新 -> ${String(payload.status || "")}`,
        payload
      };
      setStreamEvents((prev) => [event, ...prev].slice(0, 60));
      queryClient.invalidateQueries({ queryKey: ["run", runId] }).catch(() => undefined);
      queryClient.invalidateQueries({ queryKey: ["task", taskId] }).catch(() => undefined);
    },
    onStep(payload) {
      const event: TimelineEvent = {
        id: `stream-step-${Date.now()}`,
        ts: String(payload.created_at || ""),
        agent: String(payload.step_key || "sse.step"),
        type: "step",
        status: String(payload.status || ""),
        title: `步骤事件 -> ${String(payload.step_key || "")}`,
        payload
      };
      setStreamEvents((prev) => [event, ...prev].slice(0, 60));
      queryClient.invalidateQueries({ queryKey: ["run", runId] }).catch(() => undefined);
    },
    onDone() {
      pushToast({ kind: "info", title: "SSE 结束", description: "运行已进入终态。" });
    },
    onError(message) {
      pushToast({ kind: "warning", title: "SSE 异常", description: message });
    },
    onAuthExpired() {
      auth.forceSessionEnded("SSE 鉴权失效，请重新登录。");
    }
  });

  const mergedTimeline = useMemo(() => [...streamEvents, ...timeline], [streamEvents, timeline]);

  return (
    <AuthGate>
      <div className="stack-gap-md">
        {runQuery.isLoading || taskQuery.isLoading ? <PageSkeleton /> : null}
        {runQuery.error ? <ErrorBanner error={runQuery.error} onRetry={() => runQuery.refetch()} /> : null}
        {taskQuery.error ? <ErrorBanner error={taskQuery.error} onRetry={() => taskQuery.refetch()} /> : null}

        {!runQuery.data ? null : (
          <>
            <SectionCard
              title="运行概览"
              subtitle="多智能体闭环状态、预算、判决与 trace 上下文"
              actions={
                <div className="inline-actions">
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={() => downloadJson(`run-${runId}-trajectory.json`, mergedTimeline)}
                  >
                    导出轨迹
                  </button>
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={() =>
                      downloadJson(`run-${runId}-snapshot.json`, {
                        run: runQuery.data?.run,
                        task: taskQuery.data?.task,
                        state: snapshot,
                        critic,
                        metrics
                      })
                    }
                  >
                    导出快照
                  </button>
                </div>
              }
            >
              <div className="grid cols-4">
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">运行状态</p>
                  <StatusBadge status={runStatus} />
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">判定结果</p>
                  <p className="metric-value">{critic.verdict}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">阶段 / 轮次</p>
                  <p className="metric-value">
                    {snapshot.phase || "-"} / {snapshot.turn ?? "-"}
                  </p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">图执行引擎</p>
                  <p className="metric-value">{graphEngine}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">重试预算</p>
                  <p className="metric-value">{snapshot.retry_budget?.remaining ?? "-"}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">时延预算（毫秒）</p>
                  <p className="metric-value">{snapshot.latency_budget?.remaining_ms ?? "-"}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">风险等级</p>
                  <p className="metric-value">{snapshot.risk_level || "unknown"}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">运行成本</p>
                  <p className="metric-value">{formatCurrency(Number(runQuery.data.run?.run_cost_usd || 0))}</p>
                </div>
              </div>

              <div className="grid cols-3">
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">Trace ID</p>
                  <p className="mono">{String(runQuery.data.run?.trace_id || "-")}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">创建时间</p>
                  <p>{formatDateTime(String(runQuery.data.run?.created_at || ""))}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">结束时间</p>
                  <p>{formatDateTime((runQuery.data.run?.ended_at as string | undefined) || undefined)}</p>
                </div>
              </div>

              <div className="sub-panel stack-gap-sm">
                <p className="panel-subtitle">闭环阶段</p>
                <PhaseStepper phase={snapshot.phase} />
              </div>
            </SectionCard>

            <div className="grid cols-2">
              <SectionCard
                title="智能体时间线"
                subtitle="多智能体事件流，支持结构化 JSON 展开和回退重规划高亮。"
                actions={
                  <span className="muted-text">SSE 事件：{streamEvents.length > 0 ? `${streamEvents.length} 条` : "未接入"}</span>
                }
              >
                {mergedTimeline.length > 0 ? (
                  <AgentTimeline events={mergedTimeline} />
                ) : (
                  <EmptyState title="暂无消息流" description="当前运行未暴露 protocol messages，已回退展示步骤级事件。" />
                )}
              </SectionCard>

              <div className="stack-gap-md">
                <SectionCard title="Critic 评测" subtitle="判决、失败类型、修复指令">
                  <CriticPanel verdict={critic} />
                </SectionCard>

                <SectionCard title="证据包" subtitle="支持搜索、展开原始内容，并展示冲突标记。">
                  <EvidencePanel evidence={evidence} />
                </SectionCard>

                <SectionCard title="运行指标" subtitle="查看核心指标与原始 metrics JSON。">
                  <MetricsPanel metrics={metrics} />
                </SectionCard>
              </div>
            </div>

            <SectionCard title="最终输出" subtitle="用户可读输出 / artifacts / 原始结构">
              <JsonViewer value={outputPayload} defaultExpanded title="输出结构" />
              <div className="grid cols-2">
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">运行模式</p>
                  <p>{envelope?.mode || "workflow"}</p>
                </div>
                <div className="sub-panel stack-gap-xs">
                  <p className="panel-subtitle">失败语义</p>
                  <p>{envelope?.failure_semantic || "-"}</p>
                </div>
              </div>
            </SectionCard>
          </>
        )}
      </div>
    </AuthGate>
  );
}
