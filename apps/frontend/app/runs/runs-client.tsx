"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useMemo } from "react";

import { AuthGate } from "../../components/auth-gate";
import { EmptyState } from "../../components/empty-state";
import { ErrorBanner } from "../../components/error-banner";
import { SectionCard } from "../../components/section-card";
import { PageSkeleton } from "../../components/skeleton";
import { StatusBadge } from "../../components/status-badge";
import { useAuth } from "../../lib/auth-context";
import { listRunSummaries } from "../../lib/api";
import { formatCurrency, formatDateTime, formatRelativeTime, truncate } from "../../lib/format";
import { resolveRunsTimeWindowRange, sanitizeRunsTimeWindow, type RunsTimeWindow } from "../../lib/runs-filters";

const STATUS_OPTIONS = [
  "",
  "QUEUED",
  "VALIDATING",
  "PLANNING",
  "RUNNING",
  "WAITING_TOOL",
  "WAITING_HUMAN",
  "REVIEWING",
  "SUCCEEDED",
  "FAILED_RETRYABLE",
  "FAILED_FINAL",
  "CANCELLED",
  "TIMED_OUT"
];

const TASK_TYPE_OPTIONS = ["", "rag_qa", "tool_flow", "ticket_email", "research_summary"];
const RISK_OPTIONS = ["all", "low", "medium", "high", "unknown"];
const WINDOW_OPTIONS: Array<{ value: RunsTimeWindow; label: string }> = [
  { value: "7d", label: "最近 7 天（默认）" },
  { value: "30d", label: "最近 30 天" },
  { value: "all", label: "全部范围" }
];

function useFilterState() {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  const value = useMemo(
    () => ({
      status: params.get("status") || "",
      taskType: params.get("taskType") || "",
      risk: params.get("risk") || "all",
      window: sanitizeRunsTimeWindow(params.get("window")),
      page: Math.max(1, Number(params.get("page") || "1"))
    }),
    [params]
  );

  const update = (patch: Partial<typeof value>) => {
    const next = new URLSearchParams(params.toString());
    const merged = { ...value, ...patch };
    if (merged.status) next.set("status", merged.status);
    else next.delete("status");
    if (merged.taskType) next.set("taskType", merged.taskType);
    else next.delete("taskType");
    if (merged.risk && merged.risk !== "all") next.set("risk", merged.risk);
    else next.delete("risk");
    if (merged.window !== "7d") next.set("window", merged.window);
    else next.delete("window");
    if (merged.page > 1) next.set("page", String(merged.page));
    else next.delete("page");
    router.replace(`${pathname}?${next.toString()}`);
  };

  return { value, update };
}

export function RunsPageClient() {
  const auth = useAuth();
  const { value, update } = useFilterState();
  const canQuery = auth.ready && auth.isAuthenticated;

  const query = useQuery({
    queryKey: ["runs", value],
    enabled: canQuery,
    queryFn: () => {
      const timeRange = resolveRunsTimeWindowRange(value.window);
      return listRunSummaries({
        token: auth.accessToken,
        status: value.status || undefined,
        taskType: value.taskType || undefined,
        risk: value.risk || "all",
        fromTs: timeRange.fromTs,
        toTs: timeRange.toTs,
        page: value.page,
        pageSize: 20
      });
    }
  });

  return (
    <AuthGate>
      <div className="stack-gap-md">
        <SectionCard
          title="闭环运行总览"
          subtitle="面向多智能体协作闭环的运行入口，支持阶段、轮次、预算和判定结果筛选。"
          actions={
            <Link href="/playground" className="btn btn-primary">
              新建运行
            </Link>
          }
        >
          <div className="grid cols-4">
            <select
              value={value.status}
              onChange={(event) => update({ status: event.target.value, page: 1 })}
              aria-label="status filter"
            >
              {STATUS_OPTIONS.map((item) => (
                <option key={item || "all"} value={item}>
                  {item || "全部状态"}
                </option>
              ))}
            </select>

            <select
              value={value.taskType}
              onChange={(event) => update({ taskType: event.target.value, page: 1 })}
              aria-label="task type filter"
            >
              {TASK_TYPE_OPTIONS.map((item) => (
                <option key={item || "all"} value={item}>
                  {item || "全部任务类型"}
                </option>
              ))}
            </select>

            <select
              value={value.risk}
              onChange={(event) => update({ risk: event.target.value, page: 1 })}
              aria-label="risk filter"
            >
              {RISK_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  风险：{item}
                </option>
              ))}
            </select>

            <select
              value={value.window}
              onChange={(event) => update({ window: sanitizeRunsTimeWindow(event.target.value), page: 1 })}
              aria-label="time window filter"
            >
              {WINDOW_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>

            <button type="button" className="btn btn-ghost" onClick={() => query.refetch()}>
              刷新
            </button>
          </div>
          <div className="inline-actions" style={{ marginTop: 12 }}>
            <span className="muted-text">默认仅拉取近期运行；扩大时间范围会显著增加加载时间。</span>
            {value.window !== "all" ? (
              <button type="button" className="btn btn-ghost" onClick={() => update({ window: "all", page: 1 })}>
                一键扩大范围
              </button>
            ) : (
              <button type="button" className="btn btn-ghost" onClick={() => update({ window: "7d", page: 1 })}>
                恢复默认范围
              </button>
            )}
          </div>
        </SectionCard>

        {query.isLoading ? <PageSkeleton /> : null}
        {query.error ? <ErrorBanner error={query.error} onRetry={() => query.refetch()} /> : null}

        {query.data && query.data.items.length === 0 ? (
          <EmptyState title="没有匹配的运行" description="调整筛选条件或前往 Playground 创建新运行。" />
        ) : null}

        {query.data ? (
          <SectionCard
            title={`运行列表（共 ${query.data.total} 条）`}
            actions={
              <div className="inline-actions">
                <button
                  type="button"
                  className="btn btn-ghost"
                  disabled={value.page <= 1}
                  onClick={() => update({ page: value.page - 1 })}
                >
                  上一页
                </button>
                <span className="muted-text">
                  {value.page} / {query.data.totalPages}
                </span>
                <button
                  type="button"
                  className="btn btn-ghost"
                  disabled={value.page >= query.data.totalPages}
                  onClick={() => update({ page: value.page + 1 })}
                >
                  下一页
                </button>
              </div>
            }
          >
            <div style={{ overflowX: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>运行 ID</th>
                    <th>任务类型</th>
                    <th>状态</th>
                    <th>阶段</th>
                    <th>轮次</th>
                    <th>重试</th>
                    <th>判定</th>
                    <th>风险</th>
                    <th>成本</th>
                    <th>创建时间</th>
                    <th>结束时间</th>
                    <th>追踪</th>
                  </tr>
                </thead>
                <tbody>
                  {query.data.items.map((item) => (
                    <tr key={item.runId}>
                      <td>
                        <Link href={`/runs/${item.runId}`} className="mono" title={item.runId}>
                          {truncate(item.runId, 12)}
                        </Link>
                      </td>
                      <td>{item.taskType}</td>
                      <td>
                        <StatusBadge status={item.status} />
                      </td>
                      <td>{item.phase || "-"}</td>
                      <td>{item.turn ?? "-"}</td>
                      <td>{item.retryRemaining ?? "-"}</td>
                      <td>{item.verdict || "-"}</td>
                      <td>{item.riskLevel || "unknown"}</td>
                      <td>{formatCurrency(item.costUsd)}</td>
                      <td title={formatDateTime(item.createdAt)}>{formatRelativeTime(item.createdAt)}</td>
                      <td title={formatDateTime(item.endedAt || undefined)}>
                        {item.endedAt ? formatRelativeTime(item.endedAt) : "-"}
                      </td>
                      <td className="mono" title={item.traceId}>
                        {truncate(item.traceId || "-", 14)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionCard>
        ) : null}
      </div>
    </AuthGate>
  );
}
