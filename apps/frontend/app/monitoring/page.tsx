"use client";

import { useQuery } from "@tanstack/react-query";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { AuthGate } from "../../components/auth-gate";
import { EmptyState } from "../../components/empty-state";
import { ErrorBanner } from "../../components/error-banner";
import { JsonViewer } from "../../components/json-viewer";
import { SectionCard } from "../../components/section-card";
import { useAuth } from "../../lib/auth-context";
import { getCostMetrics, getSummaryMetrics, listRunSummaries } from "../../lib/api";
import { formatCurrency, formatNumber } from "../../lib/format";

export default function MonitoringPage() {
  const auth = useAuth();
  const canQuery = auth.ready && auth.isAuthenticated;

  const summaryQuery = useQuery({
    queryKey: ["metrics", "summary"],
    enabled: canQuery,
    queryFn: () => getSummaryMetrics(auth.accessToken)
  });

  const costQuery = useQuery({
    queryKey: ["metrics", "cost"],
    enabled: canQuery,
    queryFn: () => getCostMetrics(auth.accessToken)
  });

  const runsQuery = useQuery({
    queryKey: ["runs", "monitoring-snapshot"],
    enabled: canQuery,
    queryFn: () =>
      listRunSummaries({
        token: auth.accessToken,
        page: 1,
        pageSize: 20,
        risk: "all"
      })
  });

  const summary = summaryQuery.data || {};
  const costs = (costQuery.data || []).map((row) => ({
    day: String(row.day || "").slice(0, 10),
    amount: Number(row.amount || 0)
  }));

  const checkpointDegraded = runsQuery.data?.items.some((item) => item.mode?.includes("degraded"));

  return (
    <AuthGate>
      <div className="stack-gap-md">
        <SectionCard
          title="运行观测"
          subtitle="关键指标、成本趋势、降级信号与原始 metrics 透视。"
          actions={
            <button
              type="button"
              className="btn btn-ghost"
              onClick={() => {
                summaryQuery.refetch().catch(() => undefined);
                costQuery.refetch().catch(() => undefined);
                runsQuery.refetch().catch(() => undefined);
              }}
            >
              刷新
            </button>
          }
        >
          <div className="grid cols-4">
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">pass_rate</p>
              <p className="metric-value">{formatNumber(Number(summary.success_rate || 0) * 100, 1)}%</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">failure_rate</p>
              <p className="metric-value">{formatNumber(Number(summary.failure_rate || 0) * 100, 1)}%</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">retry_rate</p>
              <p className="metric-value">{formatNumber(Number(summary.retry_rate || 0), 3)}</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">p95_latency_seconds</p>
              <p className="metric-value">{formatNumber(Number(summary.p95_latency_seconds || 0), 3)}</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">checkpoint_degraded</p>
              <p className="metric-value">{checkpointDegraded === undefined ? "N/A" : String(checkpointDegraded)}</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">protocol_error_total</p>
              <p className="metric-value">N/A</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">token_in</p>
              <p className="metric-value">{formatNumber(Number(summary.token_in || 0), 0)}</p>
            </div>
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">token_out</p>
              <p className="metric-value">{formatNumber(Number(summary.token_out || 0), 0)}</p>
            </div>
          </div>
        </SectionCard>

        {summaryQuery.error ? (
          <ErrorBanner error={summaryQuery.error} onRetry={() => summaryQuery.refetch()} />
        ) : null}
        {costQuery.error ? <ErrorBanner error={costQuery.error} onRetry={() => costQuery.refetch()} /> : null}

        <SectionCard title="成本趋势" subtitle="近 24h cost_ledger 按日期聚合（后端 /metrics/cost）。">
          {costs.length === 0 ? (
            <EmptyState title="暂无成本数据" />
          ) : (
            <div style={{ width: "100%", height: 280 }}>
              <ResponsiveContainer>
                <LineChart data={costs}>
                  <XAxis dataKey="day" stroke="var(--text-muted)" />
                  <YAxis stroke="var(--text-muted)" />
                  <Tooltip formatter={(value) => formatCurrency(Number(value))} />
                  <Line type="monotone" dataKey="amount" stroke="var(--primary)" strokeWidth={2.4} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </SectionCard>

        <SectionCard title="原始数据" subtitle="用于排查数据源与字段映射。">
          <div className="grid cols-2">
            <JsonViewer value={summary} defaultExpanded title="summary" />
            <JsonViewer value={costQuery.data || []} title="cost rows" />
          </div>
        </SectionCard>
      </div>
    </AuthGate>
  );
}
