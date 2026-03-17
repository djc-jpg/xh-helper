"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { AuthGate } from "../../components/auth-gate";
import { EmptyState } from "../../components/empty-state";
import { ErrorBanner } from "../../components/error-banner";
import { JsonViewer } from "../../components/json-viewer";
import { SectionCard } from "../../components/section-card";
import { StatusBadge } from "../../components/status-badge";
import { useAuth } from "../../lib/auth-context";
import { approvalAction, listApprovals } from "../../lib/api";
import { formatDateTime, truncate } from "../../lib/format";
import { useToast } from "../../lib/toast-context";

export default function ApprovalsPage() {
  const auth = useAuth();
  const queryClient = useQueryClient();
  const { pushToast } = useToast();
  const canQuery = auth.ready && auth.isAuthenticated;

  const approvalsQuery = useQuery({
    queryKey: ["approvals", "WAITING_HUMAN"],
    enabled: canQuery,
    queryFn: () => listApprovals(auth.accessToken, "WAITING_HUMAN")
  });

  const mutation = useMutation({
    mutationFn: async (input: {
      approvalId: string;
      action: "approve" | "reject" | "edit";
      body: Record<string, unknown>;
    }) => {
      return approvalAction(auth.accessToken, input.approvalId, input.action, input.body);
    },
    onSuccess() {
      pushToast({ kind: "success", title: "审批操作已提交" });
      queryClient.invalidateQueries({ queryKey: ["approvals", "WAITING_HUMAN"] }).catch(() => undefined);
    },
    onError(error) {
      pushToast({ kind: "error", title: "审批失败", description: error instanceof Error ? error.message : "unknown" });
    }
  });

  return (
    <AuthGate>
      <div className="stack-gap-md" data-testid="approvals-page">
        <SectionCard
          title="审批中心"
          subtitle="展示 ApprovalAgent 产生的待审批事项，支持 approve/reject/edit。"
          actions={
            <button type="button" className="btn btn-ghost" data-testid="approvals-refresh" onClick={() => approvalsQuery.refetch()}>
              刷新
            </button>
          }
        >
          <p className="muted-text">当前筛选：WAITING_HUMAN</p>
        </SectionCard>

        {approvalsQuery.error ? (
          <ErrorBanner error={approvalsQuery.error} onRetry={() => approvalsQuery.refetch()} />
        ) : null}

        {approvalsQuery.data && approvalsQuery.data.length === 0 ? (
          <EmptyState title="暂无待审批项" description="系统当前没有进入人工审批节点的运行。" />
        ) : null}

        {approvalsQuery.data ? (
          <SectionCard title={`待审批列表（${approvalsQuery.data.length}）`} subtitle="每个审批项都可查看原始 JSON。">
            <div style={{ overflowX: "auto" }} data-testid="approvals-table">
              <table>
                <thead>
                  <tr>
                    <th>Approval</th>
                    <th>Task</th>
                    <th>Run</th>
                    <th>状态</th>
                    <th>创建时间</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {approvalsQuery.data.map((row) => {
                    const approvalId = String(row.id || "");
                    return (
                      <tr key={approvalId} data-testid={`approval-row-${approvalId.replace(/[^a-zA-Z0-9_-]/g, "_")}`}>
                        <td className="mono">{truncate(approvalId, 12)}</td>
                        <td className="mono">{truncate(String(row.task_id || ""), 12)}</td>
                        <td className="mono">{truncate(String(row.run_id || ""), 12)}</td>
                        <td>
                          <StatusBadge status={String(row.status || "") as never} />
                        </td>
                        <td>{formatDateTime(String(row.created_at || ""))}</td>
                        <td>
                          <div className="inline-actions">
                            <button
                              type="button"
                              className="btn btn-primary"
                              data-testid={`approval-approve-${approvalId.replace(/[^a-zA-Z0-9_-]/g, "_")}`}
                              onClick={() =>
                                mutation.mutate({
                                  approvalId,
                                  action: "approve",
                                  body: { reason: "approved in console" }
                                })
                              }
                            >
                              通过
                            </button>
                            <button
                              type="button"
                              className="btn btn-danger"
                              data-testid={`approval-reject-${approvalId.replace(/[^a-zA-Z0-9_-]/g, "_")}`}
                              onClick={() =>
                                mutation.mutate({
                                  approvalId,
                                  action: "reject",
                                  body: { reason: "rejected in console" }
                                })
                              }
                            >
                              拒绝
                            </button>
                            <button
                              type="button"
                              className="btn btn-ghost"
                              onClick={() => {
                                const edited = window.prompt("请输入编辑后的输出", String(row.detail_masked || ""));
                                if (!edited) return;
                                mutation.mutate({
                                  approvalId,
                                  action: "edit",
                                  body: { edited_output: edited, reason: "edited in console" }
                                });
                              }}
                            >
                              编辑并通过
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </SectionCard>
        ) : null}

        {approvalsQuery.data?.slice(0, 1).map((row) => (
          <SectionCard key={String(row.id)} title="示例审批原始数据" subtitle="便于联调 Approval payload 字段。">
            <JsonViewer value={row} defaultExpanded title="approval JSON" />
          </SectionCard>
        ))}
      </div>
    </AuthGate>
  );
}
