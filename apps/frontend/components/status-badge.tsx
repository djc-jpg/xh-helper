import { TaskStatus } from "../lib/mas-types";

const STATUS_LABEL_MAP: Record<string, string> = {
  RECEIVED: "已接收",
  QUEUED: "排队中",
  VALIDATING: "校验中",
  PLANNING: "规划中",
  RUNNING: "执行中",
  WAITING_TOOL: "等待工具",
  WAITING_HUMAN: "等待人工",
  REVIEWING: "评测中",
  SUCCEEDED: "成功",
  FAILED_RETRYABLE: "可重试失败",
  FAILED_FINAL: "最终失败",
  CANCELLED: "已取消",
  TIMED_OUT: "已超时"
};

const STATUS_CLASS_MAP: Record<string, string> = {
  RECEIVED: "status-neutral",
  QUEUED: "status-neutral",
  VALIDATING: "status-info",
  PLANNING: "status-info",
  RUNNING: "status-running",
  WAITING_TOOL: "status-warning",
  WAITING_HUMAN: "status-warning",
  REVIEWING: "status-info",
  SUCCEEDED: "status-success",
  FAILED_RETRYABLE: "status-danger",
  FAILED_FINAL: "status-danger",
  CANCELLED: "status-neutral",
  TIMED_OUT: "status-danger"
};

export function statusLabel(status: string): string {
  return STATUS_LABEL_MAP[status] || status;
}

export function StatusBadge({ status }: { status: TaskStatus | string }) {
  const cls = STATUS_CLASS_MAP[status] || "status-neutral";
  return (
    <span className={`status-badge ${cls}`} data-status={String(status)}>
      {statusLabel(status)}
    </span>
  );
}
