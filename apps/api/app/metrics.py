from __future__ import annotations

from prometheus_client import Counter, Gauge

task_total = Counter("task_total", "Total submitted tasks")
task_success_total = Counter("task_success_total", "Total succeeded tasks")
task_failure_total = Counter("task_failure_total", "Total failed tasks")
workflow_retries_total = Counter("workflow_retries_total", "Workflow retry count")
tool_denied_total = Counter("tool_denied_total", "Denied tool calls")
tool_denied_reason_total = Counter(
    "tool_denied_reason_total",
    "Denied tool calls by reason code and category",
    ["reason_code", "category"],
)
internal_status_ignored_total = Counter("internal_status_ignored_total", "Ignored internal task status events")
internal_status_rejected_total = Counter(
    "internal_status_rejected_total",
    "Rejected internal status updates by reason",
    ["reason"],
)

task_cost_usd = Gauge("task_cost_usd", "Task cost in USD", ["task_id"])
task_budget_usd = Gauge("task_budget_usd", "Task budget in USD", ["task_id"])
