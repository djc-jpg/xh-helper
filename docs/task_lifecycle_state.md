# 任务流转状态图（统一版）

## 1. 目的与范围

本文件定义当前系统任务生命周期的统一状态流转版本，覆盖以下必需环节：

- 创建
- 审批
- 执行
- 失败
- 重试
- 取消

适用范围：`/tasks` 主流程（API + Worker + 审批信号）。

## 2. 图表交付物（draw.io）

- draw.io 源文件：`docs/task_lifecycle_state.drawio`
- 建议文档入口：本文件 + 上述 `.drawio` 作为团队统一版本。

使用方式：

1. 打开 https://app.diagrams.net/ 或本地 draw.io。
2. 选择 `File -> Open From -> Device`。
3. 导入 `docs/task_lifecycle_state.drawio`。

## 3. 高层流程（你关注的主链路）

- 创建：`POST /tasks` -> `RECEIVED` -> `QUEUED`
- 审批：`WAITING_HUMAN` -> `APPROVED/EDITED` 或 `REJECTED` 或 `TIMED_OUT`（当前 `ticket_email` 默认需要 HITL）
- 执行：`VALIDATING -> PLANNING -> RUNNING -> WAITING_TOOL -> REVIEWING`
- 失败：`FAILED_RETRYABLE`（可重试）或 `FAILED_FINAL`（不可重试）
- 重试：`POST /tasks/{task_id}/rerun` 触发新 run，状态回到 `QUEUED`
- 取消：`POST /tasks/{task_id}/cancel`，非终态可进入 `CANCELLED`

## 4. 状态定义

- `RECEIVED`：任务已入库，尚未进入执行队列。
- `QUEUED`：run 已创建，等待 worker 执行。
- `VALIDATING`：输入校验中。
- `PLANNING`：规划执行路径/工具计划。
- `RUNNING`：执行阶段。
- `WAITING_TOOL`：等待工具调用/工具准备。
- `WAITING_HUMAN`：等待人工审批。
- `REVIEWING`：结果评审与产物整理。
- `SUCCEEDED`：执行成功终态。
- `FAILED_RETRYABLE`：可重试失败。
- `FAILED_FINAL`：不可重试失败终态。
- `TIMED_OUT`：审批超时终态。
- `CANCELLED`：任务取消终态。

## 5. 全量流转规则（代码事实）

以下为状态机允许迁移（来自 `ALLOWED_TRANSITIONS`）：

- `RECEIVED` -> `QUEUED`, `VALIDATING`, `FAILED_FINAL`
- `QUEUED` -> `VALIDATING`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `VALIDATING` -> `PLANNING`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `PLANNING` -> `RUNNING`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `RUNNING` -> `WAITING_TOOL`, `REVIEWING`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `WAITING_TOOL` -> `WAITING_HUMAN`, `REVIEWING`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `WAITING_HUMAN` -> `REVIEWING`, `FAILED_FINAL`, `TIMED_OUT`, `CANCELLED`
- `REVIEWING` -> `SUCCEEDED`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`
- `FAILED_RETRYABLE` -> `VALIDATING`, `FAILED_FINAL`, `CANCELLED`
- 说明：`POST /tasks/{id}/rerun` 属于 API 侧新建 run 回放，任务状态会先回到 `QUEUED`，随后再进入 `VALIDATING`。
- `FAILED_FINAL` -> `FAILED_FINAL`
- `SUCCEEDED` -> `SUCCEEDED`
- `TIMED_OUT` -> `TIMED_OUT`
- `CANCELLED` -> `CANCELLED`

终态集合（不可再取消）：

- `SUCCEEDED`
- `FAILED_RETRYABLE`（当前 run 终态；可通过 `rerun` 新建下一次 run）
- `FAILED_FINAL`
- `TIMED_OUT`
- `CANCELLED`

## 6. 无遗漏检查清单（完成标准）

- 创建：已覆盖（`POST /tasks` -> `RECEIVED` -> `QUEUED`）
- 审批：已覆盖（`WAITING_HUMAN` + `APPROVED/EDITED/REJECTED` + `TIMED_OUT`）
- 执行：已覆盖（`VALIDATING/PLANNING/RUNNING/WAITING_TOOL/REVIEWING`）
- 失败：已覆盖（`FAILED_RETRYABLE` 与 `FAILED_FINAL`）
- 重试：已覆盖（`POST /tasks/{id}/rerun` -> 新 run -> `QUEUED`）
- 取消：已覆盖（任一非终态 -> `CANCELLED`）

结论：当前版本可作为团队统一状态图基线。

## 7. 事实来源（代码）

- 状态机定义：`apps/api/app/state_machine.py`
- 创建/取消/重试入口：`apps/api/app/services/task_service.py`
- Worker 状态推进：`apps/worker/workflows.py`
- 状态更新校验：`apps/api/app/services/internal_service.py`
- 状态枚举：`infra/postgres/init.sql`
