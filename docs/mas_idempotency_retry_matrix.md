# MAS 幂等与重试错误矩阵（冻结版）

## 1. 文档状态

- 名称：`MAS-Idempotency-Retry-Matrix`
- 版本：`v1.0`
- 状态：`FROZEN`（冻结）
- 冻结日期：`2026-03-01`
- 适用范围：任务创建、审批、执行、状态回写、重跑、取消及 MAS 代理执行链路

## 2. 统一规则

1. 每个操作必须定义唯一幂等键。
2. 只对“可恢复的瞬时错误”做自动重试。
3. 重试必须有上限，超过上限后进入接管机制（告警/回退/人工介入）。
4. 所有重试结果必须可追踪（`task_id`、`run_id`、`correlation_id`、错误码）。

## 3. 幂等键设计（标准）

| 操作 | 幂等键（标准） | 说明 | 现状 |
|---|---|---|---|
| 任务创建 `POST /tasks` | `tenant_id + client_request_id` | 同一客户端请求重复提交返回同一任务 | 已实现（`tasks` 唯一约束） |
| Worker 状态回写 `/internal/tasks/{id}/status` | `tenant_id + run_id + status_event_id` | 防止重复状态写入和重复计费 | 已实现（`steps` 冲突忽略） |
| 工具调用 `/internal/tool-gateway/execute` | `tool_call_id` | 同一调用只执行一次，重复请求走回放 | 已实现（`tool_calls` 冲突回放） |
| 审批决策（approve/reject/edit） | `tenant_id + approval_id + status_text + edited_output_hash` | 相同决策重复提交返回幂等成功 | 已实现（事务内 `idempotent` 判定） |
| 审批信号分发（outbox） | `tenant_id + approval_id` | 同一审批只维护一条 outbox 记录 | 已实现（outbox upsert） |
| 任务重跑 `POST /tasks/{id}/rerun` | `task_id + run_no` | 每次重跑创建新 run，run_no 唯一 | 已实现（唯一约束+冲突重试） |
| 任务取消 `POST /tasks/{id}/cancel` | `task_id + latest_run_id + action=cancel` | 建议显式幂等键，重复取消返回同终态 | 部分实现（终态保护，建议补请求键） |
| MAS 执行代理单次执行 | `task_id + run_id + attempt + step` | 保障代理内部动作可追踪 | 设计标准（部分由事件字段承载） |

## 4. 错误矩阵（重试规则）

| 操作 | 错误类型/错误码 | 是否重试 | 最大重试 | 策略 | 超限接管机制 |
|---|---|---|---|---|---|
| 任务创建 `create_task` | DB 唯一冲突 `23505`（同 `client_request_id`） | 不做重试（幂等返回） | 0 | 直接查回已存在任务 | 返回幂等结果（`idempotent=true`） |
| 任务创建 `create_task` | `start_workflow` 启动失败 | 不在 API 内重试 | 0 | 立即失败 | 标记 `FAILED_RETRYABLE(workflow_start_failed)`，由调度/人工 rerun 接管 |
| Worker 状态回写 | 重复 `status_event_id` | 不重试（幂等成功） | 0 | 冲突忽略 | 返回 `idempotent=true` |
| Worker 状态回写 | 非法状态迁移 | 不重试 | 0 | 返回 409 | 人工排查状态机或修复事件顺序 |
| 工具调用 | `adapter_http_408`/`429`/`5xx`/`timeout` | 是 | 3（由 Workflow Activity 重试） | 指数退避（1s 起，最大 10s） | 达上限后任务标记 `FAILED_RETRYABLE`，交由 rerun/调度接管 |
| 工具调用 | `adapter_http_4xx`、`schema_invalid`、`policy_deny`、`approval_not_approved` | 否 | 0 | 立即失败 | 标记 `FAILED_FINAL` 或拒绝结果，人工/审批链接管 |
| 工具调用 | 重复 `tool_call_id` | 不重试（幂等回放） | 0 | 读取已执行记录 | 返回历史结果（`idempotent_hit=true`） |
| 审批决策写入 | 审批已处理（非 `WAITING_HUMAN`） | 否 | 0 | 返回 409 或幂等 | 人工核对审批状态 |
| 审批信号 dispatch | Temporal 信号发送失败（网络/服务不可达） | 是 | 6（默认） | 指数退避（2s 起，最大 30s） | outbox 置 `FAILED`，触发告警与人工重放 |
| Workflow 活动（validate/plan/execute/review） | 瞬时异常（连接超时/可重试 ApplicationError） | 是 | 3 | Temporal RetryPolicy 指数退避 | 返回 `FAILED_RETRYABLE`，调度或人工 rerun |
| Workflow 活动 | 非重试异常（`non_retryable=True`） | 否 | 0 | 立即终止该路径 | 返回 `FAILED_FINAL` |
| 任务重跑 `rerun_task` | `run_no` 并发冲突（唯一键冲突） | 是 | 5（本地循环） | 即时重取 `max_run_no` 后重试插入 | 超限返回 409，运维/调用方重试请求 |
| 任务重跑 `rerun_task` | 上一 run 未终态（冲突模式） | 否 | 0 | 返回 409 | 等待当前 run 结束后再发起 |
| 任务取消 `cancel_task` | Temporal cancel 失败（502） | 否（当前） | 0 | 立即返回失败 | 记录审计日志，人工重试取消 |
| MAS 执行代理 `TaskExecutionAgent` | `network`/`timeout`/`service_unavailable` | 是 | 3（默认） | 指数退避（`RecoveryPolicy`） | 达上限后发 `execution.failed` 给审批/调度接管 |
| MAS 执行代理 `TaskExecutionAgent` | `validation`/`permission` | 否 | 0 | 请求协作/人工 | 发送 `execution.assistance_requested`，审批链接管 |

## 5. 超限接管标准（统一）

当操作超过最大重试次数，必须执行以下至少一项接管动作：

1. 状态回退或失败落账：
- 任务进入 `FAILED_RETRYABLE` 或 `FAILED_FINAL`，并记录 `error_code`。

2. 事件/消息升级：
- 发送失败事件（如 `execution.failed`）到审批/调度。

3. 告警与可观测：
- 记录 warning/error 日志。
- 对 outbox `FAILED`、连续 `FAILED_RETRYABLE`、高重试率触发告警规则。

4. 人工接管：
- operator 通过审批、重跑、修复配置后重新触发。

## 6. 默认重试参数（基线）

| 场景 | 参数 |
|---|---|
| Workflow Activity 重试 | `maximum_attempts=3`, `initial_interval=1s`, `max_interval=10s` |
| 审批信号 outbox | `max_attempts=6`, `base_delay=2s`, `max_delay=30s` |
| MAS 执行代理恢复策略 | `max_attempts=3`, `base_delay=1s`, `max_delay=20s`（策略层） |
| rerun run_no 冲突重试 | 最多 5 次 |

## 7. 执行与审计要求

1. 所有操作日志必须至少包含：`task_id`、`run_id`、`correlation_id`、`operation`、`error_code`、`attempt`。
2. 指标至少覆盖：成功率、重试率、最终失败率、outbox 失败积压。
3. 新增操作前必须先补充本矩阵对应行，否则不得上线。

## 8. 代码映射（当前实现依据）

- 任务创建/重跑/取消：`apps/api/app/services/task_service.py`
- 状态幂等：`apps/api/app/services/internal_service.py`
- 审批与 outbox：`apps/api/app/services/approval_service.py`
- 数据约束：`apps/api/app/repositories.py`, `infra/postgres/init.sql`
- 工具调用与幂等回放：`apps/api/app/tool_gateway.py`
- Activity/Workflow 重试：`apps/worker/activities.py`, `apps/worker/workflows.py`
- MAS 执行恢复策略：`apps/worker/mas/adaptive.py`, `apps/worker/mas/agents.py`
