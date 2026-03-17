# MAS 故障演练 Runbook（v1.0）

## 1. 目标与范围
- 目标：验证 MAS 在关键异常下的预期行为与恢复路径。
- 范围：网络超时、下游 503、Redis 不可用、审批拒绝四类故障。
- 判定：每个场景都必须有“故障注入 -> 预期行为 -> 恢复动作 -> 恢复结果”闭环。

## 2. 演练前置条件
1. 本地环境服务已启动：`api`、`worker`、`fake-internal-service`、`postgres`、`temporal`、`redis`。
2. 使用 operator 账号执行演练（具备审批/重跑权限）。
3. 监控可用：Prometheus/Grafana 已加载规则与看板。

## 3. 场景矩阵

| 场景 | 注入方式 | 预期行为 | 恢复路径 | 成功判定 |
|---|---|---|---|---|
| 网络超时 | `tool_flow` 查询 `q=force_timeout_once` | 首次 run 进入 `FAILED_RETRYABLE`，`tool_calls.reason_code` 包含 `timeout` | 故障清除后 `POST /tasks/{id}/rerun` | rerun 终态为 `SUCCEEDED` |
| 下游 503 | `tool_flow` 查询 `q=force_503_once` | 首次 run 进入 `FAILED_RETRYABLE`，`reason_code=adapter_http_5xx` | 下游恢复后执行 rerun | rerun 终态为 `SUCCEEDED` |
| Redis 不可用 | 停止 Redis 后，用 `mas_message_backend=redis` 启动 MAS runtime 探针 | runtime 自动降级 `InMemoryMessageQueue/InMemoryRateLimiter` | 重启 Redis，验证 `redis-cli ping` | 降级生效且 Redis 恢复可用 |
| 审批拒绝 | `ticket_email` 任务进入 `WAITING_HUMAN` 后执行 reject | 任务终态 `FAILED_FINAL`，步骤含 `approval_rejected` | rerun 后重新审批 `approve` | 任务最终为 `SUCCEEDED` |

## 4. 自动化演练脚本

脚本：`scripts/run_failure_drill.py`

执行：

```bash
python scripts/run_failure_drill.py \
  --base-url http://localhost:18000 \
  --operator-email operator@example.com \
  --operator-password ChangeMe123! \
  --output artifacts/drills/failure_drill_report.json
```

输出：
- 控制台打印 JSON 报告。
- 文件落盘到 `artifacts/drills/failure_drill_report.json`。
- 若任一场景失败，脚本返回码为 `2`。

## 5. 观测与排障建议
- 核对任务状态与步骤：
  - `/tasks/{task_id}` 的 `task.status`、`steps.step_key`、`tool_calls.reason_code`。
- 核对告警与指标：
  - `MasRetryRateHigh`、`MasAgentFailureTypeSpike`、`MasQueueBacklogHigh/Critical`、`MasRateLimitHitRateHigh/Critical`。
- 核对 Redis 恢复：
  - `docker compose exec -T redis redis-cli ping` 返回 `PONG`。

## 6. 完成标准
1. 四个场景全部执行完成并有结果记录。
2. 每个场景都具备明确恢复路径，且恢复动作可执行。
3. 演练报告可审计（含 task_id / status / reason_code / recovery result）。
