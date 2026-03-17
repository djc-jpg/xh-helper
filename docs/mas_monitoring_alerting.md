# MAS 监控与告警规范（Prometheus + Grafana）

## 1. 文档状态
- 名称：`MAS-Monitoring-Alerting-Standard`
- 版本：`v1.0`
- 状态：`FROZEN`
- 冻结日期：`2026-03-01`
- 适用范围：MAS 四层架构（感知决策、知识支撑、执行、评测）及任务链路。

## 2. 监控指标与阈值

| 指标 | PromQL 口径（摘要） | 阈值 | 告警级别 |
|---|---|---|---|
| Agent 成功率 | `1 - failed/total`（按 `agent_id`） | `<95%` 持续 10m（且 15m 样本量 >= 20） | `critical` |
| 失败类型 | `increase(mas_agent_retries_total[10m])`（按 `agent_id`,`failure_type`） | `>=10` 次/10m | `warning` |
| 队列堆积 | `mas_message_queue_backlog`（按 `receiver`,`backend`） | `>100` 持续 10m；`>250` 持续 5m | `warning` / `critical` |
| 限流命中率 | `THROTTLE/decisions`（`execution_agent`） | `>10%` 持续 10m；`>30%` 持续 5m | `warning` / `critical` |
| 重试率 | `retries/execution_total`（`execution_agent`） | `>5%` 持续 10m | `warning` |
| 重试风暴 | `increase(mas_agent_retries_total[15m])` | `>=50` 次/15m | `critical` |

## 3. 告警规则（已落地）

Prometheus 规则文件：`infra/prometheus/alerts.yml`

新增规则：
- `MasAgentSuccessRateLow`
- `MasAgentFailureTypeSpike`
- `MasQueueBacklogHigh`
- `MasQueueBacklogCritical`
- `MasRateLimitHitRateHigh`
- `MasRateLimitHitRateCritical`
- `MasRetryRateHigh`
- `MasRetryBurstCritical`

## 4. 仪表盘（已落地）

Grafana 仪表盘：`infra/grafana/dashboards/platform-overview.json`

新增面板：
- `MAS Agent Success Rate`
- `MAS Queue Backlog`
- `MAS Failure Types (Retries)`
- `Execution Agent Rate-Limit Hit Ratio`
- `MAS Retry Ratio`
- `MAS Agent Inflight`
- `MAS Step p95 Latency`

## 5. 指标采集变更

新增指标：
- `mas_message_queue_backlog{receiver,backend}`（Gauge）

采集点：
- 文件：`apps/worker/mas/messaging.py`
- 内存队列：发送/消费后以 `qsize()` 更新 backlog。
- Redis 队列：发送/消费后以 `LLEN` 更新 backlog。

## 6. 定位问题最小字段集

告警排障时至少保留以下标签或字段：
- `agent_id`
- `failure_type`
- `receiver`
- `backend`
- `task_id`
- `run_id`
- `correlation_id`

说明：`task_id/run_id/correlation_id` 由日志与消息协议字段提供，用于从聚合告警回溯到单任务。

## 7. 触发验证步骤

1. 启动监控组件与服务：
```bash
docker compose up -d prometheus grafana api worker
```
2. 校验 Prometheus 规则已加载：
```bash
curl -s http://localhost:9090/api/v1/rules
```
3. 校验告警状态接口可见新增规则：
```bash
curl -s http://localhost:9090/api/v1/alerts
```
4. 在 Grafana 查看 `XH Platform Overview`，确认新增 MAS 面板有序列数据。

## 8. 完成标准

满足以下条件视为本项完成：
1. Prometheus 告警规则与 Grafana 面板均已配置并可加载。
2. 成功率、失败类型、队列堆积、限流命中率、重试次数均可观测。
3. 告警包含可定位标签，能够回溯到具体 Agent 和任务链路。
