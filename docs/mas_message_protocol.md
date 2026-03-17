# MAS 统一消息协议（冻结版）

## 1. 协议状态

- 协议名称：`MAS-Message-Protocol`
- 版本：`v1.0`
- 状态：`FROZEN`（冻结）
- 冻结日期：`2026-03-01`
- 适用范围：MAS 内部所有 Agent 间异步消息通信

说明：除非触发正式变更流程，不允许修改本协议的核心字段与语义。

## 2. 核心字段（必须）

以下字段为必备通信字段，所有 Agent 必须遵守：

1. `task_id`
- 含义：任务唯一标识。
- 类型：`string`
- 约束：同一业务任务在全链路保持不变。

2. `run_id`
- 含义：任务执行实例唯一标识（同一 `task_id` 可有多个 `run_id`）。
- 类型：`string`
- 约束：每次 rerun/重跑应使用新的 `run_id`。

3. `correlation_id`
- 含义：跨消息链路追踪 ID，用于串联同一事务/同一会话相关消息。
- 类型：`string`
- 约束：同一处理链路内保持稳定。

4. `topic`
- 含义：消息主题，用于区分消息类型。
- 类型：`string`
- 约束：采用点分命名（例如 `approval.granted`、`execution.failed`）。

5. `payload`
- 含义：业务数据载荷（任务上下文、执行结果、错误信息等）。
- 类型：`object`
- 约束：必须是 JSON 对象，不允许非对象类型。

6. `timestamp`
- 含义：消息生成时间戳。
- 类型：`number`
- 约束：Unix Epoch 秒（浮点），由消息发送端生成。

## 3. 扩展字段（允许）

为支持可观测与路由，可使用扩展字段：

- `message_id`：消息唯一 ID（建议 UUID）
- `sender`：发送方 Agent 标识
- `receiver`：接收方 Agent 标识
- `priority`：消息优先级

扩展字段不得改变核心字段语义。

## 4. 标准消息结构

```json
{
  "message_id": "2b8f3e32-66a8-4c34-bfb8-b8a2dd8d89a8",
  "sender": "scheduler_agent",
  "receiver": "execution_agent",
  "task_id": "t-123",
  "run_id": "r-7",
  "correlation_id": "corr-20260301-001",
  "topic": "execution.requested",
  "payload": {
    "step": "run_task",
    "attempt": 1
  },
  "timestamp": 1772332800.123,
  "priority": 8
}
```

## 5. topic 规范

- 推荐格式：`<domain>.<action>`
- 示例：
  - `approval.granted`
  - `approval.denied`
  - `execution.requested`
  - `execution.succeeded`
  - `execution.failed`
  - `quality.pass`
  - `quality.revise`

## 6. 一致性与幂等要求

- 消费端应优先使用 `message_id` 做去重（如有）。
- 跨消息追踪必须依赖 `correlation_id`。
- 任务实例级别追踪必须依赖 `task_id + run_id`。

## 7. 兼容性策略

- 兼容历史消息：若消息仅包含旧字段 `created_at`，消费端可将其映射为 `timestamp`。
- 新发送端必须生成 `timestamp` 字段。
- `run_id` 不得省略；若发送方无法显式提供，需从上下文推断并填充。

## 8. 合规校验（团队执行）

消息生产端必须满足：

1. 发送时包含 6 个核心字段。
2. `payload` 为对象类型。
3. `topic` 非空且符合命名规范。

消息消费端必须满足：

1. 对缺失核心字段的消息记录错误并拒收。
2. 保留 `correlation_id` 日志链路。
3. 记录 `task_id`、`run_id`、`topic`、`timestamp` 用于审计。

## 9. 项目内落地位置

- 协议模型实现：`apps/worker/mas/messaging.py`
- 协议测试：`apps/worker/tests/test_messaging_protocol.py`
- 本文档：`docs/mas_message_protocol.md`
