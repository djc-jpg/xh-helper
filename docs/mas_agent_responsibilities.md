# MAS Agent 职责边界（四层结构统一版）

## 1. 目标

本文档将 MAS 统一为四层结构：

1. 感知决策层
2. 知识支撑层
3. 执行层
4. 评测层

目标：每项关键操作只有一个责任 Owner，避免职责重叠，并围绕客户体验优化端到端流程。

## 2. 四层总览

### 2.1 感知决策层

包含 Agent：

- `Perceptor Agent`（意图/情绪/实体感知）
- `Planner Agent`（任务分解与计划生成）
- `Approval Agent`（准入审批）
- `Scheduler Agent`（队列与优先级流转）

层职责：

- 理解用户输入与任务上下文。
- 形成可执行计划与优先级建议。
- 给出准入决策与下一跳分发。

### 2.2 知识支撑层

包含 Agent：

- `Knowledge Resolution Agent`（知识整合与证据组装）

层职责：

- 汇总内部知识库、历史工单、标准流程，形成结构化知识包。
- 对知识充分性给出判定，标记是否存在知识缺口。

### 2.3 执行层

包含 Agent：

- `Execution Agent`（执行编排与失败恢复）
- `Researcher Agent`（网络搜索与外部信息检索）
- `Weather Agent`（实时天气查询）
- `Writer Agent`（依据客户需求生成文本）

层职责：

- 按计划执行任务并调用专用执行 Agent。
- 输出执行结果、检索结果和对客文案草稿。
- 处理执行期失败分类、重试与退避。

### 2.4 评测层

包含 Agent：

- `Critic Agent`（质量评测与发布门禁）

层职责：

- 对执行结果做质量检查（正确性、完整性、语气、合规）。
- 决定结果“通过/退回修正/升级人工”。

## 3. Agent 级职责与边界

### 3.1 Perceptor Agent

负责：

- 解析用户输入，识别意图、情绪、紧急程度、关键实体。
- 输出 `intent_profile` 给 Planner / Scheduler。

不负责：

- 不做计划编排。
- 不做知识整合或外部检索。
- 不直接执行任务。

### 3.2 Planner Agent

负责：

- 将任务拆解为执行步骤与依赖。
- 明确哪些步骤需要知识支撑、审批或人工介入。
- 输出 `execution_plan`。

不负责：

- 不做审批结论。
- 不进行最终执行。
- 不负责质量验收。

### 3.3 Approval Agent

负责：

- 基于预算、时限、风险规则做 `APPROVED/REJECTED` 准入决策。
- 在需要人工确认时维持审批上下文。

不负责：

- 不执行任务内容。
- 不决定全局队列顺序。
- 不做最终质量评分。

### 3.4 Scheduler Agent

负责：

- 队列管理、优先级排序、跨 Agent 分发。
- 根据事件回流推进任务流转（含回队、重试路由、停流）。

不负责：

- 不做业务审批规则判断。
- 不做任务内容执行。
- 不做知识整合与质量评测。

### 3.5 Knowledge Resolution Agent

负责：

- 汇总内部知识（知识库/历史工单/SOP），输出结构化 `knowledge_pack`。
- 提供知识覆盖判断（是否有 `knowledge_gap`）。

不负责：

- 不做实时外网检索执行（由 `Researcher Agent` 负责）。
- 不做天气查询。
- 不做最终执行动作与发布门禁。

### 3.6 Execution Agent

负责：

- 执行编排：按 `execution_plan` 调用 `Researcher/Weather/Writer`。
- 聚合执行阶段产物并输出 `execution_result`。
- 处理执行期失败恢复（重试/退避/升级）。

不负责：

- 不做审批通过/拒绝。
- 不拥有队列调度权。
- 不做最终质量放行。

### 3.7 Researcher Agent

负责：

- 专注网络搜索、外部资料检索与事实抓取。
- 输出 `research_pack`（要点、来源、时间戳、置信度）。

不负责：

- 不生成最终对客文案。
- 不做审批决策和队列调度。
- 不替代评测层质量门禁。

### 3.8 Weather Agent

负责：

- 提供实时天气/短期预报查询能力。
- 输出结构化 `weather_data`（地点、时间、温度、天气现象、降水/风力等）。

不负责：

- 不处理非天气领域知识检索。
- 不生成最终客户回复。
- 不做审批与评测决策。

### 3.9 Writer Agent

负责：

- 依据客户需求与执行上下文生成对客文本草稿。
- 按语气、渠道和格式要求输出 `draft_response`。

不负责：

- 不进行外部事实检索。
- 不执行审批或调度。
- 不替代 Critic 做最终放行。

### 3.10 Critic Agent

负责：

- 对 `execution_result` 与 `draft_response` 进行质量评测。
- 输出 `PASS / REVISE / ESCALATE`。
- 提供可执行反馈，驱动回到 Planner 或 Execution 修正。

不负责：

- 不参与业务执行。
- 不做队列优先级决策。
- 不替代审批策略判断。

## 4. 无重叠 Owner 矩阵

| 操作 | Owner Agent |
|---|---|
| 用户输入语义/情绪解析 | `Perceptor Agent` |
| 任务分解与计划生成 | `Planner Agent` |
| 准入审批（通过/拒绝） | `Approval Agent` |
| 队列排序与下一跳分发 | `Scheduler Agent` |
| 内部知识整合与知识缺口判定 | `Knowledge Resolution Agent` |
| 网络搜索与外部信息检索 | `Researcher Agent` |
| 实时天气查询 | `Weather Agent` |
| 客户文本生成 | `Writer Agent` |
| 执行编排与失败恢复（重试/退避） | `Execution Agent` |
| 结果质量评测与发布门禁 | `Critic Agent` |

## 5. 交接契约（推荐事件）

1. `Perceptor -> Planner`：`cx.intent.completed`（`intent_profile`）
2. `Planner -> Knowledge`：`plan.knowledge.requested`（`execution_plan`）
3. `Knowledge -> Scheduler/Execution`：`knowledge.ready`（`knowledge_pack`）
4. `Scheduler -> Approval`：`approval.requested`
5. `Approval -> Scheduler/Execution`：`approval.granted` / `approval.denied`
6. `Execution -> Researcher`：`research.requested`
7. `Researcher -> Execution`：`research.completed`（`research_pack`）
8. `Execution -> Weather`：`weather.requested`
9. `Weather -> Execution`：`weather.completed`（`weather_data`）
10. `Execution -> Writer`：`writing.requested`
11. `Writer -> Execution`：`writing.completed`（`draft_response`）
12. `Execution -> Critic`：`execution.completed`（`execution_result` + `draft_response`）
13. `Critic -> Scheduler`：`quality.pass` / `quality.revise` / `quality.escalate`

## 6. 与当前项目实现的对应

当前代码已实现（运行中）：

- `ApprovalAgent`（审批）
- `TaskExecutionAgent`（执行）
- `TaskScheduler + MultiAgentCoordinator`（调度角色）

设计已定义、待实现：

- `Perceptor Agent`
- `Planner Agent`
- `Knowledge Resolution Agent`
- `Researcher Agent`
- `Weather Agent`
- `Writer Agent`
- `Critic Agent`

## 7. 完成标准（验收）

满足以下条件，即可认定“四层职责清晰且无重叠”：

1. 四层结构在文档中明确且每层至少一个 Owner Agent。
2. 每个 Agent 都包含“负责/不负责”边界。
3. Owner 矩阵中每个关键操作仅出现一个 Owner。
4. 层间输入输出契约明确（`intent_profile`、`execution_plan`、`knowledge_pack`、`research_pack`、`weather_data`、`draft_response`、`quality verdict`）。
5. 可映射到任务流程：创建、审批、执行、失败、重试、取消。

## 8. 代码参考

- `apps/worker/mas/agents.py`
- `apps/worker/mas/orchestration.py`
- `apps/worker/mas/adaptive.py`
- `docs/task_lifecycle_state.md`
