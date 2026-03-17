# MAS 影子模式（Shadow Mode）运行规范

## 1. 目标

在不影响现网执行结果的前提下，让 MAS “只算不执行”，并将 MAS 预测与现有系统真实结果持续对比，用于评估一致性与稳定性。

## 2. 运行方式

- 主链路：继续由现有工作流执行（不变）。
- 影子链路：在任务进入终态时，触发 `shadow_compare_activity`：
  - 使用同一任务上下文进行 MAS 预测（无外部副作用）。
  - 生成 `predicted_status`。
  - 与真实 `actual_status` 比对，记录 `consistent`。
  - 结果落盘到 `artifacts/shadow_mode/<run_id>.json`。

## 3. 配置项

- `MAS_SHADOW_MODE`：是否启用影子模式（默认 `false`）。

示例：

```env
MAS_SHADOW_MODE=true
```

## 4. 影子对比产物格式

每条 run 会生成一个 JSON，核心字段包括：

- `task_id`
- `run_id`
- `trace_id`
- `task_type`
- `timestamp`
- `actual_status`
- `predicted_status`
- `comparable`（是否纳入一致率计算）
- `consistent`（是否一致）
- `path`（影子决策路径）

## 5. 7 天稳定性检查

运行检查脚本：

```bash
python scripts/check_shadow_mode.py \
  --artifact-dir artifacts/shadow_mode \
  --days 7 \
  --min-consistency 0.95 \
  --min-comparable 50
```

检查逻辑：

1. 最近 7 天必须有连续覆盖（至少 7 个自然日有数据）。
2. 可比样本数达到下限（默认 50）。
3. 一致率达到阈值（默认 95%）。

## 6. 完成标准

满足以下条件视为达成影子模式目标：

1. `MAS_SHADOW_MODE=true` 连续运行 7 天。
2. 检查脚本返回 `PASS`。
3. 对 mismatch 样本完成分类复盘并形成修正闭环。

## 7. 代码落点

- 影子模拟逻辑：`apps/worker/mas/shadow.py`
- 影子对比活动：`apps/worker/activities.py` -> `shadow_compare_activity`
- 工作流接入：`apps/worker/workflows.py`
- Worker 注册：`apps/worker/worker.py`
- 7 天检查脚本：`scripts/check_shadow_mode.py`
