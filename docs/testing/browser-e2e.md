# Browser E2E (Playwright)

这套用例用于验证用户可见主链路是否真实可运行，不替代后端单元/集成测试。

## 覆盖范围

- 助手控制台可访问与基础 UI 加载
- 普通请求（直接回答）
- 工作流任务创建与状态推进
- 审批/HITL 链路（等待审批 -> 通过）
- 异常输入的用户可见错误反馈
- 任务历史与 trace 可视化（plan/retrieval/tool/steps/output）

## 前置条件

1. 启动依赖与服务：
   - `docker compose up -d --build`
2. 初始化默认账号：
   - `docker compose exec -T api python -m app.seed`
3. 确认前端可访问：
   - [http://localhost:3000](http://localhost:3000)

## 安装浏览器测试依赖

在 `apps/frontend` 下执行：

```bash
npm install
npm run test:e2e:install
```

## 运行方式

在 `apps/frontend` 下执行：

```bash
npm run test:e2e
```

可选参数：

```bash
npm run test:e2e:headed
npm run test:e2e:ui
```

## 可配置环境变量

- `E2E_BASE_URL`（默认 `http://localhost:3000`）
- `E2E_USER_EMAIL`（默认 `user@example.com`）
- `E2E_USER_PASSWORD`（默认 `ChangeMe123!`）
- `E2E_OPERATOR_EMAIL`（默认 `operator@example.com`）
- `E2E_OPERATOR_PASSWORD`（默认 `ChangeMe123!`）

示例：

```bash
E2E_BASE_URL=http://localhost:3000 npm run test:e2e
```

## 用例文件

- `apps/frontend/e2e/assistant-user-visible-flows.spec.ts`
