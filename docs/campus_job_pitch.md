# xh-helper 求职版项目说明

这份文档专门给校招 / 实习场景使用。  
重点不是把项目写得“很大”，而是把已经实现的能力讲清楚、讲可信。

## 一句话版本

我做了一个面向真实任务执行的 Agent Runtime 项目 `xh-helper`。  
它把 goal、action、state、policy、reflection 收敛到同一条运行时主线上，支持 durable workflow、审批等待与恢复、外部事件驱动、策略记忆和 agent-grade eval。

## 简历 2 行版

- 基于 FastAPI、Temporal、Postgres 和 Next.js 搭建 `xh-helper`，实现了面向真实任务执行的 Agent Runtime，而不是普通的聊天助手或静态工作流编排。
- 统一了 `goal / action / state / policy / reflection` 主线，支持 durable workflow、审批等待/恢复、external signal、policy memory、shadow/canary 和 runtime debugger。

## 简历 3 条版

- 独立设计并实现通用智能体运行时项目 `xh-helper`，用 FastAPI + Temporal + Postgres 构建 durable task/workflow 主链，支持 goal/subgoal、wait/resume、approval、external signal 和失败恢复。
- 将 agent 决策收敛为统一的 `goal / action / state / policy / reflection` 语义，落地 policy engine、goal scheduler、policy memory、shadow/canary 和 runtime debugger，避免逻辑散落在 API、worker 和前端。
- 使用 Next.js 重构聊天式工作台和运行调试界面，支持查看 goal、当前动作、why-not、task trace、runtime debugger；同时补充 golden cases 和 eval runner 验证 agent 主链路。

## 面试 60 秒讲法

这个项目最开始是一个助手编排系统，但我后面把它往 Agent Runtime 的方向推进了。  
我重点解决的不是“怎么多接几个工具”，而是“怎么让系统长期执行、能恢复、能解释、能被评测”。  
所以我把整个系统收敛成一条统一的 runtime 主线：用户请求会先被解析成 goal、action、state、policy 和 reflection，然后再由 Temporal workflow、worker、tool gateway、approval、external signal 和 policy memory 去推进。  
前端我也做成了聊天式工作台，但保留了 runtime debugger、why-not 和 task trace，这样它不仅能演示，也能解释系统为什么这么决策。

## 面试 3 分钟讲法

### 项目背景

很多“智能体”项目其实更像增强版工作流或者聊天助手：能调工具，但状态不连续；能跑流程，但一失败就断；能展示 trace，但很难解释为什么选择某个动作。  
我做 `xh-helper` 的目标，是把这些问题收敛到一个真正的 Agent Runtime 里。

### 我做的核心工作

1. 统一运行时语义  
把系统里的核心对象统一成 `goal / action / state / policy / reflection`，而不是用很多 route 和 if/else 去拼。

2. 做 durable 执行链  
用 Temporal + worker 承载任务执行、审批等待、恢复、取消、失败分类，保证 workflow 不是一次请求结束就消失。

3. 做长期控制能力  
增加 goal/subgoal、wake graph、portfolio scheduler、preempt/replan/resume，让系统不只是单轮对话，而是能围绕长期目标持续推进。

4. 做策略学习和验证  
增加 episode / policy memory、shadow/canary、agent eval，使历史反馈和评测结果能影响后续策略，而不是只做日志展示。

5. 做产品化前端  
把前端收成中文友好的聊天式工作台，既像真实产品，又能查看 runtime debugger、task trace 和 why-not，适合面试演示。

### 我最想让面试官看到的能力

- 我能把 LLM / Agent 系统从 Demo 提升到更可运行、可恢复、可治理的工程系统
- 我不只是会调模型 API，还会做 runtime、状态管理、调度、评测和前端展示
- 我会诚实表达系统边界，不会伪造没有实现的能力

## 如果面试官问“你这个项目最难的点是什么”

可以这样回答：

> 最难的不是把模型接进来，而是把 goal、workflow、approval、external event、memory 和 eval 真正收敛成同一条 runtime 主线。  
> 如果这些能力是割裂的，系统就只能演示；只有它们共享同一个控制骨架，项目才更像真正的 Agent Runtime。

## 如果面试官问“和普通 AI 助手项目的区别”

可以这样回答：

- 普通 AI 助手项目更关注“单轮对话是否聪明”
- `xh-helper` 更关注“长期执行是否稳定、可恢复、可解释、可评测”
- 所以它更像 Agent 工程项目，而不是简单的聊天产品 Demo
