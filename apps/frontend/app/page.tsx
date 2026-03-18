import Link from "next/link";

import { SectionCard } from "../components/section-card";

const coreModules = [
  {
    title: "运行时主线",
    description: "请求会先被整理成 goal、action、state 和 policy，再决定是直接回答、工具执行还是持久任务。"
  },
  {
    title: "持久执行",
    description: "后端使用 Temporal 和 worker 承载长任务，支持等待、恢复、审批和外部事件回流。"
  },
  {
    title: "目标调度",
    description: "系统维护 goal、subgoal、wake condition 和长期调度状态，不只是一个 task status。"
  },
  {
    title: "策略与评测",
    description: "policy memory、shadow/canary 和 eval 会约束策略演化，减少只靠感觉调系统。"
  }
];

const productViews = [
  {
    title: "对话工作台",
    href: "/assistant",
    description: "从对话入口发起任务，同时查看当前目标、动作、why-not 和调试信息。"
  },
  {
    title: "运行追踪",
    href: "/runs",
    description: "查看 workflow、执行步骤、证据、失败语义和最终状态。"
  },
  {
    title: "审批中心",
    href: "/approvals",
    description: "处理人工确认、高风险动作控制和等待恢复。"
  },
  {
    title: "发起任务",
    href: "/playground",
    description: "从更工程化的入口创建任务，观察系统在不同输入下的行为。"
  }
];

const walkthrough = [
  "用户提交请求，API 先做 goal 和 action 建模。",
  "运行时主线选择 direct answer、tool task 或 workflow task。",
  "如果进入 workflow，worker 负责执行、等待、恢复和状态回写。",
  "审批、外部事件、任务完成信号都会回到同一份 runtime state。",
  "前端可以直接查看 trace、why-not、runtime debugger 和运行结果。"
];

const projectFacts = [
  "支持持久任务执行、等待恢复、审批和外部事件回流。",
  "前端可以直接查看运行追踪、任务状态和调试信息。",
  "可以比较完整地体现一个 Agent Runtime 项目是怎样组织起来的。"
];

export default function HomePage() {
  return (
    <div className="landing-shell">
      <section className="panel landing-hero-simple">
        <div className="landing-hero-copy">
          <span className="landing-kicker">概览</span>
          <h1 className="landing-title-simple">xh-helper</h1>
          <p className="landing-subtitle-simple">
            一个面向真实任务执行场景的 Agent Runtime 项目。它不把请求简单分流成几个 route，而是尝试用统一的
            goal、action、state、policy 和 durable workflow 去推进任务，并把审批、外部事件、恢复和评测放进同一条主链路。
          </p>
        </div>

        <div className="landing-hero-actions">
          <Link href="/assistant" className="btn btn-primary">
            进入对话工作台
          </Link>
          <Link href="/runs" className="btn btn-ghost">
            查看运行追踪
          </Link>
          <Link href="/playground" className="btn btn-ghost">
            发起任务
          </Link>
        </div>

        <div className="landing-summary-grid">
          <div className="landing-summary-card">
            <span className="landing-summary-label">项目定位</span>
            <strong>任务执行与调试平台</strong>
            <p className="muted-text">更偏 Agent 工程，不是单纯的聊天页面。</p>
          </div>
          <div className="landing-summary-card">
            <span className="landing-summary-label">后端主线</span>
            <strong>FastAPI + Temporal + Postgres</strong>
            <p className="muted-text">围绕持久执行、状态回写、审批和恢复组织系统。</p>
          </div>
          <div className="landing-summary-card">
            <span className="landing-summary-label">主要入口</span>
            <strong>/assistant 与 /runs</strong>
            <p className="muted-text">一个看交互，一个看完整运行链路和调试信息。</p>
          </div>
        </div>
      </section>

      <div className="grid cols-2">
        <SectionCard title="系统在做什么" subtitle="用比较直白的方式说明项目能力。">
          <div className="landing-plain-list">
            {coreModules.map((item) => (
              <div key={item.title} className="landing-plain-item">
                <strong>{item.title}</strong>
                <p className="muted-text">{item.description}</p>
              </div>
            ))}
          </div>
        </SectionCard>

        <SectionCard title="主要页面" subtitle="如果第一次打开项目，可以从这些入口开始。">
          <div className="landing-tour-list">
            {productViews.map((item) => (
              <Link key={item.href} href={item.href} className="landing-tour-row">
                <div className="stack-gap-xs">
                  <strong>{item.title}</strong>
                  <p className="muted-text">{item.description}</p>
                </div>
                <span className="landing-tour-link">打开</span>
              </Link>
            ))}
          </div>
        </SectionCard>
      </div>

      <div className="grid cols-2">
        <SectionCard title="一条典型链路" subtitle="这部分概括了一次任务从进入到完成的大致过程。">
          <ol className="landing-flow-list">
            {walkthrough.map((item) => (
              <li key={item} className="landing-flow-item">
                {item}
              </li>
            ))}
          </ol>
        </SectionCard>

        <SectionCard title="当前项目状态" subtitle="这里列的是现在已经比较稳定的几项能力。">
          <div className="landing-plain-list">
            {projectFacts.map((item) => (
              <div key={item} className="landing-plain-item">
                {item}
              </div>
            ))}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
