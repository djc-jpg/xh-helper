import Link from "next/link";

import { SectionCard } from "../components/section-card";

const capabilityCards = [
  {
    title: "统一运行时骨干",
    description: "把 goal、action、state、policy、reflection 放进同一条主链路，而不是散落在多个服务里。",
    bullets: ["共享 runtime state", "动作契约 + why-not", "面向调试的 trace 投影"]
  },
  {
    title: "面向真实任务执行",
    description: "后端不是一次性请求处理，而是支持 durable workflow、等待、恢复、超时和人工介入。",
    bullets: ["Temporal + Worker", "审批 / wait-resume", "external signal 恢复"]
  },
  {
    title: "长期目标调度",
    description: "系统支持 goal / subgoal / wake graph / portfolio scheduling，不只是 task status 面板。",
    bullets: ["goal / subgoal", "replan / preempt / resume", "长期调度与优先级"]
  },
  {
    title: "策略学习与评测",
    description: "不是只有日志和 trace，还把 episode、policy memory、shadow/canary 和 eval 接进了主干。",
    bullets: ["policy memory", "agent eval", "shadow / canary"]
  }
];

const showcaseRoutes = [
  {
    title: "智能体工作台",
    href: "/assistant",
    description: "用聊天式界面发起任务，同时查看 goal、动作、策略、why-not 和 runtime debugger。"
  },
  {
    title: "运行实况",
    href: "/runs",
    description: "查看 durable workflow、循环步骤、证据包、判定结果和失败语义。"
  },
  {
    title: "发起运行",
    href: "/playground",
    description: "从更工程化的入口发起任务，验证不同输入和预算下的行为。"
  },
  {
    title: "审批中心",
    href: "/approvals",
    description: "演示高风险动作的审批、等待、人工确认与恢复。"
  }
];

const interviewFlow = [
  "用户输入 -> API 做 goal / action / policy 建模",
  "Runtime Backbone 决定 direct answer、tool task 或 workflow task",
  "Worker / Workflow 执行 plan、tool、review、approval、wait、resume",
  "状态经由 internal status writeback 回到统一 runtime",
  "前端展示 task trace、runtime debugger、why-not 和运行结果"
];

const recruiterSignals = [
  "不是普通聊天 Demo，而是带 durable workflow 和长期状态的 Agent Runtime。",
  "重点展示的是 Agent / LLM 应用工程能力，包括状态管理、等待恢复、审批治理和评测闭环。",
  "项目本身已经具备适合面试演示的工作台、运行页和 eval 入口。"
];

export default function HomePage() {
  return (
    <div className="landing-shell">
      <section className="panel landing-hero landing-hero-grid">
        <div className="stack-gap-md">
          <div className="landing-kicker">Agent / LLM 应用作品展示</div>
          <div className="stack-gap-sm">
            <h1 className="landing-title">xh-helper：一个面向真实任务执行的 Agent Runtime</h1>
            <p className="landing-subtitle">
              它不是“多接几个工具的聊天助手”，而是把
              <span className="landing-highlight"> goal / action / state / policy / reflection </span>
              收敛到同一条运行时主线里，再通过 durable workflow、审批、人类介入、external signal、
              policy memory 和 eval 持续推进任务。
            </p>
          </div>

          <div className="inline-actions">
            <Link href="/assistant" className="btn btn-primary">
              进入智能体工作台
            </Link>
            <Link href="/runs" className="btn btn-ghost">
              查看运行追踪
            </Link>
            <Link href="/playground" className="btn btn-ghost">
              发起一个演示任务
            </Link>
          </div>

          <div className="landing-badges">
            <span className="landing-badge">Goal / Subgoal</span>
            <span className="landing-badge">Durable Workflow</span>
            <span className="landing-badge">Approval / Wait / Resume</span>
            <span className="landing-badge">Policy Memory</span>
            <span className="landing-badge">Agent Eval</span>
          </div>

          <div className="grid cols-4 landing-metrics">
            <div className="landing-metric-card">
              <span className="landing-metric-label">运行时主线</span>
              <strong className="landing-metric-value">Goal / Action / Policy</strong>
            </div>
            <div className="landing-metric-card">
              <span className="landing-metric-label">执行方式</span>
              <strong className="landing-metric-value">Temporal + Worker</strong>
            </div>
            <div className="landing-metric-card">
              <span className="landing-metric-label">恢复能力</span>
              <strong className="landing-metric-value">Wait / Resume / Replan</strong>
            </div>
            <div className="landing-metric-card">
              <span className="landing-metric-label">验证能力</span>
              <strong className="landing-metric-value">Trace + Eval + Canary</strong>
            </div>
          </div>
        </div>

        <div className="landing-showcase-card">
          <div className="landing-showcase-top">
            <span className="landing-window-dot dot-red" />
            <span className="landing-window-dot dot-yellow" />
            <span className="landing-window-dot dot-green" />
            <span className="landing-showcase-label">作品演示视图</span>
          </div>

          <div className="landing-showcase-body">
            <div className="landing-showcase-pane">
              <p className="landing-pane-kicker">当前目标</p>
              <strong>准备 incident response summary，并在需要时走 durable runtime</strong>
              <div className="landing-mini-tags">
                <span className="landing-mini-tag">高解释性</span>
                <span className="landing-mini-tag">可引用</span>
                <span className="landing-mini-tag">必要时审批</span>
              </div>
            </div>

            <div className="landing-showcase-grid">
              <div className="landing-showcase-pane">
                <p className="landing-pane-kicker">下一动作</p>
                <strong>workflow_call</strong>
                <p className="muted-text">因为需要持久执行、恢复语义与结构化追踪。</p>
              </div>
              <div className="landing-showcase-pane">
                <p className="landing-pane-kicker">为什么不是别的动作</p>
                <strong>why-not</strong>
                <p className="muted-text">不是 direct answer，因为需要更长链路和运行时状态。</p>
              </div>
              <div className="landing-showcase-pane">
                <p className="landing-pane-kicker">运行状态</p>
                <strong>WAITING_APPROVAL</strong>
                <p className="muted-text">等待人工确认后可恢复，不丢失上下文和 task trace。</p>
              </div>
              <div className="landing-showcase-pane">
                <p className="landing-pane-kicker">调试视角</p>
                <strong>runtime debugger</strong>
                <p className="muted-text">可查看 state before / decision / reflection / state after。</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <div className="grid cols-2">
        <SectionCard
          title="招聘方 30 秒应该看到什么"
          subtitle="先让人知道这不是普通问答 Demo，再让人知道它有工程深度。"
        >
          <div className="landing-list">
            {recruiterSignals.map((item) => (
              <div key={item} className="landing-list-item">
                {item}
              </div>
            ))}
          </div>
        </SectionCard>

        <SectionCard title="推荐演示路径" subtitle="如果你用这个项目做面试展示，按这个顺序最容易讲清楚。">
          <div className="landing-tour-grid">
            {showcaseRoutes.map((item) => (
              <Link key={item.href} href={item.href} className="landing-tour-card">
                <div className="stack-gap-xs">
                  <strong>{item.title}</strong>
                  <p className="muted-text">{item.description}</p>
                </div>
                <span className="landing-tour-link">打开页面</span>
              </Link>
            ))}
          </div>
        </SectionCard>
      </div>

      <SectionCard title="这个项目最值得讲的工程能力" subtitle="下面这些内容都能在当前仓库和页面里直接找到对应实现。">
        <div className="landing-feature-grid">
          {capabilityCards.map((item) => (
            <article key={item.title} className="landing-feature-card">
              <div className="stack-gap-sm">
                <div className="stack-gap-xs">
                  <h3 className="landing-feature-title">{item.title}</h3>
                  <p className="muted-text">{item.description}</p>
                </div>
                <div className="landing-bullets">
                  {item.bullets.map((bullet) => (
                    <span key={bullet} className="landing-bullet">
                      {bullet}
                    </span>
                  ))}
                </div>
              </div>
            </article>
          ))}
        </div>
      </SectionCard>

      <div className="grid cols-2">
        <SectionCard title="典型运行链路" subtitle="面试里可以照着这一段讲完整个系统。">
          <ol className="landing-flow-list">
            {interviewFlow.map((item) => (
              <li key={item} className="landing-flow-item">
                {item}
              </li>
            ))}
          </ol>
        </SectionCard>

        <SectionCard title="技术栈与定位" subtitle="更偏 Agent / LLM 应用工程，而不是模型训练或单纯前端页面展示。">
          <div className="landing-badges">
            {["FastAPI", "Temporal", "Postgres", "Next.js", "TypeScript", "LangGraph / MAS", "Prometheus", "Grafana"].map(
              (item) => (
                <span key={item} className="landing-badge landing-badge-muted">
                  {item}
                </span>
              )
            )}
          </div>
        </SectionCard>
      </div>
    </div>
  );
}
