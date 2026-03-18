import Link from "next/link";

import { SectionCard } from "../components/section-card";

const highlights = [
  {
    title: "统一的 Agent Runtime 主线",
    description:
      "把 goal、action、state、policy、reflection、approval、wait/resume、trace 拉到同一条控制主线上，而不是把逻辑散在 API、worker 和前端。"
  },
  {
    title: "面向真实执行而不是 Demo",
    description:
      "后端基于 FastAPI + Temporal + Postgres，支持持久任务、审批等待、外部事件唤醒、失败恢复、长期 goal/subgoal 调度。"
  },
  {
    title: "有学习闭环，不只是会调用工具",
    description:
      "支持 episode / policy memory、shadow/canary、agent-grade eval，让历史反馈和评测结果能影响后续策略。"
  },
  {
    title: "前端可调试、可解释",
    description:
      "不是普通聊天页，能够查看 goal、当前动作、why-not、runtime debugger 和 task trace，方便演示和定位问题。"
  }
];

const quickTour = [
  {
    title: "智能体工作台",
    href: "/assistant",
    description: "像 ChatGPT 一样发起对话，同时查看目标、策略、动作契约与运行时调试信息。"
  },
  {
    title: "运行实况",
    href: "/runs",
    description: "查看 durable workflow、循环步骤、证据、判定结果和失败语义。"
  },
  {
    title: "发起运行",
    href: "/playground",
    description: "用更工程化的方式发起任务，验证不同输入和预算下的系统行为。"
  },
  {
    title: "审批与治理",
    href: "/approvals",
    description: "查看高风险动作审批、等待恢复和受控执行链路。"
  }
];

const stack = [
  "FastAPI",
  "Temporal",
  "Postgres",
  "Next.js",
  "TypeScript",
  "LangGraph / MAS",
  "Prometheus + Grafana",
  "Docker Compose"
];

const recruiterPoints = [
  "这是一个更接近 Agent Runtime 的项目，而不是“多接几个工具”的聊天助手。",
  "项目里有真实的 durable workflow、审批恢复、外部事件驱动、policy memory 和评测闭环。",
  "代码里已经提供 agent 级别的 golden cases 和 eval runner，能证明系统行为而不只是演示效果。"
];

const honestBoundaries = [
  "它已经具备通用智能体运行时的主干能力，但仍然是一个工程项目，不是“无所不能”的 AGI。",
  "重点放在长期控制、状态恢复、策略调试和评测闭环，而不是堆砌一堆模型能力名词。",
  "仓库适合展示 Agent / LLM 应用工程能力、后端架构能力和产品化意识。"
];

export default function HomePage() {
  return (
    <div className="landing-shell">
      <section className="panel landing-hero">
        <div className="landing-kicker">Agent / LLM 应用求职作品</div>
        <div className="stack-gap-md">
          <div className="stack-gap-sm">
            <h1 className="landing-title">xh-helper：面向真实任务执行的通用智能体运行时</h1>
            <p className="landing-subtitle">
              这个项目不是“会调用工具的聊天助手”，而是一套围绕
              <span className="landing-highlight"> goal / action / state / policy / reflection </span>
              组织起来的 Agent Runtime。它支持 durable workflow、goal/subgoal、审批等待与恢复、外部事件驱动、
              policy memory、shadow/canary 和 agent-grade eval。
            </p>
          </div>

          <div className="inline-actions">
            <Link href="/assistant" className="btn btn-primary">
              立即体验 xh-helper
            </Link>
            <Link href="/runs" className="btn btn-ghost">
              查看运行与追踪
            </Link>
            <Link href="/playground" className="btn btn-ghost">
              发起一条任务流
            </Link>
          </div>

          <div className="landing-badges">
            <span className="landing-badge">通用智能体 Runtime</span>
            <span className="landing-badge">Durable Workflow</span>
            <span className="landing-badge">Goal / Subgoal</span>
            <span className="landing-badge">Policy Memory</span>
            <span className="landing-badge">Shadow / Canary</span>
            <span className="landing-badge">Agent Eval</span>
          </div>
        </div>

        <div className="grid cols-4 landing-metrics">
          <div className="landing-metric-card">
            <span className="landing-metric-label">运行时主线</span>
            <strong className="landing-metric-value">Goal / Action / Policy</strong>
          </div>
          <div className="landing-metric-card">
            <span className="landing-metric-label">持久执行</span>
            <strong className="landing-metric-value">Temporal + Worker</strong>
          </div>
          <div className="landing-metric-card">
            <span className="landing-metric-label">恢复能力</span>
            <strong className="landing-metric-value">Wait / Resume / Replan</strong>
          </div>
          <div className="landing-metric-card">
            <span className="landing-metric-label">可验证性</span>
            <strong className="landing-metric-value">Trace + Eval + Canary</strong>
          </div>
        </div>
      </section>

      <div className="grid cols-2">
        <SectionCard
          title="招聘方 30 秒能看懂什么"
          subtitle="先告诉面试官这不是普通聊天助手，再告诉他为什么它有工程含金量。"
        >
          <div className="landing-list">
            {recruiterPoints.map((point) => (
              <div key={point} className="landing-list-item">
                {point}
              </div>
            ))}
          </div>
        </SectionCard>

        <SectionCard title="当前最适合展示的能力" subtitle="全部是仓库里已经实现、能跑、能验证的内容。">
          <div className="landing-list">
            {highlights.map((item) => (
              <div key={item.title} className="landing-list-item">
                <strong>{item.title}</strong>
                <p className="muted-text">{item.description}</p>
              </div>
            ))}
          </div>
        </SectionCard>
      </div>

      <div className="grid cols-2">
        <SectionCard title="建议演示路径" subtitle="如果你在面试里做现场演示，按这个顺序最容易讲清楚。">
          <div className="landing-tour-grid">
            {quickTour.map((item) => (
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

        <SectionCard title="技术栈与工程化侧重点" subtitle="更偏 Agent / LLM 应用工程，而不是模型算法研究。">
          <div className="stack-gap-md">
            <div className="landing-badges">
              {stack.map((item) => (
                <span key={item} className="landing-badge landing-badge-muted">
                  {item}
                </span>
              ))}
            </div>
            <div className="landing-list">
              <div className="landing-list-item">
                <strong>后端重点</strong>
                <p className="muted-text">FastAPI、Temporal、状态回写、审批恢复、external signal、policy memory。</p>
              </div>
              <div className="landing-list-item">
                <strong>前端重点</strong>
                <p className="muted-text">聊天式工作台、runtime debugger、why-not、task trace、中文友好控制台。</p>
              </div>
              <div className="landing-list-item">
                <strong>验证重点</strong>
                <p className="muted-text">golden cases、agent eval、shadow/canary、运行时解释能力。</p>
              </div>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="诚实边界" subtitle="适合求职时主动说明“我做到了什么，也清楚地知道它还不是什么”。">
        <div className="landing-list">
          {honestBoundaries.map((item) => (
            <div key={item} className="landing-list-item">
              {item}
            </div>
          ))}
        </div>
      </SectionCard>
    </div>
  );
}
