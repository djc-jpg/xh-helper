const PHASES = ["PERCEIVE", "PLAN", "KNOWLEDGE", "EXECUTE", "EVALUATE", "ASK_USER", "TERMINATE"] as const;

const PHASE_LABEL: Record<string, string> = {
  PERCEIVE: "感知",
  PLAN: "规划",
  KNOWLEDGE: "证据",
  EXECUTE: "执行",
  EVALUATE: "评测",
  ASK_USER: "问询",
  TERMINATE: "结束"
};

export function PhaseStepper({ phase }: { phase?: string }) {
  const index = phase ? PHASES.indexOf(phase as (typeof PHASES)[number]) : -1;
  return (
    <div className="phase-stepper" role="list" aria-label="closed loop phase">
      {PHASES.map((item, idx) => {
        const done = index >= idx;
        const active = index === idx;
        return (
          <div key={item} role="listitem" className={`phase-node ${done ? "done" : ""} ${active ? "active" : ""}`}>
            <span className="phase-dot" />
            <span className="phase-text">{PHASE_LABEL[item]}</span>
          </div>
        );
      })}
    </div>
  );
}
