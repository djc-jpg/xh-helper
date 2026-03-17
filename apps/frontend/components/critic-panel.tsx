"use client";

import { CriticVerdict } from "../lib/mas-types";

export function CriticPanel({ verdict }: { verdict: CriticVerdict }) {
  const statusClass =
    verdict.verdict === "PASS" ? "tag-success" : verdict.verdict === "FAIL" ? "tag-danger" : "tag-warning";

  return (
    <div className="stack-gap-sm">
      <div className="inline-actions">
        <span className={`tag ${statusClass}`}>Critic: {verdict.verdict}</span>
        {verdict.failureType ? <span className="tag tag-neutral">{verdict.failureType}</span> : null}
        {verdict.failureSemantic ? <span className="tag tag-neutral">{verdict.failureSemantic}</span> : null}
      </div>
      {verdict.stopReason ? <p className="muted-text">{verdict.stopReason}</p> : null}
      <div className="stack-gap-xs">
        <p className="panel-subtitle">修复指令</p>
        {verdict.fixInstructions.length === 0 ? (
          <p className="muted-text">无</p>
        ) : (
          <ul className="plain-list">
            {verdict.fixInstructions.map((item, index) => (
              <li key={`${index}-${item}`}>{item}</li>
            ))}
          </ul>
        )}
      </div>
      {verdict.fixInstructions.length > 0 ? (
        <button
          type="button"
          className="btn btn-ghost"
          onClick={() => navigator.clipboard.writeText(verdict.fixInstructions.join("\n")).catch(() => undefined)}
        >
          复制修复指令
        </button>
      ) : null}
    </div>
  );
}
