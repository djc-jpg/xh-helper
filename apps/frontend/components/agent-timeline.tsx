"use client";

import { useMemo, useState } from "react";

import { formatDateTime } from "../lib/format";
import { TimelineEvent } from "../lib/mas-types";
import { JsonViewer } from "./json-viewer";

function uniqueValues(items: TimelineEvent[], field: "agent" | "type" | "status"): string[] {
  const set = new Set<string>();
  for (const item of items) {
    const value = item[field];
    if (value) set.add(value);
  }
  return Array.from(set).sort();
}

export function AgentTimeline({ events }: { events: TimelineEvent[] }) {
  const [agentFilter, setAgentFilter] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");

  const agents = useMemo(() => uniqueValues(events, "agent"), [events]);
  const types = useMemo(() => uniqueValues(events, "type"), [events]);
  const statuses = useMemo(() => uniqueValues(events, "status"), [events]);

  const filtered = useMemo(() => {
    return events.filter((event) => {
      if (agentFilter !== "all" && event.agent !== agentFilter) return false;
      if (typeFilter !== "all" && event.type !== typeFilter) return false;
      if (statusFilter !== "all" && event.status !== statusFilter) return false;
      return true;
    });
  }, [events, agentFilter, typeFilter, statusFilter]);

  return (
    <div className="stack-gap-sm">
      <div className="grid cols-3">
        <select value={agentFilter} onChange={(event) => setAgentFilter(event.target.value)}>
          <option value="all">全部 Agent</option>
          {agents.map((agent) => (
            <option key={agent} value={agent}>
              {agent}
            </option>
          ))}
        </select>
        <select value={typeFilter} onChange={(event) => setTypeFilter(event.target.value)}>
          <option value="all">全部类型</option>
          {types.map((type) => (
            <option key={type} value={type}>
              {type}
            </option>
          ))}
        </select>
        <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
          <option value="all">全部状态</option>
          {statuses.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>
      </div>

      <ol className="timeline-list" aria-label="智能体时间线">
        {filtered.map((event) => (
          <li
            key={event.id}
            className={`timeline-item ${event.fallbackHighlighted ? "timeline-fallback" : ""}`}
            data-fallback={event.fallbackHighlighted ? "true" : "false"}
          >
            <div className="timeline-head">
              <div className="timeline-title-row">
                <strong>{event.title}</strong>
                {event.fallbackHighlighted ? <span className="tag tag-warning">评审失败 -&gt; 重新规划</span> : null}
              </div>
              <div className="timeline-meta">
                <span>{event.agent}</span>
                <span>{event.type}</span>
                {event.phase ? <span>阶段={event.phase}</span> : null}
                {event.turn ? <span>轮次={event.turn}</span> : null}
                {event.status ? <span>状态={event.status}</span> : null}
                {event.verdict ? <span>判定={event.verdict}</span> : null}
                {event.ts ? <span>{formatDateTime(event.ts)}</span> : null}
              </div>
            </div>
            <JsonViewer value={event.payload} title="结构化内容" />
          </li>
        ))}
      </ol>
    </div>
  );
}
