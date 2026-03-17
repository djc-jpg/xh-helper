import {
  FINAL_STATES,
  MASState,
  ProtocolMessage,
  StepRecord,
  TimelineEvent,
  type CriticVerdict,
  type EvidenceItem,
  type Metrics
} from "./mas-types";

interface MasEnvelope {
  mode?: string;
  status?: string;
  turn?: number;
  state?: MASState;
  protocol_messages?: ProtocolMessage[];
  failure_type?: string;
  failure_semantic?: string;
  reason?: string;
  result?: Record<string, unknown> | null;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asArray<T = unknown>(value: unknown): T[] {
  if (Array.isArray(value)) {
    return value as T[];
  }
  return [];
}

function asString(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value);
}

function asNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function normalizeProtocolMessage(raw: unknown, idx: number): ProtocolMessage {
  const record = asRecord(raw);
  const payload = asRecord(record.payload);
  return {
    id: asString(record.id || `${idx}`),
    ts: asString(record.ts || record.timestamp),
    turn: asNumber(record.turn),
    phase: asString(record.phase),
    agent: asString(record.agent || record.role || record.sender || record.node),
    type: asString(record.type || record.event_type || record.kind),
    status: asString(record.status),
    verdict: asString(record.verdict),
    content: asString(record.content || record.summary || record.message),
    payload,
    raw: record
  };
}

export function isFinalState(status: string | undefined): boolean {
  return Boolean(status && FINAL_STATES.includes(status as (typeof FINAL_STATES)[number]));
}

export function extractMasEnvelope(steps: StepRecord[]): MasEnvelope | null {
  const reversed = [...steps].reverse();
  for (const step of reversed) {
    const payload = asRecord(step.payload_masked);
    const masGate = asRecord(payload.mas_gate);
    if (Object.keys(masGate).length > 0) {
      const protocolMessages = asArray(masGate.protocol_messages).map((item, idx) => normalizeProtocolMessage(item, idx));
      return {
        mode: asString(masGate.mode),
        status: asString(masGate.status),
        turn: asNumber(masGate.turn),
        state: asRecord(masGate.state) as MASState,
        protocol_messages: protocolMessages,
        failure_type: asString(masGate.failure_type),
        failure_semantic: asString(masGate.failure_semantic),
        reason: asString(masGate.reason || masGate.error),
        result: asRecord(masGate.result)
      };
    }
  }
  return null;
}

export function extractMasState(steps: StepRecord[]): MASState {
  const envelope = extractMasEnvelope(steps);
  if (envelope?.state) {
    return envelope.state;
  }

  // Fallback to best effort from step payload fields.
  const fallback: MASState = {};
  const reversed = [...steps].reverse();
  for (const step of reversed) {
    const payload = asRecord(step.payload_masked);
    if (!fallback.turn) {
      const turn = asNumber(payload.turn);
      if (turn !== undefined) {
        fallback.turn = turn;
      }
    }
    if (!fallback.phase && step.step_key.startsWith("mas_")) {
      if (step.step_key.includes("planner")) fallback.phase = "PLAN";
      if (step.step_key.includes("execution")) fallback.phase = "EXECUTE";
      if (step.step_key.includes("critic")) fallback.phase = "EVALUATE";
    }
  }
  return fallback;
}

export function extractEvidence(steps: StepRecord[]): EvidenceItem[] {
  const envelope = extractMasEnvelope(steps);
  const stateEvidence = asArray(envelope?.state?.evidence);
  if (stateEvidence.length > 0) {
    return stateEvidence.map((item) => {
      const record = asRecord(item);
      return {
        id: asString(record.id),
        source: asString(record.source || record.doc),
        title: asString(record.title),
        snippet: asString(record.snippet || record.content),
        confidence: asNumber(record.confidence),
        conflict: Boolean(record.conflict || record.is_conflict),
        tags: asArray<string>(record.tags).map((tag) => asString(tag)),
        raw: record
      };
    });
  }

  const reversed = [...steps].reverse();
  for (const step of reversed) {
    const payload = asRecord(step.payload_masked);
    const evidence = asArray(payload.evidence);
    if (evidence.length > 0) {
      return evidence.map((item) => {
        const record = asRecord(item);
        return {
          source: asString(record.source || record.doc),
          title: asString(record.title),
          snippet: asString(record.snippet || record.content),
          conflict: Boolean(record.conflict),
          raw: record
        };
      });
    }
  }

  return [];
}

export function extractCriticVerdict(steps: StepRecord[]): CriticVerdict {
  const envelope = extractMasEnvelope(steps);
  const state = envelope?.state ?? {};
  const messages = envelope?.protocol_messages ?? [];
  const instructions = extractFixInstructions(messages, state);
  const rawVerdict = asString(state.verdict || envelope?.failure_semantic || envelope?.status);
  let verdict: CriticVerdict["verdict"] = "PENDING";
  if (rawVerdict.includes("PASS") || rawVerdict === "SUCCEEDED") {
    verdict = "PASS";
  } else if (rawVerdict.includes("NEED_INFO")) {
    verdict = "NEED_INFO";
  } else if (rawVerdict.includes("FAIL") || rawVerdict.includes("FAILED")) {
    verdict = "FAIL";
  }
  return {
    verdict,
    failureType: asString(envelope?.failure_type),
    failureSemantic: asString(envelope?.failure_semantic),
    stopReason: asString(state.stop_reason || envelope?.reason),
    fixInstructions: instructions
  };
}

function extractFixInstructions(messages: ProtocolMessage[], state: MASState): string[] {
  const fromState = asArray<string>(state.fix_instructions).map((item) => asString(item)).filter(Boolean);
  if (fromState.length > 0) {
    return fromState;
  }
  const all: string[] = [];
  for (const message of messages) {
    if (!message.payload) continue;
    const payload = asRecord(message.payload);
    const fromPayload = asArray<string>(payload.fix_instructions)
      .map((item) => asString(item).trim())
      .filter(Boolean);
    all.push(...fromPayload);
  }
  return Array.from(new Set(all));
}

export function extractMetrics(steps: StepRecord[]): Metrics {
  const envelope = extractMasEnvelope(steps);
  const metrics = asRecord(envelope?.state?.metrics);
  return {
    messageTotal: asNumber(metrics.message_total),
    tokenIn: asNumber(metrics.token_in),
    tokenOut: asNumber(metrics.token_out),
    totalCost: asNumber(metrics.total_cost),
    elapsedMs: asNumber(metrics.elapsed_ms),
    raw: metrics
  };
}

function protocolToTimeline(messages: ProtocolMessage[]): TimelineEvent[] {
  return messages.map((msg, index) => ({
    id: msg.id || `${index}`,
    ts: msg.ts,
    turn: msg.turn,
    phase: msg.phase,
    agent: msg.agent || "unknown_agent",
    type: msg.type || "message",
    status: msg.status,
    verdict: msg.verdict,
    title: msg.content || `${msg.agent || "agent"} ${msg.type || "message"}`,
    payload: asRecord(msg.raw || msg.payload)
  }));
}

function stepsToTimeline(steps: StepRecord[]): TimelineEvent[] {
  return steps.map((step) => {
    const payload = asRecord(step.payload_masked);
    return {
      id: `${step.id}`,
      ts: step.created_at,
      agent: step.step_key,
      type: "step",
      status: step.status,
      title: `${step.step_key} -> ${step.status}`,
      payload
    };
  });
}

export function computeFallbackHighlights(events: TimelineEvent[]): TimelineEvent[] {
  const marked = [...events];
  for (let i = 0; i < marked.length; i++) {
    const current = marked[i];
    const currentAgent = current.agent.toLowerCase();
    const currentVerdict = (current.verdict || current.status || "").toUpperCase();
    if (!(currentAgent.includes("critic") && currentVerdict.includes("FAIL"))) {
      continue;
    }
    for (let j = i + 1; j < Math.min(marked.length, i + 5); j++) {
      const next = marked[j];
      const nextAgent = next.agent.toLowerCase();
      if (nextAgent.includes("planner") || nextAgent.includes("revision") || nextAgent.includes("replan")) {
        marked[j] = { ...next, fallbackHighlighted: true };
        break;
      }
    }
  }
  return marked;
}

export function buildTimeline(steps: StepRecord[]): TimelineEvent[] {
  const envelope = extractMasEnvelope(steps);
  const protocol = envelope?.protocol_messages ?? [];
  const base = protocol.length > 0 ? protocolToTimeline(protocol) : stepsToTimeline(steps);
  return computeFallbackHighlights(base);
}

export function extractOutputPayload(steps: StepRecord[]): Record<string, unknown> {
  const reversed = [...steps].reverse();
  for (const step of reversed) {
    if (step.step_key === "done") {
      return asRecord(step.payload_masked);
    }
  }
  const envelope = extractMasEnvelope(steps);
  return asRecord(envelope?.result);
}

export function inferGraphEngine(steps: StepRecord[]): string {
  const envelope = extractMasEnvelope(steps);
  if (envelope?.mode && envelope.mode.includes("closed_loop")) {
    return "langgraph";
  }
  if (steps.some((step) => step.step_key.startsWith("mas_"))) {
    return "langgraph";
  }
  return "workflow";
}

export function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "{}";
  }
}
