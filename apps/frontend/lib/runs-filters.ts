export type RunsTimeWindow = "7d" | "30d" | "all";

const MS_PER_DAY = 24 * 60 * 60 * 1000;

export function sanitizeRunsTimeWindow(value: string | null | undefined): RunsTimeWindow {
  if (value === "30d") return "30d";
  if (value === "all") return "all";
  return "7d";
}

export function resolveRunsTimeWindowRange(
  windowValue: RunsTimeWindow,
  now: Date = new Date()
): { fromTs?: string; toTs?: string } {
  if (windowValue === "all") {
    return {};
  }
  const days = windowValue === "30d" ? 30 : 7;
  const fromTs = new Date(now.getTime() - days * MS_PER_DAY).toISOString();
  return {
    fromTs,
    toTs: now.toISOString()
  };
}

