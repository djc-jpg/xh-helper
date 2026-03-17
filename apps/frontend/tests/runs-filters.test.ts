import { describe, expect, it } from "vitest";

import { resolveRunsTimeWindowRange, sanitizeRunsTimeWindow } from "../lib/runs-filters";

describe("runs filters", () => {
  it("sanitizes invalid time window to default 7d", () => {
    expect(sanitizeRunsTimeWindow("unknown")).toBe("7d");
    expect(sanitizeRunsTimeWindow(null)).toBe("7d");
    expect(sanitizeRunsTimeWindow(undefined)).toBe("7d");
  });

  it("returns empty range for all window", () => {
    expect(resolveRunsTimeWindowRange("all", new Date("2026-03-03T00:00:00.000Z"))).toEqual({});
  });

  it("computes recent ranges for 7d and 30d", () => {
    const now = new Date("2026-03-03T12:00:00.000Z");
    const range7d = resolveRunsTimeWindowRange("7d", now);
    const range30d = resolveRunsTimeWindowRange("30d", now);

    expect(range7d.toTs).toBe("2026-03-03T12:00:00.000Z");
    expect(range7d.fromTs).toBe("2026-02-24T12:00:00.000Z");
    expect(range30d.toTs).toBe("2026-03-03T12:00:00.000Z");
    expect(range30d.fromTs).toBe("2026-02-01T12:00:00.000Z");
  });
});

