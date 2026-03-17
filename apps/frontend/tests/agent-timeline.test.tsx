import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { AgentTimeline } from "../components/agent-timeline";
import { computeFallbackHighlights } from "../lib/mas-utils";
import { TimelineEvent } from "../lib/mas-types";

describe("AgentTimeline fallback highlight", () => {
  it("highlights planner revision after critic fail", () => {
    const events: TimelineEvent[] = [
      {
        id: "1",
        agent: "critic",
        type: "evaluation",
        status: "FAILED_RETRYABLE",
        verdict: "FAIL",
        title: "critic verdict fail",
        payload: {}
      },
      {
        id: "2",
        agent: "planner",
        type: "revision",
        status: "PLANNING",
        title: "planner revision",
        payload: {}
      }
    ];

    const highlighted = computeFallbackHighlights(events);
    render(<AgentTimeline events={highlighted} />);

    expect(screen.getByText("Critic FAIL -> Planner 修订")).toBeInTheDocument();
    expect(screen.getByText("planner revision")).toBeInTheDocument();
  });
});
