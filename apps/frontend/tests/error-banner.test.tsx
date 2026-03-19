import React from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ErrorBanner } from "../components/error-banner";
import { ApiError } from "../lib/api";

describe("ErrorBanner", () => {
  it("renders humanized ApiError detail and copies request id", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(window.navigator, "clipboard", {
      configurable: true,
      value: { writeText }
    });
    const user = userEvent.setup();

    render(<ErrorBanner error={new ApiError({ status: 500, detail: "assistant_stream_failed", requestId: "rid-123" })} />);

    expect(screen.getByText("这次回复在生成途中中断了，请稍后再试。")).toBeInTheDocument();
    expect(screen.getByText("rid-123")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "复制 Request ID" }));
    expect(screen.getByRole("button", { name: "已复制" })).toBeInTheDocument();
  });

  it("renders retry for generic error", async () => {
    const onRetry = vi.fn();
    const user = userEvent.setup();

    render(<ErrorBanner error={new Error("boom")} onRetry={onRetry} />);
    expect(screen.getByText("boom")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "重试" }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });
});
