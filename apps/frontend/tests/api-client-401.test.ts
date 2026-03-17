import { describe, expect, it, vi } from "vitest";

import { ApiError, requestJson, setUnauthorizedHandler } from "../lib/api";

describe("api 401 interceptor", () => {
  it("calls unauthorized handler and throws ApiError on 401", async () => {
    const handler = vi.fn();
    setUnauthorizedHandler(handler);

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify({ detail: "missing auth" }), {
        status: 401,
        headers: { "Content-Type": "application/json" }
      })
    );

    await expect(
      requestJson("/tasks", {
        token: "expired-token"
      })
    ).rejects.toBeInstanceOf(ApiError);

    expect(handler).toHaveBeenCalledTimes(1);
    vi.restoreAllMocks();
    setUnauthorizedHandler(null);
  });

  it("calls unauthorized handler and throws when token is missing locally", async () => {
    const handler = vi.fn();
    setUnauthorizedHandler(handler);
    const fetchSpy = vi.spyOn(globalThis, "fetch");

    await expect(requestJson("/tasks")).rejects.toMatchObject({
      status: 401,
      detail: "missing_token"
    });

    expect(handler).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
    vi.restoreAllMocks();
    setUnauthorizedHandler(null);
  });

  it("throttles duplicated unauthorized callbacks in 1 second window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-03T00:00:00.000Z"));

    const handler = vi.fn();
    setUnauthorizedHandler(handler);

    await Promise.allSettled([requestJson("/tasks"), requestJson("/tasks")]);
    expect(handler).toHaveBeenCalledTimes(1);

    vi.setSystemTime(new Date("2026-03-03T00:00:01.100Z"));
    await expect(requestJson("/tasks")).rejects.toMatchObject({
      status: 401,
      detail: "missing_token"
    });
    expect(handler).toHaveBeenCalledTimes(2);

    vi.useRealTimers();
    vi.restoreAllMocks();
    setUnauthorizedHandler(null);
  });

  it("does not call unauthorized handler for allowAnonymous requests", async () => {
    const handler = vi.fn();
    setUnauthorizedHandler(handler);

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    );

    await expect(
      requestJson("/auth/login", {
        method: "POST",
        allowAnonymous: true,
        body: { email: "owner@example.com", password: "password" }
      })
    ).resolves.toEqual({ ok: true });

    expect(handler).not.toHaveBeenCalled();
    vi.restoreAllMocks();
    setUnauthorizedHandler(null);
  });
});
