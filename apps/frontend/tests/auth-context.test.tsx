import React from "react";
import { act, render, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { AuthProvider, useAuth } from "../lib/auth-context";

const pushToast = vi.fn();

vi.mock("../lib/toast-context", () => ({
  useToast: () => ({
    pushToast
  })
}));

vi.mock("../lib/auth-storage", () => ({
  canOverrideStorageMode: () => true,
  clearMemoryMarker: vi.fn(),
  clearTokens: vi.fn(),
  consumedMemorySessionMarker: () => false,
  defaultStorageMode: () => "memory",
  getStorageMode: () => "memory",
  markMemorySession: vi.fn(),
  readTokens: () => ({ accessToken: "", refreshToken: "" }),
  saveTokens: vi.fn(),
  setStorageMode: vi.fn()
}));

describe("AuthProvider session end dedupe", () => {
  it("deduplicates forceSessionEnded calls within 1 second", async () => {
    pushToast.mockReset();

    let contextRef: ReturnType<typeof useAuth> | null = null;

    function Probe() {
      contextRef = useAuth();
      return null;
    }

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>
    );

    await waitFor(() => expect(contextRef?.ready).toBe(true));
    expect(contextRef).not.toBeNull();

    act(() => {
      contextRef!.forceSessionEnded("登录已过期，请重新登录。");
      contextRef!.forceSessionEnded("登录已过期，请重新登录。");
    });

    expect(pushToast).toHaveBeenCalledTimes(1);
  });
});
