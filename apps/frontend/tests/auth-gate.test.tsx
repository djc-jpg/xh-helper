import React from "react";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import { AuthGate } from "../components/auth-gate";
import { AuthContext } from "../lib/auth-context";
import { type AuthStorageMode } from "../lib/mas-types";

vi.mock("next/link", () => ({
  default: ({ href, children, className }: { href: string; children: ReactNode; className?: string }) => (
    <a href={href} className={className}>
      {children}
    </a>
  )
}));

const baseContext = {
  ready: true,
  isAuthenticated: false,
  user: null,
  accessToken: "",
  refreshToken: "",
  storageMode: "memory" as AuthStorageMode,
  canOverrideMode: true,
  sessionHint: "",
  signIn: vi.fn(),
  signUp: vi.fn(),
  signOut: vi.fn(),
  forceSessionEnded: vi.fn(),
  updateStorageMode: vi.fn(),
  tryRefresh: vi.fn()
};

describe("AuthGate", () => {
  it("shows login guide and blocks children when unauthenticated", () => {
    render(
      <AuthContext.Provider value={baseContext}>
        <AuthGate>
          <div>secret content</div>
        </AuthGate>
      </AuthContext.Provider>
    );

    expect(screen.getByText("请先登录")).toBeInTheDocument();
    expect(screen.getByText("登录后就可以继续对话、查看任务进展，并处理需要确认的操作。")).toBeInTheDocument();
    expect(screen.queryByText("secret content")).not.toBeInTheDocument();
  });

  it("renders protected content when authenticated", () => {
    render(
      <AuthContext.Provider value={{ ...baseContext, isAuthenticated: true, accessToken: "token" }}>
        <AuthGate>
          <div>secret content</div>
        </AuthGate>
      </AuthContext.Provider>
    );

    expect(screen.getByText("secret content")).toBeInTheDocument();
  });
});
