"use client";

import Link from "next/link";
import { ReactNode } from "react";

import { useAuth } from "../lib/auth-context";
import { PageSkeleton } from "./skeleton";

export function AuthGate({ children }: { children: ReactNode }) {
  const auth = useAuth();

  if (!auth.ready) {
    return <PageSkeleton />;
  }

  if (!auth.isAuthenticated) {
    return (
      <div className="panel stack-gap-md">
        <h2 className="page-title">请先登录</h2>
        <p className="muted-text">{auth.sessionHint || "登录后就可以继续对话、查看任务进展，并处理需要确认的操作。"}</p>
        <div className="inline-actions">
          <Link href="/login" className="btn btn-primary">
            去登录
          </Link>
          <Link href="/register" className="btn btn-ghost">
            创建账号
          </Link>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
