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
        <h2 className="page-title">需要登录</h2>
        <p className="muted-text">{auth.sessionHint || "当前会话未登录。请先登录再使用 MAS 控制台功能。"}</p>
        <div className="inline-actions">
          <Link href="/login" className="btn btn-primary">
            前往登录
          </Link>
          <Link href="/register" className="btn btn-ghost">
            注册账号
          </Link>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
