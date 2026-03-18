"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { useAuth } from "../lib/auth-context";
import { useLocale } from "../lib/locale-context";
import { useTheme } from "../lib/theme-context";
import { SidebarNav } from "./sidebar-nav";
import { ToastCenter } from "./toast-center";

const AUTH_PAGES = new Set(["/login", "/register"]);

function resolveWorkspaceCopy(pathname: string): { title: string; subtitle: string } {
  if (pathname === "/") {
    return {
      title: "项目概览",
      subtitle: "先了解 xh-helper 的定位、主要页面和系统链路"
    };
  }
  if (pathname.startsWith("/assistant")) {
    return {
      title: "对话工作台",
      subtitle: "从对话入口发起任务，同时查看目标、动作、why-not 和调试信息"
    };
  }
  if (pathname.startsWith("/runs")) {
    return {
      title: "运行追踪",
      subtitle: "查看 workflow、执行步骤、证据、判定结果和状态变化"
    };
  }
  if (pathname.startsWith("/playground")) {
    return {
      title: "发起任务",
      subtitle: "从工程化入口创建任务，验证不同输入和预算下的系统行为"
    };
  }
  if (pathname.startsWith("/approvals")) {
    return {
      title: "审批中心",
      subtitle: "处理高风险动作、人工确认和等待恢复"
    };
  }
  if (pathname.startsWith("/monitoring")) {
    return {
      title: "系统观测",
      subtitle: "查看健康度、吞吐和整体运行情况"
    };
  }
  if (pathname.startsWith("/settings")) {
    return {
      title: "系统设置",
      subtitle: "调整策略、访问控制和界面偏好"
    };
  }
  return {
    title: "xh-helper",
    subtitle: "任务执行与调试平台"
  };
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const { t } = useTranslation();
  const auth = useAuth();
  const { theme, toggleTheme } = useTheme();
  const { language, setLanguage } = useLocale();
  const workspaceCopy = resolveWorkspaceCopy(pathname);

  useEffect(() => {
    if (!auth.ready || auth.isAuthenticated || !auth.sessionHint) {
      return;
    }
    if (AUTH_PAGES.has(pathname)) {
      return;
    }
    router.replace("/login");
  }, [auth.isAuthenticated, auth.ready, auth.sessionHint, pathname, router]);

  if (AUTH_PAGES.has(pathname)) {
    return (
      <div className="auth-shell">
        <main className="auth-main">{children}</main>
        <ToastCenter />
      </div>
    );
  }

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="app-sidebar-top">
          <Link href="/" className="app-brand">
            <span className="app-brand-mark">xh</span>
            <div className="stack-gap-xs">
              <span className="app-brand-title">xh-helper</span>
              <span className="app-brand-subtitle">任务执行与调试平台</span>
            </div>
          </Link>

          <Link href="/assistant" className="btn btn-primary app-compose-link">
            新建对话
          </Link>
        </div>

        <SidebarNav />

        <div className="app-sidebar-footer">
          <div className="user-chip app-user-chip">
            <span>{auth.user?.email || "访客"}</span>
            <span className="muted-text">{auth.user?.role || "viewer"}</span>
          </div>

          <div className="app-sidebar-controls">
            <button type="button" className="btn btn-ghost" onClick={toggleTheme}>
              {theme === "dark" ? "浅色" : "深色"}
            </button>
            <button
              type="button"
              className="btn btn-ghost"
              onClick={() => setLanguage(language === "zh" ? "en" : "zh")}
            >
              {language === "zh" ? "EN" : "中文"}
            </button>
            {auth.isAuthenticated ? (
              <button
                type="button"
                className="btn btn-ghost"
                onClick={() => {
                  auth.signOut().catch(() => undefined);
                  router.push("/login");
                }}
              >
                {t("logout")}
              </button>
            ) : (
              <Link href="/login" className="btn btn-primary">
                {t("login")}
              </Link>
            )}
          </div>
        </div>
      </aside>

      <div className="app-main">
        <header className="app-topbar">
          <div className="app-topbar-copy">
            <span className="app-topbar-kicker">xh-helper</span>
            <h1 className="app-topbar-title">{workspaceCopy.title}</h1>
            <p className="app-topbar-subtitle">{workspaceCopy.subtitle}</p>
          </div>
        </header>

        <main className="page-content">{children}</main>
      </div>
      <ToastCenter />
    </div>
  );
}
