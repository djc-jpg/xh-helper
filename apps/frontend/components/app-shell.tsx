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
      subtitle: "从求职作品视角快速了解 xh-helper 的 Agent 能力与工程亮点"
    };
  }
  if (pathname.startsWith("/assistant")) {
    return {
      title: "智能体工作台",
      subtitle: "和 xh-helper 对话，查看目标、策略、动作与运行时调试信息"
    };
  }
  if (pathname.startsWith("/runs")) {
    return {
      title: "运行实况",
      subtitle: "查看持久执行、循环过程、证据、判定与任务追踪"
    };
  }
  if (pathname.startsWith("/playground")) {
    return {
      title: "发起运行",
      subtitle: "启动新的受控任务，验证不同输入和预算下的系统行为"
    };
  }
  if (pathname.startsWith("/approvals")) {
    return {
      title: "审批中心",
      subtitle: "处理高风险动作、人工确认与等待恢复链路"
    };
  }
  if (pathname.startsWith("/monitoring")) {
    return {
      title: "运行观测",
      subtitle: "追踪系统健康度、告警信号、吞吐表现和运行趋势"
    };
  }
  if (pathname.startsWith("/settings")) {
    return {
      title: "系统设置",
      subtitle: "调整访问、策略和工作台偏好设置"
    };
  }
  return {
    title: "xh-helper",
    subtitle: "通用智能体运行时控制台"
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
              <span className="app-brand-subtitle">通用智能体运行时</span>
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
