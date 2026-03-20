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
  if (pathname.startsWith("/assistant")) {
    return {
      title: "\u0041\u0049 \u52a9\u624b",
      subtitle: "\u76f4\u63a5\u8f93\u5165\u95ee\u9898\u6216\u4efb\u52a1\uff0c\u9700\u8981\u65f6\u518d\u6253\u5f00\u8fd0\u884c\u4fe1\u606f\u3002"
    };
  }
  if (pathname === "/") {
    return {
      title: "\u9879\u76ee\u6982\u89c8",
      subtitle:
        "\u5148\u4e86\u89e3 xh-helper \u7684\u5b9a\u4f4d\u3001\u4e3b\u8981\u9875\u9762\u548c\u7cfb\u7edf\u94fe\u8def"
    };
  }
  if (pathname.startsWith("/runs")) {
    return {
      title: "\u8fd0\u884c\u8ffd\u8e2a",
      subtitle:
        "\u67e5\u770b workflow\u3001\u6267\u884c\u6b65\u9aa4\u3001\u8bc1\u636e\u3001\u5224\u5b9a\u7ed3\u679c\u548c\u72b6\u6001\u53d8\u5316"
    };
  }
  if (pathname.startsWith("/playground")) {
    return {
      title: "\u53d1\u8d77\u4efb\u52a1",
      subtitle:
        "\u4ece\u5de5\u7a0b\u5316\u5165\u53e3\u521b\u5efa\u4efb\u52a1\uff0c\u9a8c\u8bc1\u4e0d\u540c\u8f93\u5165\u548c\u9884\u7b97\u4e0b\u7684\u7cfb\u7edf\u884c\u4e3a"
    };
  }
  if (pathname.startsWith("/approvals")) {
    return {
      title: "\u5ba1\u6279\u4e2d\u5fc3",
      subtitle: "\u5904\u7406\u9ad8\u98ce\u9669\u52a8\u4f5c\u3001\u4eba\u5de5\u786e\u8ba4\u548c\u7b49\u5f85\u6062\u590d"
    };
  }
  if (pathname.startsWith("/monitoring")) {
    return {
      title: "\u7cfb\u7edf\u89c2\u6d4b",
      subtitle: "\u67e5\u770b\u5065\u5eb7\u5ea6\u3001\u541e\u5410\u548c\u6574\u4f53\u8fd0\u884c\u60c5\u51b5"
    };
  }
  if (pathname.startsWith("/settings")) {
    return {
      title: "\u7cfb\u7edf\u8bbe\u7f6e",
      subtitle: "\u8c03\u6574\u7b56\u7565\u3001\u8bbf\u95ee\u63a7\u5236\u548c\u754c\u9762\u504f\u597d"
    };
  }
  return {
    title: "xh-helper",
    subtitle: "\u901a\u7528\u667a\u80fd\u4f53\u8fd0\u884c\u65f6"
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
  const isAssistantWorkspace = pathname.startsWith("/assistant");

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
    <div className={isAssistantWorkspace ? "app-shell app-shell-assistant-focus" : "app-shell"}>
      <aside className="app-sidebar">
        <div className="app-sidebar-top">
          <Link href="/" className="app-brand">
            <span className="app-brand-mark">xh</span>
            <div className="stack-gap-xs">
              <span className="app-brand-title">xh-helper</span>
              <span className="app-brand-subtitle">
                {isAssistantWorkspace ? "\u804a\u5929\u4e3a\u4e3b\uff0c\u9700\u65f6\u518d\u6267\u884c" : "\u901a\u7528\u667a\u80fd\u4f53\u8fd0\u884c\u65f6"}
              </span>
            </div>
          </Link>

          <Link href="/assistant" className="btn btn-primary app-compose-link">
            {"\u65b0\u5bf9\u8bdd"}
          </Link>
        </div>

        <SidebarNav compact={isAssistantWorkspace} />

        <div className="app-sidebar-footer">
          <div className="user-chip app-user-chip">
            <span>{auth.user?.email || "\u8bbf\u5ba2"}</span>
            <span className="muted-text">{auth.user?.role || "viewer"}</span>
          </div>

          <div className="app-sidebar-controls">
            <button type="button" className="btn btn-ghost" onClick={toggleTheme}>
              {theme === "dark" ? "\u6d45\u8272" : "\u6df1\u8272"}
            </button>
            <button
              type="button"
              className="btn btn-ghost"
              onClick={() => setLanguage(language === "zh" ? "en" : "zh")}
            >
              {language === "zh" ? "EN" : "\u4e2d\u6587"}
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
            {isAssistantWorkspace ? null : <span className="app-topbar-kicker">xh-helper</span>}
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
