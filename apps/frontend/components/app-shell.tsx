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

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const { t } = useTranslation();
  const auth = useAuth();
  const { theme, toggleTheme } = useTheme();
  const { language, setLanguage } = useLocale();

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
    <div className="console-shell">
      <aside className="sidebar">
        <div className="brand-wrap">
          <p className="brand-title">多智能体闭环协作</p>
          <p className="brand-subtitle">MAS + LangGraph</p>
        </div>
        <SidebarNav />
      </aside>

      <div className="console-main-wrap">
        <header className="topbar">
          <div className="topbar-left">
            <h1 className="console-title">{t("appTitle")}</h1>
            <p className="console-subtitle">{t("appSubtitle")}</p>
          </div>
          <div className="topbar-right">
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
              <>
                <div className="user-chip">
                  <span>{auth.user?.email}</span>
                  <span className="muted-text">{auth.user?.role}</span>
                </div>
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
              </>
            ) : (
              <Link href="/login" className="btn btn-primary">
                {t("login")}
              </Link>
            )}
          </div>
        </header>

        <main className="page-content">{children}</main>
      </div>
      <ToastCenter />
    </div>
  );
}
