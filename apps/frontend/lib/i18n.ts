"use client";

import i18n from "i18next";
import { initReactI18next } from "react-i18next";

const resources = {
  zh: {
    translation: {
      appTitle: "多智能体闭环协作控制台",
      appSubtitle: "MAS + LangGraph 运行观测与调试",
      loginExpired: "登录已过期，请重新登录。",
      sessionEnded: "会话已结束（内存会话在刷新后会丢失），请重新登录。",
      loginRequired: "请先登录后再访问此页面。",
      noData: "暂无数据",
      loading: "加载中...",
      retry: "重试",
      assistant: "通用助手",
      runs: "运行",
      playground: "发起 Run",
      approvals: "审批",
      monitoring: "监控",
      settings: "设置",
      login: "登录",
      register: "注册",
      logout: "退出登录"
    }
  },
  en: {
    translation: {
      appTitle: "Multi-Agent Closed-Loop Console",
      appSubtitle: "MAS + LangGraph Operations and Debugging",
      loginExpired: "Session expired. Please sign in again.",
      sessionEnded: "Session ended. Memory mode is cleared after refresh.",
      loginRequired: "Please sign in before using this page.",
      noData: "No data",
      loading: "Loading...",
      retry: "Retry",
      assistant: "Assistant",
      runs: "Runs",
      playground: "Playground",
      approvals: "Approvals",
      monitoring: "Monitoring",
      settings: "Settings",
      login: "Login",
      register: "Register",
      logout: "Sign out"
    }
  }
} as const;

let initialized = false;

export function setupI18n(defaultLanguage: "zh" | "en" = "zh"): void {
  if (initialized) {
    return;
  }
  i18n.use(initReactI18next).init({
    resources,
    lng: defaultLanguage,
    fallbackLng: "zh",
    react: {
      useSuspense: false
    },
    interpolation: {
      escapeValue: false
    }
  });
  initialized = true;
}

export { i18n };
