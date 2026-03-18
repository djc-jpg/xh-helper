import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";

import { AppShell } from "../components/app-shell";
import { Providers } from "../lib/providers";

export const metadata: Metadata = {
  title: "xh-helper",
  description: "xh-helper 是一个面向持久目标、策略调试、审批治理和外部事件驱动执行的通用智能体工作台。"
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="zh-CN">
      <body>
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
