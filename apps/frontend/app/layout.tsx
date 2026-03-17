import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";

import { AppShell } from "../components/app-shell";
import { Providers } from "../lib/providers";

export const metadata: Metadata = {
  title: "多智能体闭环协作控制台",
  description: "MAS + LangGraph 专业控制台，提供闭环可观测、调试与灰度能力。"
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
