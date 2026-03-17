"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTranslation } from "react-i18next";

const ITEMS = [
  { href: "/assistant", key: "assistant", label: "通用助手" },
  { href: "/runs", key: "runs", label: "闭环运行" },
  { href: "/playground", key: "playground", label: "发起 Run" },
  { href: "/approvals", key: "approvals", label: "审批流" },
  { href: "/monitoring", key: "monitoring", label: "观测" },
  { href: "/settings", key: "settings", label: "设置" }
];

export function SidebarNav() {
  const pathname = usePathname();
  const { t } = useTranslation();

  return (
    <nav className="sidebar-nav" aria-label="primary navigation">
      {ITEMS.map((item) => {
        const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
        return (
          <Link key={item.href} href={item.href} className={active ? "nav-item active" : "nav-item"}>
            <span>{t(item.key, item.label)}</span>
          </Link>
        );
      })}
    </nav>
  );
}
