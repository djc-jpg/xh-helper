"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const ITEMS = [
  { href: "/assistant", label: "对话", hint: "和 xh-helper 协作" },
  { href: "/runs", label: "运行", hint: "查看持久循环与追踪" },
  { href: "/playground", label: "发起", hint: "启动新的受控任务" },
  { href: "/approvals", label: "审批", hint: "处理高风险动作" },
  { href: "/monitoring", label: "观测", hint: "跟踪运行时健康状态" },
  { href: "/settings", label: "设置", hint: "调整策略与访问控制" }
];

export function SidebarNav() {
  const pathname = usePathname();

  return (
    <nav className="app-nav" aria-label="primary navigation">
      <div className="app-nav-section">
        <p className="app-nav-label">工作区</p>
        {ITEMS.map((item) => {
          const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
          return (
            <Link key={item.href} href={item.href} className={active ? "app-nav-item active" : "app-nav-item"}>
              <span className="app-nav-item-title">{item.label}</span>
              <span className="app-nav-item-hint">{item.hint}</span>
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
