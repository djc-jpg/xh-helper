"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const PRIMARY_ITEMS = [
  { href: "/assistant", label: "对话", hint: "和 xh-helper 协作" },
];

const SECONDARY_ITEMS = [
  { href: "/runs", label: "运行", hint: "查看持久执行与追踪" },
  { href: "/playground", label: "发起", hint: "启动新的受控任务" },
  { href: "/approvals", label: "审批", hint: "处理高风险动作" },
  { href: "/monitoring", label: "观测", hint: "查看系统运行情况" },
  { href: "/settings", label: "设置", hint: "调整策略与访问控制" }
];

function NavList({ items, pathname }: { items: Array<{ href: string; label: string; hint: string }>; pathname: string }) {
  return (
    <>
      {items.map((item) => {
        const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
        return (
          <Link key={item.href} href={item.href} className={active ? "app-nav-item active" : "app-nav-item"}>
            <span className="app-nav-item-title">{item.label}</span>
            <span className="app-nav-item-hint">{item.hint}</span>
          </Link>
        );
      })}
    </>
  );
}

export function SidebarNav({ compact = false }: { compact?: boolean }) {
  const pathname = usePathname();
  const secondaryActive = SECONDARY_ITEMS.some((item) => pathname === item.href || pathname.startsWith(`${item.href}/`));
  const helperCopy = compact
    ? "先在这里完成对话；只有需要追踪任务、审批或排查问题时，再展开其它页面。"
    : "对话适合大多数场景，其它页面更适合排查和运维。";

  return (
    <nav className="app-nav" aria-label="primary navigation">
      <div className="app-nav-section">
        <p className="app-nav-label">{compact ? "当前使用" : "工作区"}</p>
        <NavList items={PRIMARY_ITEMS} pathname={pathname} />
      </div>

      {compact ? (
        <details className="app-nav-secondary" open={secondaryActive}>
          <summary>更多页面</summary>
          <div className="app-nav-section">
            <NavList items={SECONDARY_ITEMS} pathname={pathname} />
          </div>
        </details>
      ) : (
        <div className="app-nav-section">
          <p className="app-nav-label">更多页面</p>
          <NavList items={SECONDARY_ITEMS} pathname={pathname} />
        </div>
      )}

      <p className="app-nav-help">{helperCopy}</p>
    </nav>
  );
}
