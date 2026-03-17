"use client";

import { AuthGate } from "../../components/auth-gate";
import { SectionCard } from "../../components/section-card";
import { useAuth } from "../../lib/auth-context";
import { storageModeLabel } from "../../lib/api";
import { useLocale } from "../../lib/locale-context";
import { AuthStorageMode } from "../../lib/mas-types";
import { useTheme } from "../../lib/theme-context";

export default function SettingsPage() {
  const auth = useAuth();
  const { theme, setTheme } = useTheme();
  const { language, setLanguage } = useLocale();

  return (
    <AuthGate>
      <div className="stack-gap-md">
        <SectionCard title="控制台设置" subtitle="主题、语言与会话策略（开发可灰度）。">
          <div className="grid cols-3">
            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">语言</p>
              <select value={language} onChange={(event) => setLanguage(event.target.value as "zh" | "en")}> 
                <option value="zh">中文</option>
                <option value="en">English</option>
              </select>
            </div>

            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">主题</p>
              <select value={theme} onChange={(event) => setTheme(event.target.value as "light" | "dark")}> 
                <option value="light">浅色</option>
                <option value="dark">深色</option>
              </select>
            </div>

            <div className="sub-panel stack-gap-xs">
              <p className="panel-subtitle">Auth 存储模式</p>
              <select
                value={auth.storageMode}
                onChange={(event) => auth.updateStorageMode(event.target.value as AuthStorageMode)}
                disabled={!auth.canOverrideMode}
              >
                <option value="memory">memory</option>
                <option value="sessionStorage">sessionStorage</option>
                <option value="localStorage">localStorage</option>
              </select>
            </div>
          </div>
        </SectionCard>

        <SectionCard title="会话与安全说明" subtitle="面向生产的推荐策略与风险提示。">
          <div className="stack-gap-sm">
            <p>
              当前模式：<strong>{storageModeLabel(auth.storageMode)}</strong>
            </p>
            <p className="muted-text">
              生产默认建议使用 <code>memory</code> 或边缘网关托管的 HttpOnly Secure Cookie。若使用 memory，刷新后会话会失效，
              页面将提示重新登录，这是安全默认行为。
            </p>
            <p className="muted-text">
              <code>localStorage</code> 仅建议开发或受信设备调试使用，存在更高的 XSS 持久化风险。
            </p>
          </div>
        </SectionCard>
      </div>
    </AuthGate>
  );
}
