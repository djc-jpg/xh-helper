"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { AuthGate } from "../../components/auth-gate";
import { ErrorBanner } from "../../components/error-banner";
import { useAuth } from "../../lib/auth-context";
import { getDisplayErrorMessage } from "../../lib/api";
import { AuthStorageMode } from "../../lib/mas-types";

const schema = z.object({
  email: z.string().email("请输入有效邮箱"),
  password: z.string().min(8, "密码至少 8 位"),
  rememberMode: z.enum(["default", "sessionStorage", "localStorage"])
});

type FormValue = z.infer<typeof schema>;

export default function LoginPage() {
  const auth = useAuth();
  const router = useRouter();
  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
    setError
  } = useForm<FormValue>({
    resolver: zodResolver(schema),
    defaultValues: {
      email: "",
      password: "",
      rememberMode: "default"
    }
  });

  useEffect(() => {
    if (auth.ready && auth.isAuthenticated) {
      router.replace("/assistant");
    }
  }, [auth.isAuthenticated, auth.ready, router]);

  const storageHint = useMemo(() => {
    if (auth.storageMode === "memory") {
      return "当前默认是 memory：刷新页面后会话将失效。";
    }
    return `当前默认存储模式：${auth.storageMode}`;
  }, [auth.storageMode]);

  const onSubmit = handleSubmit(async (values) => {
    try {
      const override: AuthStorageMode | null =
        values.rememberMode === "default" ? null : (values.rememberMode as AuthStorageMode);
      await auth.signIn({
        email: values.email,
        password: values.password,
        modeOverride: override
      });
      router.replace("/assistant");
    } catch (error) {
      setError("root", { message: getDisplayErrorMessage(error) });
    }
  });

  return (
    <div className="panel stack-gap-md">
      <div className="stack-gap-xs">
        <h2 className="page-title">登录你的账号</h2>
        <p className="page-subtitle">登录后就可以继续对话、查看任务进展，并处理需要你确认的操作。</p>
      </div>

      {auth.sessionHint ? <ErrorBanner message={auth.sessionHint} /> : null}

      <form className="stack-gap-sm" onSubmit={onSubmit} data-testid="login-form">
        <div className="stack-gap-xs">
          <label htmlFor="email">邮箱</label>
          <input
            id="email"
            data-testid="login-email"
            {...register("email")}
            placeholder="owner@example.com"
            autoComplete="email"
          />
          {errors.email ? <p className="muted-text">{errors.email.message}</p> : null}
        </div>

        <div className="stack-gap-xs">
          <label htmlFor="password">密码</label>
          <input
            id="password"
            data-testid="login-password"
            type="password"
            {...register("password")}
            autoComplete="current-password"
          />
          {errors.password ? <p className="muted-text">{errors.password.message}</p> : null}
        </div>

        <div className="stack-gap-xs">
          <label htmlFor="rememberMode">保持登录（受信设备）</label>
          <select id="rememberMode" data-testid="login-remember-mode" {...register("rememberMode")} disabled={!auth.canOverrideMode}>
            <option value="default">使用系统默认</option>
            <option value="sessionStorage">仅当前标签页（sessionStorage）</option>
            <option value="localStorage">跨刷新保持（localStorage）</option>
          </select>
          <p className="muted-text">
            {storageHint} 如果你在自己的设备上使用，可以选择跨刷新保持登录。
          </p>
        </div>

        {errors.root?.message ? <ErrorBanner message={errors.root.message} /> : null}

        <div className="inline-actions">
          <button type="submit" className="btn btn-primary" disabled={isSubmitting} data-testid="login-submit">
            {isSubmitting ? "登录中..." : "登录"}
          </button>
          <Link href="/register" className="btn btn-ghost">
            没有账号？去创建
          </Link>
        </div>
      </form>

      {auth.isAuthenticated ? (
        <AuthGate>
          <p className="muted-text">已登录，正在跳转到运行页面...</p>
        </AuthGate>
      ) : null}
    </div>
  );
}
