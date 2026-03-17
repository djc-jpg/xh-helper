"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { ErrorBanner } from "../../components/error-banner";
import { useAuth } from "../../lib/auth-context";
import { ApiError } from "../../lib/api";
import { AuthStorageMode } from "../../lib/mas-types";

const schema = z
  .object({
    email: z.string().email("请输入有效邮箱"),
    password: z.string().min(8, "密码至少 8 位"),
    confirmPassword: z.string().min(8, "请再次输入密码"),
    rememberMode: z.enum(["default", "sessionStorage", "localStorage"])
  })
  .refine((value) => value.password === value.confirmPassword, {
    path: ["confirmPassword"],
    message: "两次输入密码不一致"
  });

type FormValue = z.infer<typeof schema>;

export default function RegisterPage() {
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
      confirmPassword: "",
      rememberMode: "default"
    }
  });

  useEffect(() => {
    if (auth.ready && auth.isAuthenticated) {
      router.replace("/runs");
    }
  }, [auth.isAuthenticated, auth.ready, router]);

  const onSubmit = handleSubmit(async (values) => {
    try {
      const override: AuthStorageMode | null =
        values.rememberMode === "default" ? null : (values.rememberMode as AuthStorageMode);
      await auth.signUp({
        email: values.email,
        password: values.password,
        modeOverride: override
      });
      router.replace("/runs");
    } catch (error) {
      const message = error instanceof ApiError ? error.detail : error instanceof Error ? error.message : "注册失败";
      setError("root", { message });
    }
  });

  return (
    <div className="panel stack-gap-md">
      <div className="stack-gap-xs">
        <h2 className="page-title">创建控制台账号</h2>
        <p className="page-subtitle">注册后将自动登录并进入 MAS 闭环运行页。</p>
      </div>

      <form className="stack-gap-sm" onSubmit={onSubmit}>
        <div className="stack-gap-xs">
          <label htmlFor="email">邮箱</label>
          <input id="email" {...register("email")} autoComplete="email" placeholder="user@example.com" />
          {errors.email ? <p className="muted-text">{errors.email.message}</p> : null}
        </div>

        <div className="stack-gap-xs">
          <label htmlFor="password">密码</label>
          <input id="password" type="password" {...register("password")} autoComplete="new-password" />
          {errors.password ? <p className="muted-text">{errors.password.message}</p> : null}
        </div>

        <div className="stack-gap-xs">
          <label htmlFor="confirmPassword">确认密码</label>
          <input id="confirmPassword" type="password" {...register("confirmPassword")} autoComplete="new-password" />
          {errors.confirmPassword ? <p className="muted-text">{errors.confirmPassword.message}</p> : null}
        </div>

        <div className="stack-gap-xs">
          <label htmlFor="rememberMode">会话存储模式</label>
          <select id="rememberMode" {...register("rememberMode")} disabled={!auth.canOverrideMode}>
            <option value="default">使用系统默认</option>
            <option value="sessionStorage">sessionStorage</option>
            <option value="localStorage">localStorage（仅受信设备）</option>
          </select>
        </div>

        {errors.root?.message ? <ErrorBanner message={errors.root.message} /> : null}

        <div className="inline-actions">
          <button type="submit" className="btn btn-primary" disabled={isSubmitting}>
            {isSubmitting ? "创建中..." : "注册并登录"}
          </button>
          <Link href="/login" className="btn btn-ghost">
            已有账号？去登录
          </Link>
        </div>
      </form>
    </div>
  );
}
