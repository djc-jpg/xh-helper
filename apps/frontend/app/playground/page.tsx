"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useMemo } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { AuthGate } from "../../components/auth-gate";
import { ErrorBanner } from "../../components/error-banner";
import { JsonViewer } from "../../components/json-viewer";
import { SectionCard } from "../../components/section-card";
import { useAuth } from "../../lib/auth-context";
import { ApiError, createRun } from "../../lib/api";
import { useToast } from "../../lib/toast-context";

const TASK_TEMPLATES: Record<string, string> = {
  rag_qa: JSON.stringify({ question: "什么是多智能体闭环系统？", top_k: 3 }, null, 2),
  tool_flow: JSON.stringify({ action: "query", query: "force_500" }, null, 2),
  ticket_email: JSON.stringify({ content: "客户反馈支付异常，需要人工确认" }, null, 2),
  research_summary: JSON.stringify({ query: "LangGraph 闭环策略", top_k: 3 }, null, 2)
};

const schema = z.object({
  taskType: z.enum(["rag_qa", "tool_flow", "ticket_email", "research_summary"]),
  payloadText: z.string().min(2, "请输入 JSON 对象"),
  budget: z.coerce.number().positive("预算必须大于 0")
});

type FormValue = z.infer<typeof schema>;

export default function PlaygroundPage() {
  const auth = useAuth();
  const router = useRouter();
  const { pushToast } = useToast();
  const {
    register,
    handleSubmit,
    watch,
    setValue,
    formState: { errors }
  } = useForm<FormValue>({
    resolver: zodResolver(schema),
    defaultValues: {
      taskType: "rag_qa",
      payloadText: TASK_TEMPLATES.rag_qa,
      budget: 1
    }
  });

  const taskType = watch("taskType");
  const payloadText = watch("payloadText");

  const parsePreview = useMemo(() => {
    try {
      return JSON.parse(payloadText);
    } catch {
      return { error: "JSON 格式错误" };
    }
  }, [payloadText]);

  const mutation = useMutation({
    mutationFn: async (value: FormValue) => {
      if (!auth.accessToken) {
        throw new Error("token 为空，无法提交。请重新登录。");
      }
      let payload: Record<string, unknown>;
      try {
        const parsed = JSON.parse(value.payloadText) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          throw new Error("payload 必须是 JSON 对象");
        }
        payload = parsed as Record<string, unknown>;
      } catch {
        throw new Error("输入 JSON 无法解析，请修正后重试。");
      }
      return createRun({
        token: auth.accessToken,
        taskType: value.taskType,
        payload,
        budget: value.budget
      });
    },
    onSuccess(result) {
      pushToast({ kind: "success", title: "Run 已创建", description: `run_id=${result.run_id}` });
      router.push(`/runs/${result.run_id}`);
    },
    onError(error) {
      const message = error instanceof ApiError ? error.detail : error instanceof Error ? error.message : "创建失败";
      pushToast({ kind: "error", title: "创建失败", description: message });
    }
  });

  const onSubmit = handleSubmit((values) => mutation.mutate(values));

  return (
    <AuthGate>
      <div className="stack-gap-md">
        <SectionCard title="Playground：发起闭环 Run" subtitle="中文输入友好，支持模板、预算与 JSON 结构预览。">
          {!auth.accessToken ? (
            <ErrorBanner message="当前 token 为空，提交已禁用。请重新登录。" />
          ) : null}

          <form className="stack-gap-sm" onSubmit={onSubmit}>
            <div className="grid cols-3">
              <div className="stack-gap-xs">
                <label htmlFor="taskType">任务类型</label>
                <select
                  id="taskType"
                  {...register("taskType")}
                  onChange={(event) => {
                    const nextType = event.target.value as FormValue["taskType"];
                    setValue("taskType", nextType, { shouldValidate: true });
                    setValue("payloadText", TASK_TEMPLATES[nextType], { shouldValidate: true });
                  }}
                >
                  <option value="rag_qa">rag_qa</option>
                  <option value="tool_flow">tool_flow</option>
                  <option value="ticket_email">ticket_email</option>
                  <option value="research_summary">research_summary</option>
                </select>
              </div>

              <div className="stack-gap-xs">
                <label htmlFor="budget">预算（USD）</label>
                <input id="budget" type="number" step="0.1" {...register("budget")} />
              </div>

              <div className="stack-gap-xs">
                <span>模板快捷填充</span>
                <div className="inline-actions">
                  <button type="button" className="btn btn-ghost" onClick={() => setValue("payloadText", TASK_TEMPLATES[taskType])}>
                    按当前类型加载
                  </button>
                </div>
              </div>
            </div>

            <div className="stack-gap-xs">
              <label htmlFor="payloadText">输入 Payload（JSON）</label>
              <textarea id="payloadText" {...register("payloadText")} />
              {errors.payloadText ? <p className="muted-text">{errors.payloadText.message}</p> : null}
            </div>

            {mutation.error ? <ErrorBanner error={mutation.error} /> : null}

            <div className="inline-actions">
              <button type="submit" className="btn btn-primary" disabled={mutation.isPending || !auth.accessToken}>
                {mutation.isPending ? "创建中..." : "创建并追踪 Run"}
              </button>
            </div>
          </form>
        </SectionCard>

        <SectionCard title="Payload 预览" subtitle="用于提交前检查字段与结构。">
          <JsonViewer value={parsePreview} defaultExpanded title="解析结果" />
        </SectionCard>
      </div>
    </AuthGate>
  );
}
