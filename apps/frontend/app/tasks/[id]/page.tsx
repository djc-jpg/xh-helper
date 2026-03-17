"use client";

import { useQuery } from "@tanstack/react-query";
import { useParams, useRouter } from "next/navigation";
import { useEffect } from "react";

import { AuthGate } from "../../../components/auth-gate";
import { ErrorBanner } from "../../../components/error-banner";
import { PageSkeleton } from "../../../components/skeleton";
import { useAuth } from "../../../lib/auth-context";
import { getTaskDetail } from "../../../lib/api";

export default function LegacyTaskDetailRedirectPage() {
  const auth = useAuth();
  const router = useRouter();
  const params = useParams();
  const taskId = String(params.id || "");
  const canQuery = auth.ready && auth.isAuthenticated;

  const query = useQuery({
    queryKey: ["legacy-task", taskId],
    enabled: canQuery && Boolean(taskId),
    queryFn: () => getTaskDetail(auth.accessToken, taskId)
  });

  useEffect(() => {
    if (!query.data) return;
    const runs = query.data.runs || [];
    if (runs.length === 0) {
      router.replace("/runs");
      return;
    }
    const sorted = [...runs].sort((a, b) => Number(a.run_no || 0) - Number(b.run_no || 0));
    const latest = sorted[sorted.length - 1];
    router.replace(`/runs/${String(latest.id)}`);
  }, [query.data, router]);

  return (
    <AuthGate>
      {query.isLoading ? <PageSkeleton /> : null}
      {query.error ? <ErrorBanner error={query.error} onRetry={() => query.refetch()} /> : null}
      {!query.isLoading && !query.error ? <p className="muted-text">正在跳转到 Run 详情...</p> : null}
    </AuthGate>
  );
}
