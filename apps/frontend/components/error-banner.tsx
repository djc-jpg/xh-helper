import { useMemo, useState } from "react";

import { ApiError, getDisplayErrorMessage } from "../lib/api";

interface ErrorBannerProps {
  message?: string;
  error?: unknown;
  onRetry?: () => void;
}

function asApiError(error: unknown): ApiError | null {
  if (error instanceof ApiError) {
    return error;
  }
  if (error && typeof error === "object") {
    const record = error as Record<string, unknown>;
    if (
      typeof record.status === "number" &&
      typeof record.detail === "string" &&
      typeof record.requestId === "string"
    ) {
      return new ApiError({
        status: record.status,
        detail: record.detail,
        requestId: record.requestId
      });
    }
  }
  return null;
}

export function ErrorBanner({ message, error, onRetry }: ErrorBannerProps) {
  const [copied, setCopied] = useState(false);
  const apiError = asApiError(error);
  const text = useMemo(() => {
    if (message) return message;
    return getDisplayErrorMessage(apiError || error);
  }, [apiError, error, message]);

  const copyRequestId = async () => {
    if (!apiError?.requestId) {
      return;
    }
    try {
      await navigator.clipboard.writeText(apiError.requestId);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div className="error-banner" role="alert">
      <span>{text}</span>
      {apiError?.requestId ? (
        <span className="inline-actions">
          <code>{apiError.requestId}</code>
          <button type="button" className="btn btn-ghost" onClick={() => copyRequestId().catch(() => undefined)}>
            {copied ? "已复制" : "复制 Request ID"}
          </button>
        </span>
      ) : null}
      {onRetry ? (
        <button type="button" className="btn btn-ghost" onClick={onRetry}>
          重试
        </button>
      ) : null}
    </div>
  );
}
