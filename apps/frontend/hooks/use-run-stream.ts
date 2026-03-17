"use client";

import { useEffect, useRef, useState } from "react";

import { ApiError, API_BASE, createEventsToken } from "../lib/api";

interface StreamCallbacks {
  onStatus?: (payload: Record<string, unknown>) => void;
  onStep?: (payload: Record<string, unknown>) => void;
  onDone?: () => void;
  onError?: (message: string) => void;
  onAuthExpired?: () => void;
}

interface UseRunStreamInput extends StreamCallbacks {
  accessToken: string;
  taskId: string;
  enabled: boolean;
  shouldStop: boolean;
  maxRetries?: number;
  maxReconnectDurationMs?: number;
  pauseWhenHidden?: boolean;
}

function parseEventData(raw: string): Record<string, unknown> {
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return { raw };
  }
}

export function useRunStream({
  accessToken,
  taskId,
  enabled,
  shouldStop,
  maxRetries = 30,
  maxReconnectDurationMs = 5 * 60 * 1000,
  pauseWhenHidden = true,
  onStatus,
  onStep,
  onDone,
  onError,
  onAuthExpired
}: UseRunStreamInput) {
  const [connected, setConnected] = useState(false);
  const retriesRef = useRef(0);
  const eventSourceRef = useRef<EventSource | null>(null);
  const stoppedRef = useRef(false);
  const reconnectStartedAtRef = useRef(0);
  const connectPromiseRef = useRef<Promise<void> | null>(null);

  useEffect(() => {
    stoppedRef.current = shouldStop;
    if (shouldStop) {
      eventSourceRef.current?.close();
      eventSourceRef.current = null;
      setConnected(false);
    }
  }, [shouldStop]);

  useEffect(() => {
    if (!enabled || !accessToken || !taskId || shouldStop) {
      return;
    }

    reconnectStartedAtRef.current = Date.now();
    let cancelled = false;
    let reconnectTimer: number | null = null;

    const cleanup = () => {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      eventSourceRef.current?.close();
      eventSourceRef.current = null;
      setConnected(false);
    };

    const scheduleReconnect = () => {
      if (cancelled || stoppedRef.current) {
        return;
      }
      if (retriesRef.current >= maxRetries) {
        onError?.("连接不稳定，请点击重试。");
        return;
      }
      if (Date.now() - reconnectStartedAtRef.current >= maxReconnectDurationMs) {
        onError?.("连接不稳定，请点击重试。");
        return;
      }
      const delay = Math.min(1000 * 2 ** retriesRef.current, 8000);
      retriesRef.current += 1;
      reconnectTimer = window.setTimeout(() => {
        connect().catch(() => undefined);
      }, delay);
    };

    const connect = async () => {
      if (cancelled || stoppedRef.current || eventSourceRef.current) {
        return;
      }
      if (pauseWhenHidden && typeof document !== "undefined" && document.hidden) {
        setConnected(false);
        return;
      }
      try {
        const tokenPayload = await createEventsToken(accessToken, taskId);
        if (cancelled || stoppedRef.current) {
          return;
        }

        const url = `${API_BASE}/events?task_id=${encodeURIComponent(taskId)}&sse_token=${encodeURIComponent(
          tokenPayload.token
        )}`;
        const es = new EventSource(url);
        eventSourceRef.current = es;

        es.addEventListener("open", () => {
          retriesRef.current = 0;
          reconnectStartedAtRef.current = Date.now();
          setConnected(true);
        });

        es.addEventListener("status", (event) => {
          const message = event as MessageEvent;
          onStatus?.(parseEventData(message.data));
        });

        es.addEventListener("step", (event) => {
          const message = event as MessageEvent;
          onStep?.(parseEventData(message.data));
        });

        es.addEventListener("done", () => {
          stoppedRef.current = true;
          onDone?.();
          cleanup();
        });

        es.onerror = () => {
          setConnected(false);
          es.close();
          scheduleReconnect();
        };
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          onAuthExpired?.();
          return;
        }
        onError?.(error instanceof Error ? error.message : "SSE 初始化失败");
      }
    };

    const ensureConnected = () => {
      if (connectPromiseRef.current) {
        return connectPromiseRef.current;
      }
      const pending = connect().finally(() => {
        if (connectPromiseRef.current === pending) {
          connectPromiseRef.current = null;
        }
      });
      connectPromiseRef.current = pending;
      return pending;
    };

    const onVisibilityChange = () => {
      if (!pauseWhenHidden) {
        return;
      }
      if (document.hidden) {
        eventSourceRef.current?.close();
        eventSourceRef.current = null;
        setConnected(false);
        return;
      }
      if (!cancelled && !stoppedRef.current && !eventSourceRef.current) {
        ensureConnected().catch(() => undefined);
      }
    };

    if (pauseWhenHidden && typeof document !== "undefined") {
      document.addEventListener("visibilitychange", onVisibilityChange);
    }

    ensureConnected().catch(() => undefined);

    return () => {
      cancelled = true;
      if (pauseWhenHidden && typeof document !== "undefined") {
        document.removeEventListener("visibilitychange", onVisibilityChange);
      }
      cleanup();
    };
  }, [
    accessToken,
    enabled,
    maxReconnectDurationMs,
    maxRetries,
    onAuthExpired,
    onDone,
    onError,
    onStatus,
    onStep,
    pauseWhenHidden,
    shouldStop,
    taskId
  ]);

  return {
    connected
  };
}
