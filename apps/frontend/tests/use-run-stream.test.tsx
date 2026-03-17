import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../lib/api", async () => {
  const actual = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return {
    ...actual,
    createEventsToken: vi.fn()
  };
});

import { useRunStream } from "../hooks/use-run-stream";
import { ApiError, createEventsToken } from "../lib/api";

class MockEventSource {
  static instances: MockEventSource[] = [];

  static reset(): void {
    MockEventSource.instances = [];
  }

  readonly url: string;
  readonly withCredentials = false;
  readyState = 0;
  onopen: ((this: EventSource, ev: Event) => unknown) | null = null;
  onmessage: ((this: EventSource, ev: MessageEvent) => unknown) | null = null;
  onerror: ((this: EventSource, ev: Event) => unknown) | null = null;
  private closed = false;
  private listeners = new Map<string, Array<(event: Event) => void>>();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    const list = this.listeners.get(type) || [];
    if (typeof listener === "function") {
      list.push(listener as (event: Event) => void);
    } else {
      list.push((event: Event) => listener.handleEvent(event));
    }
    this.listeners.set(type, list);
  }

  removeEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    const list = this.listeners.get(type) || [];
    const next = list.filter((item) => item !== listener);
    this.listeners.set(type, next);
  }

  dispatchEvent(_event: Event): boolean {
    return true;
  }

  close(): void {
    this.closed = true;
    this.readyState = 2;
  }

  emitOpen(): void {
    this.readyState = 1;
    const event = new Event("open");
    this.onopen?.call(this as unknown as EventSource, event);
    for (const listener of this.listeners.get("open") || []) {
      listener(event);
    }
  }

  emit(type: string, payload: Record<string, unknown>): void {
    const event = new MessageEvent(type, { data: JSON.stringify(payload) });
    for (const listener of this.listeners.get(type) || []) {
      listener(event);
    }
  }

  emitError(): void {
    const event = new Event("error");
    this.onerror?.call(this as unknown as EventSource, event);
  }

  isClosed(): boolean {
    return this.closed;
  }
}

const originalEventSource = globalThis.EventSource;
const originalHiddenDescriptor = Object.getOwnPropertyDescriptor(document, "hidden");

describe("useRunStream", () => {
  beforeEach(() => {
    MockEventSource.reset();
    vi.clearAllMocks();
    Object.defineProperty(globalThis, "EventSource", {
      configurable: true,
      writable: true,
      value: MockEventSource
    });
  });

  afterEach(() => {
    Object.defineProperty(globalThis, "EventSource", {
      configurable: true,
      writable: true,
      value: originalEventSource
    });
    if (originalHiddenDescriptor) {
      Object.defineProperty(document, "hidden", originalHiddenDescriptor);
    } else {
      delete (document as { hidden?: boolean }).hidden;
    }
  });

  it("stops stream when receiving done event", async () => {
    vi.mocked(createEventsToken).mockResolvedValue({
      token: "sse-token",
      expires_in_sec: 60
    });
    const onDone = vi.fn();

    const { result } = renderHook(() =>
      useRunStream({
        accessToken: "access-token",
        taskId: "task-1",
        enabled: true,
        shouldStop: false,
        onDone
      })
    );

    await waitFor(() => expect(MockEventSource.instances).toHaveLength(1));
    const source = MockEventSource.instances[0];

    act(() => {
      source.emitOpen();
    });
    await waitFor(() => expect(result.current.connected).toBe(true));

    act(() => {
      source.emit("done", { done: true });
    });
    await waitFor(() => expect(onDone).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.connected).toBe(false));
    expect(source.isClosed()).toBe(true);
  });

  it("calls onAuthExpired when events token request returns 401", async () => {
    vi.mocked(createEventsToken).mockRejectedValue(
      new ApiError({ status: 401, detail: "missing auth", requestId: "req-401" })
    );
    const onAuthExpired = vi.fn();

    renderHook(() =>
      useRunStream({
        accessToken: "expired-token",
        taskId: "task-2",
        enabled: true,
        shouldStop: false,
        onAuthExpired
      })
    );

    await waitFor(() => expect(onAuthExpired).toHaveBeenCalledTimes(1));
    expect(MockEventSource.instances).toHaveLength(0);
  });

  it("closes stream immediately when shouldStop turns true", async () => {
    vi.mocked(createEventsToken).mockResolvedValue({
      token: "sse-token-stop",
      expires_in_sec: 60
    });

    const { result, rerender } = renderHook(
      (props: { shouldStop: boolean }) =>
        useRunStream({
          accessToken: "access-token",
          taskId: "task-3",
          enabled: true,
          shouldStop: props.shouldStop
        }),
      {
        initialProps: { shouldStop: false }
      }
    );

    await waitFor(() => expect(MockEventSource.instances).toHaveLength(1));
    const source = MockEventSource.instances[0];

    act(() => {
      source.emitOpen();
    });
    await waitFor(() => expect(result.current.connected).toBe(true));

    rerender({ shouldStop: true });
    await waitFor(() => expect(source.isClosed()).toBe(true));
    await waitFor(() => expect(result.current.connected).toBe(false));
  });

  it("pauses connecting while page is hidden and resumes after visible", async () => {
    let hidden = true;
    Object.defineProperty(document, "hidden", {
      configurable: true,
      get: () => hidden
    });
    vi.mocked(createEventsToken).mockResolvedValue({
      token: "sse-token-hidden",
      expires_in_sec: 60
    });

    renderHook(() =>
      useRunStream({
        accessToken: "access-token",
        taskId: "task-4",
        enabled: true,
        shouldStop: false,
        pauseWhenHidden: true
      })
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(createEventsToken).not.toHaveBeenCalled();
    hidden = false;
    act(() => {
      document.dispatchEvent(new Event("visibilitychange"));
    });

    await waitFor(() => expect(createEventsToken).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(MockEventSource.instances).toHaveLength(1));
  });
});
