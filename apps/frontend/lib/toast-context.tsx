"use client";

import { createContext, useCallback, useContext, useMemo, useState } from "react";

export type ToastKind = "info" | "success" | "warning" | "error";

export interface ToastMessage {
  id: string;
  title: string;
  description?: string;
  kind: ToastKind;
}

interface ToastContextValue {
  toasts: ToastMessage[];
  pushToast: (toast: Omit<ToastMessage, "id">) => void;
  removeToast: (id: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  const removeToast = useCallback((id: string) => {
    setToasts((prev) => prev.filter((item) => item.id !== id));
  }, []);

  const pushToast = useCallback((toast: Omit<ToastMessage, "id">) => {
    const id = crypto.randomUUID();
    setToasts((prev) => [...prev, { ...toast, id }]);
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((item) => item.id !== id));
    }, 4500);
  }, []);

  const value = useMemo(
    () => ({
      toasts,
      pushToast,
      removeToast
    }),
    [toasts, pushToast, removeToast]
  );

  return <ToastContext.Provider value={value}>{children}</ToastContext.Provider>;
}

export function useToast(): ToastContextValue {
  const context = useContext(ToastContext);
  if (!context) {
    throw new Error("useToast must be used inside ToastProvider");
  }
  return context;
}
