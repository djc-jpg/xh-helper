"use client";

import { useToast } from "../lib/toast-context";

export function ToastCenter() {
  const { toasts, removeToast } = useToast();

  return (
    <div className="toast-center" aria-live="polite" aria-atomic="true" data-testid="toast-center">
      {toasts.map((toast) => (
        <button
          key={toast.id}
          type="button"
          data-testid={`toast-item-${toast.kind}`}
          className={`toast-item toast-${toast.kind}`}
          onClick={() => removeToast(toast.id)}
        >
          <span className="toast-title">{toast.title}</span>
          {toast.description ? <span className="toast-desc">{toast.description}</span> : null}
        </button>
      ))}
    </div>
  );
}
