"use client";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

import { ApiError } from "./api";
import { AuthProvider } from "./auth-context";
import { LocaleProvider } from "./locale-context";
import { ThemeProvider } from "./theme-context";
import { ToastProvider } from "./toast-context";

function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 5000,
        refetchOnWindowFocus: true,
        retry(failureCount, error) {
          if (!(error instanceof ApiError)) {
            return failureCount < 2;
          }
          if (error.status === 401 || error.status === 403 || error.status === 404) {
            return false;
          }
          if (error.status === 429) {
            return failureCount < 3;
          }
          return error.status >= 500 && failureCount < 2;
        },
        retryDelay(attempt) {
          return Math.min(1000 * 2 ** attempt, 8000);
        }
      },
      mutations: {
        retry(failureCount, error) {
          if (!(error instanceof ApiError)) {
            return failureCount < 1;
          }
          return error.status >= 500 && failureCount < 1;
        }
      }
    }
  });
}

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(createQueryClient);

  return (
    <ThemeProvider>
      <LocaleProvider>
        <ToastProvider>
          <AuthProvider>
            <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
          </AuthProvider>
        </ToastProvider>
      </LocaleProvider>
    </ThemeProvider>
  );
}
