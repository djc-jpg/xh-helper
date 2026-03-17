"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";

import {
  authLogin,
  authLogout,
  authRefresh,
  authRegister,
  decodeUserClaims,
  setUnauthorizedHandler,
  type UserClaims
} from "./api";
import {
  canOverrideStorageMode,
  clearMemoryMarker,
  clearTokens,
  consumedMemorySessionMarker,
  defaultStorageMode,
  getStorageMode,
  markMemorySession,
  readTokens,
  saveTokens,
  setStorageMode
} from "./auth-storage";
import { AuthStorageMode } from "./mas-types";
import { useToast } from "./toast-context";

interface AuthContextValue {
  ready: boolean;
  isAuthenticated: boolean;
  user: UserClaims | null;
  accessToken: string;
  refreshToken: string;
  storageMode: AuthStorageMode;
  canOverrideMode: boolean;
  sessionHint: string;
  signIn: (params: { email: string; password: string; modeOverride?: AuthStorageMode | null }) => Promise<void>;
  signUp: (params: { email: string; password: string; modeOverride?: AuthStorageMode | null }) => Promise<void>;
  signOut: () => Promise<void>;
  forceSessionEnded: (hint: string) => void;
  updateStorageMode: (mode: AuthStorageMode) => void;
  tryRefresh: () => Promise<boolean>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function isTokenExpired(claims: UserClaims | null): boolean {
  if (!claims?.exp) return true;
  const nowSec = Math.floor(Date.now() / 1000);
  return claims.exp <= nowSec;
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const { pushToast } = useToast();
  const lastSessionEndedAtRef = useRef(0);
  const [ready, setReady] = useState(false);
  const [accessToken, setAccessToken] = useState("");
  const [refreshToken, setRefreshToken] = useState("");
  const [user, setUser] = useState<UserClaims | null>(null);
  const [storageMode, setStorageModeState] = useState<AuthStorageMode>(defaultStorageMode());
  const [sessionHint, setSessionHint] = useState("");

  const handleSessionEnded = useCallback(
    (hint: string) => {
      const now = Date.now();
      if (now - lastSessionEndedAtRef.current < 1000) {
        return;
      }
      lastSessionEndedAtRef.current = now;
      clearTokens();
      setAccessToken("");
      setRefreshToken("");
      setUser(null);
      setSessionHint(hint);
      pushToast({
        kind: "warning",
        title: "会话失效",
        description: hint
      });
    },
    [pushToast]
  );

  useEffect(() => {
    const mode = getStorageMode();
    setStorageModeState(mode);
    const tokens = readTokens(mode);
    const claims = decodeUserClaims(tokens.accessToken);

    if (tokens.accessToken && claims && !isTokenExpired(claims)) {
      setAccessToken(tokens.accessToken);
      setRefreshToken(tokens.refreshToken);
      setUser(claims);
      setSessionHint("");
    } else {
      clearTokens();
      if (mode === "memory" && consumedMemorySessionMarker()) {
        setSessionHint("会话已结束（内存会话在刷新后会丢失），请重新登录。");
      }
    }
    setReady(true);
  }, []);

  useEffect(() => {
    setUnauthorizedHandler(() => {
      handleSessionEnded("登录已过期，请重新登录。");
    });
    return () => {
      setUnauthorizedHandler(null);
    };
  }, [handleSessionEnded]);

  useEffect(() => {
    if (storageMode !== "memory") {
      clearMemoryMarker();
      return;
    }
    const onBeforeUnload = () => {
      if (accessToken) {
        markMemorySession();
      }
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => window.removeEventListener("beforeunload", onBeforeUnload);
  }, [accessToken, storageMode]);

  const persistTokens = useCallback(
    (
      tokens: {
        access_token: string;
        refresh_token: string;
      },
      modeOverride?: AuthStorageMode | null
    ) => {
      const mode = modeOverride || storageMode;
      if (modeOverride && canOverrideStorageMode()) {
        setStorageMode(modeOverride);
        setStorageModeState(modeOverride);
      }
      saveTokens(mode, tokens.access_token, tokens.refresh_token);
      const claims = decodeUserClaims(tokens.access_token);
      setAccessToken(tokens.access_token);
      setRefreshToken(tokens.refresh_token);
      setUser(claims);
      setSessionHint("");
    },
    [storageMode]
  );

  const signIn = useCallback(
    async ({ email, password, modeOverride }: { email: string; password: string; modeOverride?: AuthStorageMode | null }) => {
      const tokens = await authLogin(email, password);
      persistTokens(tokens, modeOverride);
      pushToast({ kind: "success", title: "登录成功", description: "欢迎回来" });
    },
    [persistTokens, pushToast]
  );

  const signUp = useCallback(
    async ({ email, password, modeOverride }: { email: string; password: string; modeOverride?: AuthStorageMode | null }) => {
      const tokens = await authRegister(email, password);
      persistTokens(tokens, modeOverride);
      pushToast({ kind: "success", title: "注册成功", description: "已自动登录" });
    },
    [persistTokens, pushToast]
  );

  const signOut = useCallback(async () => {
    try {
      if (accessToken && refreshToken) {
        await authLogout(accessToken, refreshToken);
      }
    } catch {
      // Best effort.
    }
    clearTokens();
    setAccessToken("");
    setRefreshToken("");
    setUser(null);
    setSessionHint("");
    pushToast({ kind: "info", title: "已退出登录" });
  }, [accessToken, refreshToken, pushToast]);

  const updateStorageMode = useCallback(
    (mode: AuthStorageMode) => {
      if (!canOverrideStorageMode()) {
        return;
      }
      setStorageMode(mode);
      setStorageModeState(mode);
      if (accessToken && refreshToken) {
        saveTokens(mode, accessToken, refreshToken);
      }
      pushToast({ kind: "info", title: "鉴权存储模式已更新", description: `当前模式：${mode}` });
    },
    [accessToken, refreshToken, pushToast]
  );

  const tryRefresh = useCallback(async () => {
    if (!refreshToken) {
      return false;
    }
    try {
      const tokens = await authRefresh(refreshToken);
      persistTokens(tokens);
      return true;
    } catch {
      handleSessionEnded("会话刷新失败，请重新登录。");
      return false;
    }
  }, [refreshToken, persistTokens, handleSessionEnded]);

  const value = useMemo<AuthContextValue>(
    () => ({
      ready,
      isAuthenticated: Boolean(accessToken && user),
      user,
      accessToken,
      refreshToken,
      storageMode,
      canOverrideMode: canOverrideStorageMode(),
      sessionHint,
      signIn,
      signUp,
      signOut,
      forceSessionEnded: handleSessionEnded,
      updateStorageMode,
      tryRefresh
    }),
    [
      ready,
      accessToken,
      refreshToken,
      user,
      storageMode,
      sessionHint,
      signIn,
      signUp,
      signOut,
      handleSessionEnded,
      updateStorageMode,
      tryRefresh
    ]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return context;
}

export { AuthContext };
