import { AuthStorageMode } from "./mas-types";

const ACCESS_TOKEN_KEY = "xh_access_token";
const REFRESH_TOKEN_KEY = "xh_refresh_token";
const MODE_OVERRIDE_KEY = "xh_auth_storage_override";
const MEMORY_SESSION_MARKER_KEY = "xh_memory_session_marker";

let memoryAccessToken = "";
let memoryRefreshToken = "";

function normalizeMode(value: string | null | undefined): AuthStorageMode {
  if (value === "sessionStorage") return "sessionStorage";
  if (value === "localStorage") return "localStorage";
  return "memory";
}

export function defaultStorageMode(): AuthStorageMode {
  const fromEnv = (process.env.NEXT_PUBLIC_AUTH_STORAGE || "").trim();
  if (fromEnv === "sessionStorage") return "sessionStorage";
  if (fromEnv === "localStorage") return "localStorage";
  if (fromEnv === "memory") return "memory";
  return process.env.NODE_ENV === "production" ? "memory" : "localStorage";
}

export function canOverrideStorageMode(): boolean {
  return process.env.NODE_ENV !== "production";
}

export function getStorageMode(): AuthStorageMode {
  if (typeof window === "undefined") {
    return defaultStorageMode();
  }
  const base = defaultStorageMode();
  if (!canOverrideStorageMode()) {
    return base;
  }
  const override = normalizeMode(window.localStorage.getItem(MODE_OVERRIDE_KEY));
  return override || base;
}

export function setStorageMode(mode: AuthStorageMode): void {
  if (typeof window === "undefined" || !canOverrideStorageMode()) {
    return;
  }
  window.localStorage.setItem(MODE_OVERRIDE_KEY, mode);
}

function getStorage(mode: AuthStorageMode): Storage | null {
  if (typeof window === "undefined") {
    return null;
  }
  if (mode === "localStorage") {
    return window.localStorage;
  }
  if (mode === "sessionStorage") {
    return window.sessionStorage;
  }
  return null;
}

export function readTokens(mode: AuthStorageMode): { accessToken: string; refreshToken: string } {
  if (mode === "memory") {
    return {
      accessToken: memoryAccessToken,
      refreshToken: memoryRefreshToken
    };
  }
  const storage = getStorage(mode);
  return {
    accessToken: storage?.getItem(ACCESS_TOKEN_KEY) || "",
    refreshToken: storage?.getItem(REFRESH_TOKEN_KEY) || ""
  };
}

export function saveTokens(mode: AuthStorageMode, accessToken: string, refreshToken: string): void {
  if (mode === "memory") {
    memoryAccessToken = accessToken;
    memoryRefreshToken = refreshToken;
    markMemorySession();
    return;
  }
  const storage = getStorage(mode);
  storage?.setItem(ACCESS_TOKEN_KEY, accessToken);
  storage?.setItem(REFRESH_TOKEN_KEY, refreshToken);
  clearMemoryMarker();
}

export function clearTokens(): void {
  memoryAccessToken = "";
  memoryRefreshToken = "";
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.removeItem(ACCESS_TOKEN_KEY);
  window.localStorage.removeItem(REFRESH_TOKEN_KEY);
  window.sessionStorage.removeItem(ACCESS_TOKEN_KEY);
  window.sessionStorage.removeItem(REFRESH_TOKEN_KEY);
  clearMemoryMarker();
}

export function markMemorySession(): void {
  if (typeof window === "undefined") {
    return;
  }
  window.sessionStorage.setItem(MEMORY_SESSION_MARKER_KEY, "1");
}

export function clearMemoryMarker(): void {
  if (typeof window === "undefined") {
    return;
  }
  window.sessionStorage.removeItem(MEMORY_SESSION_MARKER_KEY);
}

export function consumedMemorySessionMarker(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  const hadMarker = window.sessionStorage.getItem(MEMORY_SESSION_MARKER_KEY) === "1";
  return hadMarker;
}
