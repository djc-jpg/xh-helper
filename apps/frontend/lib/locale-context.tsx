"use client";

import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { I18nextProvider } from "react-i18next";

import { i18n, setupI18n } from "./i18n";

type Language = "zh" | "en";

setupI18n("zh");

interface LocaleContextValue {
  language: Language;
  setLanguage: (lang: Language) => void;
}

const LOCALE_STORAGE_KEY = "xh_console_locale";
const LocaleContext = createContext<LocaleContextValue | null>(null);

export function LocaleProvider({ children }: { children: React.ReactNode }) {
  const [language, setLanguageState] = useState<Language>("zh");

  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = window.localStorage.getItem(LOCALE_STORAGE_KEY);
    if (stored === "zh" || stored === "en") {
      setLanguageState(stored);
      i18n.changeLanguage(stored).catch(() => undefined);
    }
  }, []);

  const setLanguage = (lang: Language) => {
    setLanguageState(lang);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LOCALE_STORAGE_KEY, lang);
    }
    i18n.changeLanguage(lang).catch(() => undefined);
  };

  const value = useMemo(
    () => ({
      language,
      setLanguage
    }),
    [language]
  );

  return (
    <I18nextProvider i18n={i18n}>
      <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>
    </I18nextProvider>
  );
}

export function useLocale(): LocaleContextValue {
  const context = useContext(LocaleContext);
  if (!context) {
    throw new Error("useLocale must be used within LocaleProvider");
  }
  return context;
}
