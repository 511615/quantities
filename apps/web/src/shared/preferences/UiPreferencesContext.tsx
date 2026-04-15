import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { type Locale, setI18nLocale } from "../lib/i18n";

export type ThemeMode = "light" | "dark";

export type UiPreferences = {
  locale: Locale;
  theme: ThemeMode;
};

type UiPreferencesContextValue = UiPreferences & {
  setLocale: (locale: Locale) => void;
  setTheme: (theme: ThemeMode) => void;
};

const STORAGE_KEYS = {
  locale: "qp.ui.locale",
  theme: "qp.ui.theme",
} as const;

const DEFAULT_PREFERENCES: UiPreferences = {
  locale: "zh-CN",
  theme: "light",
};

const UiPreferencesContext = createContext<UiPreferencesContextValue | null>(null);

function readStoredLocale(): Locale {
  if (typeof window === "undefined") {
    return DEFAULT_PREFERENCES.locale;
  }
  const stored = window.localStorage.getItem(STORAGE_KEYS.locale);
  return stored === "en-US" || stored === "zh-CN" ? stored : DEFAULT_PREFERENCES.locale;
}

function readStoredTheme(): ThemeMode {
  if (typeof window === "undefined") {
    return DEFAULT_PREFERENCES.theme;
  }
  const stored = window.localStorage.getItem(STORAGE_KEYS.theme);
  return stored === "dark" || stored === "light" ? stored : DEFAULT_PREFERENCES.theme;
}

export function UiPreferencesProvider({ children }: { children: ReactNode }) {
  const [locale, setLocale] = useState<Locale>(readStoredLocale);
  const [theme, setTheme] = useState<ThemeMode>(readStoredTheme);

  setI18nLocale(locale);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEYS.locale, locale);
    document.documentElement.lang = locale;
  }, [locale]);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEYS.theme, theme);
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  const value = useMemo(
    () => ({
      locale,
      theme,
      setLocale,
      setTheme,
    }),
    [locale, theme],
  );

  return <UiPreferencesContext.Provider value={value}>{children}</UiPreferencesContext.Provider>;
}

export function useUiPreferences(): UiPreferencesContextValue {
  const context = useContext(UiPreferencesContext);
  if (!context) {
    throw new Error("useUiPreferences must be used within UiPreferencesProvider");
  }
  return context;
}
