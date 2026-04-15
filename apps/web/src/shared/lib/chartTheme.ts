import { useUiPreferences, type ThemeMode } from "../preferences/UiPreferencesContext";

export type ChartTheme = {
  legendText: string;
  axisText: string;
  axisLine: string;
  splitLine: string;
  accent: string;
  accentAlt: string;
  warning: string;
  danger: string;
};

const THEMES: Record<ThemeMode, ChartTheme> = {
  light: {
    legendText: "#44525f",
    axisText: "#566577",
    axisLine: "rgba(82, 95, 110, 0.22)",
    splitLine: "rgba(82, 95, 110, 0.12)",
    accent: "#72a832",
    accentAlt: "#2d8db5",
    warning: "#d98c36",
    danger: "#d36060",
  },
  dark: {
    legendText: "#d6d2c4",
    axisText: "#b9b0a0",
    axisLine: "rgba(213, 207, 193, 0.2)",
    splitLine: "rgba(213, 207, 193, 0.08)",
    accent: "#c7ff73",
    accentAlt: "#7fe3ff",
    warning: "#ffb479",
    danger: "#ff8d70",
  },
};

export function getChartTheme(theme: ThemeMode): ChartTheme {
  return THEMES[theme];
}

export function useChartTheme(): ChartTheme {
  const { theme } = useUiPreferences();
  return getChartTheme(theme);
}
