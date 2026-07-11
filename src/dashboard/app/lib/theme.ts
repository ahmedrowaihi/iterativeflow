import { signal } from "@preact/signals";

type Theme = "light" | "dark";

export const THEME_KEY = "iterativeflow.dashboard.theme";

const current = (): Theme =>
  typeof document !== "undefined" && document.documentElement.classList.contains("dark")
    ? "dark"
    : "light";

export const theme = signal<Theme>(current());

export const toggleTheme = (): void => {
  const next: Theme = theme.value === "dark" ? "light" : "dark";
  theme.value = next;
  document.documentElement.classList.toggle("dark", next === "dark");
  localStorage.setItem(THEME_KEY, next);
};
