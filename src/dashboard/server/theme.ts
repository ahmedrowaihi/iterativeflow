/** A shadcn/ui token the dashboard reads, written without the `--` prefix. */
export type ThemeToken =
  | "background"
  | "foreground"
  | "card"
  | "card-foreground"
  | "popover"
  | "popover-foreground"
  | "primary"
  | "primary-foreground"
  | "secondary"
  | "secondary-foreground"
  | "muted"
  | "muted-foreground"
  | "accent"
  | "accent-foreground"
  | "destructive"
  | "border"
  | "input"
  | "ring"
  | "radius"
  | "status-pending"
  | "status-running"
  | "status-sleeping"
  | "status-awaiting_signal"
  | "status-retrying"
  | "status-done"
  | "status-failed"
  | "status-canceled"
  | "status-ok"
  | "status-warn";

/** A partial map of {@link ThemeToken} to CSS values, e.g. `{ primary: "oklch(0.55 0.22 264)" }`. */
export type ThemeTokens = Partial<Record<ThemeToken, string>>;

/** Token overrides for the dashboard, applied over the built-in shadcn defaults. */
export interface DashboardTheme {
  /** Applied to `:root` — the light palette and the default. */
  light?: ThemeTokens;
  /** Applied to `.dark` — used when the OS prefers a dark scheme. */
  dark?: ThemeTokens;
  /**
   * Raw CSS appended after the generated token blocks, for anything the maps
   * can't express. Trusted host config — injected verbatim, not escaped.
   */
  css?: string;
}

const cssBlock = (selector: string, tokens: ThemeTokens): string => {
  const decls = Object.entries(tokens)
    .map(([token, value]) => `--${token}: ${value};`)
    .join(" ");
  return decls ? `${selector} { ${decls} }` : "";
};

export const renderTheme = (theme: DashboardTheme | undefined): string => {
  if (!theme) return "";
  const css = [
    theme.light ? cssBlock(":root", theme.light) : "",
    theme.dark ? cssBlock(".dark", theme.dark) : "",
    theme.css ?? "",
  ]
    .filter(Boolean)
    .join("\n");
  return css ? `<style id="iflow-theme">${css}</style>` : "";
};
