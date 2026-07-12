import { computed, signal } from "@preact/signals";
import type { CustomHistory } from "preact-router";

const readHash = (): string => location.hash.replace(/^#/, "") || "/";

export const hash = signal(readHash());
if (typeof window !== "undefined") {
  const sync = () => {
    hash.value = readHash();
  };
  window.addEventListener("hashchange", sync);
  window.addEventListener("popstate", sync);
}

export const pathOf = (h: string): string => h.split("?")[0] || "/";
export const currentPath = (): string => pathOf(hash.value);
export const path = computed(() => pathOf(hash.value));

export const query = (): URLSearchParams => {
  try {
    return new URLSearchParams(hash.value.split("?")[1] ?? "");
  } catch {
    return new URLSearchParams();
  }
};

const applyHash = (next: string, replace: boolean): void => {
  try {
    const url = `${location.pathname}${location.search}#${next}`;
    if (replace) window.history.replaceState(window.history.state, "", url);
    else window.history.pushState(window.history.state, "", url);
  } catch {
    location.hash = next;
  }
  hash.value = next;
};

export const setParams = (
  patch: Record<string, string | null | undefined>,
  opts?: { replace?: boolean },
): void => {
  const q = query();
  for (const [key, value] of Object.entries(patch)) {
    if (value === null || value === undefined || value === "") q.delete(key);
    else q.set(key, value);
  }
  const qs = q.toString();
  const p = pathOf(hash.value);
  applyHash(qs ? `${p}?${qs}` : p, opts?.replace ?? false);
};

const toLocation = (): Location =>
  ({ pathname: currentPath(), search: "", hash: "" }) as unknown as Location;

export const history: CustomHistory = {
  get location() {
    return toLocation();
  },
  listen(cb) {
    const handler = () => cb(toLocation());
    window.addEventListener("hashchange", handler);
    window.addEventListener("popstate", handler);
    return () => {
      window.removeEventListener("hashchange", handler);
      window.removeEventListener("popstate", handler);
    };
  },
  push(to: string) {
    location.hash = to;
  },
  replace(to: string) {
    location.replace(`${location.href.split("#")[0]}#${to}`);
  },
};

export const navigate = (to: string): void => {
  history.push(to);
};
