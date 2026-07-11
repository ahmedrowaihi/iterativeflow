import { signal } from "@preact/signals";

export const SIDEBAR_KEY = "iterativeflow.dashboard.sidebar";

export const collapsed = signal(
  typeof localStorage !== "undefined" && localStorage.getItem(SIDEBAR_KEY) === "collapsed",
);

export const toggleSidebar = (): void => {
  collapsed.value = !collapsed.value;
  localStorage.setItem(SIDEBAR_KEY, collapsed.value ? "collapsed" : "expanded");
};
