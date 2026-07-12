import { signal } from "@preact/signals";

export const SIDEBAR_KEY = "iterativeflow.dashboard.sidebar";

const readCollapsed = (): boolean => {
  try {
    return localStorage.getItem(SIDEBAR_KEY) === "collapsed";
  } catch {
    return false;
  }
};

export const collapsed = signal(readCollapsed());

export const toggleSidebar = (): void => {
  collapsed.value = !collapsed.value;
  try {
    localStorage.setItem(SIDEBAR_KEY, collapsed.value ? "collapsed" : "expanded");
  } catch {
    // storage blocked — keep the in-memory value
  }
};
