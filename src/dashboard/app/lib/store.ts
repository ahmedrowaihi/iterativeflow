import { computed, signal } from "@preact/signals";
import { cancelRunReq, retryRunReq, signalRunReq, triggerCronReq } from "@/lib/mutations";
import { query, setParams } from "@/lib/router";
import { RUN_STATUSES } from "@/lib/types";
import type { CappedJson, Filters, RunListItem, RunStatus } from "@/lib/types";

type CronResult = { at: string; result?: CappedJson | null; error?: string };

export const sheetRunId = computed(() => query().get("run"));
export const openRun = (id: string): void => setParams({ run: id });
export const closeSheet = (): void => setParams({ run: null });

export const sheetCronName = computed(() => query().get("cron"));
export const openCron = (name: string): void => setParams({ cron: name });
export const closeCronSheet = (): void => setParams({ cron: null });

export const filters = computed<Filters>(() => {
  const q = query();
  const status = q.get("status") ?? "";
  return {
    name: q.get("name") ?? "",
    status: (RUN_STATUSES as readonly string[]).includes(status) ? (status as RunStatus) : "",
    tag: q.get("tag") ?? "",
    since: q.get("since") ?? "",
    until: q.get("until") ?? "",
  };
});
export const setFilter = (patch: Partial<Filters>): void => setParams(patch, { replace: true });
export const clearFilters = (): void =>
  setParams({ name: null, status: null, tag: null, since: null, until: null }, { replace: true });

const toggle = (set: Set<string>, key: string): Set<string> => {
  const copy = new Set(set);
  if (copy.has(key)) copy.delete(key);
  else copy.add(key);
  return copy;
};
export const expanded = signal<Set<string>>(new Set());
export const toggleStep = (key: string): void => {
  expanded.value = toggle(expanded.value, key);
};

export const cronResults = signal<Map<string, CronResult>>(new Map());

export const jsonView = signal<{ title: string; capped: CappedJson } | null>(null);
export const openJson = (title: string, capped: CappedJson): void => {
  jsonView.value = { title, capped };
};
export const closeJson = (): void => {
  jsonView.value = null;
};

export const toastMsg = signal<string | null>(null);
let toastTimer: ReturnType<typeof setTimeout>;
export const showToast = (msg: string): void => {
  toastMsg.value = msg;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    toastMsg.value = null;
  }, 2500);
};

export const confirmState = signal<{
  title: string;
  message: string;
  danger: boolean;
  resolve: (ok: boolean) => void;
} | null>(null);
const confirm = (title: string, message: string, danger = false): Promise<boolean> =>
  new Promise((resolve) => {
    confirmState.value = { title, message, danger, resolve };
  });
export const answerConfirm = (ok: boolean): void => {
  const c = confirmState.value;
  confirmState.value = null;
  c?.resolve(ok);
};

export const signalTarget = signal<{ runId: string; name: string } | null>(null);
export const openSignal = (runId: string, name: string): void => {
  signalTarget.value = { runId, name };
};
export const closeSignal = (): void => {
  signalTarget.value = null;
};
export const sendSignal = async (runId: string, name: string, payload?: unknown): Promise<void> => {
  try {
    const res = await signalRunReq(runId, name, payload);
    showToast(`signal ${name}: ${res.kind}`);
    closeSignal();
  } catch (err) {
    showToast(`signal failed: ${(err as Error).message}`);
  }
};

export const AUTOREFRESH_KEY = "iterativeflow.dashboard.autorefresh";
export const autoRefresh = signal(
  typeof localStorage !== "undefined" && localStorage.getItem(AUTOREFRESH_KEY) !== "off",
);
export const setAutoRefresh = (on: boolean): void => {
  autoRefresh.value = on;
  localStorage.setItem(AUTOREFRESH_KEY, on ? "on" : "off");
};

export const cancelRun = async (run: RunListItem): Promise<void> => {
  const ok = await confirm(
    "Cancel run",
    `${run.name} · ${run.id.slice(0, 8)} — the run stops and will not resume.`,
    true,
  );
  if (!ok) return;
  try {
    await cancelRunReq(run.id);
    showToast("run canceled");
  } catch (err) {
    showToast(`cancel failed: ${(err as Error).message}`);
  }
};

export const retryRun = async (run: RunListItem): Promise<void> => {
  const ok = await confirm(
    "Retry run",
    `${run.name} · ${run.id.slice(0, 8)} — replays from the failed step; memoized steps are kept.`,
  );
  if (!ok) return;
  try {
    await retryRunReq(run.id);
    showToast("retry queued");
  } catch (err) {
    showToast(`retry failed: ${(err as Error).message}`);
  }
};

export const triggerCron = async (name: string): Promise<void> => {
  const ok = await confirm("Run now", `${name} — runs immediately, outside its regular schedule.`);
  if (!ok) return;
  const at = new Date().toISOString();
  try {
    const data = await triggerCronReq(name);
    cronResults.value = new Map(cronResults.value).set(name, { at, result: data.result });
    showToast(`cron triggered: ${name}`);
  } catch (err) {
    cronResults.value = new Map(cronResults.value).set(name, { at, error: (err as Error).message });
    showToast(`trigger failed: ${(err as Error).message}`);
  }
};
