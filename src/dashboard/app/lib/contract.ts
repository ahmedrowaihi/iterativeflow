import type { Filters, RunsPage } from "@/lib/types";

export const PAGE_SIZE = 25;

export const HEALTH_PATH = "api/health";
export const CRONS_PATH = "api/crons";

export const runPath = (id: string): string => `api/runs/${encodeURIComponent(id)}`;
export const runSignalPath = (id: string): string => `${runPath(id)}/signal`;
export const cronTag = (name: string): string => `cron:${name}`;
export const cronRunPath = (name: string): string => `api/crons/${encodeURIComponent(name)}/run`;

export const runsPath = (filters: Filters, cursor: RunsPage["next"]): string => {
  const p = new URLSearchParams();
  if (filters.name) p.set("name", filters.name);
  if (filters.status) p.set("status", filters.status);
  if (filters.tag) p.set("tag", filters.tag);
  if (filters.since) p.set("since", filters.since);
  if (filters.until) p.set("until", filters.until);
  p.set("limit", String(PAGE_SIZE));
  if (cursor) {
    p.set("cursorCreatedAt", cursor.createdAt);
    p.set("cursorId", cursor.id);
  }
  return `api/runs?${p.toString()}`;
};
