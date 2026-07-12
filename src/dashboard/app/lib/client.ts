import {
  CRONS_PATH,
  HEALTH_PATH,
  cronRunPath,
  runPath,
  runSignalPath,
  runsPath,
} from "@/lib/contract";
import { fetcher, post } from "@/lib/fetcher";
import type {
  CronRow,
  CronTriggerResult,
  Filters,
  HealthReport,
  RunDetail,
  RunsPage,
  SignalDeliveryResult,
} from "@/lib/types";

export const client = {
  health: () => fetcher<HealthReport>(HEALTH_PATH),
  runs: {
    list: (filters: Filters, cursor: RunsPage["next"] = null) =>
      fetcher<RunsPage>(runsPath(filters, cursor)),
    get: (id: string) => fetcher<RunDetail>(runPath(id)),
    cancel: (id: string) => post<{ ok: true }>(`${runPath(id)}/cancel`),
    retry: (id: string) => post<{ kind: string }>(`${runPath(id)}/retry`),
    signal: (id: string, name: string, payload?: unknown) =>
      post<SignalDeliveryResult>(runSignalPath(id), { name, payload }),
  },
  crons: {
    list: () => fetcher<{ crons: CronRow[] }>(CRONS_PATH),
    trigger: (name: string) => post<CronTriggerResult>(cronRunPath(name)),
  },
};
