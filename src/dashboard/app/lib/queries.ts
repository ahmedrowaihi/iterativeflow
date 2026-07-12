import useSWR from "swr";
import useSWRInfinite from "swr/infinite";
import { client } from "@/lib/client";
import { CRONS_PATH, HEALTH_PATH, runPath, runsPath } from "@/lib/contract";
import { fetcher } from "@/lib/fetcher";
import type { Filters, RunsPage } from "@/lib/types";

const EMPTY: Filters = { name: "", status: "", tag: "", since: "", until: "" };

export const useHealth = () => useSWR(HEALTH_PATH, client.health);

export const useCrons = () => useSWR(CRONS_PATH, client.crons.list);

export const useRun = (id: string | null) =>
  useSWR(id ? runPath(id) : null, () => client.runs.get(id as string));

export const useOverviewRuns = () => useSWR(runsPath(EMPTY, null), () => client.runs.list(EMPTY));

export const useRunsInfinite = (filters: Filters) =>
  useSWRInfinite<RunsPage>(
    (index, prev) =>
      prev && !prev.next ? null : runsPath(filters, index === 0 ? null : (prev?.next ?? null)),
    fetcher,
    { revalidateFirstPage: false },
  );
