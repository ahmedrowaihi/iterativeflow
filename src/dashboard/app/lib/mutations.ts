import { mutate } from "swr";
import { client } from "@/lib/client";
import { runPath } from "@/lib/contract";
import type { CronTriggerResult, SignalDeliveryResult } from "@/lib/types";

export const revalidateRuns = () =>
  mutate((key) => typeof key === "string" && key.startsWith("api/runs"));

const revalidateRun = (id: string) => mutate(runPath(id));

export const cancelRunReq = async (id: string): Promise<void> => {
  await client.runs.cancel(id);
  await revalidateRuns();
};

export const retryRunReq = async (id: string): Promise<void> => {
  await client.runs.retry(id);
  await revalidateRuns();
};

export const triggerCronReq = (name: string): Promise<CronTriggerResult> =>
  client.crons.trigger(name);

export const signalRunReq = async (
  id: string,
  name: string,
  payload?: unknown,
): Promise<SignalDeliveryResult> => {
  const result = await client.runs.signal(id, name, payload);
  await Promise.all([revalidateRun(id), revalidateRuns()]);
  return result;
};
