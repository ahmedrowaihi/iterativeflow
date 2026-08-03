import { ACTIVE_STATUSES } from "#status";
import { nextCronAfter, parseCron } from "#engine/cron";
import type { Flow } from "#engine/flow";
import type { Backend } from "#ports/outbox";

/** The tag every cron-spawned run carries, so `listRuns({ tag })` finds a cron's history. */
export const cronTag = (name: string): string => `cron:${name}`;

/** A recurring schedule: fire `flow(input)` on the cron `schedule` (5-field, UTC). */
export interface CronDef<I> {
  name: string;
  schedule: string;
  flow: Flow<I, unknown>;
  input: I;
  /** `skip` won't start a new run while a prior run of this cron is still active. Default `allow`. */
  overlap?: "allow" | "skip";
}

/**
 * Register (or update) a cron. Validates the expression and computes the first fire from `now`.
 * Re-registering keeps the existing schedule timing (the store preserves `nextRunAt`).
 */
export const registerCron = async <I>(
  backend: Backend,
  def: CronDef<I>,
  now: () => Date = () => new Date(),
): Promise<void> => {
  parseCron(def.schedule);
  await backend.store.upsertCron({
    name: def.name,
    schedule: def.schedule,
    flowName: def.flow.name,
    flowVersion: def.flow.version,
    input: def.input,
    overlap: def.overlap,
    nextRunAt: nextCronAfter(def.schedule, now()),
  });
};

/**
 * Fire every due cron once. Each occurrence's run is deduped across a fleet by the occurrence-scoped
 * idempotency key (`cron:name:fireTime`), and the schedule is advanced by the `advanceCron` CAS so
 * only one worker moves it forward. Run this on a slow interval or as an internal cron. Returns how
 * many occurrences this worker advanced.
 */
export const runDueCrons = async (
  backend: Backend,
  now: () => Date = () => new Date(),
): Promise<number> => {
  const due = await backend.store.dueCrons(now(), 100);
  let fired = 0;
  for (const c of due) {
    const next = nextCronAfter(c.schedule, now());
    const advance = () => backend.store.advanceCron(c.name, c.nextRunAt, next, now());

    if (c.overlap === "skip") {
      const active = await backend.store.listRuns(
        { status: ACTIVE_STATUSES, tag: cronTag(c.name) },
        { limit: 1 },
      );
      // Consume the occurrence (advance) without starting, so it doesn't re-fire while a run is live.
      if (active.runs.length > 0) {
        await advance();
        continue;
      }
    }
    // Start (idempotent) BEFORE advancing the CAS: a crash between re-drives the occurrence, never drops it.
    const { runId, created } = await backend.store.startRun({
      name: c.flowName,
      version: c.flowVersion,
      input: c.input,
      idempotencyKey: `cron:${c.name}:${c.nextRunAt.getTime()}`,
      tags: [cronTag(c.name)],
    });
    if (created) await backend.queue.enqueue(runId);
    if (await advance()) fired += 1;
  }
  return fired;
};
