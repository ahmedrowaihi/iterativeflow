import type { Backend } from "@iterativeflow/core/backend";
import { claimFilterConformance } from "#claim-filter";
import { concurrencyConformance } from "#concurrency";
import { cronConformance } from "#cron";
import { engineConformance } from "#engine";
import { outboxConformance } from "#outbox";
import { pendingWorkConformance } from "#pending-work";
import { queueConformance } from "#queue";
import { reconcileConformance } from "#reconcile";
import { shardedClaimConformance } from "#sharded-claim";
import { signalConformance } from "#signal";
import { storeConformance } from "#store";
import { timerConformance } from "#timer";
import { wakeupConformance } from "#wakeup";

/** What a backend is, as far as which suites apply to it. */
export interface BackendTraits {
  /** Whether separate workers can write concurrently — a fleet over one database. A single-writer
   *  substrate (in-process memory, one SQLite file, a Durable Object) skips the concurrent-claim suite. */
  concurrentWriters: boolean;
}

/**
 * Register every conformance suite against `makeBackend`, which returns a fresh {@link Backend} per
 * case. This is what "a correct backend" means; a backend that passes it needs no suite list of its own.
 */
export const backendConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
  traits: BackendTraits,
): void => {
  storeConformance(label, async () => (await makeBackend()).store);
  queueConformance(label, async () => (await makeBackend()).queue);
  timerConformance(label, async () => (await makeBackend()).timer);
  wakeupConformance(label, async () => (await makeBackend()).wakeup);
  cronConformance(label, async () => (await makeBackend()).store);
  outboxConformance(label, makeBackend);
  signalConformance(label, makeBackend);
  reconcileConformance(label, makeBackend);
  claimFilterConformance(label, makeBackend);
  pendingWorkConformance(label, makeBackend);
  engineConformance(label, makeBackend);
  if (traits.concurrentWriters) {
    concurrencyConformance(label, makeBackend);
    shardedClaimConformance(label, makeBackend);
  }
};
