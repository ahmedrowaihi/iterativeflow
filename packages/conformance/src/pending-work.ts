import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";

/**
 * Pins the autoscaling-backlog reads — `Queue.depth(now, names)`, `Timer.dueCount(now, names)`, and
 * `Store.dueCronCount(now, names)` — that `engine.pendingWork` composes. Every backend must count the
 * same due work (claimable jobs, due timers, due crons), exclude leased/future work, and filter by
 * flow name identically.
 *
 * A row whose run is gone counts under EVERY name filter, matching `Queue.claim` — a sharded fleet
 * has to see the backlog it can actually lease, or it stays scaled to zero and the row never drains.
 * An empty `names` is the exception: it means "this worker handles nothing", so it counts nothing.
 */
export const pendingWorkConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`pending-work conformance (${label})`, () => {
    const now = new Date("2030-01-01T00:00:00Z");
    const past = new Date(now.getTime() - 60_000);
    const future = new Date(now.getTime() + 3_600_000);

    const seed = async (): Promise<Backend> => {
      const be = await makeBackend();
      const start = async (name: string): Promise<string> =>
        (await be.store.startRun({ name, version: 1, input: {} })).runId;

      const leased = await start("a");
      await be.queue.enqueue(leased);
      await be.queue.claim({ limit: 1, leaseMs: 600_000, now });

      const a1 = await start("a");
      await be.queue.enqueue(a1);
      const b1 = await start("b");
      await be.queue.enqueue(b1);
      const sleeping = await start("a");
      await be.timer.schedule(sleeping, past);
      const napping = await start("a");
      await be.timer.schedule(napping, future);
      await be.timer.schedule("orphan-run", past);
      await be.queue.enqueue("orphan-job");
      const futureJob = await start("a");
      await be.queue.enqueue(futureJob, { runAt: future });
      await be.store.upsertCron({
        name: "cron-a",
        schedule: "* * * * *",
        flowName: "a",
        flowVersion: 1,
        input: {},
        nextRunAt: past,
      });
      return be;
    };

    it("depth counts claimable jobs (not leased/future), filtered by flow name", async () => {
      const be = await seed();
      expect((await be.queue.depth(now)).claimable).toBe(3);
      // a1 + the run-less job, which every name filter passes because anyone may lease and ack it
      expect((await be.queue.depth(now, ["a"])).claimable).toBe(2);
      expect((await be.queue.depth(now, ["b"])).claimable).toBe(2);
      expect((await be.queue.depth(now, [])).claimable).toBe(0);
    });

    it("dueCount counts due timers incl. run-less (not future), filtered by flow name", async () => {
      const be = await seed();
      expect(await be.timer.dueCount(now)).toBe(2);
      expect(await be.timer.dueCount(now, ["a"])).toBe(2);
      expect(await be.timer.dueCount(now, ["b"])).toBe(1);
      expect(await be.timer.dueCount(now, [])).toBe(0);
    });

    it("dueCronCount counts due crons, filtered by flow name", async () => {
      const be = await seed();
      expect(await be.store.dueCronCount(now)).toBe(1);
      expect(await be.store.dueCronCount(now, ["a"])).toBe(1);
      expect(await be.store.dueCronCount(now, ["b"])).toBe(0);
    });
  });
};
