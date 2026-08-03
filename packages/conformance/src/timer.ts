import type { Timer } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The Timer contract as executable invariants — durable deadlines that fire exactly once,
 * earliest first, with upsert-reschedule and cancel. Clock injected for determinism.
 */
export const timerConformance = (label: string, makeTimer: () => Timer | Promise<Timer>): void => {
  describe(`Timer conformance — ${label}`, () => {
    it("a scheduled timer is due once its fireAt has passed", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(1000));
      expect(await tm.dueBatch({ now: at(999), limit: 10 })).toEqual([]);
      expect(await tm.dueBatch({ now: at(1000), limit: 10 })).toEqual(["r1"]);
    });

    it("dueBatch fires exactly once — a consumed timer is not returned again", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(0));
      expect(await tm.dueBatch({ now: at(1), limit: 10 })).toEqual(["r1"]);
      expect(await tm.dueBatch({ now: at(2), limit: 10 })).toEqual([]);
    });

    it("reschedule is an upsert — the latest fireAt wins", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(1000));
      await tm.schedule("r1", at(5000)); // pushed later
      expect(await tm.dueBatch({ now: at(2000), limit: 10 })).toEqual([]);
      expect(await tm.dueBatch({ now: at(5000), limit: 10 })).toEqual(["r1"]);
    });

    it("reschedule EARLIER also wins — upsert, not a LEAST(fire_at) merge", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(5000));
      await tm.schedule("r1", at(1000)); // pulled earlier
      expect(await tm.dueBatch({ now: at(1000), limit: 10 })).toEqual(["r1"]);
    });

    it("cancel removes a pending timer", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(1000));
      await tm.cancel("r1");
      expect(await tm.dueBatch({ now: at(2000), limit: 10 })).toEqual([]);
    });

    it("dueBatch honours limit and returns earliest-first", async () => {
      const tm = await makeTimer();
      await tm.schedule("late", at(300));
      await tm.schedule("early", at(100));
      await tm.schedule("mid", at(200));
      const due = await tm.dueBatch({ now: at(1000), limit: 2 });
      expect(due).toEqual(["early", "mid"]);
    });

    it("nextDueAt is null when nothing is pending", async () => {
      const tm = await makeTimer();
      expect(await tm.nextDueAt(at(0))).toBeNull();
    });

    it("nextDueAt returns the earliest timer due strictly after now", async () => {
      const tm = await makeTimer();
      await tm.schedule("late", at(3000));
      await tm.schedule("early", at(1000));
      await tm.schedule("mid", at(2000));
      expect(await tm.nextDueAt(at(500))).toEqual(at(1000));
      // a timer at/before now is due (drained by the tick), not a future horizon
      expect(await tm.nextDueAt(at(1000))).toEqual(at(2000));
      expect(await tm.nextDueAt(at(3000))).toBeNull();
    });

    it("nextDueAt reflects reschedule and cancel", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(5000));
      expect(await tm.nextDueAt(at(0))).toEqual(at(5000));
      await tm.schedule("r1", at(1000)); // pulled earlier
      expect(await tm.nextDueAt(at(0))).toEqual(at(1000));
      await tm.cancel("r1");
      expect(await tm.nextDueAt(at(0))).toBeNull();
    });
  });
};
