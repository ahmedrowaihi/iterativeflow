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
      expect(await tm.dueBatch({ now: at(999), max: 10 })).toEqual([]);
      expect(await tm.dueBatch({ now: at(1000), max: 10 })).toEqual(["r1"]);
    });

    it("dueBatch fires exactly once — a consumed timer is not returned again", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(0));
      expect(await tm.dueBatch({ now: at(1), max: 10 })).toEqual(["r1"]);
      expect(await tm.dueBatch({ now: at(2), max: 10 })).toEqual([]);
    });

    it("reschedule is an upsert — the latest fireAt wins", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(1000));
      await tm.schedule("r1", at(5000)); // pushed later
      expect(await tm.dueBatch({ now: at(2000), max: 10 })).toEqual([]);
      expect(await tm.dueBatch({ now: at(5000), max: 10 })).toEqual(["r1"]);
    });

    it("reschedule EARLIER also wins — upsert, not a LEAST(fire_at) merge", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(5000));
      await tm.schedule("r1", at(1000)); // pulled earlier
      expect(await tm.dueBatch({ now: at(1000), max: 10 })).toEqual(["r1"]);
    });

    it("cancel removes a pending timer", async () => {
      const tm = await makeTimer();
      await tm.schedule("r1", at(1000));
      await tm.cancel("r1");
      expect(await tm.dueBatch({ now: at(2000), max: 10 })).toEqual([]);
    });

    it("dueBatch honours max and returns earliest-first", async () => {
      const tm = await makeTimer();
      await tm.schedule("late", at(300));
      await tm.schedule("early", at(100));
      await tm.schedule("mid", at(200));
      const due = await tm.dueBatch({ now: at(1000), max: 2 });
      expect(due).toEqual(["early", "mid"]);
    });
  });
};
