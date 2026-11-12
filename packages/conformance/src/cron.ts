import type { Store } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The cron store surface as executable invariants: upsert keeps timing on re-register,
 * `dueCrons` respects `nextRunAt`, and `advanceCron` is a CAS so exactly one worker fires an
 * occurrence (the double-fire guard on a fleet).
 */
export const cronConformance = (label: string, makeStore: () => Store | Promise<Store>): void => {
  describe(`Cron conformance — ${label}`, () => {
    const spec = {
      name: "nightly",
      schedule: "0 0 * * *",
      flowName: "report",
      flowVersion: 1,
      input: { kind: "daily" },
      overlap: "allow" as const,
    };

    it("upsertCron registers a cron that dueCrons returns once due", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(1000) });
      expect(await s.dueCrons(at(999), 10)).toEqual([]);
      const due = await s.dueCrons(at(1000), 10);
      expect(due.map((c) => c.name)).toEqual(["nightly"]);
      expect(due[0].input).toEqual({ kind: "daily" });
    });

    it("dueCrons honours its limit on a backlog of due crons", async () => {
      const s = await makeStore();
      for (const name of ["a", "b", "c"])
        await s.upsertCron({ ...spec, name, nextRunAt: at(1000) });
      const due = await s.dueCrons(at(1000), 2);
      expect(due).toHaveLength(2);
      expect(["a", "b", "c"]).toEqual(expect.arrayContaining(due.map((c) => c.name)));
    });

    it("re-registering keeps the existing nextRunAt (no schedule-timing reset)", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(5000) });
      await s.upsertCron({ ...spec, input: { kind: "changed" }, nextRunAt: at(999999) });
      const due = await s.dueCrons(at(5000), 10);
      expect(due.map((c) => c.name)).toEqual(["nightly"]);
      expect(due[0].input).toEqual({ kind: "changed" });
    });

    it("a CHANGED schedule takes its new nextRunAt (the old one would outlive the deploy)", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, schedule: "0 3 * * *", nextRunAt: at(999999) });
      // a deploy changes the schedule; the new timing must take effect now, not after the old fire
      await s.upsertCron({ ...spec, schedule: "*/5 * * * *", nextRunAt: at(5000) });
      const due = await s.dueCrons(at(5000), 10);
      expect(due.map((c) => c.name)).toEqual(["nightly"]);
      expect(due[0].schedule).toBe("*/5 * * * *");
    });

    it("nextCronAt is the earliest nextRunAt, overdue included, and null with no crons", async () => {
      const s = await makeStore();
      expect(await s.nextCronAt()).toBeNull();
      await s.upsertCron({ ...spec, name: "later", nextRunAt: at(5_000) });
      await s.upsertCron({ ...spec, name: "overdue", nextRunAt: at(-5_000) });
      expect((await s.nextCronAt())?.getTime()).toBe(at(-5_000).getTime());
      await s.removeCron("overdue");
      expect((await s.nextCronAt())?.getTime()).toBe(at(5_000).getTime());
    });

    it("listCrons returns every registered cron; removeCron stops one firing", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(1000) });
      await s.upsertCron({ ...spec, name: "weekly", nextRunAt: at(2000) });

      expect((await s.listCrons()).map((c) => c.name).sort()).toEqual(["nightly", "weekly"]);
      expect(await s.removeCron("weekly")).toBe(true);
      expect(await s.removeCron("weekly")).toBe(false);
      expect((await s.listCrons()).map((c) => c.name)).toEqual(["nightly"]);
      expect((await s.dueCrons(at(2000), 10)).map((c) => c.name)).toEqual(["nightly"]);
    });

    it("advanceCron is a CAS — only the worker matching the expected time wins", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(1000) });
      const first = await s.advanceCron("nightly", at(1000), at(2000), at(1000));
      const second = await s.advanceCron("nightly", at(1000), at(2000), at(1000)); // stale expected
      expect(first).toBe(true);
      expect(second).toBe(false);
      expect(await s.dueCrons(at(1500), 10)).toEqual([]);
    });
  });
};
