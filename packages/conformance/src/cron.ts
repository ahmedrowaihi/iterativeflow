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
    };

    it("upsertCron registers a cron that dueCrons returns once due", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(1000) });
      expect(await s.dueCrons(at(999), 10)).toEqual([]); // not due yet
      const due = await s.dueCrons(at(1000), 10);
      expect(due.map((c) => c.name)).toEqual(["nightly"]);
      expect(due[0].input).toEqual({ kind: "daily" });
    });

    it("re-registering keeps the existing nextRunAt (no schedule-timing reset)", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(5000) });
      await s.upsertCron({ ...spec, input: { kind: "changed" }, nextRunAt: at(999999) });
      const due = await s.dueCrons(at(5000), 10);
      expect(due.map((c) => c.name)).toEqual(["nightly"]); // still due at the original time
      expect(due[0].input).toEqual({ kind: "changed" }); // but payload updated
    });

    it("advanceCron is a CAS — only the worker matching the expected time wins", async () => {
      const s = await makeStore();
      await s.upsertCron({ ...spec, nextRunAt: at(1000) });
      const first = await s.advanceCron("nightly", at(1000), at(2000), at(1000));
      const second = await s.advanceCron("nightly", at(1000), at(2000), at(1000)); // stale expected
      expect(first).toBe(true);
      expect(second).toBe(false); // lost the race — won't double-fire
      expect(await s.dueCrons(at(1500), 10)).toEqual([]); // advanced past 1500
    });
  });
};
