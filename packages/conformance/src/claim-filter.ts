import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * `ClaimOpts.names` as executable invariants: a sharded worker leases only runs whose flow `name`
 * it registered. Needs the store (to create named runs), so it takes the whole backend.
 */
export const claimFilterConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Claim flow-name filter — ${label}`, () => {
    it("leases only runs whose flow name is in the set", async () => {
      const { store, queue } = await makeBackend();
      const a = await store.startRun({ name: "flow-a", version: 1, input: {} });
      const b = await store.startRun({ name: "flow-b", version: 1, input: {} });
      await queue.enqueue(a.runId);
      await queue.enqueue(b.runId);

      const onlyA = await queue.claim({ limit: 10, leaseMs: 1000, names: ["flow-a"], now: at(0) });
      expect(onlyA.map((l) => l.runId)).toEqual([a.runId]); // b left claimable for its own worker

      const onlyB = await queue.claim({ limit: 10, leaseMs: 1000, names: ["flow-b"], now: at(0) });
      expect(onlyB.map((l) => l.runId)).toEqual([b.runId]);
    });

    it("an empty set leases nothing; an omitted set leases everything", async () => {
      const { store, queue } = await makeBackend();
      const a = await store.startRun({ name: "flow-a", version: 1, input: {} });
      await queue.enqueue(a.runId);

      expect(await queue.claim({ limit: 10, leaseMs: 1000, names: [], now: at(0) })).toEqual([]);
      const all = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(all.map((l) => l.runId)).toEqual([a.runId]);
    });

    it("leases a run-less job under a name filter so it can be acked, not skipped forever", async () => {
      const { store, queue } = await makeBackend();
      const a = await store.startRun({ name: "flow-a", version: 1, input: {} });
      await queue.enqueue(a.runId);
      await queue.enqueue("orphan-run"); // a job whose run row is gone (pruned or deleted out of band)
      const leased = await queue.claim({ limit: 10, leaseMs: 1000, names: ["flow-a"], now: at(0) });
      // the orphan is leased alongside flow-a so runTick's gone-path acks it, instead of a permanently
      // claimable job the name filter drops every tick.
      expect(leased.map((l) => l.runId).sort()).toEqual([a.runId, "orphan-run"].sort());
    });
  });
};
