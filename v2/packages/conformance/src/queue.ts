import type { Queue } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The Queue contract as executable invariants. The universal lease-CAS backend and a
 * Postgres `SKIP LOCKED` backend must both satisfy exactly these — leasing is exclusive,
 * leases expire and re-claim (crash recovery), heartbeats extend, and a stale ack is a
 * no-op. Clock is injected so the suite is deterministic on any backend.
 */
export const queueConformance = (label: string, makeQueue: () => Queue | Promise<Queue>): void => {
  describe(`Queue conformance — ${label}`, () => {
    it("enqueue then claim returns the run with a lease", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const leases = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toEqual(["r1"]);
      expect(leases[0].token).toBeTruthy();
      expect(leases[0].expiresAt.getTime()).toBe(at(1000).getTime());
    });

    it("a future runAt is not claimable until due", async () => {
      const q = await makeQueue();
      await q.enqueue("r1", { runAt: at(5000) });
      expect(await q.claim({ limit: 10, leaseMs: 1000, now: at(0) })).toEqual([]);
      const due = await q.claim({ limit: 10, leaseMs: 1000, now: at(5000) });
      expect(due.map((l) => l.runId)).toEqual(["r1"]);
    });

    it("a claimed run is leased exclusively — a second claim before expiry gets nothing", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const first = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(first).toHaveLength(1);
      const second = await q.claim({ limit: 10, leaseMs: 1000, now: at(500) });
      expect(second).toEqual([]);
    });

    it("an expired lease is re-claimable (crash recovery), with a fresh token", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const first = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      const reclaim = await q.claim({ limit: 10, leaseMs: 1000, now: at(1001) });
      expect(reclaim.map((l) => l.runId)).toEqual(["r1"]);
      expect(reclaim[0].token).not.toBe(first[0].token);
    });

    it("heartbeat extends the lease so it isn't re-claimed at the original expiry", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [lease] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await q.heartbeat(lease, { leaseMs: 1000, now: at(500) }); // new expiry at(1500)
      expect(await q.claim({ limit: 10, leaseMs: 1000, now: at(1001) })).toEqual([]);
      const after = await q.claim({ limit: 10, leaseMs: 1000, now: at(1501) });
      expect(after.map((l) => l.runId)).toEqual(["r1"]);
    });

    it("heartbeat on a lost (re-claimed) lease throws", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [stale] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await q.claim({ limit: 10, leaseMs: 1000, now: at(1001) }); // re-claim, new owner
      await expect(q.heartbeat(stale, { leaseMs: 1000, now: at(1002) })).rejects.toThrow();
    });

    it("ack removes the run so it isn't claimed again", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [lease] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await q.ack(lease, { now: at(1) });
      expect(await q.claim({ limit: 10, leaseMs: 1000, now: at(2000) })).toEqual([]);
    });

    it("a stale ack is a no-op — it does not delete the run the new owner holds", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [stale] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await q.claim({ limit: 10, leaseMs: 1000, now: at(1001) }); // re-claim
      await q.ack(stale, { now: at(1002) }); // stale token — must not remove
      const still = await q.claim({ limit: 10, leaseMs: 1000, now: at(2002) });
      expect(still.map((l) => l.runId)).toEqual(["r1"]);
    });

    it("a wake that re-enqueues during a lease survives the ack — the job is released, not deleted", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [lease] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await q.enqueue("r1"); // a child-wake / postSignal bumps the version while we hold the lease
      await q.ack(lease, { now: at(1) }); // version changed → must NOT delete
      const again = await q.claim({ limit: 10, leaseMs: 1000, now: at(2) });
      expect(again.map((l) => l.runId)).toEqual(["r1"]); // still there, immediately re-claimable
    });

    it("an expired ack is a no-op — an expired owner can't delete a re-claimable run", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [lease] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) }); // expires at(1000)
      await q.ack(lease, { now: at(1001) }); // expired → CAS fails → no-op
      const still = await q.claim({ limit: 10, leaseMs: 1000, now: at(1002) });
      expect(still.map((l) => l.runId)).toEqual(["r1"]);
    });

    it("heartbeat after expiry throws (lease lost even without a re-claim)", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      const [lease] = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      await expect(q.heartbeat(lease, { leaseMs: 1000, now: at(1001) })).rejects.toThrow();
    });

    it("re-enqueue does not yank an active lease from the current worker", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      await q.claim({ limit: 10, leaseMs: 1000, now: at(0) }); // leased until at(1000)
      await q.enqueue("r1", { priority: 1 }); // re-enqueue while leased
      expect(await q.claim({ limit: 10, leaseMs: 1000, now: at(500) })).toEqual([]); // still held
    });

    it("claim honours the batch limit", async () => {
      const q = await makeQueue();
      for (let i = 0; i < 5; i++) await q.enqueue(`r${i}`);
      const leases = await q.claim({ limit: 2, leaseMs: 1000, now: at(0) });
      expect(leases).toHaveLength(2);
    });

    it("lower priority is claimed first", async () => {
      const q = await makeQueue();
      await q.enqueue("low", { priority: 10 });
      await q.enqueue("high", { priority: 1 });
      const [first] = await q.claim({ limit: 1, leaseMs: 1000, now: at(0) });
      expect(first.runId).toBe("high");
    });

    it("re-enqueue is an upsert keyed by runId — claimed once", async () => {
      const q = await makeQueue();
      await q.enqueue("r1");
      await q.enqueue("r1", { priority: 5 });
      const leases = await q.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toEqual(["r1"]);
    });
  });
};
