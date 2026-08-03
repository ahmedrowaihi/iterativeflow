import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";

/**
 * A horizontally-sharded worker pool where each pod claims only the flow names it registered.
 * Proves that two pods claiming ONE backend concurrently are SAFE — no run is ever leased by both,
 * and no pod ever leases a run outside its shard — and LIVE: together they drain both shards. Server
 * backends only; single-writer backends (sqlite / memory / durable-objects) can't issue concurrent
 * transactions, so their name-shard claim is covered sequentially by {@link claimFilterConformance}.
 *
 * Per-round throughput is backend-specific: Postgres locks only the name-matched rows, so both shards
 * drain in one round; MySQL's `FOR UPDATE OF j SKIP LOCKED` locks the eligible `job` head before the
 * `run`-side name filter applies, so the first claimer transiently blocks the other's shard, which
 * fills on the next round. Both are safe — the loop asserts safety every round and liveness overall.
 */
export const shardedClaimConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`sharded-claim conformance (${label})`, () => {
    it("two pods claim disjoint flow-name shards concurrently — no overlap, each drains its own shard", async () => {
      const backend = await makeBackend();
      const mk = async (name: string): Promise<string> => {
        const { runId } = await backend.store.startRun({ name, version: 1, input: {} });
        await backend.queue.enqueue(runId);
        return runId;
      };
      const aIds = new Set(await Promise.all([mk("a"), mk("a"), mk("a")]));
      const bIds = new Set(await Promise.all([mk("b"), mk("b"), mk("b")]));
      const now = new Date("2030-01-01T00:00:00Z");
      const gotA = new Set<string>();
      const gotB = new Set<string>();
      // A pod that has drained its shard stops competing (as a real backed-off pod would), so the
      // first round claims both shards concurrently (the safety check) and later rounds let the
      // behind pod finish — otherwise a pod re-locking a foreign head every lock-stepped poll could
      // starve the other on backends that over-lock (see MySQL note above).
      const claim = async (name: string, into: Set<string>): Promise<void> => {
        for (const l of await backend.queue.claim({
          limit: 10,
          leaseMs: 600_000,
          now,
          names: [name],
        }))
          into.add(l.runId);
      };
      for (let round = 0; round < 6 && (gotA.size < 3 || gotB.size < 3); round++) {
        await Promise.all([
          gotA.size < 3 ? claim("a", gotA) : Promise.resolve(),
          gotB.size < 3 ? claim("b", gotB) : Promise.resolve(),
        ]);
        expect([...gotA].filter((id) => gotB.has(id))).toEqual([]); // never the same run in both pods
      }
      expect([...gotA].every((id) => aIds.has(id))).toBe(true); // pod {a} only ever claimed a-runs
      expect([...gotB].every((id) => bIds.has(id))).toBe(true); // pod {b} only ever claimed b-runs
      expect(gotA.size).toBe(3); // both shards fully drained
      expect(gotB.size).toBe(3);
    });
  });
};
