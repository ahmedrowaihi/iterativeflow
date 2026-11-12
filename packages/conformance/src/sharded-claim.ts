import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { T0 } from "#clock";

/**
 * A horizontally-sharded worker pool where each pod claims only the flow names it registered.
 * Proves that two pods claiming ONE backend concurrently are SAFE — no run is ever leased by both,
 * and no pod ever leases a run outside its shard — and LIVE: together they drain both shards. Server
 * backends only; single-writer backends can't issue concurrent transactions, so their name-shard
 * claim is covered sequentially by {@link claimFilterConformance}.
 *
 * Under contention a claim may come back short (rows a concurrent claim holds are skipped, not
 * waited on) — that may cost a round, never safety. So the suite checks safety on every round and
 * liveness within a bounded number of rounds.
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
      const gotA = new Set<string>();
      const gotB = new Set<string>();
      const claim = async (name: string, into: Set<string>): Promise<void> => {
        for (const l of await backend.queue.claim({
          limit: 10,
          leaseMs: 600_000,
          now: T0,
          names: [name],
        }))
          into.add(l.runId);
      };
      // An idle pod backs off, so a pod whose shard is drained stops polling. Bounded so a claim
      // that never hands back its shard fails the test instead of hanging it.
      for (let round = 0; round < 6 && (gotA.size < 3 || gotB.size < 3); round++) {
        await Promise.all([
          gotA.size < 3 ? claim("a", gotA) : Promise.resolve(),
          gotB.size < 3 ? claim("b", gotB) : Promise.resolve(),
        ]);
        expect([...gotA].filter((id) => gotB.has(id))).toEqual([]);
      }
      expect([...gotA].every((id) => aIds.has(id))).toBe(true);
      expect([...gotB].every((id) => bIds.has(id))).toBe(true);
      expect(gotA.size).toBe(3);
      expect(gotB.size).toBe(3);
    });
  });
};
