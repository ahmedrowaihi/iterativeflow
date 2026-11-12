import { randomUUID } from "node:crypto";
import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { T0 } from "#clock";

/**
 * Races that only a backend with concurrent writers can lose: parallel checkpoints on one cursor
 * agree on a single winner, and parallel claims never lease one run twice. A claim may come back
 * short under contention, so the claim race drains over bounded rounds.
 */
export const concurrencyConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Concurrency conformance — ${label}`, () => {
    it("parallel checkpoints on one cursor key spawn exactly one child", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "p", version: 1, input: {} });
      const childIds = Array.from({ length: 8 }, () => randomUUID());
      const results = await Promise.all(
        childIds.map((cid) =>
          store
            .checkpointStep(
              { runId, cursorKey: "spawn", status: "ok", result: cid, attempts: 1 },
              { spawn: [{ runId: cid, spec: { name: "c", version: 1, input: {} } }] },
            )
            .then((o) => o.result),
        ),
      );
      expect(new Set(results).size).toBe(1);
      const created = (await store.loadRunRows(childIds)).filter(Boolean);
      expect(created.map((r) => r?.id)).toEqual([results[0]]);
    });

    it("parallel claims lease each run to exactly one worker", async () => {
      const { store, queue } = await makeBackend();
      const ids: string[] = [];
      for (let i = 0; i < 20; i++) {
        ids.push((await store.startRun({ name: "f", version: 1, input: { i } })).runId);
      }
      await queue.enqueueMany(ids.map((runId) => ({ runId })));
      const claimed: string[] = [];
      for (let round = 0; round < 10 && claimed.length < ids.length; round++) {
        const batches = await Promise.all(
          Array.from({ length: 4 }, () => queue.claim({ limit: 10, leaseMs: 600_000, now: T0 })),
        );
        claimed.push(...batches.flat().map((l) => l.runId));
      }
      expect(claimed).toHaveLength(ids.length);
      expect(new Set(claimed)).toEqual(new Set(ids));
    });
  });
};
