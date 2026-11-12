import type { Wakeup } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";

/**
 * The Wakeup contract as executable invariants: a signal wakes current waiters early, a
 * missing signal still resolves on timeout (the poll tick), and a signal for another run
 * doesn't wake unrelated waiters.
 */
export const wakeupConformance = (
  label: string,
  makeWakeup: () => Wakeup | Promise<Wakeup>,
): void => {
  // The Wakeup port is real-time by nature. Tolerances are a wide fraction of each wait so timer
  // jitter can't flake them, while a broken waiter (never woken, woken by anything, latched) still fails.
  const LONG_MS = 30_000;
  const TICK_MS = 300;
  const UNRELATED_WAIT_MS = 2_000;
  const elapsedSince = (start: number): number => Date.now() - start;

  describe(`Wakeup conformance — ${label}`, () => {
    it("signal wakes a pending waiter early", async () => {
      const w = await makeWakeup();
      const start = Date.now();
      const p = w.wait("r1", LONG_MS);
      await w.signal("r1");
      await p;
      expect(elapsedSince(start)).toBeLessThan(LONG_MS / 10);
    });

    it("wait resolves on timeout when no signal arrives (the poll tick)", async () => {
      const w = await makeWakeup();
      const start = Date.now();
      await w.wait("r1", TICK_MS);
      expect(elapsedSince(start)).toBeGreaterThanOrEqual(TICK_MS / 2);
    });

    it("a signal for another run does not wake the waiter early", async () => {
      const w = await makeWakeup();
      let resolved = false;
      const p = w.wait("r1", UNRELATED_WAIT_MS).then(() => {
        resolved = true;
      });
      await w.signal("r2");
      await new Promise((r) => setTimeout(r, UNRELATED_WAIT_MS / 10));
      expect(resolved).toBe(false);
      await p;
      expect(resolved).toBe(true);
    });

    it("signal is a no-op when there are no waiters", async () => {
      const w = await makeWakeup();
      await expect(w.signal("nobody")).resolves.toBeUndefined();
    });

    it("is edge-triggered — a signal before any waiter does not latch", async () => {
      const w = await makeWakeup();
      await w.signal("r1");
      const start = Date.now();
      await w.wait("r1", TICK_MS);
      expect(elapsedSince(start)).toBeGreaterThanOrEqual(TICK_MS / 2);
    });

    it("multiple waiters on the same run all wake on one signal", async () => {
      const w = await makeWakeup();
      const start = Date.now();
      const ps = [w.wait("r1", LONG_MS), w.wait("r1", LONG_MS)];
      await w.signal("r1");
      await Promise.all(ps);
      expect(elapsedSince(start)).toBeLessThan(LONG_MS / 10);
    });
  });
};
