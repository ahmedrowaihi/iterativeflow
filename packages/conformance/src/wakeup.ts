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
  describe(`Wakeup conformance — ${label}`, () => {
    it("signal wakes a pending waiter early", async () => {
      const w = await makeWakeup();
      let resolved = false;
      const p = w.wait("r1", 5000).then(() => {
        resolved = true;
      });
      await w.signal("r1");
      await p;
      expect(resolved).toBe(true);
    });

    it("wait resolves on timeout when no signal arrives (the poll tick)", async () => {
      const w = await makeWakeup();
      const start = Date.now();
      await w.wait("r1", 25);
      expect(Date.now() - start).toBeGreaterThanOrEqual(20);
    });

    it("a signal for another run does not wake the waiter early", async () => {
      const w = await makeWakeup();
      let resolved = false;
      const p = w.wait("r1", 60).then(() => {
        resolved = true;
      });
      await w.signal("r2");
      await new Promise((r) => setTimeout(r, 15));
      expect(resolved).toBe(false); // still waiting on r1
      await p; // eventually times out
      expect(resolved).toBe(true);
    });

    it("signal is a no-op when there are no waiters", async () => {
      const w = await makeWakeup();
      await expect(w.signal("nobody")).resolves.toBeUndefined();
    });

    it("is edge-triggered — a signal before any waiter does not latch", async () => {
      const w = await makeWakeup();
      await w.signal("r1"); // fired with no waiter — must not be remembered
      const start = Date.now();
      await w.wait("r1", 25); // must still wait the tick, not resolve instantly
      expect(Date.now() - start).toBeGreaterThanOrEqual(20);
    });

    it("multiple waiters on the same run all wake on one signal", async () => {
      const w = await makeWakeup();
      let woke = 0;
      const ps = [w.wait("r1", 5000), w.wait("r1", 5000)].map((p) =>
        p.then(() => {
          woke += 1;
        }),
      );
      await w.signal("r1");
      await Promise.all(ps);
      expect(woke).toBe(2);
    });
  });
};
