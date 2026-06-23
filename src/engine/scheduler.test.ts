import { describe, expect, it, vi } from "vitest";
import { drizzle } from "drizzle-orm/node-postgres";
import { createEngine } from "./engine";
import type { Dispatcher, DispatcherStartOpts } from "./scheduler";

// createEngine reads only pool.options?.max at construction and never connects
// until listen() — so a stub pool is enough to test the dispatcher seam offline.
// The graphile/listen integration paths are covered by the testcontainer suites.
describe("dispatcher seam", () => {
  const fakePool = { options: { max: 10 } } as never;
  const db = drizzle({ client: {} as never });

  it("createEngine({ db, pool }) exposes handleRun with the default dispatcher", () => {
    const engine = createEngine({ db, pool: fakePool });
    expect(typeof engine.handleRun).toBe("function");
  });

  it("a custom dispatcher is accepted and drives health().worker", async () => {
    let captured: DispatcherStartOpts | undefined;
    const dispatcher: Dispatcher = {
      start: vi.fn(async (opts) => {
        captured = opts;
      }),
      stop: vi.fn(async () => {}),
      running: vi.fn(() => true),
    };

    const engine = createEngine({ db, pool: fakePool, dispatcher });
    const health = await engine.health();

    expect(dispatcher.running).toHaveBeenCalled();
    expect(health.worker).toBe(true);
    expect(captured).toBeUndefined(); // start() fires only on listen()
  });
});
