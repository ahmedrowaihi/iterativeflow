import {
  cronConformance,
  engineConformance,
  outboxConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";
import { afterEach, describe, expect, it } from "vitest";
import { createSqliteBackend } from "#backend";
import { applySchema } from "#schema";
import { libsqlDb } from "#sql";

describe("sqlite backend", () => {
  const cleanups: (() => void)[] = [];

  afterEach(() => {
    for (const c of cleanups.splice(0)) c();
  });

  // A fresh temp-file database per test — a file (not `:memory:`) so the write transaction's
  // connection shares it. No container needed.
  const makeBackend = async (): Promise<Backend> => {
    const dir = mkdtempSync(join(tmpdir(), "iflow-sqlite-"));
    const client = createClient({ url: `file:${join(dir, "test.db")}` });
    cleanups.push(() => {
      client.close();
      rmSync(dir, { recursive: true, force: true });
    });
    const sql = libsqlDb(client);
    await applySchema(sql);
    return createSqliteBackend(sql);
  };

  storeConformance("sqlite", async () => (await makeBackend()).store);
  queueConformance("sqlite", async () => (await makeBackend()).queue);
  timerConformance("sqlite", async () => (await makeBackend()).timer);
  wakeupConformance("sqlite", async () => (await makeBackend()).wakeup);
  outboxConformance("sqlite", () => makeBackend());
  signalConformance("sqlite", () => makeBackend());
  reconcileConformance("sqlite", () => makeBackend());
  cronConformance("sqlite", async () => (await makeBackend()).store);
  engineConformance("sqlite", () => makeBackend());

  describe("engine end-to-end", () => {
    const TERMINAL = new Set(["done", "failed", "canceled"]);
    const drive = async (
      backend: Backend,
      flows: ReturnType<typeof registry>,
      runId: string,
    ): Promise<{ status: string; output: unknown }> => {
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 100; i++) {
        await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
        const run = (await backend.store.loadRun(runId))?.run;
        if (run && TERMINAL.has(run.status)) return { status: run.status, output: run.output };
        clock = new Date(clock.getTime() + 2_000);
      }
      throw new Error("run did not settle");
    };

    it("invokes a child flow across the outbox and resumes with its output", async () => {
      const backend = await makeBackend();
      const child = defineFlow<{ n: number }, number>({
        name: "s-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "s-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "done", output: 51 });
    });
  });
});
