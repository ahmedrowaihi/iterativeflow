import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { isTerminal } from "@iterativeflow/core/backend";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createPgBackend } from "#backend";
import { pgClassify } from "#classify";
import { applySchema } from "#schema";
import { pgPool } from "#sql";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

// The shape a driver produces: the SQLSTATE rides on `.code`, wrapped down a `.cause` chain
// (the DrizzleQueryError shape). pgClassify must walk that chain, not the surface message.
const pgError = (code: string): Error => {
  const inner = new Error(`db rejected with ${code}`);
  (inner as { code?: string }).code = code;
  return new Error("Failed query: ...", { cause: inner });
};

describe.skipIf(skip)(
  "pgClassify — transient vs permanent step errors, driven on real postgres",
  () => {
    let container: StartedPostgreSqlContainer;
    let pool: Pool;

    beforeAll(async () => {
      container = await new PostgreSqlContainer("postgres:16-alpine").start();
      pool = new Pool({ connectionString: container.getConnectionUri() });
      pool.on("error", () => undefined);
      await applySchema(pgPool(pool));
    }, 180_000);

    afterAll(async () => {
      await pool?.end().catch(() => undefined);
      await container?.stop().catch(() => undefined);
    });

    beforeEach(async () => {
      await pool.query(
        'TRUNCATE "workflow".run, "workflow".job, "workflow".timer, "workflow".cron,' +
          ' "workflow".step, "workflow".signal, "workflow".event RESTART IDENTITY CASCADE',
      );
    });

    const drive = async (
      backend: ReturnType<typeof createPgBackend>,
      flows: ReturnType<typeof registry>,
      runId: string,
    ): Promise<{ status: string; output?: unknown }> => {
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 30; i++) {
        await tickOnce(backend, flows, {
          batchMax: 8,
          leaseMs: 600_000,
          now,
          retry: { maxAttempts: 5, baseDelayMs: 1, maxDelayMs: 1 },
        });
        const run = await backend.store.loadRunRow(runId);
        if (run && isTerminal(run.status)) return run;
        clock = new Date(clock.getTime() + 60_000);
      }
      throw new Error("run did not settle");
    };

    it("retries a transient SQLSTATE (deadlock 40P01) and completes once it clears", async () => {
      const backend = createPgBackend(pgPool(pool));
      let attempts = 0;
      const flow = defineFlow({
        name: "deadlock-then-ok",
        version: 1,
        run: async (ctx): Promise<string> =>
          ctx.step(
            "write",
            () => {
              attempts += 1;
              if (attempts < 3) throw pgError("40P01"); // deadlock_detected — transient, retryable
              return "written";
            },
            { classify: pgClassify },
          ),
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId);
      expect(run).toMatchObject({ status: "done", output: "written" });
      expect(attempts).toBe(3);
    });

    it("fails fast on a permanent SQLSTATE (not_null_violation 23502) — no retry", async () => {
      const backend = createPgBackend(pgPool(pool));
      let attempts = 0;
      const flow = defineFlow({
        name: "permanent-violation",
        version: 1,
        run: async (ctx): Promise<string> =>
          ctx.step(
            "write",
            () => {
              attempts += 1;
              throw pgError("23502"); // not_null_violation — permanent, fails fast
            },
            { classify: pgClassify },
          ),
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId);
      expect(run.status).toBe("failed");
      expect(attempts).toBe(1);
    });
  },
);
