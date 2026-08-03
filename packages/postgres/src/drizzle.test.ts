import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { ddl, drizzleSchema } from "@iterativeflow/postgres";
import { generateDrizzleJson, generateMigration } from "drizzle-kit/api";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

// Two schemas, same durable tables built two ways: `ddl()` (what the engine runs on) vs the
// generated drizzle schema pushed through drizzle-kit (what a consumer owns). If the emitted file
// ever drifts from the DDL, the introspected shapes diverge and these assertions fail.
const DDL_SCHEMA = "wf_ddl";
const DRZ_SCHEMA = "wf_drz";

const NAMED_INDEXES = [
  "run_parent",
  "run_idem",
  "run_status",
  "job_claimable",
  "timer_due",
  "signal_inbox",
  "signal_idem",
  "event_run",
  "cron_due",
];

describe.skipIf(skip)("drizzle schema mirrors ddl()", () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  let tmp: string;

  beforeAll(async () => {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    pool = new Pool({ connectionString: container.getConnectionUri() });
    pool.on("error", () => undefined);

    // 1. The engine's DDL, verbatim.
    await pool.query(ddl(DDL_SCHEMA));

    // 2. The consumer's generated drizzle schema, materialized through drizzle-kit into SQL and
    //    applied to its own schema — exactly what `drizzle-kit push`/migrate would do on their side.
    const pkgRoot = fileURLToPath(new URL("..", import.meta.url));
    tmp = mkdtempSync(join(pkgRoot, "if-drz-"));
    const file = join(tmp, "schema.ts");
    writeFileSync(file, drizzleSchema(DRZ_SCHEMA));
    const mod = await import(pathToFileURL(file).href);
    const statements = await generateMigration(generateDrizzleJson({}), generateDrizzleJson(mod));
    await pool.query(`CREATE SCHEMA IF NOT EXISTS "${DRZ_SCHEMA}"`);
    for (const stmt of statements) {
      if (/^\s*CREATE SCHEMA/i.test(stmt)) continue; // created above, idempotently
      await pool.query(stmt);
    }
  }, 180_000);

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await container?.stop().catch(() => undefined);
    if (tmp) rmSync(tmp, { recursive: true, force: true });
  });

  /** Columns as (table, name, type, nullable, identity, default) — the shape a consumer queries. */
  const columns = async (schema: string) => {
    const { rows } = await pool.query(
      `SELECT table_name, column_name, data_type, is_nullable, is_identity,
              identity_generation, column_default
       FROM information_schema.columns
       WHERE table_schema = $1
       ORDER BY table_name, column_name`,
      [schema],
    );
    return rows;
  };

  /** PK + FK membership as (table, type, column, ref_table, ref_column) — names elided (they differ). */
  const keys = async (schema: string) => {
    const { rows } = await pool.query(
      `SELECT tc.table_name, tc.constraint_type, kcu.column_name,
              ccu.table_name AS ref_table, ccu.column_name AS ref_column
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       LEFT JOIN information_schema.constraint_column_usage ccu
         ON tc.constraint_name = ccu.constraint_name AND tc.constraint_type = 'FOREIGN KEY'
       WHERE tc.table_schema = $1 AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
       ORDER BY tc.table_name, tc.constraint_type, kcu.column_name`,
      [schema],
    );
    return rows;
  };

  /** The explicitly-named secondary indexes, schema-qualifier stripped, def normalized by Postgres. */
  const indexes = async (schema: string) => {
    const { rows } = await pool.query<{ indexname: string; indexdef: string }>(
      "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1",
      [schema],
    );
    return rows
      .filter((r) => NAMED_INDEXES.includes(r.indexname))
      .map((r) => ({ indexname: r.indexname, def: r.indexdef.replaceAll(`${schema}.`, "") }))
      .sort((a, b) => a.indexname.localeCompare(b.indexname));
  };

  it("has the same columns, types, nullability, identity, and defaults", async () => {
    expect(await columns(DRZ_SCHEMA)).toEqual(await columns(DDL_SCHEMA));
  });

  it("has the same primary keys and foreign keys", async () => {
    expect(await keys(DRZ_SCHEMA)).toEqual(await keys(DDL_SCHEMA));
  });

  it("has the same named indexes, including partial predicates", async () => {
    const drz = await indexes(DRZ_SCHEMA);
    const dl = await indexes(DDL_SCHEMA);
    expect(drz.map((i) => i.indexname)).toEqual(NAMED_INDEXES.slice().sort());
    expect(drz).toEqual(dl);
  });
});
