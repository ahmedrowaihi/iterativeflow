/**
 * Emit a standalone drizzle-orm schema file that mirrors the durable tables {@link ddl} creates.
 *
 * The engine never imports this — you own the emitted file. Drop it in your repo to get typed
 * reads (`db.select().from(run)`), foreign keys from your own tables to `workflow.run`, and a
 * drizzle-kit migration source you control. It is generated (not re-exported from this package)
 * on purpose: the file is written against whatever `drizzle-orm` version you have installed, so
 * a drizzle schema-builder API change can't break across our release and yours.
 *
 * `drizzle.test.ts` applies both this schema (via drizzle-kit) and {@link ddl} to a real Postgres
 * and asserts the two produce the same columns, keys, and indexes — the emitted file cannot drift
 * from the DDL the engine actually runs on.
 */

/** A durable table's shape, mapped from the DDL — the single source the emitted TS is built from. */
interface TableModel {
  /** SQL table name; also the exported binding name (every table name is a valid JS identifier). */
  sql: string;
  columns: ColumnModel[];
  /** Composite primary key column bindings; omit for a single-column `.primaryKey()`. */
  compositePk?: string[];
  extras?: IndexModel[];
}

interface ColumnModel {
  /** JS property (camelCase). */
  js: string;
  /** The full drizzle pg-core builder call, e.g. `text("id").primaryKey()`. */
  build: string;
}

interface IndexModel {
  name: string;
  unique?: boolean;
  /** Column bindings the index is `.on(...)`. */
  on: string[];
  /** Raw predicate for a partial index, referencing `t.<js>` columns. */
  where?: string;
}

const t = (js: string, build: string): ColumnModel => ({ js, build });

const MODEL: TableModel[] = [
  {
    sql: "run",
    columns: [
      t("id", `text("id").primaryKey()`),
      t("seq", `bigint("seq", { mode: "number" }).generatedAlwaysAsIdentity()`),
      t("name", `text("name").notNull()`),
      t("version", `integer("version").notNull()`),
      t("status", `text("status").notNull()`),
      t("input", `jsonb("input")`),
      t("output", `jsonb("output")`),
      t("error", `jsonb("error")`),
      t("attempts", `integer("attempts").notNull().default(0)`),
      t("idempotencyKey", `text("idempotency_key")`),
      t("tags", `text("tags").array()`),
      t("parentRunId", `text("parent_run_id")`),
      t("parentCursorKey", `text("parent_cursor_key")`),
      t("joinRemaining", `integer("join_remaining").notNull().default(0)`),
    ],
    extras: [
      { name: "run_parent", on: ["parentRunId"], where: "sql`${t.parentRunId} is not null`" },
      {
        name: "run_idem",
        unique: true,
        on: ["name", "version", "idempotencyKey"],
        where: "sql`${t.idempotencyKey} is not null`",
      },
      { name: "run_status", on: ["status"] },
    ],
  },
  {
    sql: "step",
    columns: [
      t("runId", `text("run_id").notNull().references(() => run.id)`),
      t("cursorKey", `text("cursor_key").notNull()`),
      t("status", `text("status").notNull()`),
      t("result", `jsonb("result")`),
      t("error", `jsonb("error")`),
      t("attempts", `integer("attempts").notNull()`),
      t("shape", `text("shape")`),
    ],
    compositePk: ["runId", "cursorKey"],
  },
  {
    sql: "job",
    columns: [
      t("runId", `text("run_id").primaryKey()`),
      t("runAt", `timestamp("run_at", { withTimezone: true }).notNull().defaultNow()`),
      t("priority", `integer("priority").notNull().default(0)`),
      t("version", `bigint("version", { mode: "number" }).notNull().default(0)`),
      t("leaseToken", `text("lease_token")`),
      t("leaseExpires", `timestamp("lease_expires", { withTimezone: true })`),
    ],
    extras: [
      {
        name: "job_claimable",
        on: ["priority", "runAt"],
        where: "sql`${t.leaseExpires} is null`",
      },
    ],
  },
  {
    sql: "timer",
    columns: [
      t("runId", `text("run_id").primaryKey()`),
      t("fireAt", `timestamp("fire_at", { withTimezone: true }).notNull()`),
    ],
    extras: [{ name: "timer_due", on: ["fireAt"] }],
  },
  {
    sql: "signal",
    columns: [
      t("id", `text("id").primaryKey()`),
      t("runId", `text("run_id").notNull().references(() => run.id)`),
      t("name", `text("name").notNull()`),
      t("payload", `jsonb("payload")`),
      t("seq", `bigint("seq", { mode: "number" }).generatedAlwaysAsIdentity()`),
      t("idemKey", `text("idem_key")`),
    ],
    extras: [
      { name: "signal_inbox", on: ["runId", "seq"] },
      {
        name: "signal_idem",
        unique: true,
        on: ["runId", "idemKey"],
        where: "sql`${t.idemKey} is not null`",
      },
    ],
  },
  {
    sql: "event",
    columns: [
      t("seq", `bigint("seq", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey()`),
      t("runId", `text("run_id").notNull()`),
      t("type", `text("type").notNull()`),
      t("at", `timestamp("at", { withTimezone: true }).notNull()`),
      t("data", `jsonb("data")`),
    ],
    extras: [{ name: "event_run", on: ["runId", "seq"] }],
  },
  {
    sql: "cron",
    columns: [
      t("name", `text("name").primaryKey()`),
      t("schedule", `text("schedule").notNull()`),
      t("flowName", `text("flow_name").notNull()`),
      t("flowVersion", `integer("flow_version").notNull()`),
      t("input", `jsonb("input")`),
      t("overlap", `text("overlap").notNull().default("allow")`),
      t("nextRunAt", `timestamp("next_run_at", { withTimezone: true }).notNull()`),
      t("lastRunAt", `timestamp("last_run_at", { withTimezone: true })`),
    ],
    extras: [{ name: "cron_due", on: ["nextRunAt"] }],
  },
];

const IMPORTS = [
  "bigint",
  "index",
  "integer",
  "jsonb",
  "pgSchema",
  "primaryKey",
  "text",
  "timestamp",
  "uniqueIndex",
];

const emitExtras = (extras: IndexModel[]): string => {
  const lines = extras.map((ix) => {
    const fn = ix.unique ? "uniqueIndex" : "index";
    const on = ix.on.map((c) => `t.${c}`).join(", ");
    const where = ix.where ? `.where(${ix.where})` : "";
    return `    ${fn}("${ix.name}").on(${on})${where},`;
  });
  return lines.join("\n");
};

const emitTable = (m: TableModel): string => {
  const cols = m.columns.map((c) => `    ${c.js}: ${c.build},`).join("\n");
  const parts: string[] = [];
  if (m.compositePk) {
    parts.push(`    primaryKey({ columns: [${m.compositePk.map((c) => `t.${c}`).join(", ")}] }),`);
  }
  if (m.extras) parts.push(emitExtras(m.extras));
  const extraBlock = parts.length ? `, (t) => [\n${parts.join("\n")}\n  ]` : "";
  return `export const ${m.sql} = workflow.table(\n  "${m.sql}",\n  {\n${cols}\n  }${extraBlock},\n);`;
};

/**
 * Generate the TypeScript source of a consumer-owned drizzle schema for the workflow tables.
 * Write it into your repo (see the `iterativeflow-pg-drizzle` bin) and point drizzle-kit at it.
 *
 * @param schema Postgres schema name — must match the one you pass to `createPgBackend` / `ddl`.
 */
export const drizzleSchema = (schema = "workflow"): string => {
  const tables = MODEL.map(emitTable).join("\n\n");
  const exportNames = MODEL.map((m) => m.sql).join(", ");
  const selectTypes = MODEL.map(
    (m) => `export type ${cap(m.sql)}Row = typeof ${m.sql}.$inferSelect;`,
  ).join("\n");
  return `// Generated by @iterativeflow/postgres — a drizzle schema for the iterativeflow tables.
// You own this file: run your own migrations from it, add foreign keys to "${schema}".run, and
// query the tables with full type-safety. Regenerate with \`iterativeflow-pg-drizzle\` if you
// upgrade the engine. Do NOT pass these tables back into the engine — it manages them itself.
// Uses the array-form index callback + generatedAlwaysAsIdentity — valid on drizzle-orm stable
// (>= 0.32) and the 1.0 beta (verified against 0.45 and 1.0.0-beta.22).
import { ${IMPORTS.join(", ")} } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

export const workflow = pgSchema("${schema}");

${tables}

${selectTypes}

export const schema = { ${exportNames} };
`;
};

const cap = (s: string): string => s.charAt(0).toUpperCase() + s.slice(1);
