# Migration & schema ownership

Two separate things people often mix up:

1. **Owning your schema** — creating the engine's tables from your own infrastructure-as-code (IaC)
   or migration tool, adding foreign keys from your tables to the engine's, and querying them with
   full types. New in v2; works the same whether you are new or coming from v1.
2. **Moving in-flight data from v1 to v2** — the execution model changed, so you switch over to v2
   rather than copy rows in place. Covered last.

---

## 1. Owning your Postgres schema

The engine owns and manages its tables. You get three levels of control over _how they are created_:

| You want                                 | Use                                                          | When                                                                     |
| ---------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------ |
| Zero setup, dev / quickstart             | `applySchema(sql, schema?)`                                  | Local, tests, single-owner services. Idempotent, safe on every boot.     |
| The DDL as data, provisioned by your IaC | `ddl(schema)` → run in your migration                        | Prod under least privilege — the app role has no `CREATE`.               |
| A drizzle schema **you own**             | `drizzleSchema(schema)` / the `iterativeflow-pg-drizzle` bin | You already run drizzle-kit, want typed reads and FKs to `workflow.run`. |

### The drizzle route — typed queries, FKs, your own migrations

This brings back the v1 ability to own a drizzle schema **without** the v1 mistake of passing those
tables back into the engine. The engine never imports the file — you do.

```bash
# Emit a standalone schema file into your repo. Match --schema to createPgBackend/ddl.
npx iterativeflow-pg-drizzle src/db/iterativeflow.schema.ts --schema workflow
```

The emitted file is generated, not re-exported, on purpose: it is written against **your** installed
`drizzle-orm`, so a drizzle schema-builder API change (its `pg-core` surface still moves between
releases) can never break across our release and yours. Regenerate it when you bump the engine.

**Stable and beta both work.** The emitted syntax uses the array-form index callback +
`generatedAlwaysAsIdentity` — the form shared by current stable (`drizzle-orm >= 0.32`) and the
`1.0` beta. It is verified by strict `tsc` and a runtime import against both `0.45` and
`1.0.0-beta.22`. (The beta _removed_ the old object-form index callback, so it is the pre-0.31
legacy syntax that would break beta — not what we emit.) On drizzle `< 0.31`, upgrade or hand-edit
the two index callbacks to the object form.

```ts
// drizzle.config.ts — point drizzle-kit at the emitted file; migrate it like any other schema.
import { defineConfig } from "drizzle-kit";
export default defineConfig({
  schema: "./src/db/iterativeflow.schema.ts",
  dialect: "postgresql",
  // ...your db creds
});
```

```ts
// Typed reads and a foreign key from YOUR table to the engine's run table.
import { run } from "./db/iterativeflow.schema";
import { pgTable, text, uuid } from "drizzle-orm/pg-core";

export const transcodeJob = pgTable("transcode_job", {
  id: uuid("id").primaryKey().defaultRandom(),
  // FK to the workflow run that drives this job. Runs are never deleted, so this never dangles.
  runId: text("run_id")
    .notNull()
    .references(() => run.id),
  outputKey: text("output_key"),
});

// db.select().from(run).where(eq(run.status, "failed")) — fully typed, your connection, your queries.
```

The generated schema is verified against `ddl()` on a real Postgres in `drizzle.test.ts`: same
columns, types, nullability, identity, primary/foreign keys, and partial-index predicates. If the two
ever drift, that test fails — the file you own cannot silently diverge from what the engine runs on.

> **Do not** pass these tables back into the engine (there is no `tables:` option in v2). The engine
> manages its own tables; you own the _lifecycle_ (migrations) and _read_ side. Correlate your data
> to a run via **tags + your own tables + a FK**, never by extending the engine's columns.

## 1b. Owning your DynamoDB table

Same principle. The single table + its two GSIs, as data:

```ts
import { tableSpec, REQUIRED_IAM_ACTIONS, ensureTable } from "@iterativeflow/dynamodb";

// Dev / quickstart — create it and wait until active. Needs CreateTable IAM.
await ensureTable(lowLevelClient, "iterativeflow");

// Prod — provision from tableSpec in CDK / CloudFormation / Terraform, outside the app's IAM.
new dynamodb.CfnTable(this, "Workflow", tableSpec("iterativeflow") as any);
```

`REQUIRED_IAM_ACTIONS` is the exact action set the backend needs on the table + `index/*`.
`TransactWriteItems` and `ConditionCheckItem` are the two a CDK `grantReadWriteData` omits — grant
those explicitly, or every atomic write fails at runtime once the role is locked down.

For a Lambda + EventBridge deployment (no resident worker), drive the engine with `serverlessTick`:
one invocation fires due crons, re-drives orphans, drains due timers, and advances a batch — a durable
`ctx.sleep` survives across invocations. Size `leaseMs ≤ the Lambda timeout` (see its doc), or a run
claimed by a killed invocation stays leased until the oversized lease expires.

---

## 2. Moving from v1 to v2

v2 is a rewrite, not a schema patch. The tables differ in ways that make copying **in-flight** runs
across unsafe:

|              | v1                                         | v2                                         |
| ------------ | ------------------------------------------ | ------------------------------------------ |
| Run id       | `uuid`, generated by the DB                | plain `text` from your `IdGen`             |
| Run ordering | `created_at` / `updated_at` timestamps     | a counter (`seq`); no lifecycle timestamps |
| Timers       | one per `(run_id, cursor_key)`             | one per `run_id`                           |
| Signals      | one row per await point (`delivered` flag) | an inbox (`id`, `name`, `seq`, `idem_key`) |
| Dispatch     | external (graphile-worker)                 | a built-in `job` queue table               |
| Cron         | external                                   | a built-in `cron` table                    |

Because the timer, signal, and queue models changed, you cannot rebuild a _paused_ run's exact
waiting state by copying rows. So do not migrate in-flight work by hand. Pick one of:

### Strategy A — drain & cut over (recommended)

The standard way to move between versions of a durable-execution engine. No data migration, zero risk.

1. Deploy v2 alongside v1 (new schema, or a new database).
2. Stop submitting **new** work to v1; route new submissions to v2.
3. Let in-flight v1 runs finish on v1 (workflows are usually short-to-medium lived). Keep the v1 worker
   running until its `job`/queue is empty.
4. Decommission v1.

Keep idempotency across the switch: if the same external trigger can reach both, reuse the same
`idempotencyKey` so a request already handled by v1 is not run again on v2.

### Strategy B — backfill terminal history (optional, additive)

If you query completed runs for reporting, copy **finished** runs (and their steps) into v2 so you
can query them there. Safe because finished runs are never re-run — this is plain data, not waiting state.

```sql
-- Runs: uuid → text, drop v1-only lifecycle timestamps, let seq auto-assign in created_at order.
INSERT INTO "workflow_v2".run
  (id, name, version, status, input, output, error, attempts, idempotency_key, tags,
   parent_run_id, parent_cursor_key)
SELECT id::text, name, version, status, input, output, error, attempts, idempotency_key, tags,
       parent_run_id::text, parent_cursor_key
FROM "workflow".runs
WHERE status IN ('done', 'failed', 'canceled')
ORDER BY created_at;   -- new seq follows original completion order

-- Steps: same uuid→text cast, drop per-step timestamps.
INSERT INTO "workflow_v2".step (run_id, cursor_key, status, result, error, attempts)
SELECT s.run_id::text, s.cursor_key, s.status, s.result, s.error, s.attempts
FROM "workflow".steps s
JOIN "workflow".runs r ON r.id = s.run_id
WHERE r.status IN ('done', 'failed', 'canceled');
```

Do **not** copy `timers`, `signals`, `events`, or the queue: v2 rebuilds its queue via `reconcile`,
and the timer/signal shapes are incompatible. The `event` log is observability — start it fresh.

### Strategy C — replay long-lived in-flight runs (advanced, only if you must)

For runs too long-lived to drain (days-long sleeps), re-submit them to v2 with their original
`idempotencyKey`. v2 executes from the start and re-runs every step. **Only safe if your steps are
idempotent** (the whole point of `ctx.step`, but verify external side-effects — a re-run charge, a
re-sent email). Prefer A or B unless a specific run genuinely cannot drain.
