# Serverless durable execution

iterativeflow runs durably on your Postgres. By default it runs inside a
long-lived Node process (a resident graphile-worker that polls the queue). On
serverless platforms — Vercel, AWS Lambda, Cloudflare Workers — there is no
long-lived process, so the driver changes shape:

- **State stays in Postgres.** Runs, steps, timers, signals, and step
  memoization are unchanged. Your workflow state never leaves your database.
- **The wake comes from outside.** A function invocation does one
  `claim → replay → run-to-suspend → persist` cycle (`engine.handleRun`) and
  exits. An external trigger wakes the next cycle.

This guide covers the `iterativeflow/serverless` adapter: the transactional
**wake outbox** and the entry points that drive it.

## How it works

Every place that would enqueue a graphile job instead records a row in a small
**outbox** table — written inside the same transaction that suspends or starts
the run, so the wake commits atomically with the run state (no dual write). The
outbox carries `run_at`, so a delayed resume (a sleep, or a retry backoff)
survives without graphile.

An external trigger (a cron, a queue consumer, or Postgres itself) periodically
**drains** the due rows and calls `engine.handleRun(runId)` for each.

```
start / sleep / signal  ──▶  outbox row (run_id, run_at)   [inside the run's txn]
                                      │
        scheduled /drain trigger ─────┤
                                      ▼
                         engine.handleRun(runId)   ── one claim→replay→suspend cycle
```

## Which setup do I need?

Two questions decide it — **is your app compute always-on?** and **is your
Postgres scale-to-zero?**

```
                    Is your APP compute always-on?
                    (container / VM / long-lived node)
                           │
                  YES ─────┴───── NO  (Lambda / Vercel / CF Workers)
                   │               │
            ┌──────▼──────┐        │   Is your Postgres scale-to-zero?
            │  RESIDENT    │       │   (Neon / Vercel PG / Aurora Serverless)
            │  (default)   │       │          │
            │ engine.listen│   NO ─┴─ YES      │
            └──────────────┘  (always-on PG)   │
                              │                 │
                    ┌─────────▼────────┐  ┌─────▼──────────────────┐
                    │ want zero extra  │  │  SERVERLESS + outbox    │
                    │ infra?           │  │  external /drain trigger│
                    └───┬──────────┬───┘  │  (only option here)     │
                    YES │          │ NO   └─────────────────────────┘
              ┌─────────▼───┐  ┌───▼──────────────┐
              │ IN-DB:       │  │ SERVERLESS:       │
              │ outbox/pgmq  │  │ outbox + external │
              │ + pg_cron    │  │ /drain trigger    │
              └──────────────┘  └───────────────────┘
```

| Your situation                                        | Enqueue            | Drive                  | This guide's section                                                  |
| ----------------------------------------------------- | ------------------ | ---------------------- | --------------------------------------------------------------------- |
| Always-on app + any Postgres                          | graphile (default) | `engine.listen()`      | — (the default; see README)                                           |
| Serverless app + always-on Postgres, zero extra infra | outbox or pgmq     | `pg_cron` + `pg_net`   | [Inside Postgres](#running-it-all-inside-postgres-always-on-database) |
| Serverless app + **scale-to-zero** Postgres           | outbox or pgmq     | external `/drain` cron | [Setup](#setup) + [endpoints](#the-three-endpoints)                   |

> **Forcing constraint.** `pg_cron` / `pg_net` need an always-on Postgres
> background worker — they do **not** fire on a scale-to-zero database, and
> `pg_net` is unavailable on RDS/Aurora. Scale-to-zero ⇒ external trigger is the
> only path.

## Setup

Install the outbox table once (at deploy, or from a migration) with
`createWakeOutboxTable(db)`, then build the engine with the outbox enqueue and
the no-op serverless dispatcher:

```ts
import { createEngine } from "iterativeflow";
import { createOutboxEnqueue, createServerlessDispatcher } from "iterativeflow/serverless";

export const engine = createEngine({
  db,
  pool,
  worker: { enqueue: createOutboxEnqueue() },
  dispatcher: createServerlessDispatcher(),
  results: "poll",
});
```

Register your flows as usual. **Do not call `engine.listen()`** — there is no
resident worker to start. You drive execution through HTTP routes instead.

`results: "poll"` makes `handle.result()` / `handle.wait()` throw on a
non-terminal run instead of silently blocking on a `LISTEN` connection a function
that exits cannot hold — see [Getting results](#getting-results).

## The three endpoints

A serverless deployment exposes three routes. The example below is
framework-agnostic pseudo-code — adapt the request/response handling to Vercel,
Lambda, Hono, Express, etc.

<!-- doc-check: skip (framework glue; references a local engine module) -->

```ts
import { drainAndRun } from "iterativeflow/serverless";
import { engine } from "./engine";

// POST /run  — push model: a queue/webhook hands you one runId to advance.
export async function runRoute(body: { runId: string }) {
  await engine.handleRun(body.runId);
}

// POST /drain — pull model: a scheduled trigger drains every due wake.
export async function drainRoute() {
  const { ran } = await drainAndRun(engine, db);
  return { ran };
}

// POST /cron — periodic recovery + retention (see "Guarantees" below).
export async function cronRoute() {
  await engine.reconcile();
  // optional: await engine.pruneRuns({ olderThan, ... })
}
```

You only need **one** of `/run` (push) or `/drain` (pull):

- **Pull** — schedule `/drain` every N seconds (Vercel Cron, Cloudflare Cron
  Triggers, EventBridge). Simplest; the DB is the queue.
- **Push** — give `createOutboxEnqueue` an after-commit hook that POSTs the new
  `runId` to `/run` for sub-second latency, and keep `/drain` as a low-frequency
  safety sweep for the `run_at`-in-the-future rows (sleeps, retries).

## Sleeps and signals

Both wake through the same outbox — no special handling:

- **Sleep** — the suspend writes an outbox row with `run_at = wake time`.
  `drainDueWakes` ignores it until due, then returns it.
- **Signal** — `engine.signal(runId, name, payload)` delivers the payload and
  writes an immediate outbox row, so the next drain resumes the run.

## Getting results

A blocking `handle.result()` needs a resident `LISTEN` connection, which a
function that exits cannot hold. On serverless, **poll status** instead:

<!-- doc-check: skip (illustrative) -->

```ts
const detail = await engine.status(runId);
if (detail?.run.status === "done") {
  // detail.run.output
}
```

For push-style completion, fire a webhook when a run reaches a terminal state
(wire it where the resident build fires `flow_terminal`).

## Using pgmq instead of the built-in outbox

The wake outbox above needs no extensions — it is a plain table. If you already
run [pgmq](https://github.com/pgmq/pgmq) (a Postgres-native message queue), the
`iterativeflow/pgmq` subpath swaps the outbox for a pgmq queue, which adds a
native **visibility timeout**: a wake whose `handleRun` crashes reappears
automatically after the timeout, so redelivery does not depend on the
reconciler.

```ts
import { createEngine } from "iterativeflow";
import { createServerlessDispatcher } from "iterativeflow/serverless";
import { createPgmqEnqueue } from "iterativeflow/pgmq";

export const engine = createEngine({
  db,
  pool,
  worker: { enqueue: createPgmqEnqueue() },
  dispatcher: createServerlessDispatcher(),
});
```

Run `CREATE EXTENSION pgmq;` and `createPgmqQueue(db)` once at deploy. The
`/drain` route uses `drainAndRunPgmq(engine, db)` in place of `drainAndRun` — it
reads visible messages, advances each run, and deletes the message on success.
A future `run_at` (a sleep, a retry backoff) becomes a pgmq `delay`, so the
message stays invisible until the wake is due.

> pgmq needs an always-on Postgres (it is queried, not background-worked, so it
> works on any Postgres — but a scale-to-zero database must still be woken by an
> external `/drain` trigger).

## Running it all inside Postgres (always-on database)

If your Postgres is **always-on** (self-hosted, Supabase, RDS 24/7 — not a
scale-to-zero database), you can drive the drain from inside Postgres with
`pg_cron` + `pg_net`, with **zero external scheduler**. A cron job reads the due
outbox rows and POSTs each `runId` to your `/run` endpoint:

```sql
-- requires: create extension pg_cron;  create extension pg_net;
select cron.schedule('iterativeflow-drain', '5 seconds', $$
  select net.http_post(
    url     := current_setting('app.run_url'),               -- your /run endpoint
    headers := jsonb_build_object('Content-Type', 'application/json',
                                  'Authorization', current_setting('app.run_secret')),
    body    := jsonb_build_object('runId', run_id)
  )
  from iterativeflow_wake_outbox
  where run_at <= now()
  order by priority nulls last, run_at
  limit 100;
$$);
```

> **Scale-to-zero caveat.** `pg_cron` and `pg_net` need an always-on Postgres
> background worker — they do **not** fire while a scale-to-zero database (Neon,
> Vercel Postgres, Aurora Serverless) is suspended, and `pg_net` is unavailable
> on RDS/Aurora. On a scale-to-zero database, use an external scheduler hitting
> `/drain` instead — it wakes both your function and the database.

## Guarantees and recovery

- **Exactly-once advance.** `drainDueWakes` claims a row by deleting it, and
  `engine.handleRun`'s claim makes a double-drain harmless (the second attempt
  is a no-op once the run is `running`/terminal).
- **Crash recovery.** If a process dies between the drain and `handleRun`, the
  outbox row is gone but the run is still in a non-terminal state. Schedule
  `engine.reconcile()` (the `/cron` route above) — it re-enqueues stuck runs
  back onto the outbox. **You must schedule it**; it is the recovery path the
  resident build runs automatically.
- **Retention.** `engine.pruneRuns(...)` / `engine.pruneEvents(...)` from the
  same `/cron` route keep the tables bounded.

## API

| Export                               | Purpose                                             |
| ------------------------------------ | --------------------------------------------------- |
| `createWakeOutboxTable(db, opts?)`   | Create the outbox table (run once).                 |
| `createOutboxEnqueue(opts?)`         | `TxEnqueue` that records wakes in the outbox.       |
| `drainDueWakes(db, { now, limit? })` | Claim and return due `runId`s.                      |
| `drainAndRun(engine, db, opts?)`     | Drain + `handleRun` each, in one call.              |
| `createServerlessDispatcher()`       | No-op dispatcher (so `listen()`/`stop()` are safe). |
| `engine.handleRun(runId)`            | Advance one run by one cycle.                       |
| `engine.reconcile()`                 | Re-enqueue orphaned runs.                           |

From `iterativeflow/pgmq` (optional, needs the `pgmq` extension):

| Export                               | Purpose                                                  |
| ------------------------------------ | -------------------------------------------------------- |
| `createPgmqQueue(db, opts?)`         | Create the pgmq queue (run once).                        |
| `createPgmqEnqueue(opts?)`           | `TxEnqueue` that sends wakes to pgmq (delay = sleep).    |
| `drainAndRunPgmq(engine, db, opts?)` | Read visible wakes, `handleRun` each, delete on success. |
