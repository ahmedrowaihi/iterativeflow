# Opt-in: live cross-process progress push

- **Status:** Built. `applyProgressTrigger(sql, schema?)` installs the trigger; `createPgListener`
  exposes `watch(runId)` (async iterator) and `onProgress(cb)` (fleet view) on the existing socket.
  Opt-in and off the worker hot path — install the trigger only on a dashboard host. Real-time
  step-level progress over `LISTEN/NOTIFY`; polling `engine.status()` / the event log still backstops.
- **Scope:** `@iterativeflow/postgres` only, layered on the existing opt-in event log + the one
  `createPgListener` connection.

## Why this is a separate, opt-in thing

v1 iterativeflow fired a `pg_notify` **per step of every run, always** — even with no observer — on a
shared channel. That per-step flood is exactly what forced the transcoder to disable the whole push
system (`notify:false`). The engine's hot path must never pay for progress nobody is watching. So a
v2 progress push is deliberately **not** on the run hot path: its cost is proportional to _observed_
activity, not to total steps executed.

Three properties make that true, and they are the whole design:

1. **Rides the already-opt-in event log.** A deployment only has an `event` table populated when it
   set `events: "lifecycle"` (or finer). The progress trigger lives on **that** table, so a
   deployment with events off pays nothing, and a worker pod that isn't a dashboard host never
   installs it.
2. **Reuses the one existing listener connection.** No new connection per observer — the `progress`
   channel is a third channel multiplexed on the same `createPgListener` socket that already carries
   `wake` and `done`.
3. **Scoped by payload.** An observer filters to the run(s) it cares about; the notify is not a
   global fan-out of every run's every step to every listener.

## Shape

### DDL — installed only where a dashboard runs

```ts
export const applyProgressTrigger = (sql: Sql, schema?: string) => Promise<void>;
```

```sql
CREATE OR REPLACE FUNCTION "<schema>".if_notify_progress() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('<schema>_progress',
  json_build_object('runId', NEW.run_id, 'type', NEW.type)::text); RETURN NULL; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER if_progress AFTER INSERT ON "<schema>".event
  FOR EACH ROW EXECUTE FUNCTION "<schema>".if_notify_progress();
```

Payload stays tiny (`runId` + event `type`, well under the 8 KB NOTIFY limit) — an observer that
wants the full row reads it by id, so a chatty run never ships large payloads.

### Listener API — a third channel + a scoped subscription

`createPgListener` gains a `progress` subscription. The ergonomic surface is an async iterator:

```ts
for await (const ev of listener.watch(runId)) {
  // ev: { runId, type } — one per event as it lands, across processes
}
```

or a callback for a fleet view:

```ts
const off = listener.onProgress((ev) => dashboard.push(ev)); // all runs; filter client-side
```

Internally it's the same waiter pattern as the `done` side — a `Map<runId, subscribers>` fed by the
`progress` channel — so it composes with the existing listener with no new connection or loop.

### Coalescing

Default: none — a live view usually wants every event. A per-run debounce (one notify per ~N ms per
run) is an option for very high-event-rate runs, but the real flood control is that the trigger is
installed **only where observed**, not the payload rate.

## What it deliberately is NOT

- Not on the worker/execution path — installing it on a worker pod would be a mistake; it belongs on
  the dashboard host.
- Not a correctness mechanism — like every other push here, it's pure observation latency; the event
  log is the durable source and polling it is always the backstop.
- Not a new connection — it multiplexes the existing `createPgListener` socket.

## Relationship to the other push channels

`wake` (dispatch) and `done` (completion) are about _work_ and _waiting on a run_; `progress` is
about _watching_ a run. All three share one listener connection and the same "poll/read backstops a
missed notify" contract. Enable each independently: a worker pod installs the `wake`/`done` triggers;
a dashboard host installs `progress`.
