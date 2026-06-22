# ADR 0001 — Per-flow task routing (selective workers)

- **Status:** Accepted
- **Date:** 2026-06-22
- **Deciders:** iterativeflow maintainers

## Context

A process executes a flow only if it `register`s **and** `listen`s it — that is
the documented model, and it implies a process can subscribe to a _subset_ of
flows (e.g. a dedicated `worker-clone` that runs only the heavy clone flow,
isolated from a `worker-media` running everything else, while a monolith runs
the union).

Today that subset isolation **does not hold**. All flows are enqueued and
consumed through a single graphile-worker task:

- Enqueue — `src/adapters/graphile/index.ts`: `FLOW_TASK = "flow:run"`,
  `add_job(identifier => "flow:run", payload => {runId})` for _every_ flow.
- Worker — `startGraphileWorker` registers a single `taskList["flow:run"]`
  handler that reads `runId` and calls `runWorkflow(runId)`.

graphile-worker routes jobs by `task_identifier`. Because every worker registers
the same `"flow:run"` identifier, **every worker claims every flow's job**,
regardless of what it registered. The handler then resolves the flow via
`FlowRegistry.get(name, version)` (`src/engine/registry.ts`) and, if this
process didn't register that flow, the run **fails**:

```
flow "<name>" failed: No flow registered for "<name>@<version>"
```

Reproduced in a downstream consumer running two single-purpose workers on one
database: the media worker claimed a clone job and failed it. Splitting workers
by registered flow currently produces cross-claim failures, not isolation.

Crons already avoid this: each cron is registered under its own task identifier
`${CRON_TASK_PREFIX}${name}` (`src/adapters/graphile/cron.ts`), so graphile
routes a cron only to workers that defined it. Flows are the one work type still
collapsed onto a shared identifier.

## Decision

Give each flow its own graphile task identifier, mirroring crons:

```
flowTaskId(name, version) = `flow:run:${name}@${version}`
```

- **Enqueue** under the flow's own identifier.
- **Worker** builds its `taskList` from the flows this process actually
  registered (`FlowRegistry.list()`), one entry per flow, all sharing the
  existing `runWorkflow(runId)` handler body.

graphile-worker then hands a worker only the jobs whose `task_identifier` is in
its `taskList`. A worker physically cannot claim a flow it didn't register — the
isolation becomes the queue's responsibility instead of a post-claim registry
lookup that fails. `name@version` (not bare `name`) is used so a v1-only worker
does not claim a v2 run during a rolling deploy; it mirrors the registry key
`${name}@${version}`.

This is **not** a new constraint. "A worker is interested in flow X" already
means "the process registers X". The change makes routing _honor_ that
registration rather than ignore it. The three deployment shapes all compose from
the same flow definitions:

```
            same flow defs (cloneFlow, probeFlow, scheduleFlow, transcodeFlow)
                          │
   ┌──────────────┬───────┴────────┬───────────────────────┐
  api          worker-clone     worker-media             monolith
register all   register clone   register probe,          register ALL
DON'T listen() + listen()       schedule,transcode       + listen()
                                + listen()
  │                │                   │                      │
taskList: —     [clone]           [probe,sched,trans]    [clone,probe,sched,trans]
claims: none    clone only        media only             everything
(enqueue only)
```

## Design

### 1. Adapter — `src/adapters/graphile/index.ts`

Replace the `FLOW_TASK` constant with a helper:

```ts
export const flowTaskId = (name: string, version: number): string => `flow:run:${name}@${version}`;
```

**Enqueue.** `createGraphileTxEnqueue` builds `add_job(identifier => ...)` from
the flow identity. The adapter must _not_ read the `runs` table itself —
consumers can customize table names via `EngineOpts.tables`, and the adapter
only knows the graphile schema. So the identity is supplied by the caller (see
§3). The `TxEnqueue` contract gains the routing key:

```ts
// src/storage/drizzle/types.ts
export type TxEnqueue = (
  tx: WorkflowDb,
  job: { runId: string; name: string; version: number },
  opts?: EnqueueOpts,
) => Promise<void>;
```

`createGraphileTxEnqueue` then uses `flowTaskId(job.name, job.version)` for the
`identifier` and `flow:${job.runId}` for the `job_key` (unchanged). `noopEnqueue`
updates to the new signature (no-op body).

**Worker.** `startGraphileWorker` takes the registered flows and builds one task
entry per flow:

```ts
export interface GraphileWorkerOpts {
  // ...existing fields...
  flows: ReadonlyArray<{ name: string; version: number }>;
}

const flowTasks: TaskList = Object.fromEntries(
  opt.flows.map(({ name, version }) => [
    flowTaskId(name, version),
    async (payload: unknown, helpers: JobHelpers) => {
      const { runId } = payload as { runId?: string };
      if (!runId) {
        helpers.logger.warn("flow task missing runId");
        return;
      }
      await opt.runWorkflow(runId);
    },
  ]),
);

const options: RunnerOptions = {
  // ...
  taskList: { ...flowTasks, ...cronTasks },
};
```

(The handler body is identical to today's single `FLOW_TASK` handler — only the
identifier it's keyed under changes.)

Edge case: a process that registered **zero** flows but defines crons is valid
(a cron-only worker). `flowTasks` is then empty and `taskList` is just
`cronTasks` — graphile-worker accepts that. A process with neither flows nor
crons calling `listen()` is a misconfiguration; leave existing behavior.

### 2. Registry — `src/engine/registry.ts`

Add a read accessor (the map is private):

```ts
list(): ReadonlyArray<{ name: string; version: number }> {
  return [...this.map.values()].map(({ name, version }) => ({ name, version }));
}
```

### 3. Storage resolves identity once — single SELECT, not five call sites

Five places call the injected enqueue, each with only a `runId` in hand:

| File                               | Line (approx) | Context                                      |
| ---------------------------------- | ------------- | -------------------------------------------- |
| `src/storage/drizzle/ops.ts`       | ~284          | start/outbox — run just inserted in this txn |
| `src/storage/drizzle/retry.ts`     | ~42           | re-queue a failed run (row already locked)   |
| `src/storage/drizzle/signals.ts`   | ~67           | signal delivered → wake the run              |
| `src/storage/drizzle/notify.ts`    | ~21           | terminal → re-enqueue the parent run         |
| `src/storage/drizzle/reconcile.ts` | ~88           | orphan reconciler re-enqueues stale runs     |

The storage layer owns `tables.runs` (which has `name text NOT NULL` and
`version integer NOT NULL DEFAULT 1` — `src/storage/schema.ts:92-93`), so resolve
the identity there in one helper rather than widening five call sites. In
`createDrizzleStorage` (`src/storage/drizzle/index.ts`), wrap the injected
`TxEnqueue`:

```ts
const enqueueRun = async (tx: WorkflowDb, runId: string, opts?: EnqueueOpts): Promise<void> => {
  const [r] = await tx
    .select({ name: tables.runs.name, version: tables.runs.version })
    .from(tables.runs)
    .where(eq(tables.runs.id, runId))
    .limit(1);
  if (!r) throw new Error(`enqueue: run ${runId} not found`);
  await enqueue(tx, { runId, name: r.name, version: r.version }, opts);
};
```

Pass `enqueueRun` (signature `(tx, runId, opts?) => Promise<void>`) to the slices
via `StorageSliceDeps` **in place of** the raw `enqueue`, and change the five
call sites from `enqueue(tx, runId[, opts])` to `enqueueRun(tx, runId[, opts])`.
The raw `TxEnqueue` is now an internal detail consumed only by `enqueueRun`.

The run row is always visible at enqueue time: start inserts it in the same
outbox txn (`integration.test.ts:80` — "start() inserts a graphile_worker.add_job
inside the outbox txn"); retry/signals/reconcile operate on committed runs;
notify re-enqueues an existing parent. The extra read is a primary-key lookup
inside an already-open (often already-locked) transaction — negligible.

### 4. Engine wiring — `src/engine/engine.ts`

`listen()` already constructs `startGraphileWorker`. Pass the registered flows:

```ts
worker = await startGraphileWorker({
  // ...existing...
  flows: registry.list(),
});
```

The reconciler re-enqueue now also routes correctly: a stuck `clone` run is
re-enqueued under `flow:run:clone@1` and only a clone worker can pick it up.

## Consequences

- **Worker isolation works.** A worker claims only flows it registered; a
  monolith registers all and claims everything; the API registers all (for
  `.start` handles) but never `listen()`s, so it enqueues and claims nothing.
- **No central routing table.** Adding a flow needs no config: register it in
  whichever process should run it; the monolith picks it up automatically.
- **Reconciler/retry/signal re-enqueues route to the correct worker class.**
- **`register()` after `listen()` now throws.** The worker's `taskList` is fixed
  when graphile's `run()` is called inside `listen()`, built from
  `registry.list()`. A flow registered later would produce runs no worker can
  claim (silent hang), so `register()` fails fast after `listen()` — mirroring the
  existing `defineCron()` guard. Register all flows before `listen()`.
- **Public API change.** `FLOW_TASK` (currently `export const`) is removed in
  favor of `flowTaskId`. api-extractor will flag this — run `npm run api:update`
  and commit `etc/iterativeflow.api.md`. `TxEnqueue` is exported from
  `src/storage/drizzle` (`index.ts:15`); its signature change is part of the
  surface — document it in the changelog as a minor/breaking bump per the repo's
  semver policy.

## Migration / back-compat

The task-identifier scheme changes from `"flow:run"` to `"flow:run:<name>@<v>"`.
Jobs already sitting in `graphile_worker` under `"flow:run"` will **not** be
claimed after upgrade.

- **New deployments / empty queue:** clean cutover, nothing to do.
- **Live queues:** transitional — keep a legacy `"flow:run"` handler in
  `taskList` for one minor release (`async (p) => runWorkflow(p.runId)`) so
  pre-upgrade jobs drain, then remove it. Note this legacy handler reintroduces
  cross-claim **for stragglers only** during the drain window; document it as
  deprecated and removed in the next minor.

Cron routing is unaffected (already per-name).

## Test plan

Add `src/adapters/graphile/flow-routing.test.ts` using the two-pool testcontainer
harness from `multi-instance.test.ts` (Postgres container, `poolA`/`poolB`, two
engines on one DB, `applyFlowSchema`/`dropFlowSchema`):

1. **Isolation (the bug):** engine A registers + `listen()`s flow `alpha`;
   engine B registers + `listen()`s flow `beta`. `handle.start` an `alpha` run
   from a non-listening enqueue-only engine (registers both, no listen). Assert
   the run reaches `done` and ran on A; assert B never claimed it and produced no
   `No flow registered` failure. Symmetric for `beta` on B.
2. **Monolith:** one engine registers + listens both `alpha` and `beta`; both
   run to completion.
3. **Enqueue-only API shape:** an engine that registers both but never `listen()`s
   reports `health().worker === false` and starts runs that a separate worker
   completes.
4. Update `src/adapters/graphile/integration.test.ts` assertion that inspects the
   `add_job` identifier (now `flow:run:<name>@<version>`).
5. Update `src/storage/durability.test.ts` enqueue spies (lines ~36, ~256, and
   the `reenqueueOrphans` expectations ~316) for the new `TxEnqueue` job-object
   signature.

## Tooling gates (run all before opening the PR)

```
npm run typecheck
npm run lint
npm run test
npm run api:update     # regenerate etc/iterativeflow.api.md, then commit it
npm run docs:check     # docs examples still typecheck
npm run size:check
```

## Alternatives considered and rejected

- **Resolve identity inside the graphile adapter** (adapter does `SELECT name,
version FROM runs`). Rejected: couples the adapter to the consumer's `runs`
  table, which is customizable via `EngineOpts.tables`; the adapter only knows
  the graphile schema. Resolving in storage keeps the adapter pure.
- **Thread `name`+`version` through all five call sites individually.** Rejected:
  more churn, and several sites (signals, notify) don't already load the row —
  the single `enqueueRun` helper localizes it.
- **graphile-worker `forbiddenFlags` (deny-list).** Rejected: you'd flag every
  job with its flow and make every worker forbid all-other-flows — O(flows)
  config per worker, and every new flow forces touching every worker. The
  per-task-identifier approach is allow-list by construction and needs no central
  coordination.
- **Keep one `"flow:run"` task and filter claims by payload.** Not possible:
  graphile-worker selects work by `task_identifier` (and `forbiddenFlags`), not
  by payload contents.
