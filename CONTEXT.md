# Context — iterativeflow domain glossary

Shared vocabulary for the library. Use these terms exactly — across code, docs, comments, and architecture reviews. Drift makes refactors harder than they should be.

The domain terms below (Flow, Run, Step, Sleep, Signal, Invoke, Cursor key, Snapshot, Claim, Suspend, Terminal, Wake) are stable across both codebases. The **Storage / Dispatcher / Wake** implementation terms describe **v1** (drizzle + graphile), now preserved on the `v1` branch. The four-port **v2** engine is the repo root (`packages/*`) and has its own implementation vocabulary — see [v2 ports](#v2-ports).

## Terms

**Flow**
A versioned, registered piece of logic. Identified by `(name, version)`. A _definition_ — not an execution.

**Run**
One execution of a flow against a specific input. Has a `runId`, an input, an output (or error), and a status. A flow has many runs; a run belongs to one flow.

**Step**
One memoized side-effecting unit inside a run, declared via `ctx.step(name, fn)` or builder `.step(name, fn)`. Each step is identified by its **cursor key**. On replay, a completed step short-circuits to its persisted result.

**Sleep**
A pause in a run until a future wall-clock instant, declared via `ctx.sleep(duration)` or builder `.sleep(duration)`. Persisted as a row in the `workflow.timers` table — but in the **public vocabulary** we say "sleep", not "timer". `timer` is implementation, `sleep` is intent.

**Signal**
A point at which the run suspends and waits for an external `engine.signal(runId, name, payload)` delivery, declared via `ctx.signal(name)` or builder `.signal(name)`. Persisted as a row in `workflow.signals`. (Historical name was "hook"; do not reintroduce it.)

**Invoke**
Starting a child flow from inside a parent run via `ctx.invoke(handle, input)`. The parent suspends until the child terminates. Child runs are linked to their parent via `parent_run_id` + `parent_cursor_key`.

**Cursor key**
A deterministic string that identifies one node in a run's body (a step, sleep, signal, or invoke) by its position + declared name. Generated live by `cursor.next(base)` and persisted on every `workflow.steps` / `workflow.timers` / `workflow.signals` row. Same node, same key, every attempt — replay correctness hinges on this. Avoid `stepKey`; the key covers all four node kinds, not just steps.

**Cursor**
The stateful walker that produces the cursor-key sequence for one run's execution. Lives in `RuntimeFlowContext`.

**Snapshot**
The set of already-persisted step/timer/signal rows for a run, loaded once at claim time. On replay, the body re-executes from the top; nodes look up their cursor key in the snapshot and short-circuit if present.

**Claim**
The transactional act of locking a run row, bumping `attempts`, transitioning it to `running`, and loading its snapshot — performed once per execution attempt.

**Suspend**
A non-error throw from inside the run body (a `FlowSuspend`) that the runner catches to write a transient status (`sleeping`, `awaiting_signal`, `retrying`) and re-enqueue. Suspend is control flow, not failure.

**Terminal**
A run status that doesn't move again: `done`, `failed`, `canceled`. The engine fires `pg_notify('flow_terminal', runId)` exactly when a run reaches terminal.

**Storage** _(v1)_
The Postgres-backed persistence interface that runs through drizzle-orm. Tx-scoped (`AtomicStorage`) and root-scoped (`Storage`) ops share their implementation via a closure-style builder. Don't add a second adapter unless something actually needs to vary across it.

**Dispatcher** _(v1)_
The seam that _drives_ runs: pulls work and calls `handleRun`. The default `GraphileDispatcher` owns a resident poll loop; a serverless host uses `createServerlessDispatcher` (no-op) and calls `engine.handleRun` from an HTTP route. State (the `Storage` above) is invariant across deployments; the **Dispatcher** is the line where the driver varies. See [ADR 0003](./docs/adr/0003-pluggable-scheduling-deployment-matrix.md).

**Wake**
A request to advance a run — start, resume, sleep (a future `run_at`), or signal. In v1 the seam is `TxEnqueue`, written inside the run's transaction: graphile `add_job` by default, swapped by the serverless adapters for the **wake outbox** or a pgmq message. In v2 the same intent rides the **Queue** port + the transactional **Outbox**. Say "wake", not "job".

## v2 ports

The v2 engine (`packages/*`) is backend-agnostic: the engine speaks only four port interfaces (`core/src/ports/*`), and each backend (memory, postgres, dynamodb, redis, sqlite, mysql, mongodb, durable-objects) implements them against its own primitive.

**Store** — the durable checkpoint interface: runs, one memoized write per step, signals, crons, retention. Replaces v1 `Storage`.

**Queue** — wake dispatch: `enqueue`, `claim` (lease a batch), `heartbeat`, `ack`. A **lease** is an exclusive, expiring hold on a run; a stale/expired lease is re-claimable (crash recovery). Replaces the v1 `Dispatcher`/`TxEnqueue` split.

**Timer** — durable sleeps: a run's future wake instant, drained when due.

**Wakeup** — completion signalling for `result()`: poll-first, `signal` is a best-effort latency nudge (a missed one only costs latency, never correctness).

**Outbox** — the transactional seam: side-effects (spawn, enqueue, timers, signal-consume, join) that a Store write commits in the SAME transaction, so a crash can't leave state moved but the follow-on work lost. This is what makes the engine crash-safe on every backend.

**Backend** — the four ports over one substrate. **Conformance** — the shared `@iterativeflow/conformance` suites every backend must pass; the executable definition of "correct backend".

## Stable, not negotiable

- The schema name `workflow` and the LISTEN channel `flow_terminal` are wire-level commitments — do not rename without a major version + migration.
- The `cursor key` scheme is replay-load-bearing. Any change requires a `REPLAY_INCOMPATIBLE_VERSION` boundary and a bump in `EXPECTED_SCHEMA_VERSION` if the storage representation shifts.
