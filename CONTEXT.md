# Context — iterativeflow domain glossary

Shared vocabulary for the library. Use these terms exactly — across code, docs, comments, and architecture reviews. Drift makes refactors harder than they should be.

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

**Storage**
The Postgres-backed persistence interface that runs through drizzle-orm. Tx-scoped (`AtomicStorage`) and root-scoped (`Storage`) ops share their implementation via a closure-style builder. Don't add a second adapter unless something actually needs to vary across it.

**Dispatcher**
The seam that _drives_ runs: pulls work and calls `handleRun`. The default `GraphileDispatcher` owns a resident poll loop; a serverless host uses `createServerlessDispatcher` (no-op) and calls `engine.handleRun` from an HTTP route. State (the `Storage` above) is invariant across deployments; the **Dispatcher** is the line where the driver varies. See [ADR 0003](./docs/adr/0003-pluggable-scheduling-deployment-matrix.md).

**Wake**
A request to advance a run — start, resume, sleep (a future `run_at`), or signal. The seam is `TxEnqueue`, written inside the run's transaction: graphile `add_job` by default, swapped by the serverless adapters for the **wake outbox** (an adapter-owned table) or a pgmq message, drained externally. Say "wake", not "job".

## Stable, not negotiable

- The schema name `workflow` and the LISTEN channel `flow_terminal` are wire-level commitments — do not rename without a major version + migration.
- The `cursor key` scheme is replay-load-bearing. Any change requires a `REPLAY_INCOMPATIBLE_VERSION` boundary and a bump in `EXPECTED_SCHEMA_VERSION` if the storage representation shifts.
