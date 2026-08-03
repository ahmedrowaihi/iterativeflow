# Proposal: self-scheduling `serverlessTick` (a `nextWakeAt` horizon)

- **Status:** Proposed (not built). Field report from a Lambda + EventBridge + DynamoDB
  deployment driving COLD MediaLive channel applies (stop → poll-until-IDLE → update → start).
- **Scope:** `core` — a `nextWakeAt` on `SweepResult` (and `engine.nextWakeAt()`), backed by a
  `Timer.nextDueAt()` port method every backend already can answer cheaply. Plus a documented
  self-scheduling serverless-driver recipe in `docs/serverless` (v2). Additive; no behaviour change
  for existing resident (`engine.run()`) or fixed-cadence serverless drivers.

## Why

`serverlessTick` is "one bounded cycle per invocation; an external trigger wakes the next." Today
that external trigger is a **fixed cadence** (EventBridge/Vercel/CF cron). That's fine for *starting*
new work (push a wake on submit) but wrong for **timers**: a `ctx.sleep` resumes on the next
scheduled firing, not when it's actually due. On AWS the cron floor is **1 minute**, so a flow that
polls an external resource every 15s (`poll → ctx.sleep(15s) → poll`) actually advances **once a
minute** — the durable sleep is correct, the *latency* is the cron period, not the sleep.

The driver can't fix this itself, because `SweepResult` reports what **happened**, not what's
**pending**:

```ts
interface SweepResult { fired: number; reconciled: number; results: TickResult[] }
```

During a 15s sleep every tick returns all-zeros (nothing due *yet*) even though a run is about to
wake in 12s. So a self-terminating loop can't tell "genuinely idle" from "one run sleeping, due
soon" — it either exits too early (back to cron latency) or polls the store blindly at a fixed
interval. The missing primitive is a **horizon: when is the next timer due?**

## Shape

One number closes the gap. `serverlessTick` already drains due timers off the timer index (in
`@iterativeflow/dynamodb`, `gsi1` ordered by due time; every backend keeps timers due-ordered), so
the **earliest future due time is one bounded read** — no scan, no hot-path cost.

```ts
interface SweepResult {
  fired: number;
  reconciled: number;
  results: TickResult[];
  /** Earliest FUTURE timer due (sleep / retry backoff / cron), or null when none is pending.
   *  Signals/child-joins are NOT here — those wake by enqueue (push), not a timer. */
  nextWakeAt: Date | null;
}

// Port (each backend: one limit-1 query on the already-due-ordered timer index):
interface Timer { /* … */ nextDueAt(now: Date): Promise<Date | null> }
// Surfaced standalone too, for a driver that wants the horizon without a tick:
engine.nextWakeAt(): Promise<Date | null>;
```

That turns the serverless driver from *fixed-cadence polling* into *self-scheduling*:

```ts
// One invocation: advance due work, then schedule the EXACT next wake (or exit).
const { nextWakeAt } = await engine.serverlessTick();
if (nextWakeAt) {
  await scheduleOneShot(nextWakeAt); // EventBridge Scheduler one-time | SQS delay | Step Functions wait
}
// else: nothing pending — return and let the process die. A low-frequency heartbeat
// (e.g. every few minutes) is the only always-on cost, as a safety net + orphan reconcile.
```

Combined with a **push wake on submit/signal** (already the recommended pattern — invoke the tick
once when new work is enqueued), this is the complete model:

- **submit / signal** → push a wake now → starts in ~1s.
- **timer (sleep / retry)** → the tick returns `nextWakeAt` → driver arms a one-shot for exactly
  then → resumes on time, at any granularity, with no fixed poll.
- **nothing pending** (`nextWakeAt == null`, empty `results`) → **exit; the process dies**. Cost
  drops to just the heartbeat until the next external event.

Cost scales with *pending work*, not wall-clock: idle deployments pay a heartbeat; busy ones wake
precisely, sub-second, without a resident daemon.

## Notes / open questions

- **`nextWakeAt` is the timer horizon only.** Signal- and child-waits wake by enqueue (push), not a
  due time; they're out of scope for this number (a driver still needs the submit/signal push).
  Worth stating explicitly in the type doc so nobody expects `nextWakeAt` to cover a parked signal.
- **Cheapness is a backend property.** The contract must say `nextDueAt` is O(1)-ish (a bounded read
  on the due-ordered index), so no backend implements it as a scan. Postgres = `min(run_at)` on the
  wake/timer table; DynamoDB = limit-1 query on `gsi1` where `gsi1pk = TIMER`, ascending, `sk > now`.
- **Heartbeat still required.** Even with precise self-scheduling, keep a low-freq cron as the
  reconcile/orphan path (a crash between "tick returned nextWakeAt" and "one-shot armed" would
  otherwise strand a sleep). Same role `reconcile` already plays.
- **The one-shot mechanism is the deployer's choice**, not the library's — EventBridge Scheduler
  (one-time schedule at `nextWakeAt`), an SQS message with `DelaySeconds` (≤15m), a Step Functions
  `Wait`, or `setTimeout` + self-invoke for short horizons. The library only needs to *hand over the
  time*; a recipe per platform belongs in `docs/serverless`.
- **Backwards compatible.** Existing fixed-cadence drivers ignore `nextWakeAt` and keep working;
  `engine.run()` is unaffected (it already polls on its own cadence).
