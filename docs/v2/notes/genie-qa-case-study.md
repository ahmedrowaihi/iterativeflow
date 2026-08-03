# Case study: Genie QA is a hand-rolled iterativeflow

> Status: design note. Genie QA (`Genie-sa/qa-github`, private) is an AI PR-QA product built
> independently of iterativeflow. It reached the same durable-execution architecture by hand. This
> note maps their engine to ours, states what a complete swap would replace vs. keep, and names the
> one adapter we'd need to build to make the swap real. Nothing here has been proposed to that team;
> it is our own analysis.

## Not a copy — a convergence

Genie QA has **zero** dependency on and **zero** reference to iterativeflow, and iterativeflow v2 is
unpublished alpha, so there was nothing to copy. They run on **`pg-boss` ^12** with a hand-written
orchestration layer. Both systems descend from the same durable-execution lineage (Temporal / DBOS /
Restate), so under real deadlines they converged on the same shapes: runs, sealed jobs, claims,
leases, deadlines, reconciliation, retry, supersession, and an atomic terminal commit.

That convergence is the point. An independent product team rebuilt the exact substrate iterativeflow
extracts — which is the strongest evidence the abstraction is real, and makes them a concrete first
customer rather than a competitor.

## What they built by hand

Their README describes a run moving through four stages, "each one hands the next a durable fact, so a
worker restart resumes instead of duplicating work." The implementation lives in
`packages/orchestrator/src/runner/` — roughly **60 files** whose names are, one-for-one, our ports:

| Genie QA (hand-rolled, `runner/`)                                   | iterativeflow primitive            |
| ------------------------------------------------------------------- | ---------------------------------- |
| `runner-start-claim-store`                                          | `Store.claim`                      |
| `runner-execution-lease-store`                                      | lease / lease renewal on claim     |
| `runner-execution-deadline-store`, `runner-db-deadline-budget`      | `Timer` port                       |
| `runner-reconciler*`, `runner-reconciler-runtime`, `-chaos.test`    | `reconcile` conformance suite       |
| `runner-repair-sweeper`                                             | `orphanedRuns` + `retryRun`        |
| `runner-retry-builder`, `runner-outcome-retry`                      | retry semantics                    |
| `runner-preexecution-supersession`                                  | `cancel` + a dedup key             |
| `runner-outcome-commit`, `runner-outcome-conflict`, `terminal-publication` | transactional **outbox** + terminal write |
| `runner-job-seal`, `runner-job-builder`                             | flow input (sealed, deterministic) |
| pg-boss consumers: intake, runner-start, reconciliation, repair     | `Queue` port + `tickOnce` loop     |
| the four README stages                                              | one `defineFlow` with memoized steps + `ctx.invoke` |

Everything in that table is undifferentiated plumbing they maintain (and chaos-test) themselves.
iterativeflow's `postgres` backend provides all of it, conformance-tested across nine suites, on the
same single Postgres primary they already run on Railway.

## What swaps vs. what stays

A "complete" swap replaces the **engine**, not the **product**. Their QA IP is untouched — it becomes
the body of steps.

**Swaps out (delete):**

- The entire `runner/` durable-execution substrate (claim / lease / deadline / reconcile / repair /
  retry / supersession / outcome-commit stores).
- `pg-boss` and its four consumers.
- The bespoke run/job/lease/deadline tables.

**Stays (domain — runs inside step bodies):**

- `@qa-ci/github` — webhooks, checks, comments, diffs, OAuth. Becomes the **signal source** (inbound)
  and a **terminal side-effect** (verdict → check + sticky comment).
- `@qa-ci/healthgate`, `@qa-ci/runner` (browser agent loop, DeepSeek/Qwen, evidence, receipt),
  `@qa-ci/sandbox` (Daytona), `@qa-ci/protocol` (sealed prompt), the AI proxy / billing metering, R2
  create-only uploads. All domain logic, unchanged.
- Their domain schema (repos, installations, PRs, verdicts, findings, usage). iterativeflow only owns
  the run/step/job/timer tables; domain tables live beside them in the same database.

The four stages become one flow:

```ts
const qaRun = defineFlow<IntakeInput, Verdict>({
  name: "qa-run", version: 1,
  run: async (ctx, intake) => {
    const job    = await ctx.step("seal", () => sealJob(intake));        // stage 2, frozen input
    const receipt = await ctx.step("execute", () => runInSandbox(job));  // stage 3, Daytona attempt
    return await ctx.step("publish", () => publishVerdict(receipt));     // stage 4, atomic, skip stale heads
  },
});
```

The long Daytona attempt fits their existing "receipt observed, deadline hit, or command gone" model
directly: the execute step launches the sandbox and the flow parks on a **signal or timer** — the
receipt webhook (or a poll) posts the signal that wakes it. That is exactly the
outbox + `consumeSignals` + `Timer` + `wakeup` machinery, which they rebuilt as
`runner-receipt-reader` + deadline stores.

## The one piece we'd build first: `@iterativeflow/webhooks` (built)

The swap needs a single net-new adapter on our side — the inbound webhook edge, and it's provider-
agnostic (GitHub is a preset, not the frame):

- **Inbound (signal + wakeup):** `webhookSignalBridge(backend, { verify: github(secret), correlate })`
  HMAC-verifies a webhook (`preview deployed`, `check re-run`, `/qa run` comment) and delivers it as a
  durable signal — `signalRun` → atomic `postSignal` + re-enqueue + best-effort `wakeup`, idempotent
  on the delivery id. This is their `intake.ts` / `runner-intake.ts`, generalized. A parked
  `await ctx.signal("qa:approved")` is the first-class human-approval gate — no bespoke ctx needed.
- **Outbound (verdict → check + sticky comment):** their `run-publisher` / `terminal-publication`
  stays app-side — publishing a GitHub check is their domain (they already own `@qa-ci/github`), so
  we deliberately do NOT reimplement GitHub REST in the library. It runs inside the flow's terminal
  step, with the "skip stale heads" head-SHA guard as app logic.

Everything else (claim, lease, deadline, reconcile, outbox, retry, cancel) already ships in
`@iterativeflow/postgres` and passes conformance.

## Migrating completely — strangler-fig, same database

A big-bang rewrite of a live QA product is the wrong move; the swap is incremental and reversible:

1. **Add** `@iterativeflow/postgres` alongside pg-boss on the same Postgres (its tables are additive,
   its own namespace). Build `@iterativeflow/github`.
2. **Model** the QA run as `qaRun` above, wrapping their existing already-idempotent package functions
   (`sealJob`, `runInSandbox`, `publishVerdict`) as step bodies. No QA logic changes.
3. **Dual-run:** route *new* intakes through `submit()`; let pg-boss drain in-flight runs. Compare
   verdicts on a shadow check before flipping the authoritative one.
4. **Delete reconciliation/repair** — `runner-reconciler*`, `runner-repair-sweeper` — the `reconcile`
   port + `orphanedRuns`/`retryRun` replace them.
5. **Delete** the lease / deadline / claim / retry / supersession stores as the flow subsumes them.
6. **Drop pg-boss** and the bespoke tables. The `runner/` directory collapses from ~60 files to a
   flow definition plus domain step bodies.

## Honest caveats

- **Not a throughput story.** Genie QA is human-PR-paced on a single Railway primary — comfortably
  within the pg backend. The win is *deletion of maintained plumbing and its chaos tests*, not scale.
- **Their outbox guarantee is already real.** They wrote `runner-outcome-conflict` and an atomic
  outcome commit; we're not selling them correctness they lack, we're selling them not owning it.
- **Sealed-job determinism must survive the port.** Their "sandbox receives no mutable authority"
  invariant maps to flow input being frozen and deterministic — a property to assert in migration
  tests, not assume.
- **This is our analysis, unsolicited.** Any pitch starts with the mapping table above and step 3
  (dual-run shadow), because that's the only version a team running a live product would accept.
