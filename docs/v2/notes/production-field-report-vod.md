# Production field report: vod-media-convert (v1 → evaluating alpha.3)

- **Source:** a second production user. v1 5.6.0 in prod (VOD clone/transcode, direct Aurora, no pooler,
  split worker pods). Evaluating v2 `2.0.0-alpha.3`.
- **Status:** field notes + alpha.4 wishlist. LOCAL-ONLY (like the serverless report).

## v1 problems (their words, condensed)

1. Blind re-dispatch on a stalled step — an inspect run (38k files / ~91 GB) outlived its step; the
   level-triggered reconcile backstop re-dispatched it 25× over ~4.5h, re-running the expensive parse.
2. Connection/pool bursts — dropped mid-step → DrizzleQueryError; migrate `pg_advisory_lock` flaked.
   Worked around with `pool.on("error")`, an EXTERNAL_RETRY policy, per-step `timeoutMs`.
3. Lost error cause → `[object Object]` — FlowError persists only `err.message`; the real pg cause is on
   `.cause`, uncaptured. Worked around with a `failNormalized`/`dbStep` helper.
4. No observability seam (v1) — bolted on dd-trace + manual metrics.
5. FlowSuspend-by-throwing footgun — a bare catch cancels the run; sprinkled `failUnlessSuspend`.
6. Tests need real Postgres — leaked testcontainers.
7. Rolling-deploy LISTEN drain — ~1–2 min dispatch stall window.

## What alpha.3 already solves (their acknowledgement)

Poll-first default (LISTEN opt-in), `SKIP LOCKED` + CAS leases, `ObserveOpts`, the memory backend,
typed errors, drizzle schema ownership → maps onto v1 problems 1, 2, 4, 6, 7. (Problem 5 = our field
report #1 — a `try/catch` around `ctx.*` is now safe in alpha.3 too.)

## alpha.4 wishlist — triage (grounded in alpha.3 source)

| # | Ask | Verdict | Where |
| --- | --- | --- | --- |
| 1 | `FlowError.cause` capture | **Real gap, highest value, clean.** `toFlowError` (executor.ts:78) keeps only `{code,message,stack}` — drops `.cause`. Walk the cause chain into a structured field. | `core` `toFlowError` + `FlowError` type |
| 2 | Reclaim circuit-breaker (`maxReclaims`, N=3) | **Mostly already handled.** `markRunning` bumps `attempts` on EVERY claim incl. a lease-expiry reclaim, and `attempt > maxAttempts` dead-letters WITHOUT executing (executor.ts:201-217); attempts reset to 0 on a clean suspend, so sleep-loops are safe. Their 25× loop caps at `maxAttempts` (default 10) in v2. A *separate lower* `maxReclaims` only buys killing an expensive stall faster than the shared retry budget — optional refinement, real design question. | `core` (optional) |
| 3 | pg transient-error classify preset | **Clean build.** The `classify` hook exists (alpha.3, context.ts:51). Ship a `pgTransient` classifier (connection terminated / statement timeout / deadlock 40P01 / serialization 40001) so consumers don't reinvent EXTERNAL_RETRY. | `postgres` new export |
| 4 | Error-sink recipe in observe docs | **Docs.** Leverages #1 — capture the failing step's redacted params + `.cause` so `[object Object]` never reaches a record. | docs |
| 5 | Idempotent-step helper (nice-to-have) | **Partly a non-issue.** A *committed* step is already a replay no-op via the memo. The gap is a re-claimed *in-flight* (uncommitted) step re-running — inherent at-least-once. Guidance + maybe a thin helper. | docs / small helper |

### Recommended alpha.4 cut
- **Build:** #1 (cause capture — the big one), #3 (pg classify preset), #4 (error-sink docs, folds in #5 guidance).
- **Clarify, don't build blind:** #2 — tell them it's already bounded by `maxAttempts`; add a separate
  `maxReclaims` only if they want the tighter stall-specific kill (needs a design nod on the counter).
