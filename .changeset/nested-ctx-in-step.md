---
"@iterativeflow/core": patch
---

Fix: a `ctx.*` call inside a `ctx.step` body no longer corrupts replay, and a suspend inside a step
is no longer treated as a step failure.

Two bugs on the same boundary, both reachable from the idiom the README documents — the builder hands
`ctx` to every step fn, and the docs say "sleeps, signals, and invokes happen through `ctx` inside a
step".

**Cursor skew.** `step()` took its cursor key before running the body, so a nested `ctx.sleep` /
`ctx.signal` / `ctx.invoke` consumed the keys after it while the outer step's memo committed at the
earlier one. The invocation that resumed the step wrote its memo, and the _next_ replay returned that
memo without re-running the body — so the following call landed on the nested call's memo, raised
`FlowDriftError`, parked, and re-claimed until the run dead-lettered as `RUN_ATTEMPTS_EXHAUSTED`. The
run was unrecoverable: no redeploy fixes an already-skewed cursor. A call issued inside a step body
now keys off that step (`s1.0`, `s1.1`) instead of the flat cursor, so the body can never shift the
keys of anything after it. Flows that don't nest are unaffected — their keys are byte-identical.

**Suspends ran the failure policy.** `runWithPolicy` passed the thrown control signal to `classify`
and counted it against `retries`. A `classify` written the documented way — treat anything
unrecognised as `permanent` — turned an ordinary `ctx.sleep` into a `StepFailedError` and failed the
run; with `retries > 0` a normal park instead re-ran the whole step body after `retryDelayMs`,
re-executing its side effects. Control signals now short-circuit both, matching the guard the
executor already applies.

Note the remaining property, now covered by a test: a step body containing a suspend re-runs **from
the top** on resume, because the enclosing step's memo is only written once the body returns. Put
side effects in their own `ctx.step` rather than alongside a nested sleep.

**Upgrading with runs in flight:** a run that is parked _inside_ a nested `ctx.*` call when you deploy
this resumes with the new key scheme and drift-parks — the guard catches it, it is not silently wrong,
but it needs the usual drift recovery (redeploy the old body, or bump the flow version). Runs that
never nest are unaffected: their keys are byte-identical. Drain nesting flows before upgrading if you
can.
