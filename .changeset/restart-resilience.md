---
"iterativeflow": patch
---

Boot validator + docs for restart behavior.

**Validator** — warns at engine boot when `runningStuckMs < defaultStepTimeoutMs`. The mismatch produces a real bug class: a step running between the two bounds is indistinguishable from a crashed process, so the reconciler resurrects it and you get two concurrent attempts of the same run.

```ts
createEngine({
  runningStuckMs: 60_000, // 1 min
  defaultStepTimeoutMs: 30 * 60_000, // 30 min ← BAD: step can outlive stuck threshold
});
// warns: flow.config.stuck_shorter_than_step_timeout
```

**Docs** — new "Restart behavior" section in `docs/guide.md` covers:

- What survives a restart (`running` runs → reconciler; `sleeping` runs → graphile; `awaiting_signal` runs → DB rows + NOTIFY; idempotency keys; cron advisory locks).
- What doesn't (`handle.result` / `handle.wait` in-process Promise waiters die on crash; caller must retry).
- At-least-once step semantics — make external calls idempotent.
- Crash-recovery latency = `runningStuckMs` (default 10 min); tune lower for tighter recovery, but respect the new validator.
- Multi-instance / rolling deploy safety (FOR UPDATE SKIP LOCKED + cross-instance NOTIFY).
