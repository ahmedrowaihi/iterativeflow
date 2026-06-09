---
"iterativeflow": major
---

Stop hiding consequential behavior behind defaults.

Two defaults silently took actions the developer didn't ask for. Both now hand the decision back:

- **`StepOpts.retries` defaults to `0`** (was `3`). A step runs once and its failure is terminal unless you opt in with `retries: N`. Previously every step silently retried up to 4× with exponential backoff; you had to write `retries: 0` to get a single run. (Steps re-run on crash recovery regardless, so side-effecting bodies should already be idempotent.)
- **`engine.listRuns({ limit })` throws when `limit > 500`** instead of silently clamping to 500. Asking for more than the max now surfaces an error rather than truncating the page without a signal.

Migration: if you relied on automatic step retries, add `retries: 3` (or your preferred count) to those `ctx.step(...)` / `.step(...)` calls. If you passed `listRuns({ limit })` above 500, lower it to ≤ 500.
