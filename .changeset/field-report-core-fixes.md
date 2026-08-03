---
"@iterativeflow/core": minor
---

First-user field-report fixes (v2 on Lambda + DynamoDB):

- **A `try/catch` around `ctx.*` is now safe.** `ctx.sleep` / `ctx.signal` / `ctx.invoke` suspend the
  run by _throwing_ a control signal; a `catch` that swallowed it used to commit the next checkpoint
  at the wrong cursor and drift the run permanently. The engine now re-propagates a swallowed suspend
  at the next `ctx.*` call (and when the body returns), so the suspend still reaches the engine and
  the run parks + resumes correctly — you no longer have to special-case control signals in your own
  error handling.
- **`StepPolicy.classify` gains the attempt number** — `(error, attempt) => "transient" | "permanent"`
  — and is now documented: fail fast on permanent (4xx/validation) errors instead of burning the
  in-invocation + run-level retry budget.
