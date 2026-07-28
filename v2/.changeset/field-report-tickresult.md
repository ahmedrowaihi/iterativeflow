---
"@iterativeflow/core": minor
---

Structured `TickResult` (first-user field report #5).

`serverlessTick` / `tickOnce` / `engine.tick()` reported a bare status string per run, so a driver
seeing `["flow_drift", "failed"]` had to query the store to learn WHICH run and WHY. `TickResult` is
now `{ runId, status, error?, cursorKey? }` — a failed or drifted tick carries the error and the
cursor key it drifted at, so a serverless `SweepResult` consumer can log/route a failure without
touching the store. The status-string union is now exported as `TickStatus`.

Note: this is a breaking shape change for code that compared a tick result as a string
(`result === "done"`) — read `result.status` instead.
