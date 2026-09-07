---
"@iterativeflow/core": patch
---

Close two gaps on the remote-fed surfaces: signal payloads bypassed `maxPayloadBytes`, and the
dashboard passed unvalidated input to the store.

**`maxPayloadBytes` now covers signals.** It was applied at `submit` and `submitMany` only, so the
one knob documented as "a runaway-payload guard" missed the other way a payload enters durable
storage — and the signal path is the one fed by `@iterativeflow/webhooks` and the dashboard. A signal
payload lands in the run's inbox and is re-read by every subsequent `loadRun`, so an oversized one is
amplified on each replay; on DynamoDB it can wedge the run past the 400 KB item limit at a point
where the caller can no longer intervene. Note this is a behaviour change for anyone already sending
signal payloads larger than a configured cap: they will now be rejected, which is the point.

**The dashboard validates before dispatching.** `?limit` went through `Math.min(Number(raw), 200)`,
so `-1` survived and reaches SQLite as _no limit_ — paging the entire run table, inputs and outputs
included, into one response — while `abc` yielded `NaN` and a driver error instead of a clamped
value. It is now clamped to a positive integer. The signal route cast its JSON body without checking
it, so a body with no `name` reached `postSignal` and surfaced as a NOT-NULL violation rather than a
`400`.

Also carries the "mutating routes are unauthenticated, mount behind your own auth and add CSRF" note
from the dashboard README into the `createDashboard` JSDoc, so it reaches anyone wiring it from
editor autocomplete rather than only those who read the README.
