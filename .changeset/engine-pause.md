---
"@iterativeflow/core": minor
---

`engine.pause()` / `engine.resume()` / `engine.isPaused()` — drain a running worker without stopping it.

The only lever before this was the stop function from `engine.run()`, which aborts the tick loop
_and_ clears the maintenance interval, so reconcile and crons stop with it. There was no way to say
"stop taking new work, finish what you have" — the documented workaround was to deploy a worker
registered for zero flows.

`pause()` stops the run loop claiming; in-flight runs finish, reconcile and crons keep running, and a
paused loop parked in its idle backoff **wakes immediately** rather than after up to
`maxIdleTickMs`. That is what makes it react to a start/stop command on a live pod instead of on a
poll interval: pausing flips an abort gate the loop is waiting on, so it is a push, not a poll. It
takes effect on the next claim, so a batch already claimed still drains — leases are never abandoned.

It gates the `run()` loop only. A caller driving `tick()` or `serverlessTick()` themselves pauses by
not calling them, which needs no API, so `tick()` is deliberately not gated — an explicit call means
you meant it.

`Queue.waitForWork` takes an optional `AbortSignal` so a push-listener wait is interruptible too;
previously a loop with `createPgListener` wired could sit out its full backoff before noticing either
a pause or a stop.

This is the worker-level pause only. Pausing a single flow, a single run, or a cron is durable
fleet-wide state and a separate piece of work.
