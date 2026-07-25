---
"@iterativeflow/webhooks": minor
---

New: **`@iterativeflow/webhooks`** — the inbound webhook edge. `webhookSignalBridge(backend, {
verify, correlate })` verifies a signed provider webhook (Web Crypto HMAC, timing-safe, no
`node:crypto`/`Buffer` — runs on Node, Workers, and the edge) and delivers it as a durable signal a
parked flow can `await ctx.signal(...)`, so a run can pause on an external event (a `/qa run`
comment, a preview deploy, a payment, a human approval) and resume crash-safe. Provider-agnostic: a
pluggable `WebhookVerifier` with a `github` preset and an `hmacVerifier` building block for any
hex-HMAC provider; `correlate` maps an event to the run(s) it wakes (fan-out supported). Delivery is
idempotent on the provider's delivery id (atomic delivery + re-enqueue via `signalRun`). No runtime
dependency beyond core.
