# Signals

Signals are how external systems push named messages into a running flow. The flow body awaits a named signal; an external caller (a webhook, another service, a CLI script) delivers the payload via `engine.signal(runId, name, payload)`.

## The two sides

```ts
// Inside the flow body — the receiver
const decision = await ctx.signal<{ approved: boolean }>("approve", {
  schema: z.object({ approved: z.boolean() }),
  timeout: "1h",
});

// Anywhere with a runId — the sender
const result = await engine.signal(runId, "approve", { approved: true });
```

## Why signals have a name

A single flow body can await several different signals at different points:

```ts
const review = await ctx.signal("review");
// ... do work ...
const approval = await ctx.signal("approve");
// ... do more work ...
const veto = await ctx.signal("veto");
```

Without the `name`, the sender couldn't tell the engine which awaitable to fulfill. The name is the "named slot" the workflow has reserved.

If your flow only ever awaits one signal, the name is still required — pick something descriptive (`approve`, `confirm`, `payload-received`).

## Single-consumer, not pub/sub

A signal is delivered to at most one `ctx.signal` await. There is no broadcast. If 1000 runs are awaiting `"approve"`, you call `engine.signal(runId, ...)` 1000 times with each run's ID.

If you need broadcast: query `engine.listRuns({ name, status: "awaiting_signal" })` and iterate. Acceptable for tens to low hundreds of recipients; for higher fan-out use a pub/sub layer in front (Redis pub/sub, Kafka, NATS) and translate to per-run signals.

## Delivery outcomes

`engine.signal` returns a `SignalDeliveryResult`:

```ts
const result = await engine.signal(runId, "approve", payload);
switch (result.kind) {
  case "delivered":
    // The workflow was awaiting this signal. It will resume.
    break;
  case "buffered":
    // The workflow has NOT armed this signal yet. The payload is stored;
    // when the workflow reaches `ctx.signal("approve")`, it returns
    // immediately with this payload.
    break;
  case "duplicate":
    // This signal was already delivered (or buffered). The first one wins.
    // Webhook handlers should treat this as success.
    break;
  case "expired":
    // The workflow armed this signal with a `timeout` that has elapsed.
    // The workflow already failed with SIGNAL_TIMEOUT. Reject the webhook.
    break;
}
```

## Cursor-keyed by ordinal, not by external ID

Two `ctx.signal("approve")` calls in the same flow body are distinguished by ordinal:

```ts
const first = await ctx.signal("approve"); // matches the 1st delivered "approve"
const second = await ctx.signal("approve"); // matches the 2nd
```

The cursor produces `signal:approve` and `signal:approve:1` as distinct keys. Senders don't need to know — they just call `engine.signal(runId, "approve", ...)` twice and the deliveries land in order.

This is NOT keyed by an external request ID. If you need to match a specific external entity (e.g. an HTTP request) to a specific signal, encode the entity in the payload and branch on it inside the workflow.

## Buffered signals are durable

If `engine.signal(runId, "approve", ...)` arrives BEFORE the workflow reaches `ctx.signal("approve")`, the payload is stored in the `workflow.signals` table with `delivered: true`. The buffer persists until:

- the workflow reaches `ctx.signal("approve")` and consumes it
- OR the run terminates (cascade-deletes via FK)

There is no TTL on a buffered signal. If you need expiry, use the `timeout` on `ctx.signal` (see below).

## Timeouts

```ts
try {
  const decision = await ctx.signal("approve", { timeout: "1h" });
  // got it within 1h
} catch (err) {
  if (err.code === "SIGNAL_TIMEOUT") {
    // expired — send a fallback notification, schedule a follow-up, etc.
    await ctx.step("notify-fallback", ({ signal }) => notify({ signal }));
    return { status: "abandoned" };
  }
  throw err;
}
```

The timeout is enforced when the workflow next resumes after `expiresAt`. The reconciler re-enqueues runs whose signals have expired; on resume, `ctx.signal` checks `expiresAt` vs now and throws `SIGNAL_TIMEOUT` if past.

A late `engine.signal` after expiry returns `{ kind: "expired" }`. The webhook caller should treat this as "rejected."

## Schema validation behavior

```ts
await ctx.signal("approve", { schema: z.object({ approved: z.boolean() }) });
```

**Today:** validation runs on workflow resume after the signal is consumed. Invalid payload → `FlowRuntimeError(SIGNAL_PAYLOAD_INVALID, nonRetryable: true)` → run marked failed. The webhook caller already got `delivered`; they cannot retry with a corrected payload.

**Known limitation:** see `docs/dx-hardening-plan.md` (PR-2) for delivery-time validation, which would reject the bad payload at `engine.signal` time and keep the workflow armed for a retry. Until then, validate the payload in your webhook handler before calling `engine.signal`.

## Constraints summary

- Single-consumer; no broadcast, no withdrawal
- Cursor-keyed by ordinal within the flow body
- Buffered signals persist until consumed or run terminates (no TTL)
- Timeout fires on workflow resume after `expiresAt`
- Payload size cap (`limits.maxSignalPayloadBytes`) is operator-configured; no default
- No signal authentication at the engine layer — whoever has the `runId` can deliver. Authn is the caller's responsibility (webhook signing, mutual TLS, etc.)
- Bad-payload signals currently fail the run; will move to delivery-time rejection in a future minor (see plan doc)

## Patterns

### Human-in-the-loop approval

```ts
const review = flow("review")
  .step("draft", ({ input }) => generateDraft(input))
  .signal("decision", {
    schema: z.object({ approved: z.boolean(), notes: z.string().optional() }),
    timeout: "7d",
  })
  .output(({ input }) => input)
  .build();
```

The webhook handler validates the payload, then `await engine.signal(runId, "decision", body)`.

### Multi-signer

```ts
const sign = flow("multi-sign")
  .signal("alice")
  .signal("bob")
  .signal("carol")
  .output(() => ({ signed: true }))
  .build();
```

All three signals must be delivered before the workflow completes. Order doesn't matter — `ctx.signal("alice")` waits for alice's signal specifically.

### Fan-out then collect

For more complex fan-out (start N child flows, await all), see `ctx.invoke` and the [child-flows guide](./examples/checkout.md). Signals are for external messages, not internal coordination.
