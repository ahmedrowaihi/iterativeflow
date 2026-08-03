# @iterativeflow/webhooks

Turn a signed provider webhook into a **durable signal** a parked flow can `await`. This is the
inbound edge that lets a durable run pause on an external event — a `/qa run` comment, a preview
deploy, a payment, a human approval — and resume crash-safe when it arrives.

Provider-agnostic: a pluggable **verifier** (with a `github` preset) plus a **correlate** callback
that maps the event to the runs it should wake. Zero runtime dependencies beyond
`@iterativeflow/core` — verification is Web Crypto HMAC (no `node:crypto`, no `Buffer`), so it runs
on Node, Workers, and the edge.

```ts
import { webhookSignalBridge, github } from "@iterativeflow/webhooks";

const bridge = webhookSignalBridge(backend, {
  verify: github(process.env.GITHUB_WEBHOOK_SECRET!),
  // Map a verified event to the parked run(s) it concerns. `correlate` owns the app's
  // "PR 42 → runId" lookup — the bridge can't know it.
  correlate: async (event) => {
    if (event.type !== "issue_comment") return [];
    const pr = (event.payload as { issue: { number: number } }).issue.number;
    const runId = await findRunForPr(pr); // your state
    return runId ? [{ runId, name: "qa:approved", payload: event.payload }] : [];
  },
});

// In your HTTP handler — pass the RAW body (the bytes GitHub signed):
app.post("/webhooks/github", async (req) => {
  const result = await bridge({ body: await req.text(), headers: req.headers });
  return Response.json(result); // { type, id, deliveries: [{ runId, name, delivered }] }
});
```

The flow side is plain core — park on the signal, resume with its payload:

```ts
const qa = defineFlow({
  name: "qa",
  version: 1,
  run: async (ctx, input) => {
    const approval = await ctx.signal("qa:approved"); // parks until the webhook arrives
    return runQa(input, approval);
  },
});
```

## Guarantees

- **Verify before trust.** `verify` HMAC-checks the raw body (timing-safe) and rejects a forged or
  tampered request with a `WebhookError` before the store is touched. Any re-serialization of the
  body breaks the signature — pass the exact received bytes.
- **Idempotent delivery.** Each signal is keyed by the provider's delivery id
  (`${event.id}:${runId}:${name}` by default), so a redelivery lands once. Delivery + run re-enqueue
  are atomic (durable via `signalRun`); the wakeup is a best-effort latency nudge.
- **Fan-out.** One webhook can wake many runs — `correlate` returns an array, e.g. a PR
  `synchronize` superseding every in-flight run for that PR.

## Providers

`github(secret)` ships built in. Any provider that signs the raw body with a hex HMAC in one header
works via `hmacVerifier({ secret, signatureHeader, idHeader, typeHeader, scheme?, hash? })`. A
provider with a different scheme (e.g. a timestamped or base64 signature) is a full `WebhookVerifier`
function — add it as its own file (e.g. `src/stripe.ts`) next to `github.ts` and re-export from
`index.ts`, so each preset stays independently importable and tree-shakes.
