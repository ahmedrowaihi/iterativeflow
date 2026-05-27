# Order checkout

Charge → reserve → ship → notify. Per-step retries + idempotency tokens
keep at-least-once safe.

```ts
const checkout = flow("checkout")
  .version(1)
  .input(
    z.object({
      orderId: z.string(),
      customerId: z.string(),
      cardToken: z.string(),
      amountCents: z.number(),
    }),
  )

  .step(
    "charge",
    ({ input }) =>
      payments.charge({
        idempotencyKey: `charge:${input.orderId}`,
        cardToken: input.cardToken,
        amountCents: input.amountCents,
      }),
    {
      retries: 5,
      baseBackoffMs: 1_000,
      timeoutMs: 15_000,
      classify: (err) => (err.message.includes("card_declined") ? "permanent" : "transient"),
    },
  )

  .step(
    "reserve",
    ({ input }) =>
      inventory.reserve({
        idempotencyKey: `reserve:${input.orderId}`,
        orderId: input.orderId,
      }),
    { retries: 3, timeoutMs: 10_000 },
  )

  .step(
    "ship",
    ({ input }) =>
      fulfillment.ship({
        idempotencyKey: `ship:${input.orderId}`,
        orderId: input.orderId,
      }),
    { retries: 3, timeoutMs: 30_000 },
  )

  .step(
    "notify",
    ({ input }) =>
      mail.send({
        idempotencyKey: `notify:${input.orderId}`,
        to: input.customerId,
        template: "order_shipped",
      }),
    { retries: 10, timeoutMs: 5_000 },
  )

  .output(({ input }) => ({ orderId: input.orderId, status: "fulfilled" as const }))
  .build();

const handle = engine.register(checkout);
await handle.start(orderPayload, { idempotencyKey: `order:${orderId}` });
```

Notes:

- Each step's `idempotencyKey` is derived from the deterministic `orderId` — replays don't double-charge or double-ship.
- `card_declined` is classified `permanent` so it fails terminal immediately; network errors stay `transient` and back off.
- `handle.start`'s own `idempotencyKey` makes a retried HTTP call from the user's app return the existing `runId`.
