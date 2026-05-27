# Multi-signer document signing

Send document to signer A → wait for signature → send to signer B → wait →
finalize. Each signature is a hook with a payload schema and a deadline.

```ts
const SignatureSchema = z.object({
  signerEmail: z.string().email(),
  signedAt: z.string().datetime(),
  signatureId: z.string(),
});

const signing = flow("doc-signing")
  .version(1)
  .input(
    z.object({
      docId: z.string(),
      signerA: z.string().email(),
      signerB: z.string().email(),
    }),
  )

  .step("send-to-a", ({ input }) =>
    docsign
      .sendForSignature({
        idempotencyKey: `send-a:${input.docId}`,
        docId: input.docId,
        recipient: input.signerA,
      })
      .then(() => ({ ...input, sentToA: true })),
  )

  .hook(
    "signed-by-a",
    { schema: SignatureSchema, timeout: "7d" },
    (input: { docId: string; signerA: string; signerB: string; sentToA: boolean }, payload) => ({
      ...input,
      signatureA: payload,
    }),
  )

  .step("send-to-b", ({ input }) =>
    docsign
      .sendForSignature({
        idempotencyKey: `send-b:${input.docId}`,
        docId: input.docId,
        recipient: input.signerB,
      })
      .then(() => ({ ...input, sentToB: true })),
  )

  .hook("signed-by-b", { schema: SignatureSchema, timeout: "7d" }, (input, payload) => ({
    ...input,
    signatureB: payload,
  }))

  .step("finalize", ({ input }) =>
    docsign.finalize({
      idempotencyKey: `finalize:${input.docId}`,
      docId: input.docId,
      signatures: [input.signatureA, input.signatureB],
    }),
  )

  .output(({ input }) => ({
    docId: input.docId,
    completedAt: input.signatureB.signedAt,
  }))
  .build();

engine.register(signing);

// Webhook from your e-sign provider:
//   POST /webhooks/signed → engine.signal(runId, "signed-by-a", payload)
//   POST /webhooks/signed → engine.signal(runId, "signed-by-b", payload)
```

Notes:

- Distinct hook names per signer so each webhook signals exactly one.
- `merge` carries the prior channel forward so `finalize` sees both signatures and the input.
- `timeout: "7d"` per hook → `WORKFLOW_HOOK_TIMEOUT` if a signer ghosts. Schema validates on every replay; payload-shape drift surfaces as `HOOK_PAYLOAD_INVALID`.
- Webhook arriving before `ctx.hook(...)` is reached → buffered via `preDeliverHook`; workflow consumes it the moment it gets there. No race.
- Three-signer flow → bump `.version(2)` with one more `.hook` node; v1 runs drain on the 2-signer graph.
