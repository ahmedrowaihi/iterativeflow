# Examples

Pseudo-code flows for common scenarios. The `flow().step().sleep().hook()`
API is real; the called services (`payments.charge`, `mail.send`, …) are
sketches — replace them with your own.

| File                                       | Scenario                                                        | What it shows                                                               |
| ------------------------------------------ | --------------------------------------------------------------- | --------------------------------------------------------------------------- |
| [checkout.md](checkout.md)                 | Order checkout with payment + fulfillment                       | Per-step retries, classification, idempotency tokens                        |
| [onboarding.md](onboarding.md)             | New-user drip + survey                                          | Long `sleep`, hook with timeout, hook merge to carry state                  |
| [ai-conductor.md](ai-conductor.md)         | Multi-agent dialogue with dynamic human-in-loop + infinite chat | `.loop` combinator, pre-signal trick, `defineWorkflow` for raw control flow |
| [signing.md](signing.md)                   | Multi-signer document signing                                   | Multiple hooks in sequence, hook payload schemas                            |
| [saga-trip.md](saga-trip.md)               | Saga: book trip, compensate on failure                          | Compensation steps, permanent vs transient classification                   |
| [account-deletion.md](account-deletion.md) | Soft-delete + grace + hard-delete                               | Multi-week sleep, cancel during grace, idempotency key on start             |

All examples assume:

```ts
import { createEngine, flow } from "iterativeflow";
import { z } from "zod";

const engine = createEngine({ db, pool, logger });
```
