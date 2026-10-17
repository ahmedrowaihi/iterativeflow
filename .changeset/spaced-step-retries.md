---
"@iterativeflow/core": minor
---

**A `StepPolicy` with both `timeoutMs` and `retries` must now set `retryDelayMs`.** A timeout stops
waiting for `fn` but does not stop it, so the retry used to start immediately while the timed-out
attempt could still be running. Leaving `retryDelayMs` out is now a compile error. Pass `0` to keep
the old behaviour, or pass the step's `signal` to your client so the abandoned attempt stops. Runtime
behaviour is unchanged.
