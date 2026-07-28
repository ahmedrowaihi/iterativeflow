---
"@iterativeflow/core": patch
---

Recovery & operations guide (first-user field report #3).

The field report asked for a supported heal/repair path for a stuck run, or at least documented
recovery. Since a `try/catch` around `ctx.*` is now safe (field report #1), the main way a run drifted
permanently is gone — so rather than a risky memo-clearing "heal" primitive, the recovery is composing
the existing levers. `docs/v2/RECOVERY.md` is now the lever-by-scenario guide: `retry` for a transient
failure, `park` + redeploy or a version bump for drift (keep old versions registered until in-flight
runs drain), and `cancel` + a fresh submit (new idempotency key — re-using the key returns the existing
run, not a fresh one) for an un-resumable run. Linked from the core README.
