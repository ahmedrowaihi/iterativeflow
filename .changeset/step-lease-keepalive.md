---
"@iterativeflow/core": patch
---

Fix: a step that declares `timeoutMs` now holds its run's lease while it runs.

A lease was renewed only when a step **committed**, so a single step longer than `leaseMs` could not
renew: the lease expired mid-flight, a second worker claimed the same run, and the same step body
executed concurrently. The memo stays exactly-once, so the _result_ was never wrong — the side effect
ran twice. Reproduced on the memory backend with no crash involved, and reported from production as a
run that sat `running` for ~4.5h across 25 attempts, re-running an expensive scan each time.

Declaring `StepPolicy.timeoutMs` now also keeps the lease alive for as long as the step runs. The
ceiling is the step's own declared timeout, so the engine invents no new number and no new knob: a
step that overruns is still aborted, and its lease still lapses, so a wedged worker is reclaimable
exactly as before. `leaseMs` no longer has to be hand-sized above the longest step times the batch
size.

A step with **no** `timeoutMs` is deliberately unchanged: there is no honest bound to renew to, and
renewing without one would convert a hung step into a permanent stall — worse than today, since
reclaim by another live worker is the only thing that currently recovers that case.

Also documents, on `serverlessTick`, that a renewing step widens the worst-case strand of an
un-executed batch tail to about twice `leaseMs` past a killed invocation.
