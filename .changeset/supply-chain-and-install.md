---
"@iterativeflow/core": patch
---

Turn on the supply-chain cooldown that was configured but never active, and stop dev-installing a
peer nothing imports.

`pnpm-workspace.yaml` carried a `minimumReleaseAgeExclude` list with no `minimumReleaseAge` set, so
the gate it assumed existed was off — a freshly published version of any dev dependency was
installable the moment it appeared. Now set to a one-day cooldown, which is what the exclude list was
written for.

`@op-engineering/op-sqlite` is an optional peer of `@iterativeflow/sqlite`, and `autoInstallPeers`
installed it anyway — pulling the whole React Native/Metro toolchain into every clone and CI run for
a package nothing in this repo imports (the adapter is typed structurally and its tests emulate the
driver). It is now in `ignoredOptionalDependencies`. Consumers who actually use op-sqlite are
unaffected: the peer declaration is unchanged.

Note the tree is only pruned on the next full lockfile resolution — the setting is recorded now, but
the already-resolved entries stay until then.
