#!/usr/bin/env node
import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

// Assets shipped next to their consuming module in dist/, found at runtime
// via locateShippedAsset (which falls back to the src path in dev/tests).
// The dashboard UI is emitted by `vite build` into dist/dashboard/ — only the
// schema text asset is hand-copied here.
const assets = [
  [join("src", "storage", "schema.ts"), join("dist", "iterativeflow-schema.ts.txt")],
];

for (const [srcRel, dstRel] of assets) {
  const src = join(here, "..", srcRel);
  const dst = join(here, "..", dstRel);
  mkdirSync(dirname(dst), { recursive: true });
  copyFileSync(src, dst);
  console.log(`copied ${src} -> ${dst}`);
}
