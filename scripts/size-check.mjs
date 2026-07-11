#!/usr/bin/env node
import { gzipSync } from "node:zlib";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

const BUDGET_KB = Number(process.env.SIZE_BUDGET_KB ?? 320);
const DIST = new URL("../dist/", import.meta.url).pathname;

const walk = (dir) => {
  const out = [];
  for (const entry of readdirSync(dir)) {
    // The dashboard/ dir is a bundled browser app (Preact + Tailwind), not part
    // of the library's Node surface — budget it separately from the API bundle.
    if (entry === "dashboard") continue;
    const p = join(dir, entry);
    const s = statSync(p);
    if (s.isDirectory()) out.push(...walk(p));
    else if (entry.endsWith(".js") && !entry.endsWith(".d.ts")) out.push(p);
  }
  return out;
};

const files = walk(DIST);
let total = 0;
const rows = [];
for (const f of files) {
  const buf = readFileSync(f);
  const gz = gzipSync(buf).byteLength;
  total += gz;
  rows.push({ file: f.replace(DIST, ""), gz });
}

rows.sort((a, b) => b.gz - a.gz);
for (const r of rows)
  console.log(`  ${(r.gz / 1024).toFixed(2).padStart(8)} kB  ${r.file}`);
const totalKb = total / 1024;
console.log(
  `\ntotal gzipped JS: ${totalKb.toFixed(2)} kB (budget ${BUDGET_KB} kB)`,
);

if (totalKb > BUDGET_KB) {
  console.error(
    `\nbundle exceeds budget: ${totalKb.toFixed(2)} kB > ${BUDGET_KB} kB`,
  );
  process.exit(1);
}
