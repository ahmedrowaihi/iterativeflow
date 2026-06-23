#!/usr/bin/env node
import { readFileSync, readdirSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { join, basename } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, "..");
const outDir = join(repo, "tests", "docs-examples");

// Clear stale snippets without nuking the tsconfig / globals.d.ts beside them.
mkdirSync(outDir, { recursive: true });
for (const f of readdirSync(outDir)) {
  if (/^(?:README|guide|replay-semantics|signals|serverless)-\d+\.ts$/.test(f)) {
    rmSync(join(outDir, f), { force: true });
  }
}

const sources = [
  "README.md",
  ...readdirSync(join(repo, "docs"))
    .filter((f) => f.endsWith(".md"))
    .map((f) => join("docs", f)),
];

const FENCE = /^```ts(?:\s.*)?$/;
const SKIP_MARKER = /<!--\s*doc-check:\s*skip\b[\s\S]*?-->/;

const extract = (path) => {
  const lines = readFileSync(join(repo, path), "utf8").split("\n");
  const blocks = [];
  let inBlock = false;
  let buf = [];
  let startLine = 0;
  let prevMarker = false;
  lines.forEach((line, idx) => {
    if (inBlock) {
      if (line.trim() === "```") {
        if (!prevMarker) blocks.push({ code: buf.join("\n"), startLine });
        buf = [];
        inBlock = false;
        prevMarker = false;
      } else {
        buf.push(line);
      }
    } else if (FENCE.test(line.trim())) {
      inBlock = true;
      startLine = idx + 2;
    } else if (SKIP_MARKER.test(line)) {
      prevMarker = true;
    } else if (line.trim().length > 0) {
      prevMarker = false;
    }
  });
  return blocks;
};

let total = 0;
for (const src of sources) {
  const blocks = extract(src);
  blocks.forEach((b, i) => {
    const slug = basename(src, ".md").replace(/[^a-z0-9-]/gi, "_");
    const file = `${slug}-${String(i + 1).padStart(2, "0")}.ts`;
    const header =
      `// Extracted from ${src}:${b.startLine} by scripts/extract-doc-examples.mjs.\n` +
      `// Do not edit by hand — edit the .md source and re-run.\n\n`;
    writeFileSync(join(outDir, file), header + b.code + "\n", "utf8");
    total += 1;
  });
}

console.log(`extracted ${total} TypeScript blocks to ${outDir}`);
