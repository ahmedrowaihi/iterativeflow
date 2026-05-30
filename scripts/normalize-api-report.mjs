#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const PATH = join(here, "..", "etc", "iterativeflow.api.md");

// api-extractor assigns a non-deterministic numeric suffix to `import * as <ns>N`
// aliases when the same package is referenced via more than one path. Strip the
// suffix so the report stays stable across builds.
const content = readFileSync(PATH, "utf8");
const normalized = content.replace(/(drizzle_orm|drizzle_orm_pg_core)\d+/g, "$1");
writeFileSync(PATH, normalized, "utf8");
