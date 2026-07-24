#!/usr/bin/env node
// Write a consumer-owned drizzle schema for the iterativeflow tables.
// Usage: iterativeflow-pg-drizzle [outFile] [--schema <name>]
//   outFile   defaults to ./iterativeflow.schema.ts
//   --schema  Postgres schema name (default "workflow"); match createPgBackend/ddl.
import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { drizzleSchema } from "../dist/index.mjs";

const args = process.argv.slice(2);
let out = "iterativeflow.schema.ts";
let schema = "workflow";
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--schema") schema = args[++i];
  else out = args[i];
}

const path = resolve(process.cwd(), out);
writeFileSync(path, drizzleSchema(schema));
process.stdout.write(`wrote drizzle schema for "${schema}" → ${path}\n`);
