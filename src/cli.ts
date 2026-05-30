#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { locateShippedAsset } from "./util/asset-locate";

const TOP_HELP = `iterativeflow — durable workflows on your own Postgres.

Usage:
  npx iterativeflow <command> [...args]

Commands:
  generate-schema    Emit a drizzle schema file in your project.

Run \`npx iterativeflow <command> --help\` for command-specific help.`;

const GEN_HELP = `iterativeflow generate-schema — emit a drizzle schema file in your project

Usage:
  npx iterativeflow generate-schema [--out <path>] [--force]

Options:
  --out <path>   Output path. Default: ./iterativeflow-schema.ts
  --force        Overwrite an existing file at <path>.
  -h, --help     Show this help.

The generated file is typed against YOUR drizzle-orm version. Add its path to
your \`drizzle.config.ts\` \`schema: [...]\` so drizzle-kit picks up the workflow
tables when generating migrations.`;

const HEADER = `// Initialized by \`npx iterativeflow generate-schema\`. You own this file —
// add columns, indexes, and relations freely. Engine-required columns must
// keep their names; renames are caught at boot via SCHEMA_MISMATCH.
// Re-run with --force to refresh after upgrading iterativeflow.

`;

interface GenArgs {
  out: string;
  force: boolean;
  help: boolean;
}

/** @internal */
export const parseGenArgs = (argv: ReadonlyArray<string>): GenArgs => {
  const out: GenArgs = { out: "./iterativeflow-schema.ts", force: false, help: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--force") out.force = true;
    else if (a === "--out") {
      const next = argv[i + 1];
      if (!next) throw new Error(`--out requires a path argument`);
      out.out = next;
      i++;
    } else if (a.startsWith("--out=")) {
      out.out = a.slice("--out=".length);
    } else {
      throw new Error(`unknown argument: ${a}`);
    }
  }
  return out;
};

interface RunOpts {
  cwd?: string;
  stdout?: { write(s: string): void };
  stderr?: { write(s: string): void };
  here?: string;
}

/** @internal */
export const runGenerateSchema = (
  argv: ReadonlyArray<string>,
  {
    cwd = process.cwd(),
    stdout = process.stdout,
    stderr = process.stderr,
    here = dirname(fileURLToPath(import.meta.url)),
  }: RunOpts = {},
): number => {
  let parsed: GenArgs;
  try {
    parsed = parseGenArgs(argv);
  } catch (err) {
    stderr.write(`error: ${(err as Error).message}\n\n${GEN_HELP}\n`);
    return 2;
  }
  if (parsed.help) {
    stdout.write(`${GEN_HELP}\n`);
    return 0;
  }
  const target = isAbsolute(parsed.out) ? parsed.out : resolve(cwd, parsed.out);
  if (existsSync(target) && !parsed.force) {
    stderr.write(
      `error: ${target} already exists. Re-run with --force to overwrite, or pass --out to choose a different path.\n`,
    );
    return 1;
  }
  const body = readFileSync(
    locateShippedAsset(here, "iterativeflow-schema.ts.txt", "./storage/schema.ts"),
    "utf8",
  );
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, HEADER + body, "utf8");
  stdout.write(`wrote ${target}\n`);
  stdout.write(
    `\nNext:\n` +
      `  1. Add the file to your drizzle.config.ts: schema: [..., "${parsed.out}"]\n` +
      `  2. Run drizzle-kit generate && drizzle-kit migrate\n` +
      `  3. Wire the engine: import { createEngine } from "iterativeflow"\n`,
  );
  return 0;
};

const main = (): number => {
  const [sub, ...rest] = process.argv.slice(2);
  if (!sub) {
    process.stderr.write(`${TOP_HELP}\n`);
    return 1;
  }
  if (sub === "--help" || sub === "-h") {
    process.stdout.write(`${TOP_HELP}\n`);
    return 0;
  }
  switch (sub) {
    case "generate-schema":
      return runGenerateSchema(rest);
    default:
      process.stderr.write(`error: unknown command "${sub}"\n\n${TOP_HELP}\n`);
      return 2;
  }
};

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exit(main());
}
