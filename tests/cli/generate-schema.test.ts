import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parseGenArgs as parseArgs, runGenerateSchema } from "../../src/cli";

const here = dirname(fileURLToPath(import.meta.url));
const SCHEMA_SOURCE = readFileSync(resolve(here, "../../src/storage/schema.ts"), "utf8");

class Buf {
  out = "";
  write(s: string) {
    this.out += s;
  }
}

describe("npx iterativeflow generate-schema", () => {
  it("defaults to ./iterativeflow-schema.ts in the cwd", () => {
    const cwd = mkdtempSync(join(tmpdir(), "iterativeflow-cli-"));
    const stdout = new Buf();
    const stderr = new Buf();
    try {
      const code = runGenerateSchema([], { cwd, stdout, stderr });
      expect(code).toBe(0);
      const written = readFileSync(join(cwd, "iterativeflow-schema.ts"), "utf8");
      expect(written.endsWith(SCHEMA_SOURCE)).toBe(true);
      expect(written).toMatch(/Initialized by `npx iterativeflow generate-schema`/);
      expect(written).toMatch(/export const flowTables/);
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("--out routes the file to a custom path (creating dirs as needed)", () => {
    const cwd = mkdtempSync(join(tmpdir(), "iterativeflow-cli-"));
    const stdout = new Buf();
    const stderr = new Buf();
    try {
      const code = runGenerateSchema(["--out", "src/db/iterativeflow-schema.ts"], {
        cwd,
        stdout,
        stderr,
      });
      expect(code).toBe(0);
      const written = readFileSync(join(cwd, "src/db/iterativeflow-schema.ts"), "utf8");
      expect(written.endsWith(SCHEMA_SOURCE)).toBe(true);
      expect(written).toMatch(/Initialized by `npx iterativeflow generate-schema`/);
      expect(written).toMatch(/export const flowTables/);
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("refuses to overwrite an existing file without --force", () => {
    const cwd = mkdtempSync(join(tmpdir(), "iterativeflow-cli-"));
    const stdout = new Buf();
    const stderr = new Buf();
    try {
      writeFileSync(join(cwd, "iterativeflow-schema.ts"), "existing content", "utf8");
      const code = runGenerateSchema([], { cwd, stdout, stderr });
      expect(code).toBe(1);
      expect(stderr.out).toMatch(/already exists/);
      expect(readFileSync(join(cwd, "iterativeflow-schema.ts"), "utf8")).toBe("existing content");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("--force overwrites", () => {
    const cwd = mkdtempSync(join(tmpdir(), "iterativeflow-cli-"));
    const stdout = new Buf();
    const stderr = new Buf();
    try {
      writeFileSync(join(cwd, "iterativeflow-schema.ts"), "existing content", "utf8");
      const code = runGenerateSchema(["--force"], { cwd, stdout, stderr });
      expect(code).toBe(0);
      expect(
        readFileSync(join(cwd, "iterativeflow-schema.ts"), "utf8").endsWith(SCHEMA_SOURCE),
      ).toBe(true);
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("rejects unknown args with exit 2", () => {
    const cwd = mkdtempSync(join(tmpdir(), "iterativeflow-cli-"));
    const stdout = new Buf();
    const stderr = new Buf();
    try {
      expect(runGenerateSchema(["--bogus"], { cwd, stdout, stderr })).toBe(2);
      expect(stderr.out).toMatch(/unknown argument/);
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("--help exits 0 and prints help", () => {
    const stdout = new Buf();
    const stderr = new Buf();
    expect(runGenerateSchema(["--help"], { stdout, stderr })).toBe(0);
    expect(stdout.out).toMatch(/generate-schema/);
    expect(stdout.out).toMatch(/--out <path>/);
  });
});

describe("parseArgs", () => {
  it("accepts --out=<path> shorthand", () => {
    expect(parseArgs(["--out=./foo.ts"])).toEqual({
      out: "./foo.ts",
      force: false,
      help: false,
    });
  });
  it("rejects --out without a value", () => {
    expect(() => parseArgs(["--out"])).toThrow(/requires a path/);
  });
});
