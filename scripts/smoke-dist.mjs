#!/usr/bin/env node
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, "..");

// 1. Public runtime surface
const pkg = await import("../dist/index.js");
assert.equal(typeof pkg.createEngine, "function", "createEngine missing");
assert.equal(typeof pkg.flow, "function", "flow missing");
assert.equal(typeof pkg.defineFlow, "function", "defineFlow missing");
assert.equal(typeof pkg.consoleLogger, "function", "consoleLogger missing");
assert.equal(typeof pkg.FlowRuntimeError, "function", "FlowRuntimeError missing");
assert.equal(typeof pkg.flowError, "function", "flowError missing");
assert.equal(typeof pkg.toFlowError, "function", "toFlowError missing");
assert.equal(typeof pkg.applyFlowSchema, "function", "applyFlowSchema missing");
assert.equal(typeof pkg.dropFlowSchema, "function", "dropFlowSchema missing");

// Constants + error vocabulary stay on the main entry.
assert.ok(Array.isArray(pkg.RUN_STATUSES), "RUN_STATUSES not exported");
assert.ok(Array.isArray(pkg.EVENT_TYPES), "EVENT_TYPES not exported");
assert.ok(Array.isArray(pkg.STEP_STATUSES), "STEP_STATUSES not exported");
assert.ok(Array.isArray(pkg.FLOW_ERROR_CODES), "FLOW_ERROR_CODES not exported");
assert.ok(pkg.RUN_STATUSES.includes("awaiting_signal"));
assert.ok(pkg.RUN_STATUSES.includes("retrying"));
assert.ok(pkg.FLOW_ERROR_CODES.includes("REPLAY_INCOMPATIBLE_VERSION"));
assert.ok(pkg.FLOW_ERROR_CODES.includes("SIGNAL_TIMEOUT"));
assert.ok(pkg.FLOW_ERROR_CODES.includes("SCHEMA_MISMATCH"));

// Deprecated v1 names + dropped-in-v3 ORM exports must stay gone.
assert.equal(pkg.WorkflowRuntimeError, undefined, "deprecated WorkflowRuntimeError must be gone");
assert.equal(pkg.workflowError, undefined, "deprecated workflowError must be gone");
assert.equal(pkg.toWorkflowError, undefined, "deprecated toWorkflowError must be gone");
assert.equal(pkg.WORKFLOW_ERROR_CODES, undefined, "deprecated WORKFLOW_ERROR_CODES must be gone");
assert.equal(pkg.workflowSchema, undefined, "deprecated workflowSchema must be gone");
assert.equal(pkg.flowSchema, undefined, "v3: flowSchema must not be exported from main");
assert.equal(pkg.runs, undefined, "v3: runs table must not be exported from main");
assert.equal(pkg.steps, undefined, "v3: steps table must not be exported from main");

// Builder shape still works.
const def = pkg
  .flow("smoke")
  .version(2)
  .step("a", () => "x")
  .signal("approve")
  .build();
assert.equal(def.name, "smoke");
assert.equal(def.version, 2);
assert.equal(def.nodes.length, 2);
assert.equal(def.nodes[1].kind, "signal");
assert.ok(typeof def.body === "function");

// 2. Deprecated subpath exports must be gone
const pkgJson = JSON.parse(readFileSync(join(repo, "package.json"), "utf8"));
assert.equal(pkgJson.exports["./schema"], undefined, "v3: ./schema subpath must be removed");
assert.equal(pkgJson.exports["./relations"], undefined, "v3: ./relations subpath must be removed");
assert.ok(
  !existsSync(join(repo, "dist/storage/schema.js")),
  "v3: dist/storage/schema.js should not be emitted",
);

// 3. CLI: bin entry + functional smoke
assert.ok(pkgJson.bin?.iterativeflow, "v3: bin.iterativeflow missing");
const cliPath = join(repo, "dist/cli.js");
assert.ok(existsSync(cliPath), `CLI build missing: ${cliPath}`);

const tmp = mkdtempSync(join(tmpdir(), "iterativeflow-smoke-"));
try {
  const result = spawnSync("node", [cliPath, "generate-schema", "--out", "schema.ts"], {
    cwd: tmp,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, `CLI exited non-zero:\n${result.stderr}`);
  const generated = readFileSync(join(tmp, "schema.ts"), "utf8");
  assert.match(generated, /export const flowTables/, "generated file missing flowTables");
  assert.match(
    generated,
    /export const runs = flowSchema\.table/,
    "generated file missing runs table",
  );
  assert.match(
    generated,
    /Initialized by `npx iterativeflow generate-schema`/,
    "generated file missing header",
  );
} finally {
  rmSync(tmp, { recursive: true, force: true });
}

// 4. Dashboard: subpath export + shipped HTML asset + handler smoke
assert.ok(pkgJson.exports["./dashboard"], "./dashboard subpath missing");
assert.ok(existsSync(join(repo, "dist/dashboard.js")), "dist/dashboard.js missing");
assert.ok(existsSync(join(repo, "dist/dashboard.html")), "dist/dashboard.html asset missing");

const dash = await import("../dist/dashboard.js");
assert.equal(typeof dash.createFlowsDashboard, "function", "createFlowsDashboard missing");

const stubEngine = {
  health: async () => ({ ok: true, db: true, worker: false, listen: false }),
};
const dashboard = dash.createFlowsDashboard({ engine: stubEngine });
const htmlRes = await dashboard.fetch(new Request("http://smoke.test/admin/flows"));
assert.equal(htmlRes.status, 200, "dashboard HTML route not 200");
const htmlBody = await htmlRes.text();
assert.match(htmlBody, /<base href="\/admin\/flows\/">/, "mount-path <base> not injected");
const healthRes = await dashboard.fetch(new Request("http://smoke.test/admin/flows/api/health"));
assert.equal(healthRes.status, 200, "dashboard health route not 200");
const health = await healthRes.json();
assert.equal(health.db, true, "health payload not passed through");

console.log("dist smoke ok");
