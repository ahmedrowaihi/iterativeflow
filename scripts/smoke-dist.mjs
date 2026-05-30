#!/usr/bin/env node
import assert from "node:assert/strict";

const pkg = await import("../dist/index.js");
assert.equal(typeof pkg.createEngine, "function", "createEngine missing");
assert.equal(typeof pkg.flow, "function", "flow missing");
assert.equal(typeof pkg.defineFlow, "function", "defineFlow missing");
assert.equal(typeof pkg.consoleLogger, "function", "consoleLogger missing");
assert.equal(typeof pkg.FlowRuntimeError, "function", "FlowRuntimeError missing");
assert.equal(typeof pkg.flowError, "function", "flowError missing");
assert.equal(pkg.WorkflowRuntimeError, undefined, "deprecated WorkflowRuntimeError must be gone");
assert.equal(pkg.workflowError, undefined, "deprecated workflowError must be gone");
assert.equal(pkg.toWorkflowError, undefined, "deprecated toWorkflowError must be gone");
assert.equal(pkg.WORKFLOW_ERROR_CODES, undefined, "deprecated WORKFLOW_ERROR_CODES must be gone");
assert.equal(pkg.workflowSchema, undefined, "deprecated workflowSchema must be gone");
assert.equal(typeof pkg.toFlowError, "function", "toFlowError missing");
assert.ok(Array.isArray(pkg.RUN_STATUSES), "RUN_STATUSES not exported");
assert.ok(Array.isArray(pkg.EVENT_TYPES), "EVENT_TYPES not exported");
assert.ok(Array.isArray(pkg.STEP_STATUSES), "STEP_STATUSES not exported");
assert.ok(Array.isArray(pkg.FLOW_ERROR_CODES), "FLOW_ERROR_CODES not exported");
assert.ok(pkg.RUN_STATUSES.includes("awaiting_signal"));
assert.ok(pkg.RUN_STATUSES.includes("retrying"));
assert.ok(pkg.FLOW_ERROR_CODES.includes("REPLAY_INCOMPATIBLE_VERSION"));
assert.ok(pkg.FLOW_ERROR_CODES.includes("SIGNAL_TIMEOUT"));

const schema = await import("../dist/storage/schema.js");
assert.ok(schema.runs, "runs missing");
assert.ok(schema.signals, "signals table missing");
assert.equal(schema.hooks, undefined, "deprecated hooks alias must be gone");
assert.equal(schema.workflowSchema, undefined, "deprecated workflowSchema must be gone");

const def = pkg.flow("smoke").version(2).step("a", () => "x").signal("approve").build();
assert.equal(def.name, "smoke");
assert.equal(def.version, 2);
assert.equal(def.nodes.length, 2);
assert.equal(def.nodes[1].kind, "signal");
assert.ok(typeof def.body === "function");

console.log("dist smoke ok");
