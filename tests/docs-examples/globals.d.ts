// Stubs for unbound names used in doc snippets so each block typechecks in
// isolation without needing setup boilerplate inside the example itself.
import type { Pool } from "pg";
import type { WorkflowDb } from "iterativeflow";

declare global {
  const db: WorkflowDb;
  const pool: Pool;
}

export {};
