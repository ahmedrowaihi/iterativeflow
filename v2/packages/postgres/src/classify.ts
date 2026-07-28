// SQLSTATE that fails identically on every retry, so a step fails fast rather than burn its retry
// budget. Whole classes here are permanent; class 23 (integrity) is split — a foreign-key or unique
// violation can be a concurrency race that succeeds on retry (a racing txn commits the parent, or the
// duplicate was mid-flight), so only the deterministic 23 codes (not-null, check) are permanent.
// Everything else (connection loss, statement timeout, deadlock, serialization) stays transient.
const PERMANENT_SQLSTATE_CLASS = new Set([
  "22", // data exception — invalid text/numeric input, out of range
  "42", // syntax error or access rule violation — undefined column/table, insufficient privilege
]);
const PERMANENT_SQLSTATE_CODE = new Set([
  "23502", // not_null_violation
  "23514", // check_violation
]);

const sqlState = (error: unknown): string | undefined => {
  // A driver like Drizzle wraps the real pg error, so the SQLSTATE lives down the `.cause` chain;
  // the bounded depth guards a self-referential chain.
  let c: unknown = error;
  for (let depth = 0; c && depth < 8; depth++) {
    const code = (c as { code?: unknown }).code;
    if (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code)) return code;
    c = (c as { cause?: unknown }).cause;
  }
  return undefined;
};

/**
 * A `StepPolicy.classify` preset for Postgres. Data, syntax/access, and the deterministic constraint
 * violations (not-null, check) are permanent — the step fails fast instead of retrying to
 * `maxAttempts`. Connection drops, statement timeouts, deadlocks, serialization failures,
 * foreign-key/unique violations (which can be a concurrency race), and anything unrecognized stay
 * transient and retry. Walks the `.cause` chain for the SQLSTATE, since drivers wrap the pg error.
 *
 * @example
 * await ctx.step("write", writeRow, { classify: pgClassify });
 */
export const pgClassify = (error: unknown): "transient" | "permanent" => {
  const code = sqlState(error);
  if (!code) return "transient";
  const permanent =
    PERMANENT_SQLSTATE_CLASS.has(code.slice(0, 2)) || PERMANENT_SQLSTATE_CODE.has(code);
  return permanent ? "permanent" : "transient";
};
