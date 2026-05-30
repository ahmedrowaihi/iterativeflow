/**
 * Backoff curve for step retries.
 * - `"exponential"` (default): `base * 2^(attempt - 1)` + 10% jitter, capped at `cap`.
 * - `"linear"`: `base * attempt`, capped at `cap`.
 * - `"fixed"`: always `base` (capped at `cap`).
 * - function: caller-provided `attempt -> delay ms`.
 */
export type BackoffPolicy = "exponential" | "linear" | "fixed" | ((attempt: number) => number);

export interface BackoffOpts {
  policy?: BackoffPolicy;
  baseMs?: number;
  capMs?: number;
}

const DEFAULT_BASE_MS = 1000;
const DEFAULT_CAP_MS = 5 * 60 * 1000;

export const computeBackoff = (attempt: number, opts: BackoffOpts = {}): number => {
  const policy = opts.policy ?? "exponential";
  const base = opts.baseMs ?? DEFAULT_BASE_MS;
  const cap = opts.capMs ?? DEFAULT_CAP_MS;

  if (typeof policy === "function") return Math.min(policy(attempt), cap);
  if (policy === "fixed") return Math.min(base, cap);
  if (policy === "linear") return Math.min(base * attempt, cap);

  const exp = base * 2 ** Math.max(0, attempt - 1);
  const jitter = exp * 0.1 * Math.random();
  return Math.min(exp + jitter, cap);
};
