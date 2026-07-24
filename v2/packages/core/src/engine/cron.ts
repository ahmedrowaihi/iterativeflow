/**
 * A tiny, dependency-free 5-field cron evaluator: `minute hour day-of-month month day-of-week`.
 * Supports wildcards, step values (e.g. every 15), ranges (a-b), and comma lists. Evaluated in
 * UTC. Day-of-month and day-of-week combine with cron's OR rule when BOTH are restricted.
 */

interface Fields {
  minute: Set<number>;
  hour: Set<number>;
  dom: Set<number>;
  month: Set<number>;
  dow: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
}

const expand = (spec: string, min: number, max: number): Set<number> => {
  const out = new Set<number>();
  for (const part of spec.split(",")) {
    const [range, stepStr] = part.split("/");
    const step = stepStr ? Number(stepStr) : 1;
    if (!Number.isInteger(step) || step < 1) throw new Error(`cron: bad step in "${part}"`);
    let lo: number;
    let hi: number;
    if (range === "*") {
      lo = min;
      hi = max;
    } else if (range.includes("-")) {
      const [a, b] = range.split("-").map(Number);
      lo = a;
      hi = b;
    } else {
      lo = Number(range);
      hi = Number(range);
    }
    if (!Number.isInteger(lo) || !Number.isInteger(hi) || lo < min || hi > max || lo > hi) {
      throw new Error(`cron: field "${part}" out of range ${min}-${max}`);
    }
    for (let n = lo; n <= hi; n += step) out.add(n);
  }
  return out;
};

const parse = (expr: string): Fields => {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5)
    throw new Error(`cron: expected 5 fields, got ${parts.length} in "${expr}"`);
  const [mi, ho, dm, mo, dw] = parts;
  return {
    minute: expand(mi, 0, 59),
    hour: expand(ho, 0, 23),
    dom: expand(dm, 1, 31),
    month: expand(mo, 1, 12),
    dow: expand(dw, 0, 6), // 0 = Sunday
    domRestricted: dm !== "*",
    dowRestricted: dw !== "*",
  };
};

const matches = (f: Fields, d: Date): boolean => {
  if (!f.minute.has(d.getUTCMinutes())) return false;
  if (!f.hour.has(d.getUTCHours())) return false;
  if (!f.month.has(d.getUTCMonth() + 1)) return false;
  const domOk = f.dom.has(d.getUTCDate());
  const dowOk = f.dow.has(d.getUTCDay());
  // Standard cron OR-rule: when both day fields are restricted, either matching is enough.
  if (f.domRestricted && f.dowRestricted) return domOk || dowOk;
  return domOk && dowOk;
};

/** Validate a cron expression, throwing on a malformed one. Call once at registration. */
export const parseCron = (expr: string): void => {
  parse(expr);
};

/**
 * The next instant strictly after `from` that matches `expr` (UTC, second-precision zeroed).
 * Steps minute-by-minute; throws if nothing matches within the horizon (an impossible schedule).
 * The horizon spans a full leap cycle so a sparse-but-valid schedule (e.g. Feb 29) resolves instead
 * of falsely throwing.
 */
const HORIZON_MINUTES = 4 * 366 * 24 * 60;
export const nextCronAfter = (expr: string, from: Date): Date => {
  const f = parse(expr);
  const d = new Date(from.getTime());
  d.setUTCSeconds(0, 0);
  d.setUTCMinutes(d.getUTCMinutes() + 1); // strictly after
  for (let i = 0; i < HORIZON_MINUTES; i++) {
    if (matches(f, d)) return d;
    d.setUTCMinutes(d.getUTCMinutes() + 1);
  }
  throw new Error(`cron: "${expr}" has no next fire within 4 years`);
};
