import type { FlowNode } from "../builder/types";
import type { FlowError } from "../storage/schema";
import { flowError } from "../util/errors";
import type { RunSnapshot } from "./types";

/**
 * Cursor key scheme.
 *
 * Every `ctx.step` / `ctx.sleep` / `ctx.signal` call emits a deterministic key:
 * `${base}` on the first occurrence of that base, then `${base}:1`,
 * `${base}:2`, ... on subsequent occurrences. Per-kind bases:
 *
 * - step    → the step name
 * - sleep   → `"sleep"`
 * - signal  → `` `signal:${name}` ``
 * - invoke  → `` `invoke:${name}@${version}` ``
 *
 * Two consumers go through this module: the runtime cursor (live, advances
 * once per `ctx.X` call) and the static-plan walker {@link checkCompat}
 * (replays the same sequence against a flow's node tree to detect drift).
 */

/** @internal */
export interface Cursor {
  next(base: string): string;
}

/** @internal */
export const createKeyCursor = (): Cursor => {
  const counts = new Map<string, number>();
  return {
    next(base) {
      const idx = counts.get(base) ?? 0;
      counts.set(base, idx + 1);
      return idx === 0 ? base : `${base}:${idx}`;
    },
  };
};

/** @internal */
export const sleepBase = (): string => "sleep";

/** @internal */
export const signalBase = (name: string): string => `signal:${name}`;

/**
 * Recover the base name from a recorded key by stripping a positive-integer
 * `:N` suffix. Best-effort: the cursor scheme guarantees uniqueness of the
 * suffix, not the absence of `:` in the base.
 *
 * @internal
 */
export const baseOf = (key: string): string => {
  const i = key.lastIndexOf(":");
  if (i === -1) return key;
  return /^[1-9]\d*$/.test(key.slice(i + 1)) ? key.slice(0, i) : key;
};

export interface ProducibleBag {
  /** Concrete keys producible outside any loop. Enumerable for count-drift checks. */
  keys: Set<string>;
  /** Bases producible outside any loop. Count drift IS detectable. */
  bases: Set<string>;
  /** Bases producible only inside loop bodies. Count drift is NOT detectable. */
  loopBases: Set<string>;
}

export interface Producible {
  step: ProducibleBag;
  timer: ProducibleBag;
  signal: ProducibleBag;
}

const emptyBag = (): ProducibleBag => ({
  keys: new Set(),
  bases: new Set(),
  loopBases: new Set(),
});

const visit = (
  out: Producible,
  cursor: Cursor,
  nodes: ReadonlyArray<FlowNode>,
  insideLoop: boolean,
): void => {
  for (const node of nodes) {
    if (node.kind === "step") {
      if (insideLoop) out.step.loopBases.add(node.name);
      else {
        out.step.bases.add(node.name);
        out.step.keys.add(cursor.next(node.name));
      }
    } else if (node.kind === "sleep") {
      const base = sleepBase();
      if (insideLoop) out.timer.loopBases.add(base);
      else {
        out.timer.bases.add(base);
        out.timer.keys.add(cursor.next(base));
      }
    } else if (node.kind === "signal") {
      const base = signalBase(node.name);
      if (insideLoop) out.signal.loopBases.add(base);
      else {
        out.signal.bases.add(base);
        out.signal.keys.add(cursor.next(base));
      }
    } else if (node.kind === "loop") {
      visit(out, cursor, node.nodes, true);
    }
  }
};

/**
 * Enumerate every key the given node list would produce, partitioned by the
 * snapshot bag the key lands in. Loop bodies contribute their *bases* but not
 * their concrete keys (iteration count is dynamic).
 *
 * @internal
 */
export const producibleKeys = (nodes: ReadonlyArray<FlowNode>): Producible => {
  const cursor = createKeyCursor();
  const out: Producible = { step: emptyBag(), timer: emptyBag(), signal: emptyBag() };
  visit(out, cursor, nodes, false);
  return out;
};

type DriftKind = "step" | "timer" | "signal";

/**
 * Decide whether the persisted snapshot is still compatible with the current
 * flow graph.
 *
 * A recorded key drifts when one of:
 *  - it sits in a bag the new graph doesn't write to (kind change → `REPLAY_INCOMPATIBLE_VERSION`)
 *  - its base is gone (`REPLAY_INCOMPATIBLE_VERSION`)
 *  - the occurrence count for a non-loop base shrank inside the same bag (`REPLAY_NON_DETERMINISTIC`)
 *
 * @returns `null` when the snapshot is replayable.
 * @internal
 */
export const checkCompat = (
  snapshot: RunSnapshot,
  nodes: ReadonlyArray<FlowNode>,
): FlowError | null => {
  const producible = producibleKeys(nodes);
  const pairs: ReadonlyArray<[ReadonlyMap<string, unknown>, ProducibleBag, DriftKind]> = [
    [snapshot.steps, producible.step, "step"],
    [snapshot.timers, producible.timer, "timer"],
    [snapshot.signals, producible.signal, "signal"],
  ];

  for (const [source, expected, kind] of pairs) {
    for (const key of source.keys()) {
      if (expected.keys.has(key)) continue;
      const base = baseOf(key);
      // Loop-body bases: occurrence count is dynamic, so we can't tell drift
      // from a legitimate later iteration. Accept.
      if (expected.loopBases.has(key) || expected.loopBases.has(base)) continue;
      if (expected.bases.has(key) || expected.bases.has(base)) {
        return flowError(
          "REPLAY_NON_DETERMINISTIC",
          `replay diverged: recorded ${kind} "${key}" — occurrence count for "${base}" changed`,
        );
      }
      return flowError(
        "REPLAY_INCOMPATIBLE_VERSION",
        `recorded ${kind} "${key}" is absent from the registered flow graph — removed, renamed, or changed kind`,
      );
    }
  }
  return null;
};
