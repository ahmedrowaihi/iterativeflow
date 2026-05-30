import { existsSync } from "node:fs";
import { resolve } from "node:path";

/**
 * Find a build-time asset on disk. Looks first at the shipped path next to
 * the calling module (production), then at the repo-source path (dev/tests).
 * Throws if neither exists.
 *
 * @internal
 */
export const locateShippedAsset = (here: string, distName: string, srcRelPath: string): string => {
  const candidates = [resolve(here, distName), resolve(here, srcRelPath)];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  throw new Error(`iterativeflow: asset not found. Looked in:\n  ${candidates.join("\n  ")}`);
};
