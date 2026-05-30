import { EXPECTED_SCHEMA_VERSION } from "../storage/schema";
import type { Storage } from "./types";

/**
 * Lazily-memoized "is the DB at the engine's expected schema version" check.
 * Runs at most once per engine instance. Throws `SCHEMA_MISMATCH` on first
 * call when the DB is on a different version; subsequent successful calls
 * are no-ops.
 *
 * @internal
 */
export interface SchemaVersionCheck {
  ensure(): Promise<void>;
}

/** @internal */
export const createSchemaVersionCheck = (storage: Storage): SchemaVersionCheck => {
  let promise: Promise<void> | null = null;
  return {
    ensure() {
      if (promise === null) {
        promise = (async () => {
          const version = await storage.getSchemaVersion();
          if (version !== EXPECTED_SCHEMA_VERSION) {
            const hint =
              version === 0
                ? "schema not applied — run `drizzle-kit migrate` or `psql -f` the bundled migration"
                : `schema is at v${version}, engine expects v${EXPECTED_SCHEMA_VERSION} — run \`drizzle-kit generate && drizzle-kit migrate\``;
            const err = new Error(`SCHEMA_MISMATCH: ${hint}`);
            (err as Error & { code?: string }).code = "SCHEMA_MISMATCH";
            throw err;
          }
        })();
        promise.catch(() => {
          promise = null;
        });
      }
      return promise;
    },
  };
};
