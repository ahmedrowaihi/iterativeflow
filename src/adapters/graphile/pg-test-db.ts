import { randomUUID } from "node:crypto";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { Pool } from "pg";

const externalUrl = process.env.ITERATIVE_PG_URL;
const skipContainers = process.env.SKIP_TESTCONTAINERS === "1";

/** True when there's no shared PG URL and testcontainers are disabled — skip the suite. */
export const pgUnavailable = skipContainers && !externalUrl;

export interface TestDb {
  url: string;
  close: () => Promise<void>;
}

const withDatabase = (base: string, dbName: string): string => {
  const u = new URL(base);
  u.pathname = `/${dbName}`;
  return u.toString();
};

/**
 * Acquire an isolated Postgres for one test file. With a shared `ITERATIVE_PG_URL`
 * (CI) each call creates its own database, so files running in parallel never race
 * on the same `workflow` schema; otherwise it starts a throwaway container.
 */
export const acquireTestDb = async (): Promise<TestDb> => {
  if (externalUrl) {
    const dbName = `iflow_test_${randomUUID().replace(/-/g, "")}`;
    const admin = new Pool({ connectionString: externalUrl });
    await admin.query(`CREATE DATABASE "${dbName}"`);
    await admin.end();
    return {
      url: withDatabase(externalUrl, dbName),
      close: async () => {
        const dropper = new Pool({ connectionString: externalUrl });
        await dropper
          .query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
            [dbName],
          )
          .catch(() => undefined);
        await dropper.query(`DROP DATABASE IF EXISTS "${dbName}"`).catch(() => undefined);
        await dropper.end();
      },
    };
  }
  const container: StartedPostgreSqlContainer = await new PostgreSqlContainer(
    "postgres:16-alpine",
  ).start();
  return {
    url: container.getConnectionUri(),
    close: () => container.stop().then(() => undefined),
  };
};
