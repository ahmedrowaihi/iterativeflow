import { type Pool, createPool } from "mysql2/promise";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { applySchema } from "#schema";
import { mysqlPool } from "#sql";

/** @internal */
export interface StartedMysql {
  container: StartedTestContainer;
  pool: Pool;
}

/** @internal */
export const startMysql = async (): Promise<StartedMysql> => {
  const container = await new GenericContainer("mysql:8")
    .withEnvironment({ MYSQL_ROOT_PASSWORD: "test", MYSQL_DATABASE: "iflow" })
    .withExposedPorts(3306)
    .withWaitStrategy(Wait.forLogMessage(/ready for connections/, 2))
    .withStartupTimeout(180_000)
    .start();
  const pool = createPool({
    host: container.getHost(),
    port: container.getMappedPort(3306),
    user: "root",
    password: "test",
    database: "iflow",
  });
  // MySQL logs "ready" during its init temp-server phase then restarts, so the first connections
  // can drop — ping until the real server is stable before applying the schema.
  for (let i = 0; i < 40; i++) {
    try {
      await pool.query("SELECT 1");
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  await applySchema(mysqlPool(pool));
  return { container, pool };
};

/** @internal */
export const stopMysql = async ({ container, pool }: Partial<StartedMysql>): Promise<void> => {
  await pool?.end().catch(() => undefined);
  await container?.stop().catch(() => undefined);
};
