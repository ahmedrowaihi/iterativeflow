import { MongoClient } from "mongodb";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { ensureIndexes } from "#collections";

/** @internal */
export interface StartedMongo {
  container: StartedTestContainer;
  client: MongoClient;
}

/** @internal */
export const startMongo = async (db: string): Promise<StartedMongo> => {
  const container = await new GenericContainer("mongo:7")
    .withCommand(["--replSet", "rs0", "--bind_ip_all"])
    .withExposedPorts(27017)
    .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
    .start();
  const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
  const client = new MongoClient(uri);
  await client.connect();
  await client
    .db("admin")
    .command({ replSetInitiate: {} })
    .catch(() => undefined);
  // transactions need a primary, and election takes a moment after replSetInitiate
  for (let i = 0; i < 40; i++) {
    const hello = await client.db("admin").command({ hello: 1 });
    if (hello.isWritablePrimary) break;
    await new Promise((r) => setTimeout(r, 500));
  }
  await ensureIndexes(client.db(db));
  return { container, client };
};

/** @internal */
export const stopMongo = async ({ container, client }: Partial<StartedMongo>): Promise<void> => {
  await client?.close().catch(() => undefined);
  await container?.stop().catch(() => undefined);
};
