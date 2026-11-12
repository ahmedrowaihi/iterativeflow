import { backendConformance } from "@iterativeflow/conformance";
import { type Backend } from "@iterativeflow/core";
import { Redis } from "ioredis";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe } from "vitest";
import { createRedisBackend } from "#index";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("redis backend", () => {
  let container: StartedTestContainer;
  let client: Redis;

  beforeAll(async () => {
    container = await new GenericContainer("valkey/valkey:8-alpine").withExposedPorts(6379).start();
    client = new Redis(container.getMappedPort(6379), container.getHost(), {
      maxRetriesPerRequest: null,
    });
  }, 180_000);

  afterAll(async () => {
    client?.disconnect();
    await container?.stop().catch(() => undefined);
  });

  const makeBackend = async (): Promise<Backend> => {
    await client.flushdb();
    return createRedisBackend(client);
  };

  backendConformance("redis", () => makeBackend(), { concurrentWriters: true });
});
