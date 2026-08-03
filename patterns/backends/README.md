# Backends

iterativeflow runs on one of several storage backends. They all pass the same conformance suites, so a
flow behaves the same on any of them — pick by where your data already lives and how you deploy.

| Backend         | Use it for                                                                                                                          | Guide                                   |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| Postgres        | The default for a server deployment. Most features: event timeline, push wake-ups, error classifier, in-database autoscaling query. | [postgres](./postgres.md)               |
| MySQL           | When MySQL (or PlanetScale) is already your database.                                                                               | [mysql](./mysql.md)                     |
| SQLite          | A single node, embedded apps, or on-device (React Native). Single-writer.                                                           | [sqlite](./sqlite.md)                   |
| MongoDB         | When MongoDB is already your database. Needs a replica set.                                                                         | [mongodb](./mongodb.md)                 |
| DynamoDB        | Serverless on AWS (Lambda), pay-per-use, no servers to run.                                                                         | [dynamodb](./dynamodb.md)               |
| Redis           | Low-latency claims when Redis is in your stack. Configure it for durability first.                                                  | [redis](./redis.md)                     |
| Durable Objects | Durable workflows at the edge on Cloudflare, one engine per object.                                                                 | [durable-objects](./durable-objects.md) |

There's also an in-memory backend (`@iterativeflow/memory`) for tests and local development — it keeps
nothing across a restart, so don't use it in production.

For topics that apply across backends — execution models, connection pooling, clocks and leases,
scaling, and sharding — see [deployment](../deployment.md).
