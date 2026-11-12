import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  QueryCommand,
  type QueryCommandInput,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  type ClaimOpts,
  type EnqueueRequest,
  type IdGen,
  type Lease,
  type Queue,
  distinctEnqueues,
  queueDepthOf,
  shardAdmits,
} from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { type JobItem, num, parseJob } from "#codec";
import { runNames, storedPriorities } from "#run-names";
import { JOB_GSI_PK, key } from "#schema";
import { enqueueParams } from "#statements";

// Bounded so a large wake list can't open thousands of sockets at once.
const ENQUEUE_CONCURRENCY = 25;

// The due/unleased predicate is a post-read filter, so a backlog of leased or future jobs can fill
// every ≤1MB page and bury the due ones. Capping the pages bounds one claim's read cost; a claim
// that comes back short is retried by the next tick.
const CLAIM_PAGE_CAP = 10;

const shardFilter = async (
  doc: Doc,
  table: string,
  jobs: JobItem[],
  names: readonly string[] | undefined,
): Promise<JobItem[]> => {
  if (names === undefined) return jobs;
  const nameById = await runNames(
    doc,
    table,
    jobs.map((j) => j.runId),
  );
  return jobs.filter((j) => shardAdmits(names, nameById.get(j.runId)));
};

/** @internal */
export const createDynamoQueue = (doc: Doc, table: string, id: IdGen): Queue => {
  const at = (d?: Date): number => (d ?? new Date()).getTime();

  const enqueueMany = async (requests: readonly EnqueueRequest[]): Promise<void> => {
    const rows = distinctEnqueues(requests);
    const priorities = await storedPriorities(doc, table, requests);
    for (let i = 0; i < rows.length; i += ENQUEUE_CONCURRENCY) {
      await Promise.all(
        rows
          .slice(i, i + ENQUEUE_CONCURRENCY)
          .map(([runId, opts]) =>
            doc.send(new UpdateCommand(enqueueParams(table, runId, opts, priorities.get(runId)))),
          ),
      );
    }
  };

  return {
    async enqueue(runId, opts) {
      await enqueueMany([{ runId, opts }]);
    },

    enqueueMany,

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      const t = at(now);
      const allowed: JobItem[] = [];
      let ExclusiveStartKey: QueryCommandInput["ExclusiveStartKey"];
      for (let page = 0; page < CLAIM_PAGE_CAP; page++) {
        const res = await doc.send(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi1",
            KeyConditionExpression: "gsi1pk = :job",
            FilterExpression:
              "runAt <= :now AND (attribute_not_exists(leaseExpires) OR leaseExpires <= :now)",
            ExpressionAttributeValues: { ":job": JOB_GSI_PK, ":now": t },
            ExclusiveStartKey,
          }),
        );
        allowed.push(...(await shardFilter(doc, table, (res.Items ?? []).map(parseJob), names)));
        ExclusiveStartKey = res.LastEvaluatedKey;
        // GSI is priority#runAt-ordered, so once we hold `limit` admitted jobs, later pages can't
        // rank higher. Count only after the shard filter, or other shards' jobs starve this claim.
        if (!ExclusiveStartKey || allowed.length >= limit) break;
      }
      const leases: Lease[] = [];
      for (const j of allowed) {
        if (leases.length >= limit) break;
        const token = `${id()}:${j.runId}`;
        const expires = t + leaseMs;
        try {
          const res = await doc.send(
            new UpdateCommand({
              TableName: table,
              Key: key.job(j.runId),
              UpdateExpression: "SET leaseToken = :token, leaseExpires = :exp",
              ConditionExpression:
                "attribute_exists(pk) AND (attribute_not_exists(leaseExpires) OR leaseExpires <= :now)",
              ExpressionAttributeValues: { ":token": token, ":exp": expires, ":now": t },
              // Capture `version` from the lease write itself: a wake that bumps it between the
              // candidate read and this write must be reflected in the lease, or `ack` mis-decides.
              ReturnValues: "ALL_NEW",
            }),
          );
          leases.push({
            runId: j.runId,
            token,
            expiresAt: new Date(expires),
            version: num(res.Attributes, "version"),
          });
        } catch (e) {
          if (!(e instanceof ConditionalCheckFailedException)) throw e; // lost the race — another worker leased it
        }
      }
      return leases;
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const t = at(now);
      const expires = t + leaseMs;
      try {
        await doc.send(
          new UpdateCommand({
            TableName: table,
            Key: key.job(lease.runId),
            UpdateExpression: "SET leaseExpires = :exp",
            ConditionExpression: "leaseToken = :token AND leaseExpires > :now",
            ExpressionAttributeValues: { ":exp": expires, ":token": lease.token, ":now": t },
          }),
        );
      } catch (e) {
        if (e instanceof ConditionalCheckFailedException) {
          throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`, { cause: e });
        }
        throw e;
      }
      return { ...lease, expiresAt: new Date(expires) };
    },

    async ack(lease: Lease, opts) {
      const now = at(opts?.now);
      try {
        await doc.send(
          new DeleteCommand({
            TableName: table,
            Key: key.job(lease.runId),
            ConditionExpression: "leaseToken = :token AND leaseExpires > :now AND version = :v",
            ExpressionAttributeValues: { ":token": lease.token, ":now": now, ":v": lease.version },
          }),
        );
      } catch (e) {
        if (!(e instanceof ConditionalCheckFailedException)) throw e;
        try {
          await doc.send(
            new UpdateCommand({
              TableName: table,
              Key: key.job(lease.runId),
              UpdateExpression: "SET runAt = :zero REMOVE leaseToken, leaseExpires",
              ConditionExpression: "leaseToken = :token AND leaseExpires > :now AND version <> :v",
              ExpressionAttributeValues: {
                ":zero": 0,
                ":token": lease.token,
                ":now": now,
                ":v": lease.version,
              },
            }),
          );
        } catch (e2) {
          if (!(e2 instanceof ConditionalCheckFailedException)) throw e2; // stale/expired — no-op
        }
      }
    },

    async depth(now, names) {
      const t = at(now);
      const jobs: JobItem[] = [];
      let ExclusiveStartKey: QueryCommandInput["ExclusiveStartKey"];
      do {
        const res = await doc.send(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi1",
            KeyConditionExpression: "gsi1pk = :job",
            ExpressionAttributeValues: { ":job": JOB_GSI_PK },
            ProjectionExpression: "runId, runAt, leaseExpires",
            ExclusiveStartKey,
          }),
        );
        jobs.push(...(res.Items ?? []).map(parseJob));
        ExclusiveStartKey = res.LastEvaluatedKey;
      } while (ExclusiveStartKey);
      return queueDepthOf(await shardFilter(doc, table, jobs, names), t);
    },
  };
};
