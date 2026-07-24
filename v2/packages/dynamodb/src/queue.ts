import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import { DeleteCommand, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import type { ClaimOpts, IdGen, Lease, Queue } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { JOB_GSI_PK, key } from "#schema";
import { enqueueParams } from "#statements";

interface JobItem {
  runId: string;
  runAt: number;
  priority: number;
  version?: number;
  leaseExpires?: number;
}

/** @internal */
export const createDynamoQueue = (doc: Doc, table: string, id: IdGen): Queue => {
  const send = <T = unknown>(cmd: unknown): Promise<T> => doc.send(cmd) as Promise<T>;
  const at = (d?: Date): number => (d ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await send(new UpdateCommand(enqueueParams(table, runId, opts)));
    },

    async claim({ max, leaseMs, now }: ClaimOpts) {
      const t = at(now);
      // Query only the JOB partition on GSI1 (ordered priority#runAt) — never a full-table Scan.
      // The lease/runAt predicate is a post-read filter, so a backlog of leased/future jobs can
      // fill the ≤1MB page and bury due ones; page until we hold `max`. PAGE_CAP bounds the reads.
      const PAGE_CAP = 10;
      const candidates: JobItem[] = [];
      let ExclusiveStartKey: Record<string, unknown> | undefined;
      for (let page = 0; page < PAGE_CAP; page++) {
        const res = await send<{ Items?: JobItem[]; LastEvaluatedKey?: Record<string, unknown> }>(
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
        candidates.push(...(res.Items ?? []));
        ExclusiveStartKey = res.LastEvaluatedKey;
        // GSI is priority#runAt-ordered, so once we hold `max`, later pages can't rank higher.
        if (!ExclusiveStartKey || candidates.length >= max) break;
      }
      const leases: Lease[] = [];
      for (const j of candidates) {
        if (leases.length >= max) break;
        const token = `${id()}:${j.runId}`;
        const expires = t + leaseMs;
        try {
          await send(
            new UpdateCommand({
              TableName: table,
              Key: key.job(j.runId),
              UpdateExpression: "SET leaseToken = :token, leaseExpires = :exp",
              ConditionExpression:
                "attribute_exists(pk) AND (attribute_not_exists(leaseExpires) OR leaseExpires <= :now)",
              ExpressionAttributeValues: { ":token": token, ":exp": expires, ":now": t },
            }),
          );
          leases.push({
            runId: j.runId,
            token,
            expiresAt: new Date(expires),
            version: j.version ?? 0,
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
        await send(
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
        // Version unchanged → normal completion, delete the job.
        await send(
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
          // Re-enqueued mid-lease (a wake raced this ack) → keep the job, release for re-claim.
          await send(
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
  };
};
