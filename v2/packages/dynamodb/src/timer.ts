import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import { DeleteCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { TIMER_GSI_PK, key, pad } from "#schema";

interface TimerItem {
  runId: string;
  fireAt: number;
}

/** @internal */
export const createDynamoTimer = (doc: Doc, table: string): Timer => {
  const send = <T = unknown>(cmd: unknown): Promise<T> => doc.send(cmd) as Promise<T>;

  return {
    async schedule(runId, fireAt) {
      await send(
        new PutCommand({
          TableName: table,
          Item: {
            ...key.timer(runId),
            type: "timer",
            runId,
            fireAt: fireAt.getTime(),
            gsi1pk: TIMER_GSI_PK,
            gsi1sk: pad(fireAt.getTime()),
          },
        }),
      );
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const t = (now ?? new Date()).getTime();
      const res = await send<{ Items?: TimerItem[] }>(
        new QueryCommand({
          TableName: table,
          IndexName: "gsi1",
          KeyConditionExpression: "gsi1pk = :tp AND gsi1sk <= :now",
          ExpressionAttributeValues: { ":tp": TIMER_GSI_PK, ":now": pad(t) },
          ScanIndexForward: true,
          Limit: limit,
        }),
      );
      const fired: string[] = [];
      for (const it of res.Items ?? []) {
        try {
          await send(
            new DeleteCommand({
              TableName: table,
              Key: key.timer(it.runId),
              ConditionExpression: "attribute_exists(pk)",
            }),
          );
          fired.push(it.runId); // won the fire-once delete
        } catch (e) {
          if (!(e instanceof ConditionalCheckFailedException)) throw e; // a concurrent drain took it
        }
      }
      return fired;
    },

    async cancel(runId) {
      await send(new DeleteCommand({ TableName: table, Key: key.timer(runId) }));
    },

    async nextDueAt(now) {
      const res = await send<{ Items?: TimerItem[] }>(
        new QueryCommand({
          TableName: table,
          IndexName: "gsi1",
          KeyConditionExpression: "gsi1pk = :tp AND gsi1sk > :now",
          ExpressionAttributeValues: {
            ":tp": TIMER_GSI_PK,
            ":now": pad((now ?? new Date()).getTime()),
          },
          ProjectionExpression: "fireAt",
          ScanIndexForward: true,
          Limit: 1,
        }),
      );
      const next = res.Items?.[0];
      return next ? new Date(next.fireAt) : null;
    },
  };
};
