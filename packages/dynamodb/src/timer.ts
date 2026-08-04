import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import { DeleteCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { countQuery } from "#count";
import { runNames } from "#run-names";
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
            ":now": pad(now.getTime()),
          },
          ProjectionExpression: "fireAt",
          ScanIndexForward: true,
          Limit: 1,
        }),
      );
      const next = res.Items?.[0];
      return next ? new Date(next.fireAt) : null;
    },

    async dueCount(now, names) {
      const t = now.getTime();
      const cond = "gsi1pk = :tp AND gsi1sk <= :now";
      const values = { ":tp": TIMER_GSI_PK, ":now": pad(t) };
      if (names === undefined) {
        return countQuery(doc, {
          TableName: table,
          IndexName: "gsi1",
          KeyConditionExpression: cond,
          ExpressionAttributeValues: values,
        });
      }
      const wanted = new Set(names);
      if (wanted.size === 0) return 0;
      const due: TimerItem[] = [];
      let ExclusiveStartKey: Record<string, unknown> | undefined;
      do {
        const res = await send<{ Items?: TimerItem[]; LastEvaluatedKey?: Record<string, unknown> }>(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi1",
            KeyConditionExpression: cond,
            ExpressionAttributeValues: values,
            ProjectionExpression: "runId",
            ExclusiveStartKey,
          }),
        );
        due.push(...(res.Items ?? []));
        ExclusiveStartKey = res.LastEvaluatedKey;
      } while (ExclusiveStartKey);
      const nameById = await runNames(
        doc,
        table,
        due.map((d) => d.runId),
      );
      return due.filter((d) => wanted.has(nameById.get(d.runId) ?? "")).length;
    },
  };
};
