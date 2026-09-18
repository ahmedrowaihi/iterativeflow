import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { num, parseTimer, str } from "#codec";
import { countQuery } from "#count";
import { runNames } from "#run-names";
import { TIMER_GSI_PK, key, pad } from "#schema";

/** @internal */
export const createDynamoTimer = (doc: Doc, table: string): Timer => {
  return {
    async schedule(runId, fireAt) {
      await doc.send(
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
      const res = await doc.send(
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
      for (const it of (res.Items ?? []).map(parseTimer)) {
        try {
          await doc.send(
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
      await doc.send(new DeleteCommand({ TableName: table, Key: key.timer(runId) }));
    },

    async nextDueAt(now) {
      const res = await doc.send(
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
      return next ? new Date(num(next, "fireAt")) : null;
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
      const due: string[] = [];
      let ExclusiveStartKey: QueryCommandInput["ExclusiveStartKey"];
      do {
        const res = await doc.send(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi1",
            KeyConditionExpression: cond,
            ExpressionAttributeValues: values,
            ProjectionExpression: "runId",
            ExclusiveStartKey,
          }),
        );
        due.push(...(res.Items ?? []).map((i) => str(i, "runId")));
        ExclusiveStartKey = res.LastEvaluatedKey;
      } while (ExclusiveStartKey);
      const nameById = await runNames(doc, table, due);
      // a run-less timer is unownable, so it passes every name filter (see Queue.claim)
      return due.filter((runId) => {
        const name = nameById.get(runId);
        return name === undefined || wanted.has(name);
      }).length;
    },
  };
};
