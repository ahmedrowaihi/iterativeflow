import { BatchGetCommand } from "@aws-sdk/lib-dynamodb";
import type { EnqueueRequest } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { key } from "#schema";

const runAttr = async <T>(
  doc: Doc,
  table: string,
  runIds: readonly string[],
  attr: string,
): Promise<Map<string, T>> => {
  const send = <R = unknown>(cmd: unknown): Promise<R> => doc.send(cmd) as Promise<R>;
  const byId = new Map<string, T>();
  // BatchGetItem rejects duplicate keys in one request.
  const ids = [...new Set(runIds)];
  for (let i = 0; i < ids.length; i += 100) {
    let keys = ids.slice(i, i + 100).map((rid) => key.run(rid));
    while (keys.length > 0) {
      const res = await send<{
        Responses?: Record<string, ({ id: string } & Record<string, unknown>)[]>;
        UnprocessedKeys?: Record<string, { Keys?: { pk: string; sk: string }[] }>;
      }>(
        new BatchGetCommand({
          RequestItems: {
            [table]: {
              Keys: keys,
              ProjectionExpression: "id, #attr",
              ExpressionAttributeNames: { "#attr": attr },
              ConsistentRead: true,
            },
          },
        }),
      );
      for (const r of res.Responses?.[table] ?? []) {
        if (r[attr] !== undefined) byId.set(r.id, r[attr] as T);
      }
      keys = res.UnprocessedKeys?.[table]?.Keys ?? [];
    }
  }
  return byId;
};

/** @internal */
export const runNames = (
  doc: Doc,
  table: string,
  runIds: readonly string[],
): Promise<Map<string, string>> => runAttr<string>(doc, table, runIds, "name");

/** @internal */
export const storedPriorities = async (
  doc: Doc,
  table: string,
  requests: readonly EnqueueRequest[],
): Promise<Map<string, number>> => {
  const ids = requests.filter((r) => r.opts?.priority === undefined).map((r) => r.runId);
  return ids.length === 0 ? new Map() : runAttr<number>(doc, table, ids, "priority");
};
