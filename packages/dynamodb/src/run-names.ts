import { BatchGetCommand } from "@aws-sdk/lib-dynamodb";
import type { Doc } from "#client";
import { key } from "#schema";

/** @internal */
export const runNames = async (
  doc: Doc,
  table: string,
  runIds: readonly string[],
): Promise<Map<string, string>> => {
  const send = <T = unknown>(cmd: unknown): Promise<T> => doc.send(cmd) as Promise<T>;
  const nameById = new Map<string, string>();
  for (let i = 0; i < runIds.length; i += 100) {
    let keys = runIds.slice(i, i + 100).map((rid) => key.run(rid));
    while (keys.length > 0) {
      const res = await send<{
        Responses?: Record<string, { id: string; name: string }[]>;
        UnprocessedKeys?: Record<string, { Keys?: { pk: string; sk: string }[] }>;
      }>(
        new BatchGetCommand({
          RequestItems: {
            [table]: {
              Keys: keys,
              ProjectionExpression: "id, #name",
              ExpressionAttributeNames: { "#name": "name" },
              ConsistentRead: true,
            },
          },
        }),
      );
      for (const r of res.Responses?.[table] ?? []) nameById.set(r.id, r.name);
      keys = res.UnprocessedKeys?.[table]?.Keys ?? [];
    }
  }
  return nameById;
};
