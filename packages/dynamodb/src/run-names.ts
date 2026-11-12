import { BatchGetCommand } from "@aws-sdk/lib-dynamodb";
import type { EnqueueRequest } from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import type { DocItem } from "#codec";
import { key } from "#schema";

// BatchGetItem caps at 100 keys/call and may return UnprocessedKeys under throttle — chunk and drain.
/** @internal */
export const batchGetRuns = async (
  doc: Doc,
  table: string,
  runIds: readonly string[],
  attr?: "name" | "priority",
): Promise<DocItem[]> => {
  const items: DocItem[] = [];
  // BatchGetItem rejects duplicate keys in one request.
  const ids = [...new Set(runIds)];
  const projection = attr && {
    ProjectionExpression: "id, #attr",
    ExpressionAttributeNames: { "#attr": attr },
  };
  for (let i = 0; i < ids.length; i += 100) {
    let keys: DocItem[] = ids.slice(i, i + 100).map((rid) => key.run(rid));
    while (keys.length > 0) {
      const res = await doc.send(
        new BatchGetCommand({
          RequestItems: { [table]: { Keys: keys, ConsistentRead: true, ...projection } },
        }),
      );
      items.push(...(res.Responses?.[table] ?? []));
      keys = res.UnprocessedKeys?.[table]?.Keys ?? [];
    }
  }
  return items;
};

/** @internal */
export const runNames = async (
  doc: Doc,
  table: string,
  runIds: readonly string[],
): Promise<Map<string, string>> => {
  const runs = await batchGetRuns(doc, table, runIds, "name");
  return new Map(runs.map((r): [string, string] => [r.id, r.name]));
};

/** @internal */
export const storedPriorities = async (
  doc: Doc,
  table: string,
  requests: readonly EnqueueRequest[],
): Promise<Map<string, number>> => {
  const ids = requests.filter((r) => r.opts?.priority === undefined).map((r) => r.runId);
  if (ids.length === 0) return new Map();
  const runs = await batchGetRuns(doc, table, ids, "priority");
  return new Map(
    runs.filter((r) => r.priority !== undefined).map((r): [string, number] => [r.id, r.priority]),
  );
};
