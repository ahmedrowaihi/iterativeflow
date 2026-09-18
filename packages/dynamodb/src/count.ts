import { QueryCommand, type QueryCommandInput } from "@aws-sdk/lib-dynamodb";
import type { Doc } from "#client";

/**
 * Count matching items with `Select: COUNT` — DynamoDB returns the count per page and moves no item
 * attributes over the wire, so an unfiltered backlog count transfers nothing. Pass base query params
 * without a `ProjectionExpression` (it conflicts with `Select: COUNT`).
 * @internal
 */
export const countQuery = async (doc: Doc, params: QueryCommandInput): Promise<number> => {
  let count = 0;
  let ExclusiveStartKey: QueryCommandInput["ExclusiveStartKey"];
  do {
    const res = await doc.send(new QueryCommand({ ...params, Select: "COUNT", ExclusiveStartKey }));
    count += res.Count ?? 0;
    ExclusiveStartKey = res.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return count;
};
