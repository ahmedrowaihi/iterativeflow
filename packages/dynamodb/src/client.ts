import type { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

/**
 * The send-surface the backend drives: the document client's own typed `send`. Wrap behaviour
 * (fault injection, tracing) with client middleware rather than a hand-rolled proxy.
 */
export type Doc = Pick<DynamoDBDocumentClient, "send">;

/**
 * Wrap a low-level {@link DynamoDBClient} in a document client that (un)marshals plain JS.
 * `removeUndefinedValues` keeps optional attributes (lease token, error) off the item rather
 * than rejecting the write; the backend JSON-encodes user payloads itself, so nested shapes
 * round-trip exactly regardless of Dynamo's native type coercion.
 */
export const docClient = (low: DynamoDBClient): DynamoDBDocumentClient =>
  DynamoDBDocumentClient.from(low, {
    marshallOptions: { removeUndefinedValues: true, convertClassInstanceToMap: false },
  });
