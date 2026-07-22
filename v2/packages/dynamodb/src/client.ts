import type { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

/**
 * The minimal send-surface the backend drives. The concrete {@link DynamoDBDocumentClient}
 * satisfies it, and so does any wrapper (e.g. a fault-injecting proxy in tests) — the backend
 * never depends on the client class, only on `send`.
 */
export interface Doc {
  send(command: unknown): Promise<unknown>;
}

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
