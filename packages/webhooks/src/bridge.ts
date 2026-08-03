import { type Backend, signalRun } from "@iterativeflow/core";
import type { HeaderInput, WebhookEvent, WebhookVerifier } from "#verify";

/**
 * One signal to deliver to one parked run. A single webhook can fan out to several — e.g. a PR
 * `synchronize` event superseding every in-flight run for that PR delivers one `cancel` signal per
 * run. Return an empty array to ignore a webhook.
 */
export interface SignalDelivery {
  runId: string;
  name: string;
  payload?: unknown;
  /**
   * Idempotency key for this delivery. Defaults to `${event.id}:${runId}:${name}`, so a provider
   * redelivery of the same event lands once. Override only to dedupe across distinct deliveries.
   */
  idempotencyKey?: string;
}

/** Maps a verified webhook to the runs it should wake. The correlation (PR → runId, comment →
 *  runId) is application state the bridge can't know, so the app supplies it. */
export type Correlate = (event: WebhookEvent) => SignalDelivery[] | Promise<SignalDelivery[]>;

export interface WebhookSignalBridgeOptions {
  /** Provider verifier — a preset (e.g. `github(secret)`) or a custom {@link WebhookVerifier}. */
  verify: WebhookVerifier;
  correlate: Correlate;
  /** Web Crypto implementation, forwarded to the verifier; defaults to `globalThis.crypto`. */
  crypto?: Crypto;
}

export interface BridgeResult {
  type: string;
  id: string;
  deliveries: Array<{ runId: string; name: string; delivered: boolean }>;
}

/** A verified-and-delivered webhook handler — the return of {@link webhookSignalBridge}. */
export type WebhookSignalBridge = (input: {
  body: string;
  headers: HeaderInput;
}) => Promise<BridgeResult>;

/**
 * Bridge signed webhooks to durable signals: verify the payload, correlate the event to the parked
 * runs it concerns, and deliver each as a signal (atomic delivery + re-enqueue, best-effort wakeup,
 * idempotent on the delivery id). This is the inbound edge that lets a flow `await ctx.signal(...)`
 * on an external event — a `/qa run` comment, a preview deploy, a payment, a human approval.
 *
 * @throws {import("#verify").WebhookError} when verification fails — reject the request as untrusted.
 */
export function webhookSignalBridge(
  backend: Backend,
  opts: WebhookSignalBridgeOptions,
): WebhookSignalBridge {
  return async ({ body, headers }) => {
    const event = await opts.verify({ body, headers }, opts.crypto);
    const targets = await opts.correlate(event);
    const deliveries = await Promise.all(
      targets.map(async (t) => ({
        runId: t.runId,
        name: t.name,
        delivered: await signalRun(backend, t.runId, t.name, t.payload, {
          idempotencyKey: t.idempotencyKey ?? `${event.id}:${t.runId}:${t.name}`,
        }),
      })),
    );
    return { type: event.type, id: event.id, deliveries };
  };
}
