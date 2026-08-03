/**
 * @packageDocumentation
 * The inbound webhook edge for iterativeflow: verify a signed provider webhook and deliver it as a
 * durable signal a parked flow can `await`. Provider-agnostic — a pluggable verifier + presets
 * ({@link github}) — so a flow can park on an external event (a `/qa run` comment, a preview deploy,
 * a payment, a human approval) and resume crash-safe when it arrives.
 */

export {
  hmacVerifier,
  WebhookError,
  type HeaderInput,
  type HmacVerifierOptions,
  type VerifyInput,
  type WebhookErrorCode,
  type WebhookEvent,
  type WebhookVerifier,
} from "#verify";

export { github } from "#github";

export {
  webhookSignalBridge,
  type BridgeResult,
  type Correlate,
  type SignalDelivery,
  type WebhookSignalBridge,
  type WebhookSignalBridgeOptions,
} from "#bridge";
