import type { Logger } from "./types";

export const silentLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

export const baseRunnerDeps = () => ({
  logger: silentLogger,
  maxRunAttempts: 100,
  maxInvokeDepth: 10,
  maxChildrenPerRun: 1000,
  abortSignal: new AbortController().signal,
});

export const baseContextDeps = () => ({
  logger: silentLogger,
  maxInvokeDepth: 10,
  maxChildrenPerRun: 1000,
  abortSignal: new AbortController().signal,
});
