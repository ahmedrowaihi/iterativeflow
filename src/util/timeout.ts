/** @internal */
export const runWithTimeout = async <T>(
  fn: (signal: AbortSignal) => Promise<T> | T,
  timeoutMs: number | undefined,
  label: string,
  externalSignal: AbortSignal,
): Promise<T> => {
  const controller = new AbortController();
  const abortFromExternal = () => controller.abort(externalSignal.reason);
  if (externalSignal.aborted) abortFromExternal();
  else externalSignal.addEventListener("abort", abortFromExternal, { once: true });

  const work = Promise.resolve().then(() => fn(controller.signal));

  if (!timeoutMs || timeoutMs <= 0) {
    try {
      return await work;
    } finally {
      externalSignal.removeEventListener("abort", abortFromExternal);
    }
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const err = new Error(`${label} exceeded timeoutMs=${timeoutMs}`);
      controller.abort(err);
      reject(err);
    }, timeoutMs);
  });

  try {
    return await Promise.race([work, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
    externalSignal.removeEventListener("abort", abortFromExternal);
  }
};
