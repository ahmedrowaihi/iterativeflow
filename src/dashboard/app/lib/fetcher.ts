const parse = async (res: Response) => {
  let body: unknown = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  if (!res.ok) {
    throw new Error(
      (body as { error?: string } | null)?.error ?? `${res.status} ${res.statusText}`,
    );
  }
  return body;
};

export const fetcher = <T>(path: string): Promise<T> => fetch(path).then(parse) as Promise<T>;

export const post = <T>(path: string, body?: unknown): Promise<T> =>
  fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body ?? {}),
  }).then(parse) as Promise<T>;
