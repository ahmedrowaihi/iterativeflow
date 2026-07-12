import type { ComponentChildren } from "preact";
import { Empty } from "@/components/ui/empty";

export function QueryView<T>({
  data,
  error,
  children,
}: {
  data: T | undefined;
  error?: unknown;
  children: (data: T) => ComponentChildren;
}) {
  if (error) return <Empty>Failed to load.</Empty>;
  if (data === undefined) return <Empty>Loading…</Empty>;
  return <>{children(data)}</>;
}
