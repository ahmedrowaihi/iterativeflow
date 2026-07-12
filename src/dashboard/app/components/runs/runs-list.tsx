import { useRunsInfinite } from "@/lib/queries";
import type { Filters } from "@/lib/types";
import { RunsTable } from "@/components/runs/runs-table";
import { Button } from "@/components/ui/button";
import { TableSection } from "@/components/ui/card";

export const RunsList = ({
  filters,
  title = "Runs",
  emptyLabel = "No runs match.",
}: {
  filters: Filters;
  title?: string;
  emptyLabel?: string;
}) => {
  const { data, error, size, setSize, isValidating } = useRunsInfinite(filters);
  const rows = data ? data.flatMap((p) => p.runs) : [];
  const hasMore = !!data?.[data.length - 1]?.next;
  const footer = hasMore ? (
    <>
      <span class="flex-1" />
      <Button size="xs" disabled={isValidating} onClick={() => setSize(size + 1)}>
        Load more
      </Button>
    </>
  ) : undefined;
  return (
    <TableSection title={title} count={rows.length} footer={footer}>
      <RunsTable runs={rows} empty={error ? "Failed to load." : data ? emptyLabel : "Loading…"} />
    </TableSection>
  );
};
