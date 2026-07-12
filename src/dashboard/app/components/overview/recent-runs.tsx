import type { RunListItem } from "@/lib/types";
import { RunsTable } from "@/components/runs/runs-table";
import { TableSection } from "@/components/ui/card";

export const RecentRuns = ({ runs }: { runs: RunListItem[] }) => {
  const recent = runs.slice(0, 6);
  return (
    <TableSection title="Recent runs" count={recent.length}>
      <RunsTable runs={recent} empty="No runs yet." />
    </TableSection>
  );
};
