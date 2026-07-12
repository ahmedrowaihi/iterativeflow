import { fmtAbs, rel, shortId } from "@/lib/format";
import { runDuration } from "@/lib/run";
import { openRun } from "@/lib/store";
import type { RunListItem } from "@/lib/types";
import { RunActions } from "@/components/common/run-actions";
import { StatusBadge } from "@/components/common/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Td, Tr } from "@/components/ui/table";

export const RunRow = ({ run }: { run: RunListItem }) => (
  <Tr>
    <Td class="font-mono" title={run.id}>
      {shortId(run.id)}
    </Td>
    <Td>
      {run.name} <span class="text-muted-foreground">v{run.version}</span>
    </Td>
    <Td>
      <StatusBadge status={run.status} />
    </Td>
    <Td>{run.attempts}</Td>
    <Td>
      <div class="flex flex-wrap gap-1.5">
        {(run.tags ?? []).map((t) => (
          <Badge key={t} variant="secondary">
            {t}
          </Badge>
        ))}
      </div>
    </Td>
    <Td title={fmtAbs(run.createdAt)}>{rel(run.createdAt)}</Td>
    <Td>{runDuration(run)}</Td>
    <Td>
      <div class="flex gap-1.5">
        <Button variant="default" size="xs" onClick={() => openRun(run.id)}>
          View
        </Button>
        <RunActions run={run} />
      </div>
    </Td>
  </Tr>
);
