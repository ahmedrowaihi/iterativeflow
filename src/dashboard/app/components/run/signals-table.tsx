import { fmtAbs, rel } from "@/lib/format";
import { openSignal } from "@/lib/store";
import type { SignalRow } from "@/lib/types";
import { JsonViewButton } from "@/components/common/json-view-button";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { TableSection } from "@/components/ui/card";
import { Tbody, Td, Th, Thead, Tr } from "@/components/ui/table";

export const SignalsTable = ({ runId, signals }: { runId: string; signals: SignalRow[] }) => (
  <TableSection title="Signals" count={signals.length}>
    <Thead>
      <Tr>
        <Th>Name</Th>
        <Th>State</Th>
        <Th>Armed</Th>
        <Th>Delivered</Th>
        <Th>Expires</Th>
        <Th>Payload</Th>
        <Th>Action</Th>
      </Tr>
    </Thead>
    <Tbody>
      {signals.map((s) => (
        <Tr>
          <Td class="font-mono" title={s.cursorKey}>
            {s.name}
          </Td>
          <Td>
            {s.delivered ? (
              <StatusBadge status="done" label="delivered" />
            ) : (
              <span class="text-muted-foreground">waiting</span>
            )}
          </Td>
          <Td title={fmtAbs(s.createdAt)}>{rel(s.createdAt)}</Td>
          <Td>{s.deliveredAt ? rel(s.deliveredAt) : "—"}</Td>
          <Td>{s.expiresAt ? rel(s.expiresAt) : "—"}</Td>
          <Td>
            {s.payload ? (
              <JsonViewButton title={`${s.name} payload`} capped={s.payload} />
            ) : (
              <span class="text-muted-foreground">—</span>
            )}
          </Td>
          <Td>
            {s.delivered ? (
              <span class="text-muted-foreground">—</span>
            ) : (
              <Button size="xs" onClick={() => openSignal(runId, s.name)}>
                Send
              </Button>
            )}
          </Td>
        </Tr>
      ))}
    </Tbody>
  </TableSection>
);
