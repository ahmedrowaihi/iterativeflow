import { fmtAbs, rel } from "@/lib/format";
import type { TimerRow } from "@/lib/types";
import { TableSection } from "@/components/ui/card";
import { Tbody, Td, Th, Thead, Tr } from "@/components/ui/table";

export const SleepsTable = ({ timers }: { timers: TimerRow[] }) => (
  <TableSection title="Sleeps" count={timers.length}>
    <Thead>
      <Tr>
        <Th>Cursor key</Th>
        <Th>Fires</Th>
        <Th>Fired</Th>
      </Tr>
    </Thead>
    <Tbody>
      {timers.map((t) => (
        <Tr>
          <Td class="font-mono">{t.cursorKey}</Td>
          <Td title={fmtAbs(t.fireAt)}>{rel(t.fireAt)}</Td>
          <Td>{t.firedAt ? rel(t.firedAt) : <span class="text-muted-foreground">pending</span>}</Td>
        </Tr>
      ))}
    </Tbody>
  </TableSection>
);
