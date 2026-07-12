import type { StepRow as StepData } from "@/lib/types";
import { StepRow } from "@/components/run/step-row";
import { TableSection } from "@/components/ui/card";
import { EmptyRow } from "@/components/ui/empty";
import { Tbody, Th, Thead, Tr } from "@/components/ui/table";

export const StepsTable = ({ steps }: { steps: StepData[] }) => (
  <TableSection title="Steps" count={steps.length}>
    <Thead>
      <Tr>
        <Th>Cursor key</Th>
        <Th>Status</Th>
        <Th>Attempts</Th>
        <Th>Started</Th>
        <Th>Duration</Th>
        <Th />
      </Tr>
    </Thead>
    <Tbody>
      {steps.length ? (
        steps.map((s) => <StepRow key={s.cursorKey} step={s} />)
      ) : (
        <EmptyRow colSpan={6}>No steps yet.</EmptyRow>
      )}
    </Tbody>
  </TableSection>
);
