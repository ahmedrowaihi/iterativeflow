import type { ComponentChildren } from "preact";
import type { RunListItem } from "@/lib/types";
import { RunRow } from "@/components/runs/run-row";
import { EmptyRow } from "@/components/ui/empty";
import { Tbody, Th, Thead, Tr } from "@/components/ui/table";

export const RunsTable = ({ runs, empty }: { runs: RunListItem[]; empty: ComponentChildren }) => (
  <>
    <Thead>
      <Tr>
        <Th>Run</Th>
        <Th>Flow</Th>
        <Th>Status</Th>
        <Th>Attempts</Th>
        <Th>Tags</Th>
        <Th>Created</Th>
        <Th>Duration</Th>
        <Th>Actions</Th>
      </Tr>
    </Thead>
    <Tbody>
      {runs.length ? (
        runs.map((run) => <RunRow key={run.id} run={run} />)
      ) : (
        <EmptyRow colSpan={8}>{empty}</EmptyRow>
      )}
    </Tbody>
  </>
);
