import { useCrons } from "@/lib/queries";
import { CronRow } from "@/components/crons/cron-row";
import { TableSection } from "@/components/ui/card";
import { EmptyRow } from "@/components/ui/empty";
import { Tbody, Th, Thead, Tr } from "@/components/ui/table";

export const CronsPage = (_props: { path?: string }) => {
  const { data, error } = useCrons();
  const cs = data?.crons ?? [];
  return (
    <TableSection
      title="Crons"
      count={cs.length}
      footer={
        <span>
          Triggering runs the task directly, bypassing the engine overlap lock — it can run
          alongside a scheduled fire even when overlap is skip.
        </span>
      }
    >
      <Thead>
        <Tr>
          <Th>Name</Th>
          <Th>Schedule</Th>
          <Th>Timezone</Th>
          <Th>Overlap</Th>
          <Th>Actions</Th>
        </Tr>
      </Thead>
      <Tbody>
        {error ? (
          <EmptyRow colSpan={5}>Failed to load.</EmptyRow>
        ) : cs.length ? (
          cs.map((c) => <CronRow key={c.name} cron={c} />)
        ) : (
          <EmptyRow colSpan={5}>{data ? "No crons registered." : "Loading…"}</EmptyRow>
        )}
      </Tbody>
    </TableSection>
  );
};
