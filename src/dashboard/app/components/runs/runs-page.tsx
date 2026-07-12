import { filters } from "@/lib/store";
import { RunsFilters } from "@/components/runs/runs-filters";
import { RunsList } from "@/components/runs/runs-list";

export const RunsPage = (_props: { path?: string }) => (
  <div class="flex flex-col gap-3">
    <RunsFilters />
    <RunsList filters={filters.value} />
  </div>
);
