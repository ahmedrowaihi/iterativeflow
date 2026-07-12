import { clearFilters, filters, setFilter } from "@/lib/store";
import { type Filters, RUN_STATUSES } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Input, Select } from "@/components/ui/input";

export const RunsFilters = () => {
  const f = filters.value;
  return (
    <div data-filters class="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
      <Input
        class="w-full"
        placeholder="flow name"
        value={f.name}
        onChange={(e) => setFilter({ name: e.currentTarget.value.trim() })}
      />
      <Select
        class="w-full"
        value={f.status}
        onChange={(e) => setFilter({ status: e.currentTarget.value as Filters["status"] })}
      >
        <option value="">any status</option>
        {RUN_STATUSES.map((s) => (
          <option key={s} value={s}>
            {s.replace(/_/g, " ")}
          </option>
        ))}
      </Select>
      <Input
        class="w-full"
        placeholder="tag"
        value={f.tag}
        onChange={(e) => setFilter({ tag: e.currentTarget.value.trim() })}
      />
      <Input
        class="w-full"
        type="date"
        title="created since"
        value={f.since}
        onChange={(e) => setFilter({ since: e.currentTarget.value })}
      />
      <Input
        class="w-full"
        type="date"
        title="created until"
        value={f.until}
        onChange={(e) => setFilter({ until: e.currentTarget.value })}
      />
      <Button size="xs" onClick={clearFilters}>
        Clear
      </Button>
    </div>
  );
};
