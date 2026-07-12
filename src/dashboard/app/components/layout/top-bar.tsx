import { mutate } from "swr";
import { autoRefresh, setAutoRefresh } from "@/lib/store";
import { HealthStrip } from "@/components/common/health-strip";
import { ThemeToggle } from "@/components/layout/theme-toggle";
import { Button } from "@/components/ui/button";

export const TopBar = () => (
  <header class="flex items-center gap-3 border-b border-border px-4 py-2.5">
    <HealthStrip />
    <span class="flex-1" />
    <label class="flex cursor-pointer items-center gap-2 text-xs text-muted-foreground select-none">
      <input
        class="uk-checkbox"
        type="checkbox"
        checked={autoRefresh.value}
        onChange={(e) => setAutoRefresh(e.currentTarget.checked)}
      />
      auto-refresh
    </label>
    <ThemeToggle />
    <Button onClick={() => mutate(() => true)} title="Refresh now">
      Refresh
    </Button>
  </header>
);
