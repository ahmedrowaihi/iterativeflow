import { cn } from "@/lib/cn";
import { collapsed, toggleSidebar } from "@/lib/sidebar";
import { NavItem } from "@/components/layout/nav-item";
import { Button } from "@/components/ui/button";
import { Clock, Grid, List, PanelLeft } from "@/components/ui/icon";

export const Sidebar = () => (
  <aside
    class={cn(
      "flex shrink-0 flex-col gap-4 border-r border-border bg-card p-3 transition-[width]",
      collapsed.value ? "w-14" : "w-52",
    )}
  >
    <div class={cn("flex items-center", collapsed.value ? "justify-center" : "gap-2")}>
      {collapsed.value ? null : (
        <span class="flex-1 truncate px-2 text-sm font-semibold">iterativeflow</span>
      )}
      <Button
        variant="ghost"
        size="xs"
        onClick={toggleSidebar}
        title={collapsed.value ? "Expand sidebar" : "Collapse sidebar"}
      >
        <PanelLeft />
      </Button>
    </div>
    <ul class="uk-nav uk-nav-default">
      <NavItem to="/" label="Overview" icon={Grid} />
      <NavItem to="/runs" label="Runs" icon={List} />
      <NavItem to="/crons" label="Crons" icon={Clock} />
    </ul>
  </aside>
);
