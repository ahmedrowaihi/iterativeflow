import type { ComponentType } from "preact";
import { cn } from "@/lib/cn";
import { navigate, path } from "@/lib/router";
import { collapsed } from "@/lib/sidebar";

export const NavItem = ({
  to,
  label,
  icon: Icon,
}: {
  to: string;
  label: string;
  icon: ComponentType<{ class?: string }>;
}) => (
  <li class={cn(path.value === to && "uk-active")}>
    <a
      href={`#${to}`}
      title={label}
      class={cn("flex items-center gap-2", collapsed.value && "justify-center")}
      onClick={(e) => {
        e.preventDefault();
        navigate(to);
      }}
    >
      <Icon class="size-4" />
      {collapsed.value ? null : <span>{label}</span>}
    </a>
  </li>
);
