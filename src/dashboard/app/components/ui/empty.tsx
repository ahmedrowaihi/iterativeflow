import type { ComponentChildren } from "preact";
import { Td, Tr } from "@/components/ui/table";

export const Empty = ({ children }: { children: ComponentChildren }) => (
  <div class="flex justify-center py-8 text-xs text-muted-foreground">{children}</div>
);

export const EmptyRow = ({
  colSpan,
  children,
}: {
  colSpan: number;
  children: ComponentChildren;
}) => (
  <Tr>
    <Td colSpan={colSpan}>
      <Empty>{children}</Empty>
    </Td>
  </Tr>
);
