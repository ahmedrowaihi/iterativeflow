import type { JSX } from "preact";
import { cn } from "@/lib/cn";

export const TableWrap = ({ children }: { children: JSX.Element }) => (
  <div class="overflow-x-auto">{children}</div>
);

export const Table = ({ children }: { children: JSX.Element | JSX.Element[] }) => (
  <table class="uk-table uk-table-divider uk-table-hover uk-table-sm uk-table-middle mb-0">
    {children}
  </table>
);

export const Thead = ({ children }: { children: JSX.Element }) => <thead>{children}</thead>;

export const Tbody = ({ children }: { children: JSX.Element | JSX.Element[] }) => (
  <tbody>{children}</tbody>
);

export type TrProps = JSX.IntrinsicElements["tr"] & {
  interactive?: boolean;
};
export const Tr = ({ interactive, class: cls, children, ...rest }: TrProps) => (
  <tr {...rest} class={cn(interactive && "cursor-pointer", cls)}>
    {children}
  </tr>
);

export const Th = ({ class: cls, children, ...rest }: JSX.IntrinsicElements["th"]) => (
  <th {...rest} class={cn("whitespace-nowrap", cls)}>
    {children}
  </th>
);

export const Td = ({ class: cls, children, ...rest }: JSX.IntrinsicElements["td"]) => (
  <td {...rest} class={cn("whitespace-nowrap", cls)}>
    {children}
  </td>
);
