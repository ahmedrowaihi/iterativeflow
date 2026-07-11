import type { ComponentChildren, JSX } from "preact";
import { cn } from "@/lib/cn";
import { Table, TableWrap } from "@/components/ui/table";

export const Card = ({ children }: { children: ComponentChildren }) => (
  <div class="uk-card">{children}</div>
);

export const CardHeader = ({
  class: cls,
  children,
}: {
  class?: string;
  children: ComponentChildren;
}) => <div class={cn("uk-card-header", cls)}>{children}</div>;

export const CardTitle = ({ children }: { children: ComponentChildren }) => (
  <h3 class="uk-card-title">{children}</h3>
);

export const CardBody = ({ children }: { children: ComponentChildren }) => (
  <div class="uk-card-body">{children}</div>
);

export const CardFooter = ({ children }: { children: ComponentChildren }) => (
  <div class="uk-card-footer">{children}</div>
);

const SectionHeader = ({
  title,
  count,
  actions,
}: {
  title: string;
  count?: number;
  actions?: ComponentChildren;
}) => (
  <CardHeader class="flex items-center gap-2">
    <CardTitle>
      {title}
      {count != null ? <span class="ml-1.5 normal-case text-muted-foreground">{count}</span> : null}
    </CardTitle>
    {actions ? (
      <>
        <span class="flex-1" />
        {actions}
      </>
    ) : null}
  </CardHeader>
);

export const Section = ({
  title,
  count,
  actions,
  children,
}: {
  title: string;
  count?: number;
  actions?: ComponentChildren;
  children: ComponentChildren;
}) => (
  <Card>
    <SectionHeader title={title} count={count} actions={actions} />
    <CardBody>{children}</CardBody>
  </Card>
);

export const TableSection = ({
  title,
  count,
  actions,
  footer,
  children,
}: {
  title: string;
  count?: number;
  actions?: ComponentChildren;
  footer?: ComponentChildren;
  children: JSX.Element | JSX.Element[];
}) => (
  <Card>
    <SectionHeader title={title} count={count} actions={actions} />
    <TableWrap>
      <Table>{children}</Table>
    </TableWrap>
    {footer ? <CardFooter>{footer}</CardFooter> : null}
  </Card>
);
