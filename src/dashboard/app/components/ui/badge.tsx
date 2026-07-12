import { cva, type VariantProps } from "class-variance-authority";
import type { JSX } from "preact";
import { cn } from "@/lib/cn";

const badge = cva("uk-badge", {
  variants: {
    variant: { default: "", secondary: "uk-badge-secondary", status: "badge-status" },
  },
  defaultVariants: { variant: "default" },
});

export type BadgeProps = JSX.IntrinsicElements["span"] & VariantProps<typeof badge>;

export const Badge = ({ variant, class: cls, children, ...rest }: BadgeProps) => (
  <span {...rest} class={cn(badge({ variant }), cls)}>
    {children}
  </span>
);
