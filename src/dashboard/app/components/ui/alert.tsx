import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentChildren } from "preact";
import { cn } from "@/lib/cn";

const alert = cva("uk-alert", {
  variants: {
    variant: { default: "", destructive: "uk-alert-destructive" },
  },
  defaultVariants: { variant: "default" },
});

export type AlertProps = VariantProps<typeof alert> & {
  class?: string;
  children: ComponentChildren;
};

export const Alert = ({ variant, class: cls, children }: AlertProps) => (
  <div class={cn(alert({ variant }), cls)}>{children}</div>
);
