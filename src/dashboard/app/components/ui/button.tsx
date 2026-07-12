import { cva, type VariantProps } from "class-variance-authority";
import type { JSX } from "preact";
import { cn } from "@/lib/cn";

const button = cva("uk-btn", {
  variants: {
    variant: {
      default: "uk-btn-default",
      primary: "uk-btn-primary",
      secondary: "uk-btn-secondary",
      destructive: "uk-btn-destructive",
      ghost: "uk-btn-ghost",
    },
    size: { sm: "uk-btn-sm", xs: "uk-btn-sm px-2 text-xs" },
  },
  defaultVariants: { variant: "secondary", size: "sm" },
});

export type ButtonProps = JSX.IntrinsicElements["button"] & VariantProps<typeof button>;

export const Button = ({ variant, size, class: cls, children, ...rest }: ButtonProps) => (
  <button {...rest} class={cn(button({ variant, size }), cls)}>
    {children}
  </button>
);
