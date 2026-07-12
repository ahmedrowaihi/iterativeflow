import type { JSX } from "preact";
import { cn } from "@/lib/cn";

export const Input = ({ class: cls, ...rest }: JSX.IntrinsicElements["input"]) => (
  <input {...rest} class={cn("uk-input uk-form-sm", cls)} />
);

export const Select = ({ class: cls, children, ...rest }: JSX.IntrinsicElements["select"]) => (
  <select {...rest} class={cn("uk-select uk-form-sm", cls)}>
    {children}
  </select>
);

export const Textarea = ({ class: cls, ...rest }: JSX.IntrinsicElements["textarea"]) => (
  <textarea {...rest} class={cn("uk-textarea uk-form-sm font-mono", cls)} />
);
