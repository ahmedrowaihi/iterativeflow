import { describe, expect, it } from "vitest";
import { z } from "zod";
import type { Engine } from "../engine/engine";
import { defineContract } from "./contract";
import { flow } from "./flow";

const cloneContract = defineContract<{ mediaId: string }, { status: "done" | "disabled" }>({
  name: "clone-media",
  version: 1,
  input: z.object({ mediaId: z.string() }),
});

describe("flow contract", () => {
  it("defineContract carries name/version/input verbatim", () => {
    expect(cloneContract.name).toBe("clone-media");
    expect(cloneContract.version).toBe(1);
    expect(cloneContract.input).toBeDefined();
  });

  it("flow(contract) seeds name/version and shares the one input schema", () => {
    const def = flow(cloneContract)
      .step("copy", ({ input }) => ({ id: input.mediaId }))
      .output(() => ({ status: "done" as const }))
      .build();
    expect(def.name).toBe("clone-media");
    expect(def.version).toBe(1);
    // Same schema object — enqueue side and worker side validate against one source.
    expect(def.input).toBe(cloneContract.input);
  });

  // Type-level contract — never executed. The typecheck gate covers test files,
  // so the expect-error markers below fail the build if the types stop catching.
  it("typechecks: input shape, output constraint, and step input typing", () => {
    const types = (engine: Engine) => {
      void engine.enqueueHandle(cloneContract).start({ mediaId: "m1" });
      // @ts-expect-error — wrong input shape
      void engine.enqueueHandle(cloneContract).start({ wrong: true });
      // @ts-expect-error — missing required field
      void engine.enqueueHandle(cloneContract).start({});

      void flow(cloneContract).output(() => ({ status: "done" as const }));
      // @ts-expect-error — output not assignable to the contract's { status: "done" | "disabled" }
      void flow(cloneContract).output(() => ({ status: "nope" as const }));

      void flow(cloneContract).step("s", ({ input }) => input.mediaId);
      // @ts-expect-error — `nope` does not exist on the contract input { mediaId: string }
      void flow(cloneContract).step("s", ({ input }) => input.nope);
    };
    expect(typeof types).toBe("function");
  });
});
