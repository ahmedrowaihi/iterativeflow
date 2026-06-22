import type { FlowDefinition, FlowNode } from "../builder/types";
import type { StandardSchemaV1 } from "../util/standard-schema";

export interface RegisteredFlow {
  name: string;
  version: number;
  run: FlowDefinition<unknown, unknown>["body"];
  inputSchema?: FlowDefinition<unknown, unknown>["input"];
  nodes?: ReadonlyArray<FlowNode>;
  signalSchemas?: ReadonlyMap<string, StandardSchemaV1<unknown, unknown>>;
}

export class FlowRegistry {
  private readonly map = new Map<string, RegisteredFlow>();

  private key(name: string, version: number): string {
    return `${name}@${version}`;
  }

  register(def: RegisteredFlow): void {
    const k = this.key(def.name, def.version);
    if (this.map.has(k)) {
      throw new Error(`Flow ${k} is already registered`);
    }
    this.map.set(k, def);
  }

  get(name: string, version: number): RegisteredFlow | undefined {
    return this.map.get(this.key(name, version));
  }

  list(): ReadonlyArray<{ name: string; version: number }> {
    return [...this.map.values()].map(({ name, version }) => ({ name, version }));
  }

  /**
   * Resolve the signal schema declared on `(name, version)`'s flow, if any.
   * Used by the engine to validate signal payloads at delivery time.
   */
  signalSchema(
    name: string,
    version: number,
    signalName: string,
  ): StandardSchemaV1<unknown, unknown> | undefined {
    return this.map.get(this.key(name, version))?.signalSchemas?.get(signalName);
  }
}
