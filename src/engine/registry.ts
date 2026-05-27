import type { FlowDefinition, FlowNode } from "../builder/types";

export interface RegisteredWorkflow {
  name: string;
  version: number;
  run: FlowDefinition<unknown, unknown>["run"];
  inputSchema?: FlowDefinition<unknown, unknown>["input"];
  nodes?: ReadonlyArray<FlowNode>;
}

export class WorkflowRegistry {
  private readonly map = new Map<string, RegisteredWorkflow>();

  private key(name: string, version: number): string {
    return `${name}@${version}`;
  }

  register(def: RegisteredWorkflow): void {
    const k = this.key(def.name, def.version);
    if (this.map.has(k)) {
      throw new Error(`Workflow ${k} is already registered`);
    }
    this.map.set(k, def);
  }

  get(name: string, version: number): RegisteredWorkflow | undefined {
    return this.map.get(this.key(name, version));
  }

  list(): RegisteredWorkflow[] {
    return Array.from(this.map.values());
  }
}
