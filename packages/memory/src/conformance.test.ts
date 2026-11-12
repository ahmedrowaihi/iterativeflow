import { backendConformance } from "@iterativeflow/conformance";
import { createMemoryBackend } from "#index";

backendConformance("memory", () => createMemoryBackend(), { concurrentWriters: false });
