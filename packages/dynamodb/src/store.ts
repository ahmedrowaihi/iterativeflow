import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  BatchGetCommand,
  BatchWriteCommand,
  type BatchWriteCommandInput,
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  ScanCommand,
  type ScanCommandInput,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  type CronRow,
  type DeliveredSignal,
  type IdGen,
  type RunSpec,
  type RunStatus,
  type StartResult,
  type StepOutcome,
  type OrphanView,
  type Outbox,
  type PurgeFilter,
  type RunFilter,
  type Store,
  type SuspendStatus,
  TERMINAL_STATUSES,
  ACTIVE_STATUSES,
  isOrphaned,
  purgeMatcher,
  runSetStatuses,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { countQuery } from "#count";
import { storedPriorities } from "#run-names";
import {
  type CronItem,
  type DocItem,
  type RunItem,
  dec,
  enc,
  mapRun,
  mapStep,
  nextSeq,
  parseCron,
  parseJob,
  parseRun,
  parseStep,
  parseTimer,
  runStatusOf,
  str,
} from "#codec";
import { CRON_DUE_GSI_PK, RUN_GSI2_PK, childGsiPk, key, pad } from "#schema";
import {
  MAX_TX_ITEMS,
  type TxItem,
  buildRunItem,
  cancellationReasons,
  conditionFailedAt,
  encodeSignalId,
  enqueueParams,
  outboxParts,
  spawnTx,
} from "#statements";

type WriteRequests = NonNullable<BatchWriteCommandInput["RequestItems"]>[string];

const TERMINAL_VALUES: Record<string, string> = Object.fromEntries(
  TERMINAL_STATUSES.map((s, i) => [`:t${i}`, s]),
);
const NOT_TERMINAL = `NOT (#status IN (${Object.keys(TERMINAL_VALUES).join(", ")}))`;

const mapCronItem = (c: CronItem): CronRow => ({
  name: c.cronName,
  schedule: c.schedule,
  flowName: c.flowName,
  flowVersion: c.flowVersion,
  input: dec(c.cronInput),
  overlap: c.overlap,
  nextRunAt: new Date(c.nextRunAt),
  lastRunAt: c.lastRunAt === undefined ? undefined : new Date(c.lastRunAt),
});

/** @internal */
export const createDynamoStore = (doc: Doc, table: string, id: IdGen): Store => {
  // Strong reads on the base-table decision path: a stale read could replay against an outdated run
  // or a missed step memo. GSI reads (claim/timer) can't be consistent and are CAS-guarded instead.
  const consistentGet = (Key: { pk: string; sk: string }): GetCommand =>
    new GetCommand({ TableName: table, Key, ConsistentRead: true });

  const getRun = async (runId: string): Promise<RunItem | undefined> => {
    const res = await doc.send(consistentGet(key.run(runId)));
    return res.Item && parseRun(res.Item);
  };

  const recover = async (markerRunId: string): Promise<StartResult> => {
    const existing = await getRun(markerRunId);
    if (!existing)
      throw new Error(`startRun: idempotency marker points at missing run ${markerRunId}`);
    return { runId: existing.id, created: false, status: existing.status };
  };

  const getStep = async (runId: string, cursorKey: string): Promise<StepOutcome | undefined> => {
    const res = await doc.send(consistentGet(key.step(runId, cursorKey)));
    return res.Item && mapStep(parseStep(res.Item));
  };

  const scanAll = async (params: ScanCommandInput, consistent = false): Promise<DocItem[]> => {
    const out: DocItem[] = [];
    let ExclusiveStartKey: ScanCommandInput["ExclusiveStartKey"];
    do {
      const res = await doc.send(
        new ScanCommand({ ...params, ExclusiveStartKey, ConsistentRead: consistent || undefined }),
      );
      out.push(...(res.Items ?? []));
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return out;
  };

  const queryAll = async (params: QueryCommandInput): Promise<DocItem[]> => {
    const out: DocItem[] = [];
    let ExclusiveStartKey: QueryCommandInput["ExclusiveStartKey"];
    do {
      const res = await doc.send(new QueryCommand({ ...params, ExclusiveStartKey }));
      out.push(...(res.Items ?? []));
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return out;
  };

  const matchingRuns = async (
    filter: RunFilter,
    allowed: readonly RunStatus[],
    op: string,
    limit: number,
  ): Promise<RunItem[]> => {
    const statuses = new Set<string>(runSetStatuses(filter, allowed, op));
    if (statuses.size === 0) return [];
    return (await scanRuns())
      .filter(
        (r) =>
          statuses.has(r.status) &&
          (filter.name === undefined || r.name === filter.name) &&
          (filter.version === undefined || r.version === filter.version) &&
          (filter.tag === undefined || (r.tags ?? []).includes(filter.tag)),
      )
      .sort((a, b) => a.seq - b.seq)
      .slice(0, limit);
  };

  const scanType = (type: string, consistent = false): Promise<DocItem[]> =>
    scanAll(
      {
        TableName: table,
        FilterExpression: "#type = :t",
        ExpressionAttributeNames: { "#type": "type" },
        ExpressionAttributeValues: { ":t": type },
      },
      consistent,
    );
  const scanRuns = async (): Promise<RunItem[]> => (await scanType("run")).map(parseRun);

  // A conditional idem-marker Put fails the transaction iff another creator already claimed the key,
  // which is exactly how a run dedups. Paired with the run Put in one transaction so the marker and
  // the run it points at commit together.
  const idemMarker = (spec: RunSpec, runId: string): TxItem => ({
    Put: {
      TableName: table,
      Item: { ...key.idem(spec.name, spec.version, spec.idempotencyKey!), type: "idem", runId },
      ConditionExpression: "attribute_not_exists(pk)",
    },
  });
  const runPut = (spec: RunSpec, runId: string): TxItem => ({
    Put: { TableName: table, Item: buildRunItem(spec, runId) },
  });
  const startItems = (spec: RunSpec, runId: string): TxItem[] =>
    spec.idempotencyKey ? [idemMarker(spec, runId), runPut(spec, runId)] : [runPut(spec, runId)];

  const startOne = async (spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    if (!spec.idempotencyKey) {
      await doc.send(new PutCommand({ TableName: table, Item: buildRunItem(spec, runId) }));
      return { runId, created: true, status: "pending" };
    }
    try {
      await doc.send(new TransactWriteCommand({ TransactItems: startItems(spec, runId) }));
      return { runId, created: true, status: "pending" };
    } catch (e) {
      if (!conditionFailedAt(cancellationReasons(e), 0)) throw e;
      const marker = await doc.send(
        consistentGet(key.idem(spec.name, spec.version, spec.idempotencyKey)),
      );
      if (!marker.Item)
        throw new Error(`startRun: idempotency marker missing for ${spec.idempotencyKey}`, {
          cause: e,
        });
      return recover(marker.Item.runId);
    }
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const matches = purgeMatcher(filter);
    const runs = (await scanRuns())
      .filter((r) =>
        matches({
          name: r.name,
          version: r.version,
          status: r.status,
          createdAt: r.createdAt ? new Date(r.createdAt) : undefined,
        }),
      )
      .sort((a, b) => a.seq - b.seq)
      .slice(0, limit);
    if (runs.length === 0) return 0;
    // A DeleteRequest for a missing key is a no-op, so JOB#/TIMER# can be pushed unconditionally.
    const partitions = await Promise.all(
      runs.map((r) =>
        queryAll({
          TableName: table,
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: { ":pk": key.runPk(r.id) },
        }),
      ),
    );
    const keys: { pk: string; sk: string }[] = [];
    runs.forEach((r, i) => {
      for (const it of partitions[i]) keys.push({ pk: it.pk, sk: it.sk });
      keys.push(key.job(r.id), key.timer(r.id));
    });
    // BatchWriteItem caps at 25/call and may return UnprocessedItems under throttle — drain them.
    let pending: WriteRequests = keys.map((Key) => ({ DeleteRequest: { Key } }));
    while (pending.length > 0) {
      const batch = pending.slice(0, 25);
      pending = pending.slice(25);
      const res = await doc.send(new BatchWriteCommand({ RequestItems: { [table]: batch } }));
      const left = res.UnprocessedItems?.[table];
      if (left?.length) pending.push(...left);
    }
    return runs.length;
  };

  // A transaction can't read, so the priority an enqueue inherits from its run is fetched first.
  const outbox = async (fx?: Outbox) =>
    outboxParts(table, fx, await storedPriorities(doc, table, fx?.enqueue ?? []));

  const retryOne = async (
    runId: string,
    priority: number | undefined,
  ): Promise<{ retried: boolean }> => {
    try {
      await doc.send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Update: {
                TableName: table,
                Key: key.run(runId),
                UpdateExpression: "SET #status = :pending, attempts = :zero REMOVE #error",
                ConditionExpression: "attribute_exists(pk) AND #status = :failed",
                ExpressionAttributeNames: { "#status": "status", "#error": "error" },
                ExpressionAttributeValues: {
                  ":pending": "pending",
                  ":failed": "failed",
                  ":zero": 0,
                },
              },
            },
            { Update: enqueueParams(table, runId, undefined, priority) },
          ],
        }),
      );
      return { retried: true };
    } catch (e) {
      if (conditionFailedAt(cancellationReasons(e), 0)) return { retried: false };
      throw e;
    }
  };

  const runPriority = async (runId: string): Promise<number | undefined> =>
    (await storedPriorities(doc, table, [{ runId }])).get(runId);

  const store: Store = {
    startRun: startOne,

    async startManyRuns(specs) {
      // Dedup by idempotency key first: a transaction can't hold two ops on one item.
      const slot: number[] = [];
      const uniq: { spec: RunSpec; runId: string; items: TxItem[] }[] = [];
      const seen = new Map<string, number>();
      for (const spec of specs) {
        const dedup = spec.idempotencyKey
          ? JSON.stringify([spec.name, spec.version, spec.idempotencyKey])
          : undefined;
        const prior = dedup === undefined ? undefined : seen.get(dedup);
        if (prior !== undefined) {
          slot.push(prior);
          continue;
        }
        const runId = id();
        const at = uniq.push({ spec, runId, items: startItems(spec, runId) }) - 1;
        if (dedup !== undefined) seen.set(dedup, at);
        slot.push(at);
      }
      // A raced chunk rolls back whole; redo it per-run so startOne's recover resolves the collision.
      const commit = async (chunk: typeof uniq): Promise<StartResult[]> => {
        try {
          await doc.send(
            new TransactWriteCommand({ TransactItems: chunk.flatMap((x) => x.items) }),
          );
          return chunk.map((x) => ({ runId: x.runId, created: true, status: "pending" as const }));
        } catch (e) {
          if (!cancellationReasons(e)?.some((r) => r?.Code === "ConditionalCheckFailed")) throw e;
          return Promise.all(chunk.map((x) => startOne(x.spec)));
        }
      };
      // Greedily fill a chunk up to the transaction budget, flushing before an item would overflow.
      const out: StartResult[] = [];
      let chunk: typeof uniq = [];
      let count = 0;
      for (const entry of uniq) {
        if (chunk.length > 0 && count + entry.items.length > MAX_TX_ITEMS) {
          out.push(...(await commit(chunk)));
          chunk = [];
          count = 0;
        }
        chunk.push(entry);
        count += entry.items.length;
      }
      if (chunk.length > 0) out.push(...(await commit(chunk)));
      const emitted = new Set<number>();
      return slot.map((s) => {
        if (emitted.has(s)) return { ...out[s], created: false };
        emitted.add(s);
        return out[s];
      });
    },

    async loadRun(runId) {
      const res = await doc.send(
        // Consistent: the replay read — a stale memo page would re-execute a committed step.
        new QueryCommand({
          TableName: table,
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: { ":pk": key.runPk(runId) },
          ConsistentRead: true,
        }),
      );
      const items = res.Items ?? [];
      const runItem = items.find((i) => i.type === "run");
      if (!runItem) return undefined;
      const steps = new Map<string, StepOutcome>();
      const signals: DeliveredSignal[] = [];
      for (const it of items) {
        if (it.type === "step") steps.set(it.cursorKey, mapStep(parseStep(it)));
        else if (it.type === "signal") {
          signals.push({
            id: encodeSignalId(it.pk, it.sk),
            name: it.name,
            payload: dec(it.payload),
          });
        }
      }
      return { run: mapRun(parseRun(runItem)), steps, signals };
    },

    async loadRunRow(runId) {
      const item = await getRun(runId);
      return item ? mapRun(item) : undefined;
    },

    async loadRunRows(runIds) {
      if (runIds.length === 0) return [];
      const byId = new Map<string, RunItem>();
      // BatchGetItem caps at 100 keys/call and may return UnprocessedKeys under throttle — chunk and drain.
      for (let i = 0; i < runIds.length; i += 100) {
        let keys: DocItem[] = runIds.slice(i, i + 100).map((rid) => key.run(rid));
        while (keys.length > 0) {
          const res = await doc.send(
            new BatchGetCommand({
              RequestItems: { [table]: { Keys: keys, ConsistentRead: true } },
            }),
          );
          for (const item of (res.Responses?.[table] ?? []).map(parseRun)) byId.set(item.id, item);
          keys = res.UnprocessedKeys?.[table]?.Keys ?? [];
        }
      }
      return runIds.map((rid) => {
        const item = byId.get(rid);
        return item ? mapRun(item) : undefined;
      });
    },

    async postSignal(runId, name, payload, opts) {
      const sigId = id();
      const seq = nextSeq();
      const signalPut: TxItem = {
        Put: {
          TableName: table,
          Item: {
            ...key.signal(runId, seq, sigId),
            type: "signal",
            runId,
            name,
            payload: enc(payload),
          },
        },
      };
      const enqueue: TxItem = {
        Update: enqueueParams(table, runId, undefined, await runPriority(runId)),
      };
      if (!opts?.idempotencyKey) {
        await doc.send(new TransactWriteCommand({ TransactItems: [signalPut, enqueue] }));
        return { delivered: true };
      }
      try {
        await doc.send(
          new TransactWriteCommand({
            TransactItems: [
              {
                Put: {
                  TableName: table,
                  Item: { ...key.sigIdem(runId, opts.idempotencyKey), type: "sigidem", runId },
                  ConditionExpression: "attribute_not_exists(pk)",
                },
              },
              signalPut,
              enqueue,
            ],
          }),
        );
        return { delivered: true };
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return { delivered: false };
        throw e;
      }
    },

    async markRunning(runId) {
      try {
        const res = await doc.send(
          new UpdateCommand({
            TableName: table,
            Key: key.run(runId),
            UpdateExpression: "SET #status = :running ADD attempts :one",
            ConditionExpression: `attribute_exists(pk) AND ${NOT_TERMINAL}`,
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: { ":running": "running", ":one": 1, ...TERMINAL_VALUES },
            ReturnValues: "UPDATED_NEW",
          }),
        );
        // `Attributes` is present because of `ReturnValues: "UPDATED_NEW"`; fall back to a read
        // rather than assert, so a shape change fails diagnosably instead of with a raw TypeError.
        const attempts = res.Attributes?.attempts;
        if (attempts !== undefined) return attempts;
        const fresh = await getRun(runId);
        if (!fresh) throw new Error(`markRunning: run ${runId} not found`);
        return fresh.attempts;
      } catch (e) {
        if (!(e instanceof ConditionalCheckFailedException)) throw e;
        const cur = await getRun(runId);
        if (!cur) throw new Error(`markRunning: run ${runId} not found`, { cause: e });
        return cur.attempts; // terminal — hand back attempts unchanged, do not resurrect
      }
    },

    async checkpointStep(c, fx) {
      const stepItem = {
        ...key.step(c.runId, c.cursorKey),
        type: "step",
        runId: c.runId,
        cursorKey: c.cursorKey,
        status: c.status,
        result: enc(c.result),
        error: enc(c.error),
        attempts: c.attempts,
        call: c.call,
      };
      const { nonSpawn, spawns } = await outbox(fx);
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      // ConditionCheck on the job version, riding the TransactWriteItems at index 2 (the catch below
      // matches that index). The job item is key-addressable, so this is atomic — no residual window.
      const versionGate: TxItem[] =
        fx?.requireVersion !== undefined
          ? [
              {
                ConditionCheck: {
                  TableName: table,
                  Key: key.job(c.runId),
                  ConditionExpression: "version = :rv",
                  ExpressionAttributeValues: { ":rv": fx.requireVersion },
                },
              },
            ]
          : [];
      // Step + effects commit in ONE TransactWriteItems. Core bounds the spawn batch (ctx.invoke
      // chunks fan-out), so a single checkpoint's items always fit the cap — assert it loudly if not.
      if (2 + versionGate.length + nonSpawn.length + inline.length > MAX_TX_ITEMS) {
        throw new Error(
          `checkpointStep: ${spawns.length} spawns + effects exceed the ${MAX_TX_ITEMS}-item transaction budget`,
        );
      }

      // The run gate proves the run exists (index 1 → run-not-found on ConditionalCheckFailed). When a
      // join is armed it doubles as the countdown SET — one op on the run item, since TransactWriteItems
      // forbids a second op on the same item.
      const runGate: TxItem =
        fx?.joinTarget !== undefined
          ? {
              Update: {
                TableName: table,
                Key: key.run(c.runId),
                UpdateExpression: "SET joinRemaining = :jt",
                ConditionExpression: "attribute_exists(pk)",
                ExpressionAttributeValues: { ":jt": fx.joinTarget.count },
              },
            }
          : {
              ConditionCheck: {
                TableName: table,
                Key: key.run(c.runId),
                ConditionExpression: "attribute_exists(pk)",
              },
            };
      const gate: TxItem[] = [
        {
          Put: {
            TableName: table,
            Item: stepItem,
            ConditionExpression: "attribute_not_exists(pk)",
          },
        },
        runGate,
        ...versionGate,
      ];

      try {
        await doc.send(
          new TransactWriteCommand({ TransactItems: [...gate, ...nonSpawn, ...inline] }),
        );
      } catch (e) {
        const reasons = cancellationReasons(e);
        if (!reasons) throw e;
        // The run-exists check failed but the step slot was free ⇒ unknown run.
        if (conditionFailedAt(reasons, 1) && !conditionFailedAt(reasons, 0)) {
          throw new Error(`checkpointStep: run ${c.runId} not found`, { cause: e });
        }
        // Version moved (index 2) with the step slot free — refuse; a present step (index 0) wins first.
        if (conditionFailedAt(reasons, 2) && !conditionFailedAt(reasons, 0)) {
          return { status: c.status, attempts: c.attempts, committed: false };
        }
        // Step already present ⇒ idempotent replay: skip the outbox, return the stored outcome.
        if (!conditionFailedAt(reasons, 0)) throw e;
      }
      const stored = await getStep(c.runId, c.cursorKey);
      if (!stored) throw new Error(`checkpointStep: step ${c.runId}/${c.cursorKey} vanished`);
      return stored;
    },

    async suspendRun(runId, status: SuspendStatus, fx) {
      const { nonSpawn, spawns } = await outbox(fx);
      const reset = status !== "retrying"; // forward progress resets the poison-pill cap
      const gate: TxItem = {
        Update: {
          TableName: table,
          Key: key.run(runId),
          UpdateExpression: `SET #status = :status${reset ? ", attempts = :zero" : ""}`,
          ConditionExpression: `attribute_exists(pk) AND ${NOT_TERMINAL}`,
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: reset
            ? { ":status": status, ":zero": 0, ...TERMINAL_VALUES }
            : { ":status": status, ...TERMINAL_VALUES },
        },
      };
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      try {
        await doc.send(new TransactWriteCommand({ TransactItems: [gate, ...nonSpawn, ...inline] }));
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return; // already terminal — no-op, outbox skipped
        throw e;
      }
    },

    async markTerminal(runId, outcome, fx) {
      const set = ["#status = :status"];
      const remove: string[] = [];
      const output = outcome.status === "done" ? enc(outcome.output) : undefined;
      const error = outcome.status === "done" ? undefined : enc(outcome.error);
      if (output === undefined) remove.push("#output");
      else set.push("#output = :output");
      if (error === undefined) remove.push("#error");
      else set.push("#error = :error");
      const values = {
        ":status": outcome.status,
        ...TERMINAL_VALUES,
        ...(output !== undefined && { ":output": output }),
        ...(error !== undefined && { ":error": error }),
      };
      const gate: TxItem = {
        Update: {
          TableName: table,
          Key: key.run(runId),
          UpdateExpression: `SET ${set.join(", ")}${remove.length ? ` REMOVE ${remove.join(", ")}` : ""}`,
          ConditionExpression: `attribute_exists(pk) AND ${NOT_TERMINAL}`,
          ExpressionAttributeNames: { "#status": "status", "#output": "output", "#error": "error" },
          ExpressionAttributeValues: values,
        },
      };
      const { nonSpawn, spawns } = await outbox(fx);
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      try {
        await doc.send(new TransactWriteCommand({ TransactItems: [gate, ...nonSpawn, ...inline] }));
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return; // already terminal — outbox skipped
        throw e;
      }
    },

    async arriveAtJoin(parentRunId) {
      // ADD…RETURN_VALUES: TransactWriteItems can't return the post-decrement value the wake decision
      // needs, so the decrement is its own atomic write (serializing concurrent siblings).
      try {
        const res = await doc.send(
          new UpdateCommand({
            TableName: table,
            Key: key.run(parentRunId),
            UpdateExpression: "ADD joinRemaining :neg1",
            ConditionExpression: "attribute_exists(pk)",
            ExpressionAttributeValues: { ":neg1": -1 },
            ReturnValues: "ALL_NEW",
          }),
        );
        return res.Attributes?.joinRemaining ?? 0;
      } catch (e) {
        if (e instanceof ConditionalCheckFailedException) return undefined; // parent gone
        throw e;
      }
    },

    async listRuns(filter, page) {
      const statuses = statusList(filter.status);
      const keep = (r: RunItem): boolean =>
        (!statuses || statuses.includes(r.status)) &&
        (!filter.name || r.name === filter.name) &&
        (filter.version === undefined || r.version === filter.version) &&
        (!filter.tag || (r.tags?.includes(filter.tag) ?? false));
      // gsi2 RUN partition, descending seq. With no filter this reads one page; a selective filter
      // may walk a few index pages, but never the whole table.
      // The cursor is the last row's gsi2sk, so paging resumes at `gsi2sk < :cur` regardless of how
      // that key is encoded — it stays an opaque token the caller never inspects.
      const rows: RunItem[] = [];
      let lastSortKey: string | undefined;
      let startKey: QueryCommandInput["ExclusiveStartKey"];
      do {
        const res = await doc.send(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi2",
            KeyConditionExpression: page.cursor ? "gsi2pk = :rp AND gsi2sk < :cur" : "gsi2pk = :rp",
            ExpressionAttributeValues: page.cursor
              ? { ":rp": RUN_GSI2_PK, ":cur": page.cursor }
              : { ":rp": RUN_GSI2_PK },
            ScanIndexForward: false,
            ExclusiveStartKey: startKey,
          }),
        );
        for (const item of res.Items ?? []) {
          const r = parseRun(item);
          if (keep(r)) {
            rows.push(r);
            lastSortKey = str(item, "gsi2sk");
          }
          if (rows.length === page.limit) break;
        }
        startKey = rows.length < page.limit ? res.LastEvaluatedKey : undefined;
      } while (startKey);
      const cursor = rows.length === page.limit ? lastSortKey : undefined;
      return { runs: rows.map(mapRun), cursor };
    },

    async cancelRuns(filter, limit) {
      const victims = await matchingRuns(filter, ACTIVE_STATUSES, "cancelRuns", limit);
      for (const r of victims) {
        await store.markTerminal(r.id, { status: "canceled" }, { cancelTimers: [r.id] });
      }
      return victims.length;
    },

    async retryRuns(filter, limit) {
      const victims = await matchingRuns(filter, ["failed"], "retryRuns", limit);
      let n = 0;
      for (const r of victims) if ((await retryOne(r.id, r.priority)).retried) n += 1;
      return n;
    },

    async childrenOf(runId) {
      // gsi1 parent-partition Query (eventually consistent). The cancel cascade tolerates GSI lag:
      // a just-spawned child the cascade misses self-cancels on dispatch and is re-driven by
      // reconcile. Both backstops are covered by engineConformance.
      const items = await queryAll({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :cp",
        ExpressionAttributeValues: { ":cp": childGsiPk(runId) },
      });
      return items.map(parseRun).map(mapRun);
    },

    async runStats() {
      const stats = zeroRunStats();
      // gsi2 RUN partition, projecting only status — a Query over the index, not a full-item Scan.
      const rows = await queryAll({
        TableName: table,
        IndexName: "gsi2",
        KeyConditionExpression: "gsi2pk = :rp",
        ExpressionAttributeValues: { ":rp": RUN_GSI2_PK },
        ProjectionExpression: "#s",
        ExpressionAttributeNames: { "#s": "status" },
      });
      for (const r of rows) stats[runStatusOf(r)] += 1;
      return stats;
    },

    async orphanedRuns(limit) {
      const [runs, jobItems, timerItems] = await Promise.all([
        scanRuns(),
        scanType("job"),
        scanType("timer"),
      ]);
      const jobs = new Set(jobItems.map((j) => parseJob(j).runId));
      const timers = new Set(timerItems.map((t) => parseTimer(t).runId));
      const byId = new Map(runs.map((r) => [r.id, r]));
      const view: OrphanView = {
        hasJob: (runId) => jobs.has(runId),
        hasTimer: (runId) => timers.has(runId),
        childrenOf: (runId) => runs.filter((c) => c.parentRunId === runId),
        runById: (runId) => byId.get(runId),
      };
      return runs
        .filter((r) => isOrphaned(r, view))
        .sort((a, b) => a.seq - b.seq)
        .slice(0, limit)
        .map((r) => r.id);
    },

    deleteRuns,

    deleteRunsOlderThan: (before, limit) => deleteRuns({ before }, limit),

    async retryRun(runId) {
      return retryOne(runId, await runPriority(runId));
    },

    async upsertCron(spec) {
      // `if_not_exists` can't express "unless the schedule changed", so read it first — this runs at
      // registration, not on the hot path.
      const res = await doc.send(new GetCommand({ TableName: table, Key: key.cron(spec.name) }));
      const prev = res.Item && parseCron(res.Item);
      const nextRunAt =
        prev !== undefined && prev.schedule === spec.schedule
          ? prev.nextRunAt
          : spec.nextRunAt.getTime();
      await doc.send(
        new UpdateCommand({
          TableName: table,
          Key: key.cron(spec.name),
          UpdateExpression:
            "SET #type = :type, cronName = :name, schedule = :schedule, flowName = :flowName, " +
            "flowVersion = :flowVersion, cronInput = :input, overlap = :overlap, gsi1pk = :gpk, " +
            "nextRunAt = :nextRunAt, gsi1sk = :gsk",
          ExpressionAttributeNames: { "#type": "type" },
          ExpressionAttributeValues: {
            ":type": "cron",
            ":name": spec.name,
            ":schedule": spec.schedule,
            ":flowName": spec.flowName,
            ":flowVersion": spec.flowVersion,
            ":input": enc(spec.input) ?? null,
            ":overlap": spec.overlap ?? "allow",
            ":nextRunAt": nextRunAt,
            ":gpk": CRON_DUE_GSI_PK,
            ":gsk": pad(nextRunAt),
          },
        }),
      );
    },

    async dueCrons(now, limit) {
      // gsi1 due-partition Query. Eventual consistency is safe: advanceCron is CAS-guarded, so a
      // stale/duplicate due read can't double-fire.
      const items = await queryAll({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :cd AND gsi1sk <= :now",
        ExpressionAttributeValues: { ":cd": CRON_DUE_GSI_PK, ":now": pad(now.getTime()) },
      });
      return items
        .map(parseCron)
        .sort((a, b) => a.nextRunAt - b.nextRunAt)
        .slice(0, limit)
        .map(mapCronItem);
    },

    async listCrons() {
      const items = (await scanType("cron")).map(parseCron);
      return items.sort((a, b) => a.cronName.localeCompare(b.cronName)).map(mapCronItem);
    },

    async removeCron(name) {
      const res = await doc.send(
        new DeleteCommand({
          TableName: table,
          Key: key.cron(name),
          ReturnValues: "ALL_OLD",
        }),
      );
      return res.Attributes !== undefined;
    },

    async dueCronCount(now, names) {
      const cond = "gsi1pk = :cd AND gsi1sk <= :now";
      const values = { ":cd": CRON_DUE_GSI_PK, ":now": pad(now.getTime()) };
      if (names === undefined) {
        return countQuery(doc, {
          TableName: table,
          IndexName: "gsi1",
          KeyConditionExpression: cond,
          ExpressionAttributeValues: values,
        });
      }
      const wanted = new Set(names);
      if (wanted.size === 0) return 0;
      const items = await queryAll({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: cond,
        ExpressionAttributeValues: values,
        ProjectionExpression: "flowName",
      });
      return items.filter((c) => wanted.has(str(c, "flowName"))).length;
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      try {
        await doc.send(
          new UpdateCommand({
            TableName: table,
            Key: key.cron(name),
            UpdateExpression: "SET nextRunAt = :next, lastRunAt = :last, gsi1sk = :gsk",
            ConditionExpression: "attribute_exists(pk) AND nextRunAt = :expected",
            ExpressionAttributeValues: {
              ":next": nextRunAt.getTime(),
              ":last": lastRunAt.getTime(),
              ":expected": expectedNextRunAt.getTime(),
              ":gsk": pad(nextRunAt.getTime()),
            },
          }),
        );
        return true;
      } catch (e) {
        if (e instanceof ConditionalCheckFailedException) return false;
        throw e;
      }
    },
  };
  return store;
};
