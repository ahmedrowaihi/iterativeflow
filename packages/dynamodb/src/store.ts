import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
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
  type Outbox,
  type RunSpec,
  type StartResult,
  type StepOutcome,
  type OrphanView,
  type PurgeFilter,
  type RunSelection,
  type Store,
  type SuspendStatus,
  type TerminalOutcome,
  TERMINAL_STATUSES,
  cancelSelection,
  isOrphaned,
  listSelection,
  purgeSelection,
  runMatcher,
  resetsAttempts,
  retrySelection,
  shardAdmits,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import { countQuery } from "#count";
import { batchGetRuns, storedPriorities } from "#run-names";
import {
  type CronItem,
  type DocItem,
  type RunItem,
  dec,
  enc,
  mapRun,
  mapStep,
  num,
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
  nextSeq,
  outboxParts,
  spawnTx,
} from "#statements";

type WriteRequests = NonNullable<BatchWriteCommandInput["RequestItems"]>[string];

const TERMINAL_VALUES: Record<string, string> = Object.fromEntries(
  TERMINAL_STATUSES.map((s, i) => [`:t${i}`, s]),
);
const IS_TERMINAL = `#status IN (${Object.keys(TERMINAL_VALUES).join(", ")})`;
const NOT_TERMINAL = `NOT (${IS_TERMINAL})`;

// TransactWriteItems positions of checkpointStep's gate entries; a cancellation reason is read by index.
const STEP_SLOT = 0;
const RUN_GATE = 1;
const VERSION_GATE = 2;

const selectable = (r: RunItem) => ({
  ...r,
  createdAt: r.createdAt === undefined ? undefined : new Date(r.createdAt),
});

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

  const selectRuns = async (sel: RunSelection, limit: number): Promise<RunItem[]> => {
    if (sel.statuses.length === 0) return [];
    const runs = await scanRuns();
    const statuses = new Map(runs.map((r) => [r.id, r.status]));
    const matches = runMatcher(sel, (runId) => statuses.get(runId));
    return runs
      .filter((r) => matches(selectable(r)))
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

  // The status condition re-checks the selection at delete time: the Scan is eventually consistent,
  // and a run retried failed→pending since then must survive. The idem marker goes in the same
  // transaction — left behind, a re-submit with the same key would dedup onto the purged run.
  const deleteRunGuarded = async (r: RunItem, spareIfParentLive: boolean): Promise<boolean> => {
    const items: TxItem[] = [
      {
        Delete: {
          TableName: table,
          Key: key.run(r.id),
          ConditionExpression: "#status = :s",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":s": r.status },
        },
      },
      { Delete: { TableName: table, Key: key.job(r.id) } },
      { Delete: { TableName: table, Key: key.timer(r.id) } },
    ];
    if (r.idempotencyKey) {
      items.push({
        Delete: { TableName: table, Key: key.idem(r.name, r.version, r.idempotencyKey) },
      });
    }
    const parentGate = items.length;
    if (spareIfParentLive && r.parentRunId !== undefined) {
      items.push({
        ConditionCheck: {
          TableName: table,
          Key: key.run(r.parentRunId),
          ConditionExpression: `attribute_not_exists(pk) OR ${IS_TERMINAL}`,
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: TERMINAL_VALUES,
        },
      });
    }
    try {
      await doc.send(new TransactWriteCommand({ TransactItems: items }));
      return true;
    } catch (e) {
      const reasons = cancellationReasons(e);
      if (conditionFailedAt(reasons, 0) || conditionFailedAt(reasons, parentGate)) return false;
      throw e;
    }
  };

  const batchDelete = async (keys: readonly { pk: string; sk: string }[]): Promise<void> => {
    // BatchWriteItem caps at 25/call and may return UnprocessedItems under throttle — drain them.
    let pending: WriteRequests = keys.map((Key) => ({ DeleteRequest: { Key } }));
    while (pending.length > 0) {
      const batch = pending.slice(0, 25);
      pending = pending.slice(25);
      const res = await doc.send(new BatchWriteCommand({ RequestItems: { [table]: batch } }));
      const left = res.UnprocessedItems?.[table];
      if (left?.length) pending.push(...left);
    }
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const sel = purgeSelection(filter);
    const spare = sel.spareChildrenOfLiveParents === true;
    const runs = await selectRuns(sel, limit);
    // Siblings delete one at a time: concurrent transactions that ConditionCheck the same parent
    // item cancel each other with TransactionConflict.
    const bySiblings = new Map<string, RunItem[]>();
    for (const r of runs) {
      const k = r.parentRunId ?? r.id;
      bySiblings.set(k, [...(bySiblings.get(k) ?? []), r]);
    }
    const won = new Set<string>();
    await Promise.all(
      [...bySiblings.values()].map(async (siblings) => {
        for (const r of siblings) if (await deleteRunGuarded(r, spare)) won.add(r.id);
      }),
    );
    const deleted = runs.filter((r) => won.has(r.id));
    const partitions = await Promise.all(
      deleted.map((r) =>
        queryAll({
          TableName: table,
          KeyConditionExpression: "pk = :pk",
          ExpressionAttributeValues: { ":pk": key.runPk(r.id) },
          ProjectionExpression: "pk, sk",
          ConsistentRead: true,
        }),
      ),
    );
    await batchDelete(partitions.flat().map((it) => ({ pk: str(it, "pk"), sk: str(it, "sk") })));
    return deleted.length;
  };

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

  const markTerminal = async (runId: string, outcome: TerminalOutcome, fx?: Outbox) => {
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
    const { nonSpawn, spawns } = outboxParts(table, fx);
    const inline = spawns.flatMap((s) => spawnTx(table, s));
    try {
      await doc.send(new TransactWriteCommand({ TransactItems: [gate, ...nonSpawn, ...inline] }));
      return true;
    } catch (e) {
      if (conditionFailedAt(cancellationReasons(e), 0)) return false; // already terminal — outbox skipped
      throw e;
    }
  };

  const store: Store = {
    startRun: startOne,

    async startManyRuns(specs) {
      // Dedup by idempotency key first: a transaction can't hold two ops on one item.
      const slot: number[] = [];
      const uniq: { spec: RunSpec; runId: string; items: TxItem[] }[] = [];
      const seen = new Map<string, number>();
      for (const spec of specs) {
        const dedup = spec.idempotencyKey
          ? key.idem(spec.name, spec.version, spec.idempotencyKey).pk
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
      // Consistent: the replay read — a stale memo page would re-execute a committed step.
      const items = await queryAll({
        TableName: table,
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: { ":pk": key.runPk(runId) },
        ConsistentRead: true,
      });
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
      const items = (await batchGetRuns(doc, table, runIds)).map(parseRun);
      const byId = new Map(items.map((r) => [r.id, r]));
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
            ...key.signal(runId, Date.now(), seq, sigId),
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
        return num(res.Attributes, "attempts");
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
      const { nonSpawn, spawns } = outboxParts(table, fx);
      // The job item is key-addressable, so the version ConditionCheck is atomic — no residual window.
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
      // The run gate proves the run exists. When a join is armed it doubles as the countdown SET —
      // one op on the run item, since TransactWriteItems forbids a second op on the same item.
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
      // Step + effects commit in ONE TransactWriteItems. Core bounds the spawn batch (ctx.invoke
      // chunks fan-out), so a single checkpoint's items always fit the cap — assert it loudly if not.
      if (gate.length + nonSpawn.length + 2 * spawns.length > MAX_TX_ITEMS) {
        throw new Error(
          `checkpointStep: ${spawns.length} spawns + effects exceed the ${MAX_TX_ITEMS}-item transaction budget`,
        );
      }
      const firstSpawn = gate.length + nonSpawn.length;
      const isSpawnPut = (i: number) => i >= firstSpawn && (i - firstSpawn) % 2 === 0;

      let pending = spawns;
      for (;;) {
        try {
          await doc.send(
            new TransactWriteCommand({
              TransactItems: [...gate, ...nonSpawn, ...pending.flatMap((s) => spawnTx(table, s))],
            }),
          );
          break;
        } catch (e) {
          const reasons = cancellationReasons(e);
          if (!reasons) throw e;
          // Step already present ⇒ idempotent replay: skip the outbox, return the stored outcome.
          if (conditionFailedAt(reasons, STEP_SLOT)) break;
          if (conditionFailedAt(reasons, RUN_GATE)) {
            throw new Error(`checkpointStep: run ${c.runId} not found`, { cause: e });
          }
          if (versionGate.length > 0 && conditionFailedAt(reasons, VERSION_GATE)) {
            return { status: c.status, attempts: c.attempts, committed: false };
          }
          // Spawn of an already-existing run id is first-writer-wins: drop it (and its job write)
          // and commit the rest. Any other cancellation is a real failure.
          const onlyExistingChildren = reasons.every(
            (r, i) =>
              !r.Code ||
              r.Code === "None" ||
              (isSpawnPut(i) && r.Code === "ConditionalCheckFailed"),
          );
          const fresh = pending.filter((_, i) => !conditionFailedAt(reasons, firstSpawn + 2 * i));
          if (!onlyExistingChildren || fresh.length === pending.length) throw e;
          pending = fresh;
        }
      }
      const stored = await getStep(c.runId, c.cursorKey);
      if (!stored) throw new Error(`checkpointStep: step ${c.runId}/${c.cursorKey} vanished`);
      return stored;
    },

    async suspendRun(runId, status: SuspendStatus, fx) {
      const { nonSpawn, spawns } = outboxParts(table, fx);
      const reset = resetsAttempts(status);
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

    markTerminal,

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
        return num(res.Attributes, "joinRemaining");
      } catch (e) {
        if (e instanceof ConditionalCheckFailedException) return undefined; // parent gone
        throw e;
      }
    },

    async listRuns(filter, page) {
      const matches = runMatcher(listSelection(filter));
      // gsi2 RUN partition, newest createdAt first. With no filter this reads one page; a selective filter
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
          if (matches(selectable(r))) {
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
      const victims = await selectRuns(cancelSelection(filter), limit);
      let n = 0;
      for (const r of victims) {
        if (await markTerminal(r.id, { status: "canceled" }, { cancelTimers: [r.id] })) n += 1;
      }
      return n;
    },

    async retryRuns(filter, limit) {
      const victims = await selectRuns(retrySelection(filter), limit);
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
            ":overlap": spec.overlap,
            ":nextRunAt": nextRunAt,
            ":gpk": CRON_DUE_GSI_PK,
            ":gsk": pad(nextRunAt),
          },
        }),
      );
    },

    async nextCronAt() {
      const res = await doc.send(
        new QueryCommand({
          TableName: table,
          IndexName: "gsi1",
          KeyConditionExpression: "gsi1pk = :cd",
          ExpressionAttributeValues: { ":cd": CRON_DUE_GSI_PK },
          Limit: 1,
        }),
      );
      const [item] = res.Items ?? [];
      return item ? new Date(parseCron(item).nextRunAt) : null;
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
      const items = await queryAll({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: cond,
        ExpressionAttributeValues: values,
        ProjectionExpression: "flowName",
      });
      return items.filter((c) => shardAdmits(names, str(c, "flowName"))).length;
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
