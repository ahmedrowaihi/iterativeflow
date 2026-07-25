import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  BatchGetCommand,
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
  type Store,
  type SuspendStatus,
  TERMINAL_STATUSES,
  isOrphaned,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import type { Doc } from "#client";
import {
  type CronItem,
  type RunItem,
  type RunPartitionItem,
  type StepItem,
  dec,
  enc,
  mapRun,
  mapStep,
  nextSeq,
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

const TERMINAL_VALUES: Record<string, string> = Object.fromEntries(
  TERMINAL_STATUSES.map((s, i) => [`:t${i}`, s]),
);
const NOT_TERMINAL = `NOT (#status IN (${Object.keys(TERMINAL_VALUES).join(", ")}))`;

/** @internal */
export const createDynamoStore = (doc: Doc, table: string, id: IdGen): Store => {
  const send = <T = unknown>(cmd: unknown): Promise<T> => doc.send(cmd) as Promise<T>;

  /**
   * Base-table decision-path reads funnel through here so the strong-read invariant is structural:
   * a stale read could replay against an outdated run or a missed step memo. GSI reads (claim/timer)
   * can't be consistent and are CAS-guarded — they stay off this path.
   */
  const consistentGet = (Key: { pk: string; sk: string }): GetCommand =>
    new GetCommand({ TableName: table, Key, ConsistentRead: true });

  const getRun = async (runId: string): Promise<RunItem | undefined> => {
    const res = await send<{ Item?: RunItem }>(consistentGet(key.run(runId)));
    return res.Item;
  };

  const recover = async (markerRunId: string): Promise<StartResult> => {
    const existing = await getRun(markerRunId);
    if (!existing)
      throw new Error(`startRun: idempotency marker points at missing run ${markerRunId}`);
    return { runId: existing.id, created: false, status: existing.status };
  };

  const getStep = async (runId: string, cursorKey: string): Promise<StepOutcome | undefined> => {
    const res = await send<{ Item?: StepItem }>(consistentGet(key.step(runId, cursorKey)));
    return res.Item ? mapStep(res.Item) : undefined;
  };

  const scanAll = async (
    params: ScanCommandInput,
    consistent = false,
  ): Promise<Record<string, unknown>[]> => {
    const out: Record<string, unknown>[] = [];
    let ExclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const res = await send<{
        Items?: Record<string, unknown>[];
        LastEvaluatedKey?: Record<string, unknown>;
      }>(
        new ScanCommand({ ...params, ExclusiveStartKey, ConsistentRead: consistent || undefined }),
      );
      out.push(...(res.Items ?? []));
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return out;
  };

  const queryAll = async <T>(params: QueryCommandInput): Promise<T[]> => {
    const out: Record<string, unknown>[] = [];
    let ExclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const res = await send<{
        Items?: Record<string, unknown>[];
        LastEvaluatedKey?: Record<string, unknown>;
      }>(new QueryCommand({ ...params, ExclusiveStartKey }));
      out.push(...(res.Items ?? []));
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return out as T[];
  };

  // One localized assertion: a `Scan` returns attribute bags; the caller names the item type.
  const scanType = <T>(type: string, consistent = false): Promise<T[]> =>
    scanAll(
      {
        TableName: table,
        FilterExpression: "#type = :t",
        ExpressionAttributeNames: { "#type": "type" },
        ExpressionAttributeValues: { ":t": type },
      },
      consistent,
    ) as Promise<T[]>;

  const startOne = async (spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    if (!spec.idempotencyKey) {
      await send(new PutCommand({ TableName: table, Item: buildRunItem(spec, runId) }));
      return { runId, created: true, status: "pending" };
    }
    try {
      await send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Put: {
                TableName: table,
                Item: {
                  ...key.idem(spec.name, spec.version, spec.idempotencyKey),
                  type: "idem",
                  runId,
                },
                ConditionExpression: "attribute_not_exists(pk)",
              },
            },
            { Put: { TableName: table, Item: buildRunItem(spec, runId) } },
          ],
        }),
      );
      return { runId, created: true, status: "pending" };
    } catch (e) {
      if (!conditionFailedAt(cancellationReasons(e), 0)) throw e;
      const marker = await send<{ Item?: { runId: string } }>(
        consistentGet(key.idem(spec.name, spec.version, spec.idempotencyKey)),
      );
      if (!marker.Item)
        throw new Error(`startRun: idempotency marker missing for ${spec.idempotencyKey}`, {
          cause: e,
        });
      return recover(marker.Item.runId);
    }
  };

  return {
    startRun: startOne,

    async startManyRuns(specs) {
      const markers = await Promise.all(
        specs.map((spec) =>
          spec.idempotencyKey
            ? send<{ Item?: { runId: string } }>(
                consistentGet(key.idem(spec.name, spec.version, spec.idempotencyKey)),
              )
            : Promise.resolve(undefined),
        ),
      );
      const results: StartResult[] = [];
      const tx: TxItem[] = [];
      for (let i = 0; i < specs.length; i++) {
        const spec = specs[i];
        const runId = id();
        if (spec.idempotencyKey) {
          const marker = markers[i];
          if (marker?.Item) {
            results.push(await recover(marker.Item.runId));
            continue;
          }
          tx.push({
            Put: {
              TableName: table,
              Item: {
                ...key.idem(spec.name, spec.version, spec.idempotencyKey),
                type: "idem",
                runId,
              },
              ConditionExpression: "attribute_not_exists(pk)",
            },
          });
        }
        tx.push({ Put: { TableName: table, Item: buildRunItem(spec, runId) } });
        results.push({ runId, created: true, status: "pending" });
      }
      for (let i = 0; i < tx.length; i += MAX_TX_ITEMS) {
        await send(new TransactWriteCommand({ TransactItems: tx.slice(i, i + MAX_TX_ITEMS) }));
      }
      return results;
    },

    async loadRun(runId) {
      const res = await send<{ Items?: RunPartitionItem[] }>(
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
      if (runItem?.type !== "run") return undefined;
      const steps = new Map<string, StepOutcome>();
      const signals: DeliveredSignal[] = [];
      for (const it of items) {
        if (it.type === "step") steps.set(it.cursorKey, mapStep(it));
        else if (it.type === "signal") {
          signals.push({
            id: encodeSignalId(it.pk, it.sk),
            name: it.name,
            payload: dec(it.payload),
          });
        }
      }
      return { run: mapRun(runItem), steps, signals };
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
        let keys: { pk: string; sk: string }[] = runIds
          .slice(i, i + 100)
          .map((rid) => key.run(rid));
        while (keys.length > 0) {
          const res = await send<{
            Responses?: Record<string, RunItem[]>;
            UnprocessedKeys?: Record<string, { Keys?: { pk: string; sk: string }[] }>;
          }>(
            new BatchGetCommand({
              RequestItems: { [table]: { Keys: keys, ConsistentRead: true } },
            }),
          );
          for (const item of res.Responses?.[table] ?? []) byId.set(item.id, item);
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
      const enqueue: TxItem = { Update: enqueueParams(table, runId) };
      if (!opts?.idempotencyKey) {
        await send(new TransactWriteCommand({ TransactItems: [signalPut, enqueue] }));
        return { delivered: true };
      }
      try {
        await send(
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
        const res = await send<{ Attributes?: { attempts: number } }>(
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
      const stepItem: Record<string, unknown> = {
        ...key.step(c.runId, c.cursorKey),
        type: "step",
        runId: c.runId,
        cursorKey: c.cursorKey,
        status: c.status,
        result: enc(c.result),
        error: enc(c.error),
        attempts: c.attempts,
        shape: c.shape,
      };
      const { nonSpawn, spawns } = outboxParts(table, fx);
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      // Step + effects commit in ONE TransactWriteItems. Core bounds the spawn batch (ctx.invoke
      // chunks fan-out), so a single checkpoint's items always fit the cap — assert it loudly if not.
      if (2 + nonSpawn.length + inline.length > MAX_TX_ITEMS) {
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
      ];

      try {
        await send(new TransactWriteCommand({ TransactItems: [...gate, ...nonSpawn, ...inline] }));
      } catch (e) {
        const reasons = cancellationReasons(e);
        if (!reasons) throw e;
        // The run-exists check failed but the step slot was free ⇒ unknown run.
        if (conditionFailedAt(reasons, 1) && !conditionFailedAt(reasons, 0)) {
          throw new Error(`checkpointStep: run ${c.runId} not found`, { cause: e });
        }
        // Step already present ⇒ idempotent replay: skip the outbox, return the stored outcome.
        if (!conditionFailedAt(reasons, 0)) throw e;
      }
      const stored = await getStep(c.runId, c.cursorKey);
      if (!stored) throw new Error(`checkpointStep: step ${c.runId}/${c.cursorKey} vanished`);
      return stored;
    },

    async suspendRun(runId, status: SuspendStatus, fx) {
      const { nonSpawn, spawns } = outboxParts(table, fx);
      const reset = status !== "retrying"; // forward progress resets the poison-pill cap
      const gate: TxItem = {
        Update: {
          TableName: table,
          Key: key.run(runId),
          UpdateExpression: `SET #status = :status${reset ? ", attempts = :zero" : ""}`,
          ConditionExpression: `attribute_exists(pk) AND ${NOT_TERMINAL}`,
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: {
            ":status": status,
            ...TERMINAL_VALUES,
            ...(reset ? { ":zero": 0 } : {}),
          },
        },
      };
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      try {
        await send(new TransactWriteCommand({ TransactItems: [gate, ...nonSpawn, ...inline] }));
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return; // already terminal — no-op, outbox skipped
        throw e;
      }
    },

    async markTerminal(runId, outcome, fx) {
      const set = ["#status = :status"];
      const remove: string[] = [];
      const values: Record<string, unknown> = {
        ":status": outcome.status,
        ":canceled": "canceled",
      };
      const output = outcome.status === "done" ? enc(outcome.output) : undefined;
      const error = outcome.status === "done" ? undefined : enc(outcome.error);
      if (output === undefined) remove.push("#output");
      else {
        set.push("#output = :output");
        values[":output"] = output;
      }
      if (error === undefined) remove.push("#error");
      else {
        set.push("#error = :error");
        values[":error"] = error;
      }
      const gate: TxItem = {
        Update: {
          TableName: table,
          Key: key.run(runId),
          UpdateExpression: `SET ${set.join(", ")}${remove.length ? ` REMOVE ${remove.join(", ")}` : ""}`,
          ConditionExpression: "attribute_exists(pk) AND #status <> :canceled",
          ExpressionAttributeNames: { "#status": "status", "#output": "output", "#error": "error" },
          ExpressionAttributeValues: values,
        },
      };
      const { nonSpawn, spawns } = outboxParts(table, fx);
      const inline = spawns.flatMap((s) => spawnTx(table, s));
      try {
        await send(new TransactWriteCommand({ TransactItems: [gate, ...nonSpawn, ...inline] }));
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return; // canceled is sticky — outbox skipped
        throw e;
      }
    },

    async arriveAtJoin(parentRunId) {
      // ADD…RETURN_VALUES: TransactWriteItems can't return the post-decrement value the wake decision
      // needs, so the decrement is its own atomic write (serializing concurrent siblings).
      try {
        const res = await send<{ Attributes?: { joinRemaining?: number } }>(
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
        (!filter.tag || (r.tags?.includes(filter.tag) ?? false));
      // gsi2 RUN partition, descending seq. With no filter this reads one page; a selective filter
      // may walk a few index pages, but never the whole table.
      const rows: RunItem[] = [];
      let startKey: Record<string, unknown> | undefined;
      do {
        const res = await send<{ Items?: RunItem[]; LastEvaluatedKey?: Record<string, unknown> }>(
          new QueryCommand({
            TableName: table,
            IndexName: "gsi2",
            KeyConditionExpression: page.cursor ? "gsi2pk = :rp AND gsi2sk < :cur" : "gsi2pk = :rp",
            ExpressionAttributeValues: {
              ":rp": RUN_GSI2_PK,
              ...(page.cursor ? { ":cur": pad(Number(page.cursor)) } : {}),
            },
            ScanIndexForward: false,
            ExclusiveStartKey: startKey,
          }),
        );
        for (const r of res.Items ?? []) {
          if (keep(r)) rows.push(r);
          if (rows.length === page.limit) break;
        }
        startKey = rows.length < page.limit ? res.LastEvaluatedKey : undefined;
      } while (startKey);
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(last.seq) : undefined;
      return { runs: rows.map(mapRun), cursor };
    },

    async childrenOf(runId) {
      // gsi1 parent-partition Query (eventually consistent). The cancel cascade tolerates GSI lag:
      // a just-spawned child the cascade misses self-cancels on dispatch and is re-driven by
      // reconcile. Both backstops are covered by engineConformance.
      const items = await queryAll<RunItem>({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :cp",
        ExpressionAttributeValues: { ":cp": childGsiPk(runId) },
      });
      return items.map(mapRun);
    },

    async runStats() {
      const stats = zeroRunStats();
      // gsi2 RUN partition, projecting only status — a Query over the index, not a full-item Scan.
      const rows = await queryAll<{ status: RunStatus }>({
        TableName: table,
        IndexName: "gsi2",
        KeyConditionExpression: "gsi2pk = :rp",
        ExpressionAttributeValues: { ":rp": RUN_GSI2_PK },
        ProjectionExpression: "#s",
        ExpressionAttributeNames: { "#s": "status" },
      });
      for (const r of rows) stats[r.status] += 1;
      return stats;
    },

    async orphanedRuns(max) {
      const [runs, jobItems, timerItems] = await Promise.all([
        scanType<RunItem>("run"),
        scanType<{ runId: string }>("job"),
        scanType<{ runId: string }>("timer"),
      ]);
      const jobs = new Set(jobItems.map((j) => j.runId));
      const timers = new Set(timerItems.map((t) => t.runId));
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
        .slice(0, max)
        .map((r) => r.id);
    },

    async retryRun(runId) {
      try {
        await send(
          new TransactWriteCommand({
            TransactItems: [
              {
                Update: {
                  TableName: table,
                  Key: key.run(runId),
                  UpdateExpression: "SET #status = :pending REMOVE #error",
                  ConditionExpression: "attribute_exists(pk) AND #status = :failed",
                  ExpressionAttributeNames: { "#status": "status", "#error": "error" },
                  ExpressionAttributeValues: { ":pending": "pending", ":failed": "failed" },
                },
              },
              { Update: enqueueParams(table, runId) },
            ],
          }),
        );
        return { retried: true };
      } catch (e) {
        if (conditionFailedAt(cancellationReasons(e), 0)) return { retried: false };
        throw e;
      }
    },

    async upsertCron(spec) {
      await send(
        new UpdateCommand({
          TableName: table,
          Key: key.cron(spec.name),
          UpdateExpression:
            "SET #type = :type, cronName = :name, schedule = :schedule, flowName = :flowName, " +
            "flowVersion = :flowVersion, cronInput = :input, overlap = :overlap, gsi1pk = :gpk, " +
            "nextRunAt = if_not_exists(nextRunAt, :nextRunAt), " +
            "gsi1sk = if_not_exists(gsi1sk, :gsk)",
          ExpressionAttributeNames: { "#type": "type" },
          ExpressionAttributeValues: {
            ":type": "cron",
            ":name": spec.name,
            ":schedule": spec.schedule,
            ":flowName": spec.flowName,
            ":flowVersion": spec.flowVersion,
            ":input": enc(spec.input) ?? null,
            ":overlap": spec.overlap ?? "allow",
            ":nextRunAt": spec.nextRunAt.getTime(),
            ":gpk": CRON_DUE_GSI_PK,
            ":gsk": pad(spec.nextRunAt.getTime()),
          },
        }),
      );
    },

    async dueCrons(now, max) {
      // gsi1 due-partition Query. Eventual consistency is safe: advanceCron is CAS-guarded, so a
      // stale/duplicate due read can't double-fire.
      const items = await queryAll<CronItem>({
        TableName: table,
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :cd AND gsi1sk <= :now",
        ExpressionAttributeValues: { ":cd": CRON_DUE_GSI_PK, ":now": pad(now.getTime()) },
      });
      return items
        .sort((a, b) => a.nextRunAt - b.nextRunAt)
        .slice(0, max)
        .map(
          (c): CronRow => ({
            name: c.cronName,
            schedule: c.schedule,
            flowName: c.flowName,
            flowVersion: c.flowVersion,
            input: dec(c.cronInput),
            overlap: c.overlap,
            nextRunAt: new Date(c.nextRunAt),
            lastRunAt: c.lastRunAt === undefined ? undefined : new Date(c.lastRunAt),
          }),
        );
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      try {
        await send(
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
};
