import { and, eq, sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import type { SignalRow } from "../schema";
import type { ArmResult, SignalDeliveryResult } from "../types";
import type { InternalTables, StorageSliceDeps } from "./types";

const lockRunIn = async (tx: WorkflowDb, tables: InternalTables, runId: string): Promise<void> => {
  const { runs } = tables;
  await tx.select({ id: runs.id }).from(runs).where(eq(runs.id, runId)).for("update").limit(1);
};

/**
 * Deliver a signal by name. Resolves an armed waiter (`delivered`), buffers
 * for a future arm (`buffered`), or returns `expired` / `duplicate`.
 *
 * @internal
 */
export const deliverSignal =
  ({ db, tables, enqueue }: StorageSliceDeps) =>
  async (runId: string, signalName: string, payload: unknown): Promise<SignalDeliveryResult> => {
    const canonicalKey = `signal:${signalName}`;
    const { signals, events } = tables;
    return db.transaction(async (tx) => {
      await lockRunIn(tx, tables, runId);

      const armed = await tx
        .select({ cursorKey: signals.cursorKey, expiresAt: signals.expiresAt })
        .from(signals)
        .where(
          and(
            eq(signals.runId, runId),
            eq(signals.delivered, false),
            sql`(${signals.cursorKey} = ${canonicalKey} OR ${signals.cursorKey} LIKE ${`${canonicalKey}:%`})`,
            sql`(${signals.expiresAt} IS NULL OR ${signals.expiresAt} > NOW())`,
          ),
        )
        .limit(1);

      if (armed.length > 0) {
        const targetKey = armed[0].cursorKey;
        const delivered = await tx
          .update(signals)
          .set({
            delivered: true,
            deliveredAt: new Date(),
            payload: payload === undefined ? null : (payload as object),
          })
          .where(
            and(
              eq(signals.runId, runId),
              eq(signals.cursorKey, targetKey),
              eq(signals.delivered, false),
            ),
          )
          .returning({ runId: signals.runId });

        if (delivered.length === 0) {
          return { kind: "duplicate" } satisfies SignalDeliveryResult;
        }

        await tx.insert(events).values({
          runId,
          type: "signal_delivered",
          cursorKey: targetKey,
          payload: { cursorKey: targetKey } as object,
        });
        await enqueue(tx, runId);
        await tx.execute(sql`SELECT pg_notify('flow_progress', ${`signal:${runId}:${targetKey}`})`);
        return { kind: "delivered", cursorKey: targetKey } satisfies SignalDeliveryResult;
      }

      const expired = await tx
        .select({ cursorKey: signals.cursorKey })
        .from(signals)
        .where(
          and(
            eq(signals.runId, runId),
            eq(signals.delivered, false),
            sql`(${signals.cursorKey} = ${canonicalKey} OR ${signals.cursorKey} LIKE ${`${canonicalKey}:%`})`,
            sql`${signals.expiresAt} IS NOT NULL AND ${signals.expiresAt} <= NOW()`,
          ),
        )
        .limit(1);
      if (expired.length > 0) {
        return {
          kind: "expired",
          cursorKey: expired[0].cursorKey,
        } satisfies SignalDeliveryResult;
      }

      const inserted = await tx
        .insert(signals)
        .values({
          runId,
          cursorKey: canonicalKey,
          delivered: true,
          deliveredAt: new Date(),
          payload: payload === undefined ? null : (payload as object),
        })
        .onConflictDoNothing({ target: [signals.runId, signals.cursorKey] })
        .returning({ runId: signals.runId });

      if (inserted.length === 0) {
        return { kind: "duplicate" } satisfies SignalDeliveryResult;
      }

      await tx.insert(events).values({
        runId,
        type: "signal_delivered",
        cursorKey: canonicalKey,
        payload: { cursorKey: canonicalKey, buffered: true } as object,
      });
      await tx.execute(
        sql`SELECT pg_notify('flow_progress', ${`signal:${runId}:${canonicalKey}`})`,
      );
      return { kind: "buffered", cursorKey: canonicalKey } satisfies SignalDeliveryResult;
    });
  };

/**
 * From inside a flow body: consume a buffered signal at `cursorKey`, or arm
 * an empty row so a later `deliverSignal` can find and resolve it.
 *
 * @internal
 */
export const armOrConsumeSignal =
  ({ db, tables }: StorageSliceDeps) =>
  async (runId: string, cursorKey: string, expiresAt?: Date): Promise<ArmResult> =>
    db.transaction(async (tx) => {
      await lockRunIn(tx, tables, runId);
      const { signals, events } = tables;

      const existing = await tx
        .select()
        .from(signals)
        .where(and(eq(signals.runId, runId), eq(signals.cursorKey, cursorKey)))
        .limit(1);

      if (existing[0]?.delivered) {
        return {
          kind: "consumed",
          payload: existing[0].payload,
          row: existing[0] as SignalRow,
        } satisfies ArmResult;
      }

      if (!existing[0]) {
        await tx
          .insert(signals)
          .values({
            runId,
            cursorKey,
            expiresAt: expiresAt ?? null,
            delivered: false,
          })
          .onConflictDoNothing({ target: [signals.runId, signals.cursorKey] });
        await tx.insert(events).values({
          runId,
          type: "signal_armed",
          cursorKey,
          payload: { expiresAt: expiresAt ?? null } as object,
        });
      }
      return { kind: "armed" } satisfies ArmResult;
    });
