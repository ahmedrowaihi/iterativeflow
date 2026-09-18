import {
  type ClaimOpts,
  type IdGen,
  type Lease,
  type Queue,
  type QueueDepth,
  queueDepthOf,
} from "@iterativeflow/core/backend";
import { date, int, optDate, text } from "#codec";
import { type Tables, tables } from "#schema";
import { enqueueManyStmt, enqueueStmt } from "#statements";
import type { Sql } from "#sql";

/** @internal */
export const createPgQueue = (sql: Sql, schema: string, id: IdGen): Queue => {
  const t: Tables = tables(schema);
  const at = (d?: Date): Date => d ?? new Date();

  return {
    async enqueue(runId, opts) {
      await enqueueStmt(sql, t, runId, opts);
    },

    async enqueueMany(requests) {
      await enqueueManyStmt(sql, t, requests);
    },

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      if (names?.length === 0) return [];
      const rows = await sql.query(
        `UPDATE ${t.job}
           SET lease_token = $4 || ':' || run_id,
               lease_expires = $1::timestamptz + ($2 * interval '1 millisecond')
         WHERE run_id IN (
           SELECT j.run_id FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id
           WHERE j.run_at <= $1::timestamptz
             AND (j.lease_expires IS NULL OR j.lease_expires <= $1::timestamptz)
             AND ($5::text[] IS NULL OR r.name IS NULL OR r.name = ANY($5))
           ORDER BY j.priority, j.run_at
           FOR UPDATE OF j SKIP LOCKED
           LIMIT $3
         )
         RETURNING run_id, lease_token, lease_expires, version`,
        [at(now), leaseMs, limit, id(), names ?? null],
      );
      return rows.map((r) => ({
        runId: text(r, "run_id"),
        token: text(r, "lease_token"),
        expiresAt: date(r, "lease_expires"),
        version: int(r, "version"),
      }));
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const rows = await sql.query(
        `UPDATE ${t.job} SET lease_expires = $1::timestamptz + ($2 * interval '1 millisecond')
         WHERE run_id = $3 AND lease_token = $4 AND lease_expires > $1::timestamptz
         RETURNING lease_expires`,
        [at(now), leaseMs, lease.runId, lease.token],
      );
      if (!rows[0]) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: date(rows[0], "lease_expires") };
    },

    async ack(lease: Lease, opts) {
      const params = [lease.runId, lease.token, at(opts?.now), lease.version];
      await sql.query(
        `DELETE FROM ${t.job}
         WHERE run_id = $1 AND lease_token = $2 AND lease_expires > $3::timestamptz
           AND version = $4::bigint`,
        params,
      );
      await sql.query(
        `UPDATE ${t.job}
           SET lease_token = NULL, lease_expires = NULL, run_at = 'epoch'::timestamptz
         WHERE run_id = $1 AND lease_token = $2 AND lease_expires > $3::timestamptz
           AND version <> $4::bigint`,
        params,
      );
    },

    async depth(now, names): Promise<QueueDepth> {
      if (names?.length === 0) return queueDepthOf([], now.getTime());
      // a run-less job is unownable, so it passes every name filter (see Queue.claim)
      const named = `($2::text[] IS NULL OR r.name IS NULL OR r.name = ANY($2))`;
      const claimable = `j.run_at <= $1::timestamptz AND (j.lease_expires IS NULL OR j.lease_expires <= $1::timestamptz) AND ${named}`;
      const rows = await sql.query(
        `SELECT count(*) FILTER (WHERE ${claimable})::int AS claimable,
                count(*) FILTER (WHERE j.lease_expires > $1::timestamptz AND ${named})::int AS leased,
                min(j.run_at) FILTER (WHERE ${claimable}) AS oldest
         FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id`,
        [at(now), names ?? null],
      );
      const r = rows[0];
      if (!r) throw new Error("depth: an aggregate returned no row");
      const oldest = optDate(r, "oldest");
      return {
        claimable: int(r, "claimable"),
        leased: int(r, "leased"),
        oldestClaimableAgeMs: oldest ? now.getTime() - oldest.getTime() : null,
      };
    },
  };
};
