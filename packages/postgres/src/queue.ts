import type { ClaimOpts, IdGen, Lease, Queue, QueueDepth } from "@iterativeflow/core/backend";
import { type Tables, tables } from "#schema";
import { enqueueStmt } from "#statements";
import type { Sql } from "#sql";

interface LeaseRow {
  run_id: string;
  lease_token: string;
  lease_expires: Date;
  version: number;
}

/** @internal */
export const createPgQueue = (sql: Sql, schema: string, id: IdGen): Queue => {
  const t: Tables = tables(schema);
  const at = (d?: Date): Date => d ?? new Date();

  return {
    async enqueue(runId, opts) {
      await enqueueStmt(sql, t, runId, opts);
    },

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      const rows = await sql.query<LeaseRow>(
        `UPDATE ${t.job}
           SET lease_token = $4 || ':' || run_id,
               lease_expires = $1::timestamptz + ($2 * interval '1 millisecond')
         WHERE run_id IN (
           SELECT j.run_id FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id
           WHERE j.run_at <= $1::timestamptz
             AND (j.lease_expires IS NULL OR j.lease_expires <= $1::timestamptz)
             AND ($5::text[] IS NULL OR r.name = ANY($5))
           ORDER BY j.priority, j.run_at
           FOR UPDATE OF j SKIP LOCKED
           LIMIT $3
         )
         RETURNING run_id, lease_token, lease_expires, version`,
        [at(now), leaseMs, limit, id(), names ?? null],
      );
      return rows.map((r) => ({
        runId: r.run_id,
        token: r.lease_token,
        expiresAt: r.lease_expires,
        version: Number(r.version),
      }));
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const rows = await sql.query<{ lease_expires: Date }>(
        `UPDATE ${t.job} SET lease_expires = $1::timestamptz + ($2 * interval '1 millisecond')
         WHERE run_id = $3 AND lease_token = $4 AND lease_expires > $1::timestamptz
         RETURNING lease_expires`,
        [at(now), leaseMs, lease.runId, lease.token],
      );
      if (!rows[0]) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: rows[0].lease_expires };
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
      const named = `($2::text[] IS NULL OR r.name = ANY($2))`;
      const claimable = `j.run_at <= $1::timestamptz AND (j.lease_expires IS NULL OR j.lease_expires <= $1::timestamptz) AND ${named}`;
      const rows = await sql.query<{ claimable: number; leased: number; oldest: Date | null }>(
        `SELECT count(*) FILTER (WHERE ${claimable})::int AS claimable,
                count(*) FILTER (WHERE j.lease_expires > $1::timestamptz AND ${named})::int AS leased,
                min(j.run_at) FILTER (WHERE ${claimable}) AS oldest
         FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id`,
        [at(now), names ?? null],
      );
      const r = rows[0];
      return {
        claimable: r.claimable,
        leased: r.leased,
        oldestClaimableAgeMs: r.oldest ? now.getTime() - r.oldest.getTime() : null,
      };
    },
  };
};
