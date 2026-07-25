import {
  type ClaimOpts,
  type IdGen,
  type Lease,
  type Queue,
  type QueueDepth,
  queueDepthOf,
} from "@iterativeflow/core/backend";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

interface LeaseRow {
  run_id: string;
  lease_token: string;
  lease_expires: number;
  version: number;
}

/** @internal */
export const createSqliteQueue = (sql: Sql, t: Tables, id: IdGen): Queue => {
  const ms = (d?: Date): number => (d ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await sql.query(
        `INSERT INTO ${t.job} (run_id, run_at, priority, version) VALUES (?, ?, ?, 1)
         ON CONFLICT(run_id) DO UPDATE
           SET run_at = excluded.run_at, priority = excluded.priority, version = ${t.job}.version + 1`,
        [runId, opts?.runAt ? opts.runAt.getTime() : 0, opts?.priority ?? 0],
      );
    },

    async claim({ limit, leaseMs, now }: ClaimOpts) {
      const at = ms(now);
      // No SKIP LOCKED on SQLite; the write transaction serializes claimers so select-then-update
      // is exclusive. The token is a per-claim nonce made per-row-unique by appending run_id.
      return sql.tx(async (tx) => {
        const due = await tx.query<{ run_id: string }>(
          `SELECT run_id FROM ${t.job}
             WHERE run_at <= ? AND (lease_expires IS NULL OR lease_expires <= ?)
             ORDER BY priority, run_at LIMIT ?`,
          [at, at, limit],
        );
        if (!due.length) return [];
        const ids = due.map((r) => r.run_id);
        const holes = ids.map(() => "?").join(", ");
        const rows = await tx.query<LeaseRow>(
          `UPDATE ${t.job}
             SET lease_token = ? || ':' || run_id, lease_expires = ?
           WHERE run_id IN (${holes})
           RETURNING run_id, lease_token, lease_expires, version`,
          [id(), at + leaseMs, ...ids],
        );
        return rows.map((r) => ({
          runId: r.run_id,
          token: r.lease_token,
          expiresAt: new Date(r.lease_expires),
          version: Number(r.version),
        }));
      });
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const at = ms(now);
      const rows = await sql.query<{ lease_expires: number }>(
        `UPDATE ${t.job} SET lease_expires = ?
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ?
         RETURNING lease_expires`,
        [at + leaseMs, lease.runId, lease.token, at],
      );
      if (!rows[0]) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(rows[0].lease_expires) };
    },

    async ack(lease: Lease, opts) {
      const params = [lease.runId, lease.token, ms(opts?.now), lease.version];
      await sql.query(
        `DELETE FROM ${t.job}
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version = ?`,
        params,
      );
      await sql.query(
        `UPDATE ${t.job} SET lease_token = NULL, lease_expires = NULL, run_at = 0
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version <> ?`,
        params,
      );
    },

    async depth(now): Promise<QueueDepth> {
      const rows = await sql.query<{ run_at: number; lease_expires: number | null }>(
        `SELECT run_at, lease_expires FROM ${t.job}`,
      );
      const jobs = rows.map((r) => ({
        runAt: r.run_at,
        leaseExpires: r.lease_expires ?? undefined,
      }));
      return queueDepthOf(jobs, now.getTime());
    },
  };
};
