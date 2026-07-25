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

interface ClaimRow {
  run_id: string;
  version: number | string;
}

/** @internal */
export const createMysqlQueue = (sql: Sql, t: Tables, id: IdGen): Queue => {
  const ms = (d?: Date): number => (d ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await sql.exec(
        `INSERT INTO ${t.job} (run_id, run_at, priority, version) VALUES (?, ?, ?, 1)
         ON DUPLICATE KEY UPDATE run_at = VALUES(run_at), priority = VALUES(priority), version = version + 1`,
        [runId, opts?.runAt ? opts.runAt.getTime() : 0, opts?.priority ?? 0],
      );
    },

    async claim({ limit, leaseMs, now }: ClaimOpts) {
      const at = ms(now);
      const expiresAt = new Date(at + leaseMs);
      // MySQL has no RETURNING, so select the batch under SKIP LOCKED then stamp each row in the same
      // transaction. Each row gets its own token (a per-claim nonce made per-row-unique via run_id).
      return sql.tx(async (tx) => {
        const due = await tx.query<ClaimRow>(
          `SELECT run_id, version FROM ${t.job}
             WHERE run_at <= ? AND (lease_expires IS NULL OR lease_expires <= ?)
             ORDER BY priority, run_at LIMIT ? FOR UPDATE SKIP LOCKED`,
          [at, at, limit],
        );
        const leases: Lease[] = [];
        for (const row of due) {
          const token = `${id()}:${row.run_id}`;
          await tx.exec(`UPDATE ${t.job} SET lease_token = ?, lease_expires = ? WHERE run_id = ?`, [
            token,
            at + leaseMs,
            row.run_id,
          ]);
          leases.push({ runId: row.run_id, token, expiresAt, version: Number(row.version) });
        }
        return leases;
      });
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const at = ms(now);
      const { affectedRows } = await sql.exec(
        `UPDATE ${t.job} SET lease_expires = ?
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ?`,
        [at + leaseMs, lease.runId, lease.token, at],
      );
      if (affectedRows === 0)
        throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(at + leaseMs) };
    },

    async ack(lease: Lease, opts) {
      const params = [lease.runId, lease.token, ms(opts?.now), lease.version];
      // Version unchanged → normal completion, delete the job.
      await sql.exec(
        `DELETE FROM ${t.job}
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version = ?`,
        params,
      );
      // Version bumped by a wake mid-lease → release for re-claim instead of deleting.
      await sql.exec(
        `UPDATE ${t.job} SET lease_token = NULL, lease_expires = NULL, run_at = 0
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version <> ?`,
        params,
      );
    },

    async depth(now): Promise<QueueDepth> {
      const rows = await sql.query<{
        run_at: number | string;
        lease_expires: number | string | null;
      }>(`SELECT run_at, lease_expires FROM ${t.job}`);
      const jobs = rows.map((r) => ({
        runAt: Number(r.run_at),
        leaseExpires: r.lease_expires == null ? undefined : Number(r.lease_expires),
      }));
      return queueDepthOf(jobs, now.getTime());
    },
  };
};
