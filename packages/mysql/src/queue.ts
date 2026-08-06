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

    async claim({ limit, leaseMs, now, names }: ClaimOpts) {
      if (names && names.length === 0) return []; // empty set leases nothing
      const at = ms(now);
      const expiresAt = new Date(at + leaseMs);
      return sql.tx(async (tx) => {
        const namePredicate = names
          ? ` AND (r.name IS NULL OR r.name IN (${names.map(() => "?").join(",")}))`
          : "";
        const params = names ? [at, at, ...names, limit] : [at, at, limit];
        const due = await tx.query<ClaimRow>(
          `SELECT j.run_id AS run_id, j.version AS version
             FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id
             WHERE j.run_at <= ? AND (j.lease_expires IS NULL OR j.lease_expires <= ?)${namePredicate}
             ORDER BY j.priority, j.run_at LIMIT ? FOR UPDATE OF j SKIP LOCKED`,
          params,
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
      await sql.exec(
        `DELETE FROM ${t.job}
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version = ?`,
        params,
      );
      await sql.exec(
        `UPDATE ${t.job} SET lease_token = NULL, lease_expires = NULL, run_at = 0
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ? AND version <> ?`,
        params,
      );
    },

    async depth(now, names): Promise<QueueDepth> {
      if (names && names.length === 0) return queueDepthOf([], now.getTime());
      const nowMs = now.getTime();
      const namePredicate = names ? ` WHERE r.name IN (${names.map(() => "?").join(",")})` : "";
      const rows = await sql.query<{
        claimable: number | string | null;
        leased: number | string | null;
        oldest: number | string | null;
      }>(
        `SELECT SUM(claimable) AS claimable, SUM(leased) AS leased,
                MIN(CASE WHEN claimable = 1 THEN run_at END) AS oldest
           FROM (SELECT j.run_at AS run_at,
                        (j.run_at <= ? AND (j.lease_expires IS NULL OR j.lease_expires <= ?)) AS claimable,
                        (j.lease_expires > ?) AS leased
                   FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id${namePredicate}) x`,
        [nowMs, nowMs, nowMs, ...(names ?? [])],
      );
      const r = rows[0];
      return {
        claimable: Number(r?.claimable ?? 0),
        leased: Number(r?.leased ?? 0),
        oldestClaimableAgeMs: r?.oldest == null ? null : nowMs - Number(r.oldest),
      };
    },
  };
};
