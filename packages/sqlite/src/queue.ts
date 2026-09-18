import {
  type ClaimOpts,
  type IdGen,
  type Lease,
  type Queue,
  type QueueDepth,
  queueDepthOf,
} from "@iterativeflow/core/backend";
import { int, optInt, text } from "#codec";
import type { Tables } from "#schema";
import { enqueueManyStmt, enqueueStmt } from "#statements";
import type { Sql } from "#sql";

/** @internal */
export const createSqliteQueue = (sql: Sql, t: Tables, id: IdGen): Queue => {
  const ms = (d?: Date): number => (d ?? new Date()).getTime();

  return {
    async enqueue(runId, opts) {
      await enqueueStmt(sql, t, runId, opts);
    },

    async enqueueMany(requests) {
      await enqueueManyStmt(sql, t, requests);
    },

    async claim({ limit, leaseMs, names, now }: ClaimOpts) {
      const at = ms(now);
      if (names && names.length === 0) return [];
      const nameFilter = names
        ? ` AND (r.name IS NULL OR r.name IN (${names.map(() => "?").join(", ")}))`
        : "";
      return sql.tx(async (tx) => {
        const due = await tx.query(
          `SELECT j.run_id FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id
             WHERE j.run_at <= ? AND (j.lease_expires IS NULL OR j.lease_expires <= ?)${nameFilter}
             ORDER BY j.priority, j.run_at LIMIT ?`,
          [at, at, ...(names ?? []), limit],
        );
        if (!due.length) return [];
        const ids = due.map((r) => text(r, "run_id"));
        const holes = ids.map(() => "?").join(", ");
        const rows = await tx.query(
          `UPDATE ${t.job}
             SET lease_token = ? || ':' || run_id, lease_expires = ?
           WHERE run_id IN (${holes})
           RETURNING run_id, lease_token, lease_expires, version`,
          [id(), at + leaseMs, ...ids],
        );
        return rows.map((r) => ({
          runId: text(r, "run_id"),
          token: text(r, "lease_token"),
          expiresAt: new Date(int(r, "lease_expires")),
          version: int(r, "version"),
        }));
      });
    },

    async heartbeat(lease: Lease, { leaseMs, now }) {
      const at = ms(now);
      const rows = await sql.query(
        `UPDATE ${t.job} SET lease_expires = ?
         WHERE run_id = ? AND lease_token = ? AND lease_expires > ?
         RETURNING lease_expires`,
        [at + leaseMs, lease.runId, lease.token, at],
      );
      if (!rows[0]) throw new Error(`heartbeat: lease for ${lease.runId} is no longer held`);
      return { ...lease, expiresAt: new Date(int(rows[0], "lease_expires")) };
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

    async depth(now, names): Promise<QueueDepth> {
      if (names && names.length === 0) return queueDepthOf([], now.getTime());
      const nowMs = now.getTime();
      // a run-less job is unownable, so it passes every name filter (see Queue.claim)
      const where = names
        ? ` WHERE r.name IS NULL OR r.name IN (${names.map(() => "?").join(", ")})`
        : "";
      const rows = await sql.query(
        `SELECT SUM(claimable) AS claimable, SUM(leased) AS leased,
                MIN(CASE WHEN claimable = 1 THEN run_at END) AS oldest
           FROM (SELECT j.run_at AS run_at,
                        (j.run_at <= ? AND (j.lease_expires IS NULL OR j.lease_expires <= ?)) AS claimable,
                        (j.lease_expires > ?) AS leased
                   FROM ${t.job} j LEFT JOIN ${t.run} r ON r.id = j.run_id${where}) x`,
        [nowMs, nowMs, nowMs, ...(names ?? [])],
      );
      const r = rows[0];
      const oldest = optInt(r, "oldest");
      return {
        claimable: optInt(r, "claimable") ?? 0,
        leased: optInt(r, "leased") ?? 0,
        oldestClaimableAgeMs: oldest === undefined ? null : nowMs - oldest,
      };
    },
  };
};
