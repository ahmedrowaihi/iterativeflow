/** Options for draining due timers. */
export interface TimerDueOpts {
  /** Injectable clock. Defaults to now. */
  now?: Date;
  /** Max runs to return this drain. */
  limit: number;
}

/**
 * Durable-deadline port — one of the four v2 ports. Every wait (sleep, retry backoff,
 * signal timeout) is a durable deadline here, so recovery reconciles against timers, not
 * against a queue property. One deadline per run (upsert); firing is exactly-once.
 *
 * Backends: Postgres = a `fire_at` partial index; DynamoDB = EventBridge Scheduler or a
 * `fire_at` GSI + poller; in-memory = a map. All satisfy `timerConformance`.
 */
export interface Timer {
  /** Set (upsert) the run's single wake deadline. The latest call wins. */
  schedule(runId: string, fireAt: Date): Promise<void>;

  /**
   * Return AND consume up to `limit` runs whose deadline has passed — earliest first,
   * fire-once (a consumed timer is not returned again). The caller re-enqueues them.
   */
  dueBatch(opts: TimerDueOpts): Promise<string[]>;

  /** Remove a run's pending timer (e.g. the wake landed another way, or the run ended). */
  cancel(runId: string): Promise<void>;

  /**
   * The earliest pending timer due strictly AFTER `now` (sleep / retry backoff / cron), or `null`
   * when none is pending — the serverless wake horizon. Timers due at/before `now` are drained by
   * the tick, not reported here. Must be one bounded read on the due-ordered index (min/limit-1),
   * never a scan; signals and child-joins wake by enqueue, so they are NOT covered by this.
   */
  nextDueAt(now: Date): Promise<Date | null>;
}
