import { advisoryLockKey } from './hash.js';
import type { OutboxEvent, Queryable } from './types.js';

/** A held connection: something a real advisory lock needs, since
 *  pg_advisory_lock is scoped to the session (connection) that took it,
 *  not to a logical transaction. A pooled query-per-call client
 *  wouldn't work here at all -- the lock and its later unlock have to
 *  run on the exact same connection. */
export interface PoolClientLike extends Queryable {
  release: () => void;
}

export interface PoolLike {
  connect: () => Promise<PoolClientLike>;
}

export interface RelayOptions {
  pool: PoolLike;
  /** Delivers one event. Throwing marks it failed and stops processing
   *  that aggregate's queue for this poll, so ordering is never broken
   *  by skipping ahead past a failure. */
  deliver: (event: OutboxEvent) => Promise<void>;
  pollIntervalMs?: number;
  /** Attempts before an event is marked dead_at and stops blocking the
   *  rest of its aggregate's queue. */
  maxAttempts?: number;
  /** Events fetched per aggregate per poll. */
  eventsPerAggregate?: number;
  /** Distinct pending aggregates considered per poll. */
  aggregatesPerPoll?: number;
}

const DEFAULTS = {
  pollIntervalMs: 1000,
  maxAttempts: 5,
  eventsPerAggregate: 50,
  aggregatesPerPoll: 100,
};

/**
 * Drains the outbox: finds aggregates with pending events, takes an
 * advisory lock per aggregate (so two Relay instances -- or two workers
 * in the same process -- never process the same aggregate concurrently),
 * and delivers that aggregate's events strictly in order. Different
 * aggregates are processed concurrently with each other -- ordering is
 * a per-aggregate guarantee, not a global one, which is what lets this
 * scale past a single worker. See
 * docs/adr/0003-per-aggregate-ordering-with-advisory-locks.md.
 */
export class Relay {
  private readonly opts: Required<RelayOptions>;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(opts: RelayOptions) {
    this.opts = { ...DEFAULTS, ...opts };
  }

  start(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => {
      this.pollOnce().catch((err: unknown) => {
        console.error('outboxer: poll failed', err);
      });
    }, this.opts.pollIntervalMs);
  }

  stop(): void {
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
  }

  /** Runs one poll cycle and returns how many events were successfully
   *  delivered -- exposed directly (not just via start()) so tests, and
   *  callers who want to drive their own scheduling, can await a single
   *  pass deterministically. */
  async pollOnce(): Promise<{ delivered: number }> {
    const coordinator = await this.opts.pool.connect();
    let aggregateIds: string[];
    try {
      const { rows } = await coordinator.query(
        `select distinct aggregate_id from outbox_events
         where published_at is null and dead_at is null
         order by aggregate_id
         limit $1`,
        [this.opts.aggregatesPerPoll],
      );
      aggregateIds = rows.map((r) => r.aggregate_id as string);
    } finally {
      coordinator.release();
    }

    const results = await Promise.all(aggregateIds.map((id) => this.processAggregate(id)));
    return { delivered: results.reduce((sum, n) => sum + n, 0) };
  }

  private async processAggregate(aggregateId: string): Promise<number> {
    const client = await this.opts.pool.connect();
    const key = advisoryLockKey(aggregateId);
    try {
      const { rows: lockRows } = await client.query('select pg_try_advisory_lock($1) as locked', [key]);
      const locked = lockRows[0]?.locked === true;
      if (!locked) return 0; // another worker already owns this aggregate right now

      try {
        return await this.deliverPending(client, aggregateId);
      } finally {
        await client.query('select pg_advisory_unlock($1)', [key]);
      }
    } finally {
      client.release();
    }
  }

  private async deliverPending(client: Queryable, aggregateId: string): Promise<number> {
    const { rows } = await client.query(
      `select id, seq, aggregate_type, aggregate_id, event_type, payload, created_at, attempts
       from outbox_events
       where aggregate_id = $1 and published_at is null and dead_at is null
       order by seq asc
       limit $2`,
      [aggregateId, this.opts.eventsPerAggregate],
    );

    let delivered = 0;
    for (const row of rows) {
      const event = rowToEvent(row);
      try {
        await this.opts.deliver(event);
        await client.query('update outbox_events set published_at = now() where id = $1', [event.id]);
        delivered++;
      } catch (err) {
        await this.recordFailure(client, event, err);
        break; // preserve order: don't deliver a later event past a failed one
      }
    }
    return delivered;
  }

  private async recordFailure(client: Queryable, event: OutboxEvent, err: unknown): Promise<void> {
    const attempts = event.attempts + 1;
    const message = err instanceof Error ? err.message : String(err);
    if (attempts >= this.opts.maxAttempts) {
      await client.query(
        'update outbox_events set attempts = $2, last_error = $3, dead_at = now() where id = $1',
        [event.id, attempts, message],
      );
    } else {
      await client.query('update outbox_events set attempts = $2, last_error = $3 where id = $1', [
        event.id,
        attempts,
        message,
      ]);
    }
  }
}

function rowToEvent(row: Record<string, unknown>): OutboxEvent {
  return {
    id: row.id as string,
    seq: String(row.seq),
    aggregateType: row.aggregate_type as string,
    aggregateId: row.aggregate_id as string,
    eventType: row.event_type as string,
    payload: row.payload,
    createdAt: row.created_at as Date,
    attempts: row.attempts as number,
  };
}
