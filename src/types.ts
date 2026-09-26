/**
 * The minimal shape outboxer needs from a database client -- deliberately
 * not `pg.PoolClient` or `pg.Pool` specifically. `publish()` is called
 * from inside the *caller's* transaction (see docs/adr/0001), and that
 * transaction might be a raw `pg` client, or a wrapper around one (a
 * query builder's transaction object, for instance) -- anything
 * structurally compatible works without outboxer needing to depend on
 * it. `pg.PoolClient` and `pg.Pool` both already satisfy this shape.
 */
export interface Queryable {
  query: (sql: string, params?: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>;
}

export interface NewEvent {
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  payload: unknown;
}

export interface OutboxEvent extends NewEvent {
  id: string;
  /** The ordering key within this aggregate. A string, not a bigint --
   *  node-postgres returns bigint/bigserial columns as strings by
   *  default (to avoid silent precision loss past 2^53), and outboxer
   *  never does arithmetic on it, only compares and forwards it, so
   *  there's no reason to ask every consumer to configure a custom
   *  type parser just to get a number here. */
  seq: string;
  createdAt: Date;
  attempts: number;
}
