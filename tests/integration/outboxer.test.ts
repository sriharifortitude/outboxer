import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import pg from 'pg';
import { publish } from '../../src/publish.js';
import { Relay } from '../../src/relay.js';
import type { OutboxEvent } from '../../src/types.js';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

beforeAll(async () => {
  await pool.query(readFileSync(new URL('../../schema.sql', import.meta.url), 'utf8'));
  await pool.query('create table if not exists test_orders (id text primary key, status text not null)');
});

beforeEach(async () => {
  await pool.query('delete from outbox_events');
  await pool.query('delete from test_orders');
});

afterAll(async () => {
  await pool.end();
});

async function queryRows<T extends Record<string, unknown>>(sql: string, params: unknown[]): Promise<T[]> {
  const result = await pool.query<T>(sql, params);
  return result.rows;
}

async function queryOne<T extends Record<string, unknown>>(sql: string, params: unknown[]): Promise<T> {
  const rows = await queryRows<T>(sql, params);
  const row = rows[0];
  if (row === undefined) throw new Error('expected at least one row');
  return row;
}

function recorder() {
  const delivered: OutboxEvent[] = [];
  const deliver = (event: OutboxEvent): Promise<void> => {
    delivered.push(event);
    return Promise.resolve();
  };
  return { delivered, deliver };
}

describe('publish', () => {
  it('commits atomically with the callers own transaction', async () => {
    const client = await pool.connect();
    try {
      await client.query('begin');
      await client.query('insert into test_orders (id, status) values ($1, $2)', ['order-1', 'placed']);
      await publish(client, { aggregateType: 'order', aggregateId: 'order-1', eventType: 'placed', payload: { total: 10 } });
      await client.query('commit');
    } finally {
      client.release();
    }

    const orders = await pool.query('select * from test_orders where id = $1', ['order-1']);
    const events = await pool.query('select * from outbox_events where aggregate_id = $1', ['order-1']);
    expect(orders.rows).toHaveLength(1);
    expect(events.rows).toHaveLength(1);
  });

  it('rolls back with the callers own transaction -- neither the business row nor the event survive', async () => {
    const client = await pool.connect();
    try {
      await client.query('begin');
      await client.query('insert into test_orders (id, status) values ($1, $2)', ['order-2', 'placed']);
      await publish(client, { aggregateType: 'order', aggregateId: 'order-2', eventType: 'placed', payload: {} });
      await client.query('rollback');
    } finally {
      client.release();
    }

    const orders = await pool.query('select * from test_orders where id = $1', ['order-2']);
    const events = await pool.query('select * from outbox_events where aggregate_id = $1', ['order-2']);
    expect(orders.rows).toHaveLength(0);
    expect(events.rows).toHaveLength(0);
  });
});

describe('Relay ordering', () => {
  it('delivers events for one aggregate strictly in publish order', async () => {
    const aggregateId = `order-${randomUUID()}`;
    for (const eventType of ['placed', 'paid', 'shipped']) {
      await publish(pool, { aggregateType: 'order', aggregateId, eventType, payload: {} });
    }
    const { delivered, deliver } = recorder();
    const relay = new Relay({ pool, deliver });

    await relay.pollOnce();

    expect(delivered.map((e) => e.eventType)).toEqual(['placed', 'paid', 'shipped']);
  });

  it('stops at the first failure and never delivers a later event out of order', async () => {
    const aggregateId = `order-${randomUUID()}`;
    for (const eventType of ['placed', 'paid', 'shipped']) {
      await publish(pool, { aggregateType: 'order', aggregateId, eventType, payload: {} });
    }
    const deliveredTypes: string[] = [];
    const relay = new Relay({
      pool,
      deliver: (event) => {
        if (event.eventType === 'paid') throw new Error('payment provider is down');
        deliveredTypes.push(event.eventType);
        return Promise.resolve();
      },
    });

    await relay.pollOnce();

    expect(deliveredTypes).toEqual(['placed']); // "shipped" must never be attempted
    const rows = await queryRows<{ event_type: string; attempts: number; published_at: Date | null }>(
      'select event_type, attempts, published_at from outbox_events where aggregate_id = $1 order by seq',
      [aggregateId],
    );
    expect(rows[0]?.event_type).toBe('placed');
    expect(rows[0]?.published_at).toBeInstanceOf(Date);
    expect(rows[1]).toMatchObject({ event_type: 'paid', attempts: 1, published_at: null });
    expect(rows[2]).toMatchObject({ event_type: 'shipped', attempts: 0, published_at: null });
  });

  it('marks an event dead after maxAttempts and stops retrying it', async () => {
    const aggregateId = `order-${randomUUID()}`;
    await publish(pool, { aggregateType: 'order', aggregateId, eventType: 'placed', payload: {} });
    let attempts = 0;
    const relay = new Relay({
      pool,
      maxAttempts: 2,
      deliver: () => {
        attempts++;
        return Promise.reject(new Error('permanently broken'));
      },
    });

    await relay.pollOnce();
    await relay.pollOnce();
    expect(attempts).toBe(2);

    const row = await queryOne<{ attempts: number; dead_at: Date | null }>(
      'select attempts, dead_at from outbox_events where aggregate_id = $1',
      [aggregateId],
    );
    expect(row.attempts).toBe(2);
    expect(row.dead_at).not.toBeNull();

    await relay.pollOnce(); // a third poll must not attempt a dead event
    expect(attempts).toBe(2);
  });

  it('processes different aggregates independently -- one aggregates failure does not block another', async () => {
    const brokenId = `order-${randomUUID()}`;
    const healthyId = `order-${randomUUID()}`;
    await publish(pool, { aggregateType: 'order', aggregateId: brokenId, eventType: 'placed', payload: {} });
    await publish(pool, { aggregateType: 'order', aggregateId: healthyId, eventType: 'placed', payload: {} });

    const { delivered, deliver: healthyDeliver } = recorder();
    const relay = new Relay({
      pool,
      deliver: async (event) => {
        if (event.aggregateId === brokenId) throw new Error('broken');
        await healthyDeliver(event);
      },
    });

    await relay.pollOnce();

    expect(delivered).toHaveLength(1);
    expect(delivered[0]?.aggregateId).toBe(healthyId);
  });
});

describe('Relay concurrency', () => {
  it('never lets two concurrent relays deliver the same aggregates events twice', async () => {
    const aggregateId = `order-${randomUUID()}`;
    for (const eventType of ['placed', 'paid']) {
      await publish(pool, { aggregateType: 'order', aggregateId, eventType, payload: {} });
    }

    const deliveryCounts = new Map<string, number>();
    const slowDeliver = async (event: OutboxEvent) => {
      // an artificial delay opens a real race window: without the
      // advisory lock, a second relay's poll could interleave and pick
      // up the same still-unpublished rows before the first relay
      // marks them published.
      await new Promise((resolve) => setTimeout(resolve, 50));
      deliveryCounts.set(event.id, (deliveryCounts.get(event.id) ?? 0) + 1);
    };

    const relayA = new Relay({ pool, deliver: slowDeliver });
    const relayB = new Relay({ pool, deliver: slowDeliver });

    await Promise.all([relayA.pollOnce(), relayB.pollOnce()]);

    expect([...deliveryCounts.values()]).toEqual([1, 1]);
  });

  it('releases the lock so a later poll can process the same aggregate again', async () => {
    const aggregateId = `order-${randomUUID()}`;
    await publish(pool, { aggregateType: 'order', aggregateId, eventType: 'placed', payload: {} });
    const { delivered, deliver } = recorder();
    const relay = new Relay({ pool, deliver });

    await relay.pollOnce();
    expect(delivered).toHaveLength(1);

    await publish(pool, { aggregateType: 'order', aggregateId, eventType: 'paid', payload: {} });
    await relay.pollOnce();

    expect(delivered.map((e) => e.eventType)).toEqual(['placed', 'paid']);
  });
});
