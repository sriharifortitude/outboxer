// A runnable demo: places three "orders," publishing an event for each
// in the same transaction as the order row, then runs one relay poll
// and prints what it delivered. Requires DATABASE_URL (see
// docker-compose.yml / .env.example) and schema.sql already applied.
//
//   npm run db:up
//   psql "$DATABASE_URL" -f schema.sql
//   npm run demo
import pg from 'pg';
import { publish } from '../src/publish.js';
import { Relay } from '../src/relay.js';
import type { OutboxEvent } from '../src/types.js';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function placeOrder(id: string, total: number): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    await client.query(
      `create table if not exists demo_orders (id text primary key, total integer not null)`,
    );
    await client.query('insert into demo_orders (id, total) values ($1, $2)', [id, total]);
    await publish(client, { aggregateType: 'order', aggregateId: id, eventType: 'placed', payload: { total } });
    await client.query('commit');
  } catch (err) {
    await client.query('rollback');
    throw err;
  } finally {
    client.release();
  }
}

function deliver(event: OutboxEvent): Promise<void> {
  console.log(`delivering ${event.eventType} for ${event.aggregateId} (attempt ${event.attempts + 1})`);
  return Promise.resolve();
}

async function main(): Promise<void> {
  await placeOrder('order-1', 4200);
  await placeOrder('order-2', 1500);
  await placeOrder('order-3', 9900);

  const relay = new Relay({ pool, deliver });
  const { delivered } = await relay.pollOnce();
  console.log(`relay delivered ${delivered} events in one poll`);

  await pool.end();
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
