# outboxer

[![CI](https://github.com/sriharifortitude/outboxer/actions/workflows/ci.yml/badge.svg)](https://github.com/sriharifortitude/outboxer/actions/workflows/ci.yml)

A transactional outbox for Postgres. Writing a business row and telling
the rest of the system about it are two operations against two
different systems -- if you do them separately, a crash between the two
either loses the event or duplicates it, the "dual write" problem every
message queue's docs eventually mention and few actually solve.
outboxer solves it the way the pattern is meant to be solved: the event
is inserted in the *same transaction* as the business write, so it
lives or dies with it by construction, not by any coordination this
library has to get right at runtime.

```ts
import { publish, Relay } from 'outboxer';

// 1. Publish inside your own transaction, alongside the write it describes.
const client = await pool.connect();
await client.query('begin');
await client.query('insert into orders (id, total) values ($1, $2)', [order.id, order.total]);
await publish(client, {
  aggregateType: 'order',
  aggregateId: order.id,
  eventType: 'placed',
  payload: order,
});
await client.query('commit');

// 2. Somewhere else -- a worker process -- drain the outbox.
const relay = new Relay({
  pool,
  deliver: async (event) => {
    await webhookClient.send(event); // must be idempotent: see "at-least-once" below
  },
});
relay.start();
```

## What it does

- **`publish()` takes your transaction, not a connection of its own.**
  There is no separate step where outboxer "sends" the event to
  anywhere -- it's a row in your own database, written by your own
  commit. See [ADR 1](docs/adr/0001-publish-takes-the-callers-transaction.md).
- **Per-aggregate ordering, not a single global worker.** Events for
  the same `aggregateId` are delivered strictly in the order they were
  published; events for *different* aggregates are delivered
  concurrently with each other. A Postgres advisory lock keyed on the
  aggregate id is what makes both halves of that true at once -- see
  [ADR 3](docs/adr/0003-per-aggregate-ordering-with-advisory-locks.md).
- **A failure stops that aggregate's queue, not the whole relay.** If
  `deliver()` throws, that event's `attempts` increments and processing
  for *that aggregate* stops for this poll -- a later event is never
  delivered ahead of one that hasn't succeeded, and other aggregates
  keep moving.
- **Dead-lettering without a separate table.** After `maxAttempts`, an
  event is marked `dead_at` and stops blocking its aggregate; nothing
  pages anyone automatically (a real, stated gap -- see ADR 2).
- **ORM-agnostic on purpose.** `publish()` and `Relay` take a
  `Queryable` -- a two-method structural shape, not `pg.Client`
  specifically -- so whatever you already use to run your own
  transaction can be handed straight in as long as it has a compatible
  `.query()`.

## At-least-once, honestly

`deliver()` can be called more than once for the same event -- a crash
between a successful delivery and the row being marked `published_at`
is possible and not specially handled, because handling it would mean
taking on the two-phase-commit problem this pattern exists to avoid.
Use the event's `id` as an idempotency key on the receiving end. See
[ADR 2](docs/adr/0002-at-least-once-never-exactly-once.md) for the full
reasoning, including why this is judged the right trade rather than a
gap to eventually close.

## Running it

```bash
npm install outboxer pg
```

```sql
-- schema.sql in this repo -- run it once against your database.
```

```bash
docker compose up -d --wait   # Postgres 17 on 127.0.0.1:5437, for local dev
psql "$DATABASE_URL" -f schema.sql
npm run demo                  # examples/demo.ts: places three orders, runs one relay poll
```

## Checks

```bash
npm run typecheck
npm run lint
npm test                 # 5 tests: the FNV-1a advisory-lock hash, checked against
                          # published test vectors -- no database
npm run test:integration # against real Postgres: transactional atomicity (commit
                          # and rollback both proven, not just the happy path),
                          # per-aggregate ordering, a failure never skipping ahead,
                          # dead-lettering after maxAttempts, and -- the test worth
                          # reading first -- two concurrent Relay instances racing
                          # on the same aggregate, proving neither event is ever
                          # delivered twice
```

The concurrency test runs two `Relay`s against the same aggregate at
once with an artificial delay in `deliver()`, specifically to open the
race window that would exist without the advisory lock, then asserts
each event was delivered exactly once. That -- not a unit test of the
lock function in isolation -- is what actually proves the ordering
claim in the section above.

## What it deliberately does not do

- **Not a general-purpose queue.** If your workload doesn't need
  per-aggregate ordering, a plain job queue will out-parallelize this
  -- an aggregate with a continuous stream of events is a serial
  bottleneck by design (ADR 3's consequences).
- **No dead-letter alerting.** `dead_at` gets set; nothing notifies
  anyone. `select count(*) from outbox_events where dead_at is not
  null` is the one-line check a real deployment should alert on.
- **No built-in retention/cleanup job.** Published rows accumulate;
  this library doesn't decide when it's safe to delete them, since
  that depends on how long you want replay/audit capability for, which
  is a decision this library shouldn't make on your behalf.
- **Postgres only.** The advisory-lock mechanism (`pg_advisory_lock`)
  is Postgres-specific; there is no MySQL or SQLite backend.

## Licence

MIT.
