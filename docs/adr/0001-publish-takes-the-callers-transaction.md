# 1. publish() writes to the caller's own transaction, not a connection of its own

Status: accepted — 2026-09-26

## Context

A service that writes a business row and also needs to tell the rest of
the system about it has two honest-looking options that both have a
well-known failure mode: write the row, then separately publish a
message (a crash between the two loses the event even though the write
succeeded, or a retry after a timeout publishes it twice even though
the write actually failed) -- the "dual write" problem every message
queue's documentation eventually has to address. Two-phase commit across
a database and a message broker solves it in theory and is not used in
practice by almost anyone, because coordinating two different systems'
commit protocols is its own source of outages.

## Decision

`publish()` takes a `Queryable` -- the exact transaction the caller is
already running the business write on -- and inserts into
`outbox_events` using it, not a connection of its own:

```ts
await client.query('begin');
await client.query('insert into orders (...) values (...)');
await publish(client, { aggregateType: 'order', aggregateId: order.id, eventType: 'placed', payload: order });
await client.query('commit');
```

The event row and the business row now live or die together by
construction, because they are one transaction, not because outboxer
coordinated anything. `Queryable` is a two-line structural interface
(`docs/adr/0002`), not `pg.Client` specifically, so this works whatever
the caller's own transaction object actually is.

## Consequences

- outboxer never opens its own connection to do the insert, and has no
  transaction-boundary decision to make -- it participates in whichever
  transaction it's handed, which is what makes the atomicity guarantee
  actually hold rather than being a documentation claim about a library
  that secretly does two separate writes.
- The event is only as durable as the caller's own commit. If the
  caller's transaction never commits, the event correctly never exists
  either -- there is no scenario where `publish()` "succeeded" but the
  business write it was describing did not happen.
- What this does not solve: the *relay* (docs/adr/0002, 0003) still
  delivers at-least-once, not exactly-once, to whatever `deliver()`
  ends up calling. Atomicity between the outbox row and the business
  write is solved; atomicity between the outbox row and an external
  system receiving it is a different, harder problem this pattern
  deliberately doesn't attempt -- see ADR 2.
