# 2. Delivery is at-least-once, on purpose, and the library says so

Status: accepted — 2026-09-26

## Context

Once an event is safely in the outbox (ADR 1), something still has to
call `deliver()` and then mark the row published -- and those are two
separate steps with a real window between them: the relay's process
could crash, or lose its database connection, after `deliver()` has
already succeeded but before the `update ... set published_at` commits.
The next poll (by this relay or another) sees the row as still pending
and calls `deliver()` again. "Exactly-once" delivery across two
independent systems (the database and whatever `deliver()` talks to) is
not achievable without both sides participating in the same distributed
transaction -- which is the two-phase-commit problem ADR 1 exists
specifically to avoid taking on.

## Decision

outboxer is at-least-once and states this as the contract, not a bug to
eventually fix. `deliver()` may be called more than once for the same
event; the caller is responsible for making whatever it does in
response to an event idempotent (using the event's own `id` as an
idempotency key is the natural choice, since it is stable and unique per
event). The `Queryable` interface (`src/types.ts`) is deliberately
minimal for the same reason ADR 1's transaction-passing works at all: it
is a two-method structural shape any `pg.Client`, `pg.PoolClient` or
compatible wrapper already satisfies, not a type this library needs
callers to construct specially.

## Consequences

- The tests that matter most here don't assert "no duplicates ever" --
  they assert the actual guarantee: `tests/integration/outboxer.test.ts`'s
  "never lets two concurrent relays deliver the same aggregate's events
  twice" proves the advisory-lock mechanism (ADR 3) prevents the *common*
  duplicate-delivery case (two workers racing on the same aggregate),
  which is the failure mode actually worth engineering against; the
  crash-between-deliver-and-commit window is inherent to at-least-once
  and is handled by documentation and idempotency, not by trying to make
  it impossible.
- A consumer that isn't idempotent will eventually double-process an
  event. That is a real, stated cost of this design, not a hidden one --
  the alternative (best-effort at-most-once, silently dropping an event
  on a crash) is worse for the systems this pattern actually gets used
  for (billing, order fulfillment, anything where a missed event is more
  expensive than a duplicate one a consumer can dedupe).
- There is no built-in dead-letter *notification* -- an event that hits
  `maxAttempts` gets `dead_at` set and stops blocking its aggregate's
  queue (docs/adr/0003), but nothing pages anyone. A production
  deployment needs its own alert on `select count(*) from outbox_events
  where dead_at is not null`; that query is one line, and shipping a
  whole alerting integration wasn't judged worth the added surface for
  v0.1.0.
