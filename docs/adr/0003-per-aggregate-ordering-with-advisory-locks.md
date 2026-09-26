# 3. Per-aggregate ordering via Postgres advisory locks, not a single global worker

Status: accepted — 2026-09-26

## Context

Most events that matter are only meaningful in order relative to *their
own* aggregate: an order's `paid` event has to be processed after its
`placed` event, but an unrelated order's events have no ordering
relationship to it at all. A relay that guarantees ordering by running a
single worker for the whole outbox gets correctness for free but throws
away all the parallelism a workload with many independent aggregates
actually has. A relay that runs several workers pulling from the same
queue with no coordination gets parallelism but can deliver two events
for the *same* aggregate out of order, or twice at once, the moment two
workers happen to pick up that aggregate's rows in the same poll.

## Decision

`Relay.pollOnce()` (`src/relay.ts`) finds every aggregate with pending
events, then processes each one independently: before touching an
aggregate's events, it takes a Postgres session-level advisory lock
keyed on a hash of the aggregate id (`src/hash.ts`, FNV-1a into the
signed 64-bit range `pg_advisory_lock` expects) via `pg_try_advisory_lock`
-- non-blocking, so a worker that loses the race simply skips that
aggregate this poll rather than queueing up behind it. The lock is taken
and released on one dedicated connection checked out from the pool for
exactly that aggregate's processing, because advisory locks are scoped
to the session that took them, not to a logical transaction; a pooled
per-query client would release the lock the instant the query
finished, defeating the whole point.

Once the lock is held, that aggregate's pending events are fetched in
`seq` order (a `bigserial`, not `created_at` -- two events inserted in
the same transaction can share a timestamp down to the microsecond) and
delivered one at a time, stopping at the first failure rather than
skipping ahead, so a later event is never delivered before an earlier
one that hasn't succeeded yet.

## Consequences

- Different aggregates process fully in parallel with each other --
  `tests/integration/outboxer.test.ts`'s "processes different aggregates
  independently" test proves one aggregate's permanent failure never
  blocks another's delivery. Ordering is a per-aggregate guarantee, not
  a whole-outbox one, which is the trade this pattern is named for.
- Running more `Relay` instances than there are aggregates with pending
  work doesn't help -- the extra instances will simply lose every
  `pg_try_advisory_lock` race and poll uselessly. This is expected, not
  a bug: aggregate-level parallelism has a ceiling equal to the number
  of aggregates actually being written to concurrently.
- A single aggregate with a very large, continuous stream of events is
  a serial bottleneck by design -- everything for that aggregate goes
  through whichever one worker currently holds its lock. That is the
  correct trade for an aggregate whose events must stay ordered; a
  workload that doesn't need per-key ordering at all would be better
  served by a plain queue, not this pattern, and outboxer says so in the
  README's "what it deliberately does not do" rather than pretending to
  be a general-purpose queue.
