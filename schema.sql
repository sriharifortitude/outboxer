-- The outbox table. Not a queue in its own right -- a durable staging
-- area written to inside the caller's own transaction (see
-- docs/adr/0001-publish-takes-the-callers-transaction.md), then drained
-- by the relay in this package.
--
-- seq is the ordering key within one aggregate, not created_at: two
-- events committed in the same transaction can share a timestamp down
-- to the microsecond on a fast machine, but a bigserial is always
-- strictly increasing in insertion order.
create table if not exists outbox_events (
  id            uuid primary key default gen_random_uuid(),
  seq           bigserial not null,
  aggregate_type text not null,
  aggregate_id   text not null,
  event_type     text not null,
  payload        jsonb not null,
  created_at     timestamptz not null default now(),
  published_at   timestamptz,
  attempts       integer not null default 0,
  last_error     text,
  dead_at        timestamptz
);

-- The relay's core query is "which aggregates have pending work,"
-- filtered to rows that are neither published nor given up on.
create index if not exists outbox_events_pending_idx
  on outbox_events (aggregate_id, seq)
  where published_at is null and dead_at is null;
