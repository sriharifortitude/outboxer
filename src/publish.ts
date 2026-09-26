import type { NewEvent, Queryable } from './types.js';

/**
 * Records an event in the outbox, using the caller's own transaction --
 * `tx` must be the same client/transaction the business write that
 * caused this event is running on, not a fresh connection. That's the
 * entire mechanism: if the transaction commits, the business row and
 * the event both exist; if it rolls back, neither does. See
 * docs/adr/0001-publish-takes-the-callers-transaction.md.
 */
export async function publish(tx: Queryable, event: NewEvent): Promise<void> {
  await tx.query(
    `insert into outbox_events (aggregate_type, aggregate_id, event_type, payload)
     values ($1, $2, $3, $4)`,
    [event.aggregateType, event.aggregateId, event.eventType, JSON.stringify(event.payload)],
  );
}
