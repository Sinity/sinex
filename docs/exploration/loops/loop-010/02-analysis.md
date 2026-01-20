# Loop 010 - Confirmation Buffer and Backpressure

Scope
- `JetStreamEventConsumer` and `ConfirmationBuffer` in `sinex-node-sdk`.

Flow Summary
1) Raw event stream
- Raw events are pulled from `events.raw.>` and parsed into `ProvisionalEvent`.
- Each raw event is inserted into `ConfirmationBuffer` and immediately acked.
  - `crate/lib/sinex-node-sdk/src/jetstream_consumer.rs` `consume_raw_events()` calls `buffer.add_provisional(event)` then `msg.ack().await`.

2) Confirmation stream
- Confirmations are pulled from `events.confirmations.>` and parsed into `EventConfirmation`.
- On confirmation, the buffer removes the matching provisional and forwards it to the confirmed handler.
  - `crate/lib/sinex-node-sdk/src/jetstream_consumer.rs` `consume_confirmations()` calls `buffer.confirm(...)` then `confirmed_handler.handle_confirmed(&event)`.
- If no provisional is found, confirmation is logged and dropped.
  - `consume_confirmations()` logs `Confirmation for unknown event` and still acks.

3) Timeout cleanup
- A periodic task scans for timed-out events and removes them from the buffer.
  - `check_timeouts()` uses `ConfirmationBuffer::check_timeouts()` and `remove_timed_out()`.
- Optional rollback is invoked for timed-out provisional events.

Backpressure and Buffer Limits
- `ConfirmationBuffer` has no size cap; it stores all pending events in a `HashMap`.
  - `crate/lib/sinex-node-sdk/src/confirmation_handler.rs` `ConfirmationBuffer` has no limit or eviction.
- Raw events are acked immediately, so JetStream `max_ack_pending` does not apply to confirmation backlog.
  - `JetStreamEventConsumerConfig` has `max_ack_pending`, but the code acknowledges raw messages after buffering.

Ordering/Timing Risks
- Confirmations can be received before the corresponding raw event is buffered.
  - Separate consumers read from two streams; `consume_confirmations()` drops unknown confirmations.
- Dropped confirmations leave the provisional event in the buffer until timeout cleanup.
  - There is no mechanism to retry or replay confirmations that arrived early.

Findings
- Confirmation backlog is unbounded; memory use can grow if confirmations lag or are dropped.
- Immediate ack on raw events removes JetStream backpressure from the confirmation pipeline.
- Confirmation ordering is not enforced; early confirmations are dropped.

Risks
- High-volume streams can fill the buffer and increase memory usage without flow control.
- Out-of-order confirmations can cause unnecessary timeouts and rollback paths.

Opportunities
- Introduce a size cap or semaphore for `ConfirmationBuffer` to apply backpressure.
- Delay acking raw events until confirmation or until buffer capacity allows.
- Store unmatched confirmations briefly to handle ordering skew.
