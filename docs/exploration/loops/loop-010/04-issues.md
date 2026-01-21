# Loop 010 - Concrete Issues

1) Confirmation buffer has no size limit or backpressure
- Evidence: `ConfirmationBuffer` stores pending events in an unbounded `HashMap` and raw messages are immediately acked (`crate/lib/sinex-node-sdk/src/confirmation_handler.rs`, `crate/lib/sinex-node-sdk/src/jetstream_consumer.rs`).
- Impact: confirmation lag can cause unbounded memory growth and loss of flow control.

2) Confirmations received before raw events are dropped
- Evidence: `consume_confirmations()` logs and discards confirmations when `buffer.confirm()` returns `None` (`crate/lib/sinex-node-sdk/src/jetstream_consumer.rs`).
- Impact: out-of-order delivery can force unnecessary timeouts/rollbacks for events that were already persisted.
