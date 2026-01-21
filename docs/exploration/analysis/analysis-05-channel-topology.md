# Channel Topology and Backpressure

Scope
- Map key mpsc/oneshot usages and how backpressure is handled (await vs drop).

Method
- rg "mpsc::channel" and review emit/handler code paths.

Key topology points
- Core event pipeline uses bounded mpsc channels; EventEmitter awaits send, so backpressure propagates upstream (crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs:92-118).
- System ingestor uses bounded watcher channels with explicit capacity (WATCHER_CHANNEL_CAPACITY = 1024) to avoid unbounded growth (crate/nodes/sinex-system-ingestor/src/unified_processor.rs:94-105).
- Confirmation to automata uses try_send and drops when full, preferring liveness over completeness (crate/lib/sinex-node-sdk/src/automaton_base.rs:293-305).
- BlobManager event emission uses try_send and drops when full, with a warning (crate/lib/sinex-node-sdk/src/annex/blob_manager.rs:121-147).

Backpressure behavior
- Awaiting send (EventEmitter) provides explicit backpressure and ensures no drop unless channel is closed.
- try_send in automata and blob emission trades completeness for responsiveness; drops are logged but not surfaced.

Follow-ups
- Consider per-channel metrics for drop counts to make loss observable in ops dashboards.
- Evaluate whether drop-on-full is acceptable for confirmations; if not, switch to await send or add spill-to-disk behavior.
