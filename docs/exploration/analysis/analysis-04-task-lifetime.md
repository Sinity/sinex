# Task Lifetime Analysis

Scope
- Identify async tasks spawned via tokio::spawn and evaluate shutdown/join semantics.

Method
- rg "tokio::spawn" and manual review of production code paths.

Patterns that look solid
- ingestd tracks JoinHandles and enforces shutdown/abort with timeouts (crate/core/sinex-ingestd/src/service.rs:167-236, 360-414).
- Stream processor leader heartbeat task is aborted on shutdown (crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:772-1110).

Orphan risk / untracked tasks
- JetStream consumer spawns three tasks and uses tokio::select on JoinHandles; when one finishes, the others keep running but their JoinHandles are dropped, so they are no longer supervised (crate/lib/sinex-node-sdk/src/jetstream_consumer.rs:152-203).
- Schema broadcast listener is spawned without storing a handle; it never receives a shutdown signal (crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:150-213).
- ResourceGuard spawns cleanup tasks from Drop; these tasks are detached and unobserved by design (crate/lib/sinex-core/src/types/utils/resource_guard.rs:22-81).

Impact
- Potential background work continuing after the parent component has shut down, with log noise or dangling NATS subscriptions.

Follow-ups
- For long-lived background tasks, consider storing JoinHandles and cancelling them on shutdown.
- For tokio::select over JoinHandles, explicitly abort the remaining tasks on the first completion.
