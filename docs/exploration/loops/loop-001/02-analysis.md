# Loop 001 - Graceful Shutdown and Task Cancellation Map

Scope
- Core binaries: ingestd, gateway.
- Node runtime: stream processor runner and event processor.

Shutdown Map (evidence-based)

1. Ingestd (daemon)
- Entry point handles SIGINT/SIGTERM and calls service shutdown.
  - `crate/core/sinex-ingestd/src/main.rs` sets `shutdown_signal` and calls `service.shutdown()` on signal.
- Service uses an atomic flag and a polling loop to signal tasks.
  - `crate/core/sinex-ingestd/src/service.rs` `shutdown_signal()` polls `shutdown_flag` every 100ms.
- Background tasks are tracked and joined with timeout, then aborted.
  - `crate/core/sinex-ingestd/src/service.rs` `track_task()` pushes JoinHandles; `wait_for_tasks()` waits up to 5s and aborts on timeout.
- Shutdown path is consistent: signal -> set flag -> wait for tasks -> close pool.
  - `crate/core/sinex-ingestd/src/service.rs` `shutdown()` sets `shutdown_flag`, calls `wait_for_tasks()`, closes pool.

2. Gateway (RPC + native messaging)
- Entry point handles SIGINT/SIGTERM and exits the main select.
  - `crate/core/sinex-gateway/src/main.rs` uses `tokio::select!` between server run and shutdown signal.
- No explicit shutdown on services or replay control tasks.
  - `crate/core/sinex-gateway/src/service_container.rs` creates services and replay control client, but no shutdown hook exists.
  - `crate/core/sinex-gateway/src/replay_control.rs` spawns a subscription loop without a cancellation trigger.
- Practical effect: runtime exit ends tasks; no structured shutdown sequence for in-flight work.

3. Node runtime (StreamProcessorRunner + EventProcessor)
- Runner creates shutdown channels for the event processor but never signals them.
  - `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` creates `processor_shutdown_sender` and stores it in `event_processor_shutdown`, but there are no uses of the sender (search shows only assignment).
- Event processor expects a shutdown signal and flushes on shutdown.
  - `crate/lib/sinex-node-sdk/src/event_processor.rs` `EventProcessor::run()` selects on `self.shutdown` and flushes pending events before exit.
- The runner never sends shutdown or awaits the event processor task.
  - `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` stores `event_processor_handle` but never awaits or aborts it.
- Implication: the event processor can outlive `StreamProcessorRunner::shutdown()` and may keep running until runtime exit.

Findings
- Ingestd has a complete shutdown chain with task tracking and abort-on-timeout.
- Gateway relies on runtime termination rather than explicit service teardown.
- Node SDK stream runner builds a shutdown path for the event processor but does not trigger it; the event processor join handle is never awaited.

Risks
- Event processor task may continue publishing after a node has "shut down" at the runner level, especially in long-lived runtimes or tests that reuse the runtime.
- Lack of an explicit shutdown path in gateway services may drop in-flight work (e.g., replay control requests) on process exit.

Opportunities
- Wire `StreamProcessorRunner::shutdown()` to send the shutdown signal and await the event processor task.
- Expose a `ServiceContainer::shutdown()` for the gateway and call it on shutdown signals to close pools and stop replay control tasks cleanly.
