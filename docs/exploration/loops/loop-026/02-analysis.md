Task lifetime and shutdown audit

Summary
- Ingestd tracks background tasks and aborts them on shutdown; this is the most explicit lifecycle management in the repo.
- Gateway and several node SDK subsystems spawn long-lived tasks without cancellation or join, relying on process exit or drop to terminate.
- Some tasks have shutdown channels allocated but never signaled, suggesting incomplete shutdown plumbing.

Ingestd (explicitly tracked tasks)
- `IngestService::run` starts multiple background tasks and records their JoinHandles (`crate/core/sinex-ingestd/src/service.rs:140-215`).
- `wait_for_tasks` joins each handle with timeout and aborts hung tasks (`crate/core/sinex-ingestd/src/service.rs:382-412`).
- Shutdown sets a flag and uses the same `wait_for_tasks` logic before closing the DB pool (`crate/core/sinex-ingestd/src/service.rs:414-433`).

Gateway (mostly detached tasks)
- Gateway metrics emission spawns a periodic task with a cancel watch channel but no cancellation is ever triggered (`crate/core/sinex-gateway/src/rpc_server.rs:1305-1318`, `crate/core/sinex-gateway/src/gateway_metrics.rs:210-246`). The JoinHandle is not awaited.
- Rate limiter cleanup runs in an endless loop (`crate/core/sinex-gateway/src/rate_limit.rs:169-178`); handle is not awaited.
- Replay control server spawns a subscription loop without storing the handle (`crate/core/sinex-gateway/src/replay_control.rs:343-363`).
- Replay telemetry spawns a periodic sampler with no cancellation path (`crate/core/sinex-gateway/src/replay_control.rs:567-589`).
- GatewayAuth token file watcher uses `std::thread::spawn` and loops forever with sleep, no shutdown path (`crate/core/sinex-gateway/src/rpc_server.rs:178-255`).

Node SDK runtime (partially tracked)
- Event processor task is spawned and stored but never joined or aborted; shutdown does not signal its oneshot shutdown sender (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:468-574`, `:1090-1102`).
  - This likely relies on dropping all senders to close the channel, but the JoinHandle is left unobserved.
- Automaton event bridge spawns a JetStream consumer and does not join/abort it (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:864-884`).
- Stage-as-You-Go reconciliation spawns a task with a Drop impl that sends shutdown and aborts the handle (explicit cleanup) (`crate/lib/sinex-node-sdk/src/stage_as_you_go.rs:128-170`, `:240-282`).
- Lifecycle manager uses JoinSet and shuts it down after the main task completes (`crate/lib/sinex-node-sdk/src/lifecycle.rs:199-251`, `:362-389`).

Node ingestors (watcher tasks)
- System ingestor spawns watcher tasks and stores them in WatcherHandle; Drop aborts the tasks (`crate/nodes/sinex-system-ingestor/src/unified_processor.rs:107-150`). Shutdown calls `finalize_watcher_handle` which drops the handle (`crate/nodes/sinex-system-ingestor/src/unified_processor.rs:396-428`).
- D-Bus watcher spawns a worker task without a tracked handle; the task exits when the mpsc receiver closes, but there is no explicit join (`crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs:276-312`).

Core utilities
- ResourceGuard spawns a cleanup task and triggers it via oneshot; Drop spawns another task to deliver the resource if not already sent (`crate/lib/sinex-core/src/types/utils/resource_guard.rs:20-66`). There is no join/await for cleanup completion.

