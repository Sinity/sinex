Channel topology and backpressure audit

Summary
- The node runtime uses bounded mpsc channels for event emission and replay, with send-await backpressure; shutdown uses oneshot/watch channels.
- Ingestors vary in backpressure strategy: filesystem uses try_send with explicit drop counting, D-Bus tries to drop on full but does not actually evict, and udev uses blocking_send from a sync callback.
- Core utilities use bounded channels but generally only warn on overflow (no metrics), which can hide sustained drops.

Node SDK runtime (stream processor)
- Event emission pipeline:
  - Channel: `tokio::sync::mpsc::channel` (bounded, `DEFAULT_EVENT_CHANNEL_SIZE`).
  - Creation: `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:454`.
  - Producer: `EventEmitter::emit` sends via `.send(...).await` (backpressure by awaiting).
  - Consumer: `EventProcessor::run` batches from receiver and sends to transport (`crate/lib/sinex-node-sdk/src/event_processor.rs:110`).
- Shutdown signaling:
  - `oneshot::channel` for event processor shutdown (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:468`).
  - `watch::channel` for service shutdown in lifecycle/shutdown handlers (`crate/lib/sinex-node-sdk/src/lifecycle.rs:34`, `crate/lib/sinex-node-sdk/src/shutdown.rs:46`).

Node SDK automaton/coordination
- Confirmed event bridge for automata:
  - Channel: `mpsc::channel` (capacity 1024) (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:846`).
  - Producer: `RunnerConfirmedEventHandler::handle_confirmed` uses `.send().await`, propagates backpressure as `NodeError::Processing` if closed (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:74`).
- Leadership handoff queue:
  - Channel: `mpsc::channel(100)` (`crate/lib/sinex-node-sdk/src/coordination.rs:491`).
  - Producer: NATS subscriber loop sends `handoff_sender_clone.send(req).await` and ignores error; no metrics on drop/closed (`crate/lib/sinex-node-sdk/src/coordination.rs:500-520`).

Replay service
- Replay emitter pipeline:
  - Channel: `mpsc::channel(DEFAULT_EVENT_CHANNEL_SIZE)` and `oneshot::channel` for shutdown (`crate/lib/sinex-node-sdk/src/replay/service.rs:540-556`).
  - Uses same EventProcessor pipeline as runtime; backpressure is await-based.

System ingestor (system node)
- D-Bus watcher:
  - Internal queue: `mpsc::channel(DBUS_MESSAGE_CHANNEL_SIZE)` (`crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs:264`).
  - Send strategy: `try_send` in `start_receive` callback with comment "drop oldest", but the implementation does not evict any item; it retries and logs on full (`crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs:318-337`).
  - Effect: backpressure handling likely just drops new messages with a warning, not “drop oldest” as described.
- Unified journal/systemd and udev streams:
  - Channels: `mpsc::channel(WATCHER_CHANNEL_CAPACITY)` for per-source events (`crate/nodes/sinex-system-ingestor/src/unified_processor.rs:543`, `:567`, `:644`).
  - Forwarders: `spawn_forwarder` reads from channel and emits via EventEmitter; if emitter fails, forwarder breaks and channel closes (implicit backpressure via send await in watcher tasks).
- Udev watcher:
  - Inotify callback uses `blocking_send` into `mpsc` (`crate/nodes/sinex-system-ingestor/src/udev_watcher.rs:173-206`), which can block the notify thread when the queue is full.

Filesystem ingestor
- Watcher event channel:
  - Channel: `mpsc::channel(FS_WATCH_CHANNEL_SIZE)` (`crate/nodes/sinex-fs-ingestor/src/unified_processor.rs:650`).
  - Send strategy: `try_send` in notify callback; on full/closed it increments a drop counter and logs every 100 drops (`crate/nodes/sinex-fs-ingestor/src/unified_processor.rs:656-687`).
  - Effect: explicit drop strategy with visibility.

Core utilities
- FileWatcher (sinex-core):
  - Channel: `mpsc::channel(config.max_buffer_size)` (`crate/lib/sinex-core/src/types/utils/file_watcher.rs:64`).
  - Send strategy: `try_send` and warn on failure; no drop metrics (`crate/lib/sinex-core/src/types/utils/file_watcher.rs:74-101`).
- ResourceGuard cleanup:
  - Uses `oneshot::channel` to hand off resources to an async cleanup task (`crate/lib/sinex-core/src/types/utils/resource_guard.rs:19-37`).

Gateway
- Metrics emission uses `watch::channel(false)` for cancellation; kept alive for server lifetime (`crate/core/sinex-gateway/src/rpc_server.rs:1305-1318`).

