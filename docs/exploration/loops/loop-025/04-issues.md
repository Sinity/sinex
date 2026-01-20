Concrete issues to handle
- D-Bus watcher backpressure logic claims to “drop oldest” but does not evict any messages; it re-tries send on a full channel and then logs (likely just drops the new message). Fix or update the comment (`crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs:318-337`).
- Udev watcher uses `blocking_send` from notify callback, which can block the notifier thread when the channel is full. Consider `try_send` with drop metrics or a bounded ring buffer (`crate/nodes/sinex-system-ingestor/src/udev_watcher.rs:173-206`).
- Core `FileWatcher` uses `try_send` but does not track dropped events; add counters or structured logs for sustained drops (`crate/lib/sinex-core/src/types/utils/file_watcher.rs:74-101`).
- Coordination handoff channel ignores send errors and has no backpressure metrics; consider logging when the receiver is closed or when the send fails (`crate/lib/sinex-node-sdk/src/coordination.rs:500-520`).
