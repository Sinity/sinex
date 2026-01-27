# System Ingestor Implementation Details

The System Ingestor is a high-performance orchestration node that unifies events from several critical Linux subsystems into a single stream.

## Unified Journal Monitoring

- **Subprocess Consolidation**: Replaces multiple `journalctl` processes with a single unified watcher, reducing system overhead.
- **Dual-Stream Extraction**: Processes the journal JSON stream once and emits both raw journal entry events and synthesized systemd unit state events.
- **Deterministic IDs**: Generates event identifiers derived from the journal cursor and timestamp, ensuring idempotent ingestion and preventing duplicates across restarts.
- **Batched Cursor Persistence**: Flushes the journal cursor to disk at configurable intervals or event counts to optimize disk I/O while maintaining reliable resumption.

## D-Bus Signal Extraction

- **Multi-Bus Support**: Monitors both System and Session buses concurrently using `dbus-tokio`.
- **Specialized Payloads**: extracts high-value events from raw signals for:
  - Desktop Notifications
  - Media Playback (MPRIS)
  - Power & Battery state
  - NetworkManager connectivity
- **Worker Isolation**: Uses a bounded worker channel to separate D-Bus message reception from potentially slow event processing.

## Hardware & Device Events

- **udev Integration**: Monitors hardware lifecycle events (add, remove, change) for network interfaces, storage devices, and USB peripherals.
- **Inotify Sensing**: Watches sysfs class directories for immediate notification of device changes with sub-100ms latency.
- **Property Extraction**: Reads kernel-provided `uevent` files to capture detailed device metadata like vendor, model, and serial numbers.

## Lifecycle Management

- **Coordinated Shutdown**: Uses a system-wide cancellation token to ensure all sub-watchers and forwarders stop accepting new events and finalize their materials before the node exits.
- **Health Monitoring**: Periodically checks the liveness of each sub-watcher and reports their status via the `explore` interface.
