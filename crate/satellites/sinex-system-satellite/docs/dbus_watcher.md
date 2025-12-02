# D-Bus Watcher

`dbus_watcher.rs` listens to system D-Bus signals, converts them into Sinex
events, and forwards them to the unified processor.

- Subscribes to configured D-Bus paths and interfaces.
- Normalises payloads into strongly typed structures in `payloads`.
- Emits provenance metadata (source, host, timestamp) with each event.
