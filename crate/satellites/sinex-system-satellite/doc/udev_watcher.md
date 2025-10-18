# udev Watcher

`udev_watcher.rs` observes hardware events, emitting notifications when devices
are added, removed, or change state.

- Filters events based on configured subsystem/attribute matches.
- Converts raw udev payloads into structured Sinex events with metadata.
- Cooperates with the unified processor to merge hardware signals with other
  system sources.
