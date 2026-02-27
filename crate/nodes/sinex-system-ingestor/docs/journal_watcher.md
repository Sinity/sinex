# Journal Watcher

`journal_watcher.rs` tails the systemd journal and produces events for relevant
service notifications, errors, and log patterns.

- Applies filters configured in the crate configuration.
- Handles cursor management so restarts resume from the last seen entry.
- Works in tandem with `unified_node` to avoid duplicate dispatches.
