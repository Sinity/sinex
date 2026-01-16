# systemd Watcher

`systemd_watcher.rs` tracks systemd unit state transitions.

- Uses the `nix` crate to query systemd APIs.
- Produces events when units start, stop, fail, or reload.
- Shares checkpoint data with other watchers to keep ordering consistent.
