# systemd Integration

`systemd_integration.rs` contains the shared primitives for interacting with
systemd and journald.

- Wraps the `nix` bindings used by watchers.
- Exposes helper functions for unit queries and journal cursors.
- Provides conversions from raw system objects into the payload structures.
