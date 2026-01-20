Concrete issues to handle
- Replace blocking checkpoint file reads/writes called from async paths with `tokio::fs` or `tokio::task::spawn_blocking` in `crate/lib/sinex-node-sdk/src/simple_processor.rs:319` and `:782` (backed by `crate/lib/sinex-node-sdk/src/checkpoint.rs:179` and `:198`).
- Move `/proc/self/status` reads in heartbeat metrics onto async-friendly I/O or a blocking task; see `crate/lib/sinex-node-sdk/src/heartbeat.rs:210` called from `crate/lib/sinex-node-sdk/src/lifecycle.rs:303`.
- Convert std::process::Command usage to `tokio::process::Command` (or spawn_blocking) in async preflight functions in `crate/lib/sinex-node-sdk/src/preflight/services.rs:266`, `:290`, `:392`, `:495`.
- Replace `std::fs::read_dir` in async preflight configuration and migration discovery (`crate/lib/sinex-node-sdk/src/preflight/services.rs:689`, `crate/lib/sinex-node-sdk/src/preflight/database.rs:537`) with tokio::fs or spawn_blocking.
- Use `tokio::net::lookup_host` instead of `ToSocketAddrs` in async DNS checks (`crate/lib/sinex-node-sdk/src/preflight/resources.rs:403`, `:434`).
- Wrap systemd integration sync filesystem reads in `spawn_blocking` or convert to tokio::fs in `crate/nodes/sinex-system-ingestor/src/systemd_integration.rs` (async `read_new_entries` and `poll_changes`).
