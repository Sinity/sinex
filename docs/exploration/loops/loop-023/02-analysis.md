Blocking-in-async audit

Summary
- Confirmed several blocking calls (std::fs, std::process::Command, std::net::ToSocketAddrs) executed inside async functions or async tasks in production code.
- Most occurrences are in preflight checks and systemd integration; these can stall the Tokio runtime under load or on slow I/O.

Confirmed cases
- Async checkpoint load/save uses sync file I/O.
  - Async caller: `crate/lib/sinex-node-sdk/src/simple_processor.rs:319` (`load_state`) calls `CheckpointState::load_from_file`.
  - Sync I/O: `crate/lib/sinex-node-sdk/src/checkpoint.rs:198` uses `std::fs::read_to_string`.
  - Async caller: `crate/lib/sinex-node-sdk/src/simple_processor.rs:782` (`shutdown`) calls `save_state_to_file`.
  - Sync I/O: `crate/lib/sinex-node-sdk/src/checkpoint.rs:179` + `:182` use `std::fs::File::create` and `std::fs::rename`.
  - Impact: blocking during startup/shutdown, but still on async context. Consider `tokio::task::spawn_blocking` or tokio::fs for file operations.

- Heartbeat emission does blocking /proc reads inside async tasks.
  - Sync I/O: `crate/lib/sinex-node-sdk/src/heartbeat.rs:210` uses `std::fs::read_to_string("/proc/self/status")` in `create_heartbeat_metrics`.
  - Async caller: `crate/lib/sinex-node-sdk/src/lifecycle.rs:303` calls `emit_heartbeat` inside a `tokio::spawn` loop.
  - Impact: periodic blocking on the async runtime. Consider `tokio::fs::read_to_string` or move metrics collection into a blocking task.

- Preflight services uses std::process::Command inside async functions.
  - `crate/lib/sinex-node-sdk/src/preflight/services.rs:266` `check_binary_availability` runs `Command::new("which").output()`.
  - `crate/lib/sinex-node-sdk/src/preflight/services.rs:290` `get_binary_version` runs `Command::new(binary_name).arg(flag).output()`.
  - `crate/lib/sinex-node-sdk/src/preflight/services.rs:392` `verify_systemd_services` runs `Command::new("systemctl").output()`.
  - `crate/lib/sinex-node-sdk/src/preflight/services.rs:495` `verify_postgresql_service` runs `Command::new("psql").output()`.
  - Impact: these are blocking process invocations inside async functions; use `tokio::process::Command` or `spawn_blocking` wrappers.

- Preflight services also uses sync filesystem traversal in async context.
  - `crate/lib/sinex-node-sdk/src/preflight/services.rs:689` `verify_service_configuration` uses `std::fs::read_dir` inside an async function.
  - Impact: blocking disk scan in async context; use `tokio::fs::read_dir` or spawn_blocking.

- Preflight database migration discovery is async but uses sync fs.
  - `crate/lib/sinex-node-sdk/src/preflight/database.rs:537` `discover_migration_files` iterates `fs::read_dir`.
  - Impact: blocking I/O in async function; use `tokio::fs::read_dir` or spawn_blocking.

- Preflight resources uses blocking DNS resolution in async functions.
  - `crate/lib/sinex-node-sdk/src/preflight/resources.rs:403` `test_dns_resolution` uses `ToSocketAddrs`.
  - `crate/lib/sinex-node-sdk/src/preflight/resources.rs:434` `test_localhost_connectivity` uses `ToSocketAddrs` when fallback path executes.
  - Impact: blocking resolver calls in async context; prefer `tokio::net::lookup_host` or spawn_blocking.

- Systemd integration does sync file reads inside async loops.
  - Async callers: `crate/nodes/sinex-system-ingestor/src/systemd_integration.rs:282` (`read_new_entries`) and `:352` (`poll_changes`).
  - Sync I/O inside async: `std::fs::read_to_string` (lines 129/144/158/170), `file.metadata()` and `BufReader::new(file)` (lines 289/297), and `std::fs::read_dir` (line 86) via `list_service_units`/`get_unit_status`.
  - Impact: repeated blocking reads on the runtime thread; consider `tokio::fs` for reads or wrap the monitor calls in `spawn_blocking`.

Non-issues / checked
- Clipboard watcher uses `tokio::process::Command` via `crate::common` re-exports, so the async `Command::output()` calls are non-blocking.
- `unified_journal_watcher.rs` uses `tokio::process::Command` and tokio::fs, so no blocking I/O detected there.
