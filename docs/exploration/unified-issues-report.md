# Unified Issues & Backlog Report

**Generated:** December 2025
**Status:** Canonical Source of Truth
**Supersedes:** `TODO.md`, `implementation-plan.md`, `deep-dive-findings.md`

---

## Executive Summary

This document consolidates the project's entire backlog, technical debt, and exploration findings into a single prioritized execution plan.

| Category | Count | Focus |
|----------|-------|-------|
| **Critical** | ~16 | Data corruption, production crashes, security holes |
| **High** | ~72 | Architecture gaps, test coverage, concurrency bugs |
| **Medium** | ~90 | Performance optimization, observability, refactoring |
| **Debt/Polish** | ~150 | Style, minor cleanups, non-critical TODOs |

---

## 1. Critical & Immediate Actions (Week 1)

**Goal:** Stabilize production, prevent data loss, and close security vulnerabilities.

### 1.1 Data Integrity & Corruption

### 1.2 Production Stability (Crashes/Panics)

### 1.4 Database & Concurrency

---

## 2. High Priority (Weeks 2-3)

**Goal:** Close architecture gaps, restore disabled tests, and complete the JetStream migration.

-### 2.1 Architecture & Refactoring

- **Complete Stage-as-You-Go JetStream Migration (TODO #49, 51, 64, 88)** *(archived in `docs/exploration/unified-issues-done.md`)*
- **Consolidate HTTP Dependency Stacks (NEW)**
  - **Context:** Workspace uses duplicate versions of `hyper` (0.14/1.0), `tower`, and `reqwest`.
  - **Action:** Align all crates to `axum 0.7` stack (hyper 1.0, http 1.0, reqwest 0.12).
- **Create ProcessorBase Abstraction (TODO #52)**
  - **Context:** 600+ LOC duplicated across satellites for basic lifecycle (init, run, shutdown).
  - **Action:** Extract `StatefulStreamProcessor` boilerplate into a shared SDK `ProcessorBase<C>` struct.

### 2.2 Testing Coverage

- **Restore disabled tests (TODO #15-17)**
  - **Action:** Re-enable `BlobManager` integration tests and `schema_property_test.rs`.
- **Add JetStream Consumer Stress Tests (TODO #66)**
  - **Action:** Add soak tests for `EphemeralNats` to catch race conditions and timeouts under load.
- **Implement Missing Unit Tests (NEW)**
  - **Targets:** `LeaseManager` (0.07:1 ratio), `DlqRetryHandler` (0 tests), `CheckpointManager` error paths.

### 2.3 Operational Gaps

---

## 3. Medium Priority & Polish (Week 4+)

**Goal:** Improve performance, observability, and developer ergonomics.

### 3.1 Performance

- **Hot-path allocation audit (NEW)**
  - **Context:** `jetstream_consumer.rs` clones payloads and vectors for every event in a batch.
  - **Action:** Refactor `process_batch` to use references or `Arc` where possible.
- **Batch insert UNNEST refactor (TODO #100)**
  - **Action:** Replace loop-based INSERTs in `events.rs` with `UNNEST` for 10x throughput.

### 3.2 Observability

- **Add metrics for silent failures (TODO #98)**
  - **Action:** Count DLQ write failures, NACKs, and confirmation publish errors.
- **Gateway Structured Logging (TODO #26)**
  - **Action:** Propagate `x-request-id` through Gateway RPCs to service layers.

### 3.3 Configuration & Polish

- **Make `max_ack_pending` configurable (NEW)**
  - **File:** `jetstream_consumer.rs` (currently hardcoded to 100).
  - **Snippet:**

        ```rust
        // Add to IngestdConfig
        pub struct IngestdConfig {
            pub max_ack_pending: u32,  // Default: 100
            ...
        }
        // Use in consumer config
        ConsumerConfig {
            max_ack_pending: config.max_ack_pending as i64,
            ...
        }
        ```

- **Standardize Env Var Prefixes (NEW)**
  - **Action:** Align `SINEX_`, `INGESTD_`, `SATELLITE_` prefixes.
- **Fix Timestamp Auto-Detection (NEW)**
  - **Context:** `timestamp_helpers.rs` misclassifies year 2128+ timestamps as milliseconds.

### 3.4 Runtime Hardening

- **Graceful shutdown handles SIGTERM + removes busy polling (NEW)**
  - **Context:** Architecture deep dive flagged that `sinex-ingestd` only awaits `ctrl_c()` (SIGINT) and the shared processor runtime polls every 100 ms to detect shutdown, so `systemctl stop` can hang and watchers keep running.
  - **Action:** Catch SIGTERM in ingestd’s entrypoint and propagate cancellation tokens instead of polling loops in `StatefulStreamProcessor`/`ProcessorRunner`. Ensure shutdown drains outstanding work and background tasks stop immediately.
  - **Tests:** Integration test that `systemctl stop sinex-ingestd` (or equivalent ctrl-c) exits promptly; unit tests asserting processors receive cancellation without 100 ms sleep; leak checks for watcher tasks.
- **Add systemd sandboxing to production services (NEW)**
  - **Context:** Only the `preflight` unit sets `ProtectSystem`, `NoNewPrivileges`, etc.; ingestd/gateway/satellite services are running without hardening despite the critical finding in the deep dive.
  - **Action:** Update `nixos/modules/ingestd.nix`, `gateway.nix`, and `satellite-services.nix` to include seccomp/namespace restrictions, private tmp, and read-only root FS; document required exceptions for IPC or storage paths.
  - **Tests:** NixOS VM test ensures services still start under hardened settings and fail when permissions are too loose; add docs to deployment guide describing the sandboxing knobs.
- **Implement checkpoint reset/stats APIs (NEW)**
  - **Context:** `CheckpointManager::reset_checkpoint` and `get_checkpoint_stats` in `crate/lib/sinex-satellite-sdk/src/checkpoint.rs` still return stubs, leaving operators without a way to clear bad checkpoints or inspect progress.
  - **Action:** Wire both methods to real SQL routines (delete/reset rows, aggregate counts/timestamps) and expose commands via `sinex-cli`/xtask to call them per processor.
  - **Tests:** Unit/integration tests covering reset flows (row removed, processor restarts from scratch) and stats queries returning real totals; documentation update for ops runbooks.

---

## 4. Active Implementation Initiatives

*Tracked from `implementation-plan.md`*

3. **Channel Hygiene**: 🔄 In Progress. Audit satellites for unbounded channels.
5. **RPC Dispatcher Completion**: 🔄 In Progress.
    - Flesh out `scan` method.
    - Implement `ExplorationProvider` with real data.
    - Wire CLI commands.

---

## 5. System Reference

### 5.1 Risk Matrix

| File | Churn | LoC | Risk Level |
|------|-------|-----|------------|
| `events.rs` | 76 mods | 2,234 | **CRITICAL** |
| `material_assembler.rs` | 48 mods | 1,215 | **CRITICAL** |
| `jetstream_consumer.rs` | High | 860 | **HIGH** |
| `satellite-services.nix` | Medium | ~150 | **HIGH** |

### 5.2 Architecture Patterns

- **Idempotency**: Relies on NATS `Msg-Id` headers + DB `ON CONFLICT DO NOTHING`.
- **Backpressure**: Coordinated via Gateway limits (100 concurrent), Ingestd `max_ack_pending` (100), and DB pool size.
- **Provenance**: Enforced via XOR check (Material OR Synthesis) at both App and DB levels.

### 5.3 NixOS Deployment Status

- **Secrets**: Agenix integration is solid.
- **Hardening**: **MISSING** on production services (Critical).
- **NATS**: Missing explicit dependency in `ingestd` systemd unit.

### 5.4 Known Dependency Conflicts

- `hyper`: 0.14 vs 1.0 (Major split)
- `tower`: 0.4 vs 0.5
- `rustls`: 0.21 vs 0.23

---

# Appendix A: Detailed Implementation Specifications (Migrated from TODO.md)

## Core Architecture & Control Plane

39. **RPC dispatcher is a stub**
    - **Files:** `crate/core/sinex-rpc-dispatcher/src/lib.rs`.
    - **Steps:** Implement scan (historical/continuous) routing and `ExplorationProvider` so RPC calls fan out to satellites via JetStream instead of returning `NotImplemented`.
    - **Tests:** Add integration tests that drive `sinex-rpc-dispatcher` against ephemeral NATS + a fake satellite and assert scan/explore responses traverse the bus. Current behaviour: methods return `SatelliteError::NotImplemented`.

40. **Desktop satellite continuous monitoring is stubbed**
    - **Files:** `crate/satellites/sinex-desktop-satellite/src/unified_processor.rs`, `window_manager.rs`, `clipboard.rs`.
    - **Steps:** Wire real clipboard/window watchers (no `hyprctl`/`xdotool` shellouts), start background tasks in `start_continuous_monitoring`, and emit events via `AcquisitionManager`.
    - **Tests:** Add integration tests that assert watchers emit focus/title/clipboard events and stop cleanly on shutdown; current code only logs “monitoring_started”.

41. **Terminal satellite lacks historical scan**
    - **Files:** `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs`.
    - **Steps:** Implement historical scan (or explicitly remove the mode) instead of returning “Historical replay is not supported”; ensure continuous mode exits cleanly without `pending().await`.
    - **Tests:** Add fail-first tests covering both historical and continuous modes lifecycle.

42. **Knowledge graph path search is stubbed**
    - **Files:** `crate/lib/sinex-core/src/db/repositories/knowledge_graph.rs`.
    - **Steps:** Implement `find_paths` using recursive CTEs (or graph extension) to return real paths between entities; remove placeholder `Ok(vec![])`.
    - **Tests:** Add property/integration tests that create small graphs and assert path discovery works.

46. **Build stamping is hardcoded**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/version.rs`, build scripts.
    - **Steps:** Restore build-time git revision/binary hash injection for release artifacts; wire into Nix/CI builds.
    - **Tests:** Unit test that the version struct carries non-placeholder values in test builds; CI check that release builds fail on `"dev-unknown"`/`"hash-component"`.

47. **Edge-mode satellites still require Postgres**
    - **Files:** `crate/lib/sinex-satellite-sdk` (checkpoint manager, schema cache), `crate/core/sinex-ingestd`.
    - **Steps:** Implement a NATS-only “pure edge” mode:
        1. Add a KV-backed checkpoint store in the SDK (bucket `KV_sinex_checkpoints`, CAS per `(processor, consumer_group, consumer_name)`) and feature-flag it; retain DB-backed checkpoints as default.
        2. Teach ingestd to broadcast active schemas on startup/reload to `system.schemas.active` (snapshot + updates). Satellites subscribe and build the `EventValidator` cache from these messages (optionally persisting locally for restart).
        3. Allow satellites to run with NATS-only config (no PgPool) when edge mode is enabled; keep SDK dependency intact.
        4. Once KV + schema broadcast are in place, delete/disable redundant DB-only satellite codepaths and drop their backing tables from the base schema (no new migration—update the squashed schema and reset DB) to avoid parallel modes.
    - **Tests:** Integration that runs a satellite with only NATS (no Postgres) and verifies checkpoints persist in KV and schema validation works from the broadcast; compatibility test that DB-backed mode still works.

49. **Browser activity capture is missing**
    - **Files:** new browser extension + gateway/native messaging bridge, ingest pipeline.
    - **Steps:** Implement the browser extension event source per `docs/planning/roadmap/features/browser-extension.md`: capture URLs/titles/dom summaries with explicit opt-in; publish via native messaging → JetStream. Update native messaging auth to cover this path.
    - **Tests:** End-to-end test with a fake extension manifest/payload to ensure events flow to JetStream and are validated; privacy redaction tests for sensitive fields.

50. **JetStream harness load/regression guard**
    - **Files:** `crate/lib/sinex-test-utils/src/nats.rs`, JetStream integration tests.
    - **Steps:** Add stress/soak tests for `EphemeralNats` and ingestion consumers to catch race/timeouts under load; tune defaults (timeouts, acks) and add monitoring hooks to fail fast in CI.
    - **Tests:** Stress suite that publishes many events/material slices and asserts no timeouts/undelivered messages on the reliable profile; flaky-test guard to quarantine regressions.

51. **Agenix secrets integration incomplete/non-functional**
    - **Files:** `nixos/modules/secrets.nix`, `nixos/modules/default.nix`, `nixos/modules/secrets-management.md`, service units (ingestd/gateway/satellites).
    - **Steps:** Make agenix first-class: wire the agenix module into the flake outputs, define age secret paths (e.g., `/run/agenix/sinex-gateway-token`, TLS keys) and propagate them to systemd units; ensure options work with external NixOS configs (e.g., `/realm/sinnix` deployments) without manual patching. Remove placeholder docs once live.
    - **Tests:** NixOS VM test that provisions an age secret, runs `nixos-rebuild switch` with the module enabled, and asserts services fail fast when secrets are missing and start when secrets exist. Add a CI check to prevent fallback to plain env defaults.

52. **Standard aggregation runner not adopted universally**
    - **Files:** aggregating automata (health aggregator, analytics automaton, any stream-to-state processors), shared SDK runner.
    - **Steps:** Define a shared aggregation trait/runner (left-fold with snapshot/replay, checkpointing) and migrate all stateful automata to use it, replacing bespoke reducers. Provide snapshotting/snapshot restore, error handling, and state persistence once in the runner; make this the single supported pattern for stream-to-state processing.
    - **Tests:** Integration tests for the runner (snapshot + replay), and refactored aggregators (health, analytics) proving identical behaviour under the new abstraction.

53. **Event processing pipeline lacks universal middleware chain**
    - **Files:** ingestd pipelines, satellites’ `process_event` paths, shared processing utilities.
    - **Steps:** Introduce a composable step/middleware chain (tower-like) for validation → enrichment → transformation → side effects, and migrate all event processing paths to it to standardize metrics/tracing/error handling. Make the chain the default pattern for new processors; eliminate ad-hoc inline pipelines.
    - **Tests:** Unit tests for individual steps and composition; integration tests showing a migrated pipeline (e.g., ingestd event/material consumers, one satellite) behaves identically with the chain.

55. **Stateful automata lack sharding/affinity** — ⏳
    - **Status:** No `Shardable`/consistent hashing abstraction exists outside this report (`rg "Shardable" -g'*'` returns only the backlog entry), and processors such as `crate/satellites/sinex-health-aggregator/src/processor.rs` still rely on single-threaded `StatefulStreamProcessor` instances without key affinity.
    - **Files:** stateful automata (analytics, session-aware processors), routing helpers.
    - **Steps:** Add a `Shardable` trait + consistent hashing router for JetStream subjects to guarantee per-key ordering/affinity; adopt in stateful processors.
    - **Tests:** Integration test that events with the same shard key always hit the same worker and preserve order under parallel workers.

56. **Retry/idempotency not encoded in types** — ⏳
    - **Status:** Retry surfaces such as `crate/lib/sinex-satellite-sdk/src/dlq_retry.rs` still accept arbitrary closures/handlers and expose `DlqRetryHandler::retry_all` without any `Idempotent` marker; there are no workspace traits enforcing retry safety.
    - **Files:** retry helpers, satellite/ingestd operations.
    - **Steps:** Add marker traits (e.g., `Idempotent`) for operations eligible for automatic retry; enforce via retry wrappers.
    - **Tests:** Unit tests that non-idempotent ops are refused by retry helpers; positive test for idempotent ops.

57. **Units/size/times use raw integers** — 🟡
    - **Status:** `sinex-core::types::units::{Bytes, Seconds}` now wrap size/time values and `IngestdConfig` adopted them (`crate/core/sinex-ingestd/src/config.rs:22-120`), so CLI/defaults/documents all use explicit units. `cargo nextest run -p sinex-ingestd figment_config_tests` still fails to build until the local Postgres instance picks up the packaged TimescaleDB, so the regression reference will be captured once the DB is rebuilt. Satellite configs still expose raw integers, so the TODO stays open until those structs migrate as well.
    - **Files:** config structs and validation for timeouts/size limits in ingestd/satellites.
    - **Steps:** Introduce small newtypes for bytes/durations in new/updated configs to prevent unit mixups; adopt in validation boundaries (not a wholesale rewrite).
    - **Tests:** Config parsing tests that catch unit mixups; compile-time type checks in affected modules.

59. **Satellites still require direct DB access (violates edge isolation)** — ⏳
    - **Status:** Checkpointing keeps a hard PgPool dependency (`crate/lib/sinex-satellite-sdk/src/checkpoint.rs:41-118` stores a `sqlx::PgPool` and issues `INSERT ... ON CONFLICT`), so satellites cannot run with `DATABASE_URL` unset despite Stage-as-You-Go now publishing via JetStream.
    - **Files:** `crate/lib/sinex-satellite-sdk` (checkpoint manager, Stage-as-You-Go), `crate/core/sinex-ingestd`.
    - **Steps:** Move checkpoints to NATS KV/stream, route stage-as-you-go/material writes exclusively via JetStream/ingestd, and remove PgPool dependencies from satellites. Align with the edge-mode TODO to enforce NATS-only satellites.
    - **Tests:** Integration run of a satellite with no DATABASE_URL (NATS-only) that still succeeds; ensure duplicate ledger insert races disappear.

60. **Events repository is a God module** — ⏳
    - **Status:** `crate/lib/sinex-core/src/db/repositories/events.rs` remains a 2,236-line monolith (see `wc -l` above) that mixes ingestion, analytics, and reporting helpers in one giant impl; no reader/writer split exists yet.
    - **Files:** `crate/lib/sinex-core/src/db/repositories/events.rs`.
    - **Steps:** Split into writer/reader/analytics modules; keep cascade/helpers isolated. Reduce cognitive load and surface narrower traits for callers.
    - **Tests:** Ensure existing tests still pass; add smoke tests for the separated modules if needed.

62. **Syslog/journal watcher shells out to journalctl** — ⏳
    - **Status:** `crate/satellites/sinex-system-satellite/src/journal_watcher.rs:30-330` still spawns `journalctl` via `std::process::Command` for snapshots, tailing, and filtering; no sd-journal bindings exist yet.
    - **Files:** `crate/satellites/sinex-system-satellite/src/journal_watcher.rs`.
    - **Steps:** Replace `journalctl` subprocess parsing with a native journal API (e.g., sd-journal bindings) to reduce brittleness and improve performance.
    - **Tests:** Integration test with journal fixtures via native API; ensure existing watcher tests still pass.

63. **COUNT(*) used for event counts** — ⏳
    - **Status:** Numerous helpers still emit `COUNT(*)` queries (e.g., `count_all` and stats at `crate/lib/sinex-core/src/db/repositories/events.rs:557-1929`), so dashboards continue to scan the hypertable instead of using approximations/counters.
    - **Files:** `crate/lib/sinex-core/src/db/repositories/events.rs` (count_all, stats).
    - **Steps:** Replace exact `COUNT(*)` in dashboards/stats with estimates or a maintained counter to avoid full scans at scale.
    - **Tests:** Unit/integration test that the new count path returns reasonable estimates and doesn’t block on large tables.

## Gateway Hardening

3. **Validate native-messaging origins**
   - **Files:** `crate/core/sinex-gateway/src/native_messaging.rs`, `docs/native_messaging.md`.
## Content / Blob Pipeline

## System Satellite

## Observability & Heartbeats

## Schema Tooling

## Testing Coverage

## Additional Priorities

19. **Document ingestor job metadata**
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`.
    - **Steps:** when submitting jobs (or emitting events), include the actual material ULID and path metadata so downstream components do not rely on parsing `target_uri`.
    - **Tests:** once metadata is carried through, add an integration test that exercises `submit_document_job` + `process_material` and asserts emitted `document.ingested` events contain the ULID and path fields explicitly.

20. **Replay control bus resilience**
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `crate/core/sinex-gateway/src/replay_control`.
    - **Steps:** implement exponential backoff + monitoring when `spawn_replay_control` fails instead of silent warn-and-disable; expose health info to the gateway CLI.
    - **Tests:** integration test that currently shows the replay client missing when NATS is down; expect failure until retries/metrics exist.
    - **Status:** `service_container_should_fail_when_replay_control_unavailable` (`crate/core/sinex-gateway/tests/replay_control_resilience_test.rs`) now fails because `ServiceContainer::new` still returns `Ok` with `replay_control=None` when NATS connections error instead of surfacing the failure.

22. **Gateway performance isolation**
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `sinex-services`.
    - **Steps:** refactor long-running queries (analytics/search) to async tasks or chunked pagination so one RPC cannot hog the shared DB pool.
    - **Tests:** after the async refactor, add a stress test (or benchmark harness) that fires multiple expensive queries concurrently and ensures throughput improves; no useful fail-first coverage is practical before the refactor.
    - **Status:** `analytics_queries_block_each_other_with_single_connection` (`crate/lib/sinex-services/tests/analytics_service_test.rs`) now fails because two analytics queries against a single-connection pool block each other, demonstrating the lack of workload isolation.

25. **Watcher teardown and restart handling**
    - **Files:** `dbus_watcher.rs`, `journal_watcher.rs`, `systemd_watcher.rs`, `udev_watcher.rs`.
    - **Steps:** add explicit shutdown signals to stop spawned tasks, and ensure the unified processor can restart watchers on reconfiguration.
    - **Status:** `processors_should_stop_background_tasks_on_shutdown` (`crate/lib/sinex-satellite-sdk/tests/processor_shutdown_leak_test.rs`) now fails because the default `StatefulStreamProcessor::shutdown` leaves spawned tasks running forever.

26. **Gateway structured logging + tracing context**
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`.
    - **Steps:** introduce request IDs, user/session tags, and propagate them into service-layer logs for auditability.
    - **Tests:** when request IDs are wired, add an integration test that issues an RPC call with a tracing subscriber configured to capture events and asserts the resulting log contains the propagated `request_id`.
    - **Status:** `rpc_responses_include_request_id_header` (`crate/core/sinex-gateway/src/rpc_server.rs`) now fails because the router still responds without any `x-request-id` header or structured trace context.

29. **Replay automation coverage**
    - **Files:** `crate/lib/sinex-processor-runtime/src/lib.rs` (replay module), `crate/lib/sinex-services/src/analytics.rs`.
    - **Steps:** add integration tests for the replay control lifecycle (create → preview → approve → execute) using the gateway RPC dispatch; verify error paths and cancellation.
    - **Tests:** after the replay RPC surface stabilizes, add integration tests that exercise the full lifecycle (create → preview → approve → execute) against a mock gateway and assert the current error messages; no placeholder tests today.
    - **Status:** `replay_execution_records_outcome` (`crate/core/sinex-gateway/src/replay_control.rs`) now fails because executing a replay never records any outcome/summary, leaving `ReplayOperation.outcome` as `None` even after we drive plan → preview → approve → execute via the control plane.

30. **Gateway secret management via agenix**
    - **Files:** `nixos/modules/secrets-management.md`, `nixos/modules/default.nix`.
    - **Steps:** ensure gateway-related secrets (tokens, TLS certs) are provisioned through agenix instead of raw env vars; document rotation.
    - **Tests:** NixOS VM test verifying services refuse to start when secrets missing; fails now because they happily read env defaults.
    - **Status:** `gateway_requires_admin_token_secret` (`crate/core/sinex-gateway/tests/gateway_secret_management_test.rs`) now fails because `SINEX_GATEWAY_ADMIN_TOKEN_FILE` is unset, proving secrets aren’t wired through agenix yet.

31. **Better documentation surfacing for watchers**
    - **Files:** `crate/satellites/sinex-system-satellite/docs/README.md`, workspace docs.
    - **Steps:** explain how each watcher works, configuration knobs, and failure behavior; currently the README doesn’t mention the real implementations, leading to confusion.
    - **Tests:** documentation lint or manual review (no automated failure today), but include this task so we update the docs alongside code.
    - **Status:** README now includes a watcher matrix (subsystems, captured signals, config knobs, shutdown notes). Remove TODO once runtime docs cover failure semantics too.

32. **Upgrade plan for gateway/test infra**
    - **Files:** `docs/planning/roadmap/testing-priorities-and-roadmap.md`.
    - **Steps:** fold the new gateway/system tasks into that roadmap so engineers know the order of operations; ensures the plan stays in sync with this TODO file.
    - **Tests:** manual verification.

## SQL Ergonomics Sweep

## Legacy Cleanup & Provenance

36. **Document job monitor leaks tasks and never retires jobs**
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs` (`monitor_jobs`, `scan`).
    - **Steps:** retain the spawned JoinHandle (or run the monitor inside `ProcessorCommand::Service`), wire it to a shutdown signal, and update job status/material IDs as work completes. Remove the `NULL::ulid` placeholder and set `status='retired'` once events are emitted so jobs are not reprocessed endlessly.
    - **Tests:** add fail-first async test `document_monitor_leaks_job_loop` under `crate/satellites/sinex-document-ingestor/tests/` that asserts `tokio::spawn` count increases per scan (current behaviour) and that job statuses remain `active`. After the fix, the monitor should shut down cleanly and rows should transition to `retired`.
    - **Status:** `document_monitor_leaks_job_loop` now fails because the monitor keeps looping forever and leaves `raw.sensor_jobs.status = 'active'` despite emitting events.

37. **Desktop satellite still depends on sensd/DB connectivity**
    - **Files:** `crate/satellites/sinex-desktop-satellite/src/unified_processor.rs`.
    - **Steps:** remove the sensd job-submission remnants, instantiate real watchers (or AcquisitionManager-driven sensors), and emit events via JetStream instead of dropping metadata into Postgres.
    - **Tests:** `desktop_processor_emits_clipboard_events` (`crate/satellites/sinex-desktop-satellite/src/unified_processor.rs`) now fails because snapshot scans still don't emit any clipboard/window events.

39. **Replay planner bypasses ingestion invariants (DB target)**
    - **Files:** `cli/replay_planner.py`.
    - **Steps:** stop inserting directly into `core.events` (which fails due to generated `ts_ingest` and missing provenance). Route through ingestd or stage-as-you-go so provenance and schema checks pass.
    - **Tests:** `test_replay_planner_database_target_errors` (`cli/tests/test_replay_planner.py`) now fails because the planner still attempts direct Postgres writes.

40. **Replay planner NATS target is unimplemented**
    - **Files:** same file as task 39.
    - **Steps:** implement publishing to `sinex.control.replay` with operation IDs in message headers.
    - **Tests:** `test_replay_planner_nats_target_publishes` (`cli/tests/test_replay_planner.py`) now fails because the NATS branch remains a stub.

42. **Watcher tasks never shut down**
    - **Files:** `sinex-system-satellite` watchers, desktop watchers.
    - **Steps:** add cancellation handles so `ProcessorRunner::shutdown` stops each spawned `tokio::spawn` loop.
    - **Tests:** fail-first integration test `system_watchers_stop_on_shutdown`; today watchers run forever after shutdown.
    - **Status:** `processor_runner_triggers_processor_shutdown` (`crate/lib/sinex-processor-runtime/tests/processor_runner.rs`) now fails because `ProcessorRunner` never calls `StatefulStreamProcessor::shutdown` when handling service-mode shutdowns, so background watcher tasks keep running.

44. **Desktop clipboard/window watchers still write directly to Postgres tables**
    - **Files:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs` and related modules.
    - **Steps:** replace raw `INSERT INTO raw.source_material_registry/raw.temporal_ledger` calls with AcquisitionManager + JetStream writes so the satellite no longer requires `DATABASE_URL`.
    - **Tests:** `desktop_clipboard_requires_database_pool` (unit test inside `clipboard.rs`) now fails because `store_clipboard_source_material` returns `None` when `db_pool` is absent.

47. **System satellite emits events with invalid provenance references**
    - **Files:** `crate/satellites/sinex-system-satellite/src/dbus_watcher.rs`, `journal_watcher.rs`, `systemd_watcher.rs`, `udev_watcher.rs`.
    - **Steps:** replace the hard-coded `system_bootstrap_id` calls to `Provenance::from_synthesis_safe` with real provenance (e.g., material provenance via AcquisitionManager or actual parent events). If a bootstrap ULID is required, persist the corresponding event during startup so parent IDs exist.
    - **Status:** `system_processor_still_uses_synthetic_provenance` (`crate/satellites/sinex-system-satellite/tests/system_processor_watchers.rs`) now fails because snapshot scans continue to emit synthesis provenance instead of real material-backed IDs.

48. **Terminal history watcher re-reads entire history file each poll**
    - **Files:** `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs` (`HistoryWatcherContext::monitor`).
    - **Steps:** replace `fs::read_to_string` with incremental tailing (seek from saved offset, read chunks) so large history files don’t get reloaded every interval.
    - **Tests:** new unit test `terminal_watcher_tails_incrementally` that currently fails because memory usage scales linearly with file size per poll.

65. **MaterialAssembler lacks resilience for out-of-order slices and restarts**
    - **Files:** `crate/core/sinex-ingestd/src/material_assembler.rs`, `crate/lib/sinex-test-utils` (JetStream harness).
    - **Steps:** Handle out-of-order slices (buffer/reorder or reject to DLQ), add timeout logic for incomplete materials, detect hash mismatches and route to DLQ with metadata, and rebuild in-flight state from JetStream after crash/restart (persist minimal ledger/slice metadata to allow reconstruction).
    - **Tests:** Integration tests covering slice reordering, timeout expiry, hash-mismatch DLQ, concurrent materials isolation, and restart recovery that rebuilds state from the stream.

66. **JetStream consumer stress/regression suite is missing**
    - **Files:** `crate/lib/sinex-test-utils/src/nats.rs`, JetStream integration tests.
    - **Steps:** Add a comprehensive stress suite (ack/nack, requeue, idempotency, restart, DLQ routing under load) to catch flakes; tune EphemeralNats defaults (timeouts, acks) and add monitoring hooks to fail fast in CI.
    - **Tests:** New reliable-profile Nextest suite that publishes many events/material slices, asserts no timeouts/undelivered messages, and guards against consumer deadlocks/races.

67. **Hot-path clone/alloc audit for ingestion and checkpoints**
    - **Files:** `crate/core/sinex-ingestd/*`, `crate/lib/sinex-core/src/db/repositories/events.rs`, `crate/lib/sinex-satellite-sdk` (checkpoint/stage-as-you-go).
    - **Steps:** Profile and reduce unnecessary `.clone()`/buffer copies in ingestion and checkpoint paths; favor references/Arc and zero-copy deserialization where safe. Start with identified hotspots in material assembler and event persistence.
    - **Tests:** Benchmark or micro-benchmark harness showing reduced allocations/CPU; ensure existing ingestion tests stay green.

68. **Unsafe unwraps in cascade analyzer**
    - **Files:** `crate/core/sinex-gateway/src/cascade_analyzer.rs`.
    - **Steps:** Replace `.unwrap()` on `dependencies`/`in_degree` maps (e.g., lines ~629-630) with safe `entry`/default patterns to avoid panics on missing keys. Add defensive handling/logging for malformed graphs.
    - **Tests:** Unit test covering missing key scenarios to ensure no panic and that graph accounting remains correct.

69. **Production panics instead of Result**
    - **Files:** `crate/lib/sinex-core/src/db/models/event.rs`, `crate/satellites/sinex-fs-watcher/src/unified_processor.rs`, `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs`.
    - **Steps:** Replace `panic!`/`unwrap!` in runtime paths with typed errors and propagate via `Result`. Audit terminal/fs watchers for unsafe matches; ensure user-facing errors are surfaced without crashing.
    - **Tests:** Regression tests that previously panicked now return errors; Nextest suite should complete without panic backtraces from these modules.

70. **Dead-code suppressions hide incomplete refactors**
    - **Files:** `crate/core/sinex-gateway/src/cascade_analyzer.rs`, `crate/core/sinex-gateway/src/native_messaging.rs`, other `#[allow(dead_code)]` blocks.
    - **Steps:** Review/remediate  `#[allow(dead_code)]` usages: remove unused items or document why retained. Prefer feature flags/tests over blanket suppression.
    - **Tests:** `cargo check` with suppressions removed where possible; add narrow `cfg(test)` guards for test-only helpers.

71. **Commented-out tests/benchmarks should be restored or deleted**
    - **Files:** `crate/lib/sinex-core/tests/adversarial/chaos_engineering_test.rs` (large commented blocks), any other commented suites/benches.
    - **Steps:** Re-enable viable tests or delete obsolete ones; if blocked, mark TODO with blocker description and target milestone.
    - **Tests:** Reactivated tests should pass (or be marked fail-first with clear tracking); no lingering commented blocks.

72. **Inconsistent lock poisoning handling**
    - **Files:** `crate/core/sinex-gateway/src/replay_control.rs` and other mutex users.
    - **Steps:** Standardize mutex handling: avoid bare `.lock().unwrap()`, handle poisoned locks with recover/log-or-recreate strategy. Align patterns across the file.
    - **Tests:** Unit test simulating poisoned mutex to ensure handler path doesn’t panic; replay_control tests should still pass.

73. **Verbose parameter extraction in handlers**
    - **Files:** `crate/core/sinex-gateway/src/handlers.rs`.
    - **Steps:** Introduce helper for typed JSON-RPC parameter extraction to replace repeated `.as_str()`/`.as_i64()` calls; add validation errors instead of ad-hoc conversions.
    - **Tests:** Handler unit tests asserting structured errors on bad types; existing handler tests remain green.

74. **Docs structure: clarify current vs planning vs archived**
    - **Files:** `docs/` tree (`README.md`, JetStream migration status/progress docs, testing docs, misc analyses).
    - **Steps:** Restructure docs to clearly separate current state, planning/roadmap, vision, and archived history; add a migration guide in `docs/archived/README.md`; consolidate duplicate testing docs; remove temporary files (e.g., `tmp_seaquery_research.md`); update cross-references and the main docs README.
    - **Tests:** Manual verification: all moved docs have updated links; status/confusion between current and future docs resolved; no stray temp files remain.

75. **Refactor MaterialAssembler::restore_state for readability**
    - **Files:** `crate/core/sinex-ingestd/src/material_assembler.rs` (restore_state).
    - **Steps:** Extract helpers (e.g., restore_single_material, restore_buffered_slices, recompute_hash) to reduce nesting and clarify error handling; keep crash-recovery behavior intact.
    - **Tests:** Existing MaterialAssembler recovery tests plus new unit tests for corrupted state files and buffered-slice reconstruction; ensure restart recovery still passes.

76. **Refactor MaterialAssembler::handle_slice duplication**
    - **Files:** `crate/core/sinex-ingestd/src/material_assembler.rs` (handle_slice).
    - **Steps:** Extract common write/flush/hash/update logic and buffered flush loop into helpers (e.g., append_slice_to_file, flush_sequential_buffers); preserve out-of-order handling semantics.
    - **Tests:** Existing slice-ordering tests plus new coverage for in-order/out-of-order/duplicate slices; validate offsets and hashes unchanged.

77. **Simplify system UnifiedProcessor::scan branches**
    - **Files:** `crate/satellites/sinex-system-satellite/src/unified_processor.rs` (scan).
    - **Steps:** Extract snapshot/historical/continuous branch handlers and a stats builder; use a scan result struct to reduce inline tuple construction.
    - **Tests:** Existing system scan tests plus new unit tests per branch to ensure no behavioral drift.

78. **Reduce duplication in JetStreamConsumer::process_batch**
    - **Files:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs` (process_batch).
    - **Steps:** Introduce shared validation/DLQ helper(s) to eliminate repeated error-handling blocks across validation stages; extract batch persistence into its own method to simplify control flow.
    - **Tests:** Existing ingestion/validation tests plus new fail-first for each validation stage ensuring DLQ routing and acks still behave identically.

79. **Fix missing binaries and dual implementations in satellites**
    - **Files:** `crate/satellites/sinex-health-aggregator`, `sinex-content-automaton`, `sinex-pkm-automaton`, `sinex-analytics-automaton`, `sinex-search-automaton`.
    - **Steps:** Add `[[bin]]` entries for automata with `src/main.rs`, add missing `main.rs` for health/content/pkm automata using `processor_main!`, and remove the legacy `HotlogAutomaton` implementation in health-aggregator. Align binary names with processor_name outputs. *(Health/content/PKM now have processor_main! binaries; legacy Hotlog impl removed; Hotlog shim deprecated in SDK.)*
    - **Tests:** `cargo check`/`nextest` for these crates; ensure devenv/nixos service wiring resolves binaries without missing-target errors.

 "0.1.0" or "0.5.0" is set.
    ***Steps:** Switch to `version.workspace = true` for satellites/automata to match workspace policy unless a crate is intentionally versioned separately (document exceptions).
    *   **Tests:** `cargo metadata`/`cargo check` to ensure workspace builds after version normalization.

81. **Document and enforce error handling conventions**
    - **Files:** Error-prone satellites (`fs-watcher`, `terminal-satellite`, `desktop-satellite`, `system-satellite`, `analytics-automaton`, `search-automaton`, `health-aggregator`), plus contributing docs.
    - **Steps:** Define when to use `SatelliteError::{General,Processing,Lifecycle}`, prefer `eyre` context over `to_string`, and align logging levels (warn vs error). Refactor representative call sites to match the guideline and add a short doc section (e.g., in contributing/testing).
    - **Tests:** Existing suites should remain green; add a lint/checklist in docs; optional unit tests for error mapping if available.

82. **Unify naming patterns for processors/automata**
    - **Files:** Satellite/automaton structs and `processor_name()` implementations.
    - **Steps:** Choose a single suffix convention (e.g., capture = *Satellite, derivation =*Automaton, aggregation = *Aggregator) and align struct names, binary names, and `processor_name()` outputs; fix stray spelling (“initialised” → “initialized”).
    - **Tests:** `cargo check` across satellites/automata; adjust any string-based tests expecting old names.

83. **Standardize test organization and helpers**
    - **Files:** Satellite test trees (`crate/satellites/*/tests`), inline `#[cfg(test)]` modules.
    - **Steps:** Prefer categorized directories where practical, use `TestContext` injection (`#[sinex_test]` style) instead of manual setup, settle on one return type (`TestResult<()>`), and normalize file naming (`*_test.rs`). Add guidance to testing docs.
    - **Tests:** Ensure reorganized tests still pass; no behavioral changes expected.

84. **Harden transaction isolation for critical paths**
    - **Files:** Critical DB mutators in `crate/lib/sinex-core/src/db/repositories` and ingestd.
    - **Steps:** Identify transactions that require repeatable reads (e.g., checkpoint updates, claims) and set explicit isolation (SERIALIZABLE or SELECT FOR UPDATE where appropriate); ensure retry/backoff remains in place.
    - **Tests:** Existing concurrency/deadlock tests plus new coverage to confirm no lost updates and expected retries under contention.

85. **Instrument advisory lock/lease acquisition failures**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/coordination.rs`, `lease_manager.rs`.
    - **Steps:** Add metrics/log counters for failed advisory lock or NATS lease acquisition/renewal to detect coordination issues in production; expose via existing metrics pipeline.
    - **Tests:** Unit/integration test that increments on failure paths; ensure normal paths unchanged.

86. **Monitor DB pool contention**
    - **Files:** `crate/lib/sinex-core/src/db/pool.rs` or service entrypoints.
    - **Steps:** Add lightweight telemetry for pool acquire latency and warn/metric when thresholds are exceeded; document tuning knobs.
    - **Tests:** Smoke test that instrumentation does not alter behavior; optional benchmark to assert minimal overhead.

87. **Serialize checkpoint updates**
    - **Files:** Checkpoint/state repositories (`crate/lib/sinex-core/src/db/repositories/state.rs` and related).
    - **Steps:** Ensure checkpoint updates use `SELECT ... FOR UPDATE` (or equivalent) to avoid concurrent clobbering when multiple workers write the same key; align with retry/backoff strategy.
    - **Tests:** Concurrency test to assert single-writer semantics on the same checkpoint key under concurrent updates.

88. **Finish single-writer enforcement for material ingest**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`, `acquisition_manager.rs`, ingestd material assembler.
    - **Steps:** Remove satellite DB writes for ledger/material rows (JetStream-only), ensure ingestd is the sole writer; adjust schema/doc to reflect single-writer model.
    - **Tests:** Existing duplicate-ledger tests plus new regression ensuring no duplicate key errors when ingestd replays slices; satellite runs without DATABASE_URL.

89. **Instrument advisory lock / NATS lease failures**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/coordination.rs`, `lease_manager.rs`.
    - **Steps:** Emit metrics/log counters on failed advisory lock or KV lease acquire/renew; surface via existing metrics pipeline.
    - **Tests:** Unit/integration tests asserting counters increment on simulated failures.

90. **Audit JetStream stream configs vs docs**
    - **Files:** Stream setup (ingestd/nats bootstrap), docs describing retention/compaction.
    - **Steps:** Verify actual stream subjects/retention/compaction align with documented expectations; update code or docs; ensure bootstrap creates the intended config.
    - **Tests:** Integration test that inspects stream config (via nats CLI/API) matches expected settings.

91. **Restore async benchmarks support**
    - **Files:** `crate/lib/sinex-test-utils/src/standard_fixtures.rs`, `crate/lib/sinex-test-utils/src/db_common.rs`, benchmarking macros.
    - **Steps:** Extend `sinex_bench` (or switch to criterion) to support async benchmarks and re-enable the commented fixture benchmarks.
    - **Tests:** Bench builds succeed; re-enabled benchmarks compile/run; no impact on regular test suite.

92. **Implement schema validation fixtures**
    - **Files:** `crate/lib/sinex-test-utils/src/fixtures.rs`.
    - **Steps:** Fill the schema validation fixture TODO once schema management API is stable; add helpers to generate/register schemas for tests.
    - **Tests:** New fixture tests verifying schema registration/validation; integration tests consuming the fixtures.

93. **Resolve circular test helper dependency**
    - **Files:** `crate/lib/sinex-core/src/types/events/test_helpers.rs`.
    - **Steps:** Move/adjust `test_event_with_version` (and related) to avoid circular deps between `sinex-events` and `sinex-core`—consider relocating to a shared test-utils crate.
    - **Tests:** Ensure helper restored without circular deps; dependent tests compile.

94. **Decide on deprecated metadata field**
    - **Files:** `crate/lib/sinex-core/src/db/models/event.rs` (deprecated metadata/machine_id field).
    - **Steps:** Decide to keep or remove; if removing, update schema/docs and adjust callers; if keeping, document rationale.
    - **Tests:** Schema/model tests updated; migrations/schema squashed if field removed.

95. **Make confirmation publishing retryable**
    - **Files:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs` (confirmation publish loop).
    - **Steps:** Add retry/backoff around confirmation publishing; if confirmations repeatedly fail, avoid ACKing the batch (NACK or DLQ) to prevent silent loss; surface metrics for failures.
    - **Tests:** Integration test that forces confirmation publish failure and asserts we do not ACK the batch without a successful confirmation (or we retry/NACK as designed).

96. **Fail-safe DLQ writes in satellites**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/event_processor.rs` (local DLQ write path).
    - **Steps:** If local DLQ write fails (disk/permissions), propagate the error instead of clearing events; optionally retry or NACK upstream to avoid data loss. Add metrics/logging for DLQ write failures.
    - **Tests:** Unit/integration test simulating DLQ write failure to ensure events aren’t silently dropped.

97. **Handle NACK/DLQ publish failures explicitly**
    - **Files:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs` (NACK error handling, DLQ publish ack handling).
    - **Steps:** Stop ignoring NACK errors; log+retry or abort batch on repeated NACK failures. For DLQ routing, add fallback/metrics when publish/ack fails to avoid silent loss.
    - **Tests:** Integration test that simulates NACK/DLQ publish failure and asserts non-silent handling (no lost messages, retries or error surfaced).

98. **Add metrics/alerting for silent failure paths**
    - **Files:** ingestd confirmation/DLQ paths; satellite DLQ writer.
    - **Steps:** Emit counters for confirmation publish failures, DLQ write failures, NACK failures; integrate with existing observability pipeline.
    - **Tests:** Metric emission tests or harness that forces failures and asserts counters increment.

100. **Refactor events batch_insert_many to UNNEST**
     - **Files:** `crate/lib/sinex-core/src/db/repositories/events.rs` (batch_insert_many).
     - **Steps:** Replace per-row INSERT loop with UNNEST-based bulk insert (pattern from ingestd’s jetstream_consumer) for 10–100x throughput improvement; keep idempotency/ON CONFLICT semantics.
     - **Tests:** Existing batch ingestion tests plus a benchmark/comparison for 100–1000 events to confirm speedup.

101. **Add trigram indexes for entity name search**
     - **Files:** `crate/lib/sinex-schema/src/schema/entities.rs` (+ migration).
     - **Steps:** Enable `pg_trgm` and add GIN trigram indexes on `LOWER(name)` and `LOWER(canonical_name)` to speed LIKE searches; update schema generation/migrations accordingly.
     - **Tests:** Migration/sqlx prepare; optional EXPLAIN/benchmark showing reduced cost for partial-name queries.

102. **Evaluate payload text search indexing**
     - **Files:** `crate/lib/sinex-schema/src/schema/events.rs` (+ migration).
     - **Steps:** Consider adding a trigram or FTS index on `payload::text` for ILIKE/text search; weigh disk/maintenance cost vs. observed query patterns and adjust query to use FTS if chosen.
     - **Tests:** Migration/sqlx prepare; EXPLAIN/benchmark for representative text searches.

# Appendix B: Architecture Deep Dive

Architecture and deployment findings now live in [docs/exploration/architecture-deep-dive.md](./architecture-deep-dive.md). Refer to that document for the codebase review, idempotency/backpressure analysis, graceful shutdown posture, ingestion hot-path walk-through, provenance enforcement, checkpoint lifecycle notes, startup sequencing, NixOS deployment audit, and patterns summary.

Keeping those details separate prevents the unified issues backlog from mixing non-actionable analysis with actionable tasks.

# Appendix C: Full Implementation Plan Tasks (Migrated from Implementation Plan)

## 1. Schema Pipeline Unification

**Decision:** Rust `derive(EventPayload)` definitions are the single source of truth. JSON files under `schemas/` exist as generated artifacts for GitOps distribution and downstream SDKs—never hand‑edit them.

- [x] Amend `schemas/README.md` with a “Source of Truth” section that states the rule above and explains how to regenerate artifacts.
- [x] Create a CI job (or extend schema-validation) that runs `cargo xtask schema generate` and fails when committed JSON is stale (treat like `cargo fmt`).
- [x] Teach contributors to commit regenerated JSON (README callouts + PR-template checkbox) and backstop with an automated `schema-auto-update` workflow that opens a PR when drift is detected on `main`.
- [x] Keep `xtask schema compat`, `schema-validation.yml`, and `gitops_schema_sources` pointing at the generated bundle. Document the flow from Rust → JSON → Postgres → downstream consumers.
- [x] Document how non-Rust contributors propose schema changes (JSON PR as proposal, build fails until the matching Rust change lands).

**Exit criteria:** CI enforces zero drift; ingestd bootstraps via DB content created from the generated bundle; README explicitly warns against manual JSON edits.

---

## 2. Documentation Alignment (JetStream-only World)

**Decision:** All current docs must reflect the JetStream-only ingestion described in the canonical architecture docs under `docs/current/architecture/`. Transactional outbox references move to historical sections.

### Tasks

- [x] Sweep docs for “transactional outbox,” “sensd,” or “gRPC ingestion” references and either delete or mark as historical context. (Historical banners added to the remaining references in reports/TODO files.)
- [x] Add short banners to historical docs (e.g., `/docs/historical/**`) to clarify their status.
- [x] Update `README.md` and satellite guides to mention JetStream confirmations, DLQ subjects, and the confirmation-aware automaton flow.

**Exit criteria:** No current doc suggests that the outbox or sensd are active components; contributors find a single, coherent story that matches the JetStream-first system implementation.

---

## 3. Channel & Backpressure Hygiene

**Decision:** Follow the staging-stream guidance: satellites publish immediately to JetStream; local channels remain bounded/test-only.

### Tasks

- [ ] Audit satellites for in-process `Vec` accumulation or unbounded channel drains (e.g., `journal_watcher.rs`, clipboard watcher) and replace with streaming publishes/chunked processing. *(System satellite watchers and BlobManager emissions now use bounded 1024-capacity channels; desktop/window watchers still need direct JetStream or bounded adapters. Filesystem/terminal now upsert source materials instead of re-registering.)*
- [x] Annotate helper utilities like `ChannelReceiverExt::drain_all` as test-only, or move them under a testing feature so production code doesn’t rely on them. (Done: channel helper modules gated behind the `channel-testing` feature in `sinex-test-utils`.)
- [x] Ensure `docs/vision/streaming-architecture.md` explicitly links to the staging-stream implementation and references this policy.

**Exit criteria:** Production code no longer depends on arbitrary channel caps for flow control; tests/tools are the only consumers of the drain helpers.

---

## 4. CI & Tooling Hygiene

**Decision:** Keep CI deterministic and aligned with dev workflows.

### Tasks

- [x] Ensure the Nextest profile used in CI matches the documented “reliable” profile (or document the difference if we stick with default). (CI now runs `cargo nextest … --profile reliable` and coverage inherits it.)
- [x] Add linters/checks that fail CI when `#[tokio::test]` is used in workspace crates (outside proc-macro/test-harness contexts); everything should use `#[sinex_test]`.
- [x] Add lint or static analysis that forbids `sqlx::query(` and `sqlx::query_as(` (non-macro versions) so contributors stick to compile-time-checked macros (`query!`, `query_as!`, etc.).
- [x] CI entrypoints now call `cargo xtask` (fmt, lint+forbidden, check, nextest reliable, smoke fixtures, VM smoke, sqlx prepare, db helpers) directly without devenv task wrappers.
- [x] Workflow README now documents the active xtask-backed pipelines (ci.yml, db-checks, schema management/compat/auto-update) so old helper scripts can stay removed.

**Exit criteria:** Fresh clone CI runs without touching removed paths; container images are pinned; lint guards protect against reintroducing deprecated patterns.

---

## 5. Gateway & Security Baseline

**Decision:** Gateway RPC endpoints must require authentication when exposed beyond localhost; CLI already expects `SINEX_RPC_TOKEN`.

### Tasks

- [x] Implement token-based auth in `sinex-gateway` (Axum layer): reject unauthenticated JSON-RPC calls, allow binding to `127.0.0.1` without a token for dev shells.
- [x] Extend CLI (`cli/exo.py`) to send the token header automatically when `SINEX_RPC_TOKEN` or `--rpc-token` are set (already partially wired).
- [x] Add integration tests that exercise authenticated/unauthenticated flows.
- [x] Document the default security stance in `README.md` / gateway docs.

**Exit criteria:** Gateway refuses unauthenticated requests unless explicitly configured; tests cover the path; docs state the requirement. ✅ Complete (defer follow-on work like rate-limit telemetry to the security hardening tracker).

---

## 6. Processor Model Cleanup

**Decision:** `HotlogAutomaton` is deprecated—everything must implement `StatefulStreamProcessor`.

### Tasks

- [x] Mark the Hotlog trait as `#[deprecated(note = "...")]` and add a lint/check to fail CI on new uses. (Added `deprecated` shim + crate-level `#![deny(deprecated)]` in `sinex-satellite-sdk`.) *(Shim since removed after migration completed.)*
- [x] Port remaining automata to `StatefulStreamProcessor` + `processor_main!`. (Health, content, and PKM automata now have processor_main! binaries; search/analytics already on the unified runner.)
- [x] Remove legacy Hotlog implementation artifacts once no crate depends on them. (Deleted unused `automaton.rs` in `sinex-health-aggregator`; removed Hotlog shim from `sinex-satellite-sdk`.)
- [x] Update docs/tests to reflect the unified processor model. (Health/content/PKM automaton docs now note processor_main! and SSP entrypoints.)

**Exit criteria:** `rg HotlogAutomaton` returns zero outside deprecated shim files; all automata share one runner path; docs no longer mention Hotlog.

---

## 7. RPC Dispatcher Scan/Explore Completion

**Decision:** `sinex-rpc-dispatcher` must implement scan/explore modes per the SSP interface so CLI “scan/explore” commands work end-to-end.

### Tasks

- [ ] Flesh out the `scan` method for historical and continuous horizons (pull from Postgres logs or JetStream subjects as designed). *(In-memory checkpoints/history now recorded during scans; still needs real RPC metrics and JetStream/DB wiring.)*
- [ ] Implement the `ExplorationProvider` methods (source state, ingestion history, coverage analysis, exports) with real data. *(Stub now surfaces scan history/export paths; replace with live dispatcher insights.)*
- [ ] Add integration tests covering pagination, checkpoint updates, and restart/resume scenarios.
- [ ] Wire dispatcher CLI to the new scan/explore implementation and document expected flags/subjects in `cli/README.md`.

**Exit criteria:** RPC dispatcher scan/explore commands function via CLI/automation; tests verify behaviour; NotImplemented errors are gone.

---

## 8. Documentation Consistency Fixes

**Decision:** Resolve documentation consistency issues (sensd tense, broken links, missing status markers) across canonical docs.

### Tasks

- [x] Fix tense/temporal markers in `project-target-state.md`.
- [x] Repair or remove references to deleted files (`docs/plan_v3.txt`, `docs/TARGET_final.md`, etc.).
- [x] Add current-phase indicators to the JetStream migration roadmap or replace it with an explicit “Completed” note.
- [x] Add “Last Verified” stamps to canonical docs (only after we have automated verification baked into the workflow). (Added to `docs/current/README.md`, architecture set, provenance, and security posture.)

**Exit criteria:** Documentation consistency tasks are checked off and canonical docs remain link-clean.

---

*This report is the canonical backlog. Delete items as they are completed.*

```
