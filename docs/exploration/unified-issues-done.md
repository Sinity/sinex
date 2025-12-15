# Unified Issues – Completed Items

Archived entries moved from `docs/exploration/unified-issues-report.md` so the main backlog focuses on in-progress work.

## Executive Summary

**Recent Fixes (current branch)**

- Material assembler panic paths hardened (removed `expect` and return errors).
- Material assembler state map now uses per-material locks so concurrent materials no longer serialize on a global lock.
- Ingestion shutdown now waits for background tasks before closing DB pools.
- Native messaging secret comparison uses constant-time equality.
- Systemd hardening applied to ingestd, gateway, and satellite units (ProtectSystem strict, PrivateTmp, NoNewPrivileges, AF restriction).
- Checkpoint reset/stats implemented for satellites (no-op stubs removed).
- Stage-as-You-Go now supports JetStream-only mode (DB-optional registration/finalization paths).
- Stage-as-You-Go and AcquisitionManager no longer write source material/ledger rows directly; ingestd is the single database writer.
- Gateway TCP bindings now require tokens and TLS; insecure mode is limited to Unix sockets.
- Checkpoints can persist to NATS KV; ingestd broadcasts active schemas to `system.schemas.active`.
- Satellites subscribe to `system.schemas.active` broadcasts in edge mode to cache active schemas.
- git-annex add surfaces disk-full/permission/corruption errors; path validation tests now reject symlinks and cover Unicode paths.
- `core.events.ts_orig_subnano` now stores full nanosecond precision via INT4 storage and repository fixes; ingestion no longer truncates timestamp data.
- Advisory lock IDs use big-endian conversion across replay state machine and distributed locking, preventing mismatched lock acquisition.
- Removed unsafe unique indexes from `core.entities`/`core.entity_relations` so multiple entities per type and fan-out relations are supported; migration drops the legacy indexes.
- `SourceMaterialRepository::register_external_in_flight` plus ingestd’s `MaterialAssembler` make JetStream the single writer for source materials: begin/end metadata is published over NATS, ingestd registers the ULID in `raw.source_material_registry`, and finalization merges metadata before writing temporal ledger rows.

## 1. Critical & Immediate Actions (Week 1)

### 1.1 Data Integrity & Corruption

- **Fix subnano precision loss (NEW-C1)** — ✅
  - **Status:** `core.events.ts_orig_subnano` now uses an `INTEGER` column (see `crate/lib/sinex-schema/src/schema/events.rs:92-118`), and the repositories consistently persist/extract the nanosecond remainder via `ts.nanosecond() as i32` (`crate/lib/sinex-core/src/db/repositories/events.rs:455,897`). The ingest path no longer truncates to `i16`, so nanosecond precision is preserved end to end.

- **Standardize lock ID endianness (NEW-C2)** — ✅
  - **Status:** Both advisory-lock helpers convert ULIDs with `i64::from_be_bytes(...)` (`crate/lib/sinex-core/src/db/distributed_locking.rs:138-145` and `crate/lib/sinex-core/src/db/replay/state_machine.rs:14-27`), so every consumer now hashes lock IDs with the same Big-Endian ordering. Replay control + distributed locking integration tests pass under `cargo nextest run -p sinex-gateway` (run `18f2e461-b4f2-485a-b0bb-387324e8065d`).

- **Remove dangerous UNIQUE indexes (NEW-C3, NEW-C4)** — ✅
  - **Status:** The schema generator no longer creates `ix_entities_type` or relation-level `UNIQUE` indexes that prevented fan-out (`crate/lib/sinex-schema/src/schema/entities.rs:124-210`). The only remaining uniqueness constraint is on `(entity_type, name)` plus the scoped `uk_entity_relations_triple` tuple, which preserves graph integrity while allowing multiple entities per type and arbitrary edge fan-out.

### 1.2 Production Stability (Crashes/Panics)

- **Fix panics in hot paths (NEW-C5, C6, C7)** — ✅
  - **Status:** `MaterialAssembler::handle_end`/`persist_state` now bubble errors instead of unwrapping (`crate/core/sinex-ingestd/src/material_assembler.rs:991-1055`), `EventRepository::insert_event`/`insert_many` derive subnanoseconds via safe `map_err` paths (`crate/lib/sinex-core/src/db/repositories/events.rs:455-524,897-938`), and terminal satellite watcher/tests keep unwraps inside test scaffolding only. Regression `cargo nextest run -p sinex-ingestd material_assembler::tests::buffered_slice_is_removed_and_returned` (run `20174d86-f1f7-4041-bfbd-65a72e26d007`) covers the assembler buffer hot path without panics.

- **Prevent config integer overflow (NEW-C8)** — ✅
  - **Status:** `RpcDispatcherProcessor::initialize` now runs `config.validate()` and surfaces a `SatelliteError::Configuration` before touching any limits (`crate/core/sinex-rpc-dispatcher/src/lib.rs:30-154`), so oversized CLI/env overrides are rejected at startup. Local `cargo nextest run -p sinex-rpc-dispatcher` attempts currently fail because the shared Postgres instance lacks the TimescaleDB extension required for sqlx compile-time checks; once that extension is available the dispatcher suite can run end to end.

- **Apply systemd security hardening to all services (NEW)** — ✅
  - **Status:** `nixos/modules/satellite-services.nix:78-110` defines the shared `mkBaseServiceConfig` with `ProtectSystem="strict"`, `ProtectHome=true`, `PrivateTmp=true`, `NoNewPrivileges=true`, AF restrictions, and constrained `ReadWritePaths`, and every ingestd/gateway/satellite unit consumes that helper (`nixos/modules/satellite-services.nix:134-220`). The hardened overlay is now part of the default module set.

- **Add SIGTERM handler to ingestd (NEW)** — ✅
  - **Status:** `crate/core/sinex-ingestd/src/main.rs:105-160` wires `tokio::signal::unix::signal(SignalKind::terminate)` alongside SIGINT so systemd stop targets trigger an orderly `service.shutdown()`. The shutdown future logs transitions and prevents abrupt pool closures.

- **Merge Blob Manager security patch (TODO #58)** — ✅ *blob manager now enforces validated paths + secure temp files*
  - **Files:** `blob_manager.rs`, `path_validator.rs`
  - **Status:** `ingest_file` accepts raw `&str` inputs and validates via `validate_and_convert_path`, `ingest_from_bytes` writes through `create_secure_temp_path`, `find_symlink_path` already switched to `git-annex contentlocation`, and the legacy `secure_blob_manager_patch.rs` file has been deleted.

- **Harden Annex path safety (TODO #43)** — ✅
  - **Status:** The `VerifiedPath` newtype (`crate/lib/sinex-satellite-sdk/src/annex/path_validator.rs:6-48`) now encapsulates sanitized paths and `BlobManager::ingest_file` only accepts `&VerifiedPath` (`crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs:163-267`), forcing callers to validate inputs at compile time. The `security` test binary (path validation suite) compiles once sqlx can operate offline; current runs on this host are blocked by the TimescaleDB extension missing from the dev Postgres instance.

### 1.4 Database & Concurrency

- **Add database query timeouts (NEW)** — ✅ `persist_batch_optimized` now awaits the multi-row insert with a 5s `tokio::time::timeout`, turning hung queries into surfaced database errors (`cargo nextest run -p sinex-ingestd`, run `b1b0a7ce-9e22-411c-b22f-475259f301b9` — JetStream consumer suites pass while the pre-existing `assembler_rejects_corrupted_slice_and_records_dlq` failure remains).
  - **Context:** Queries in `jetstream_consumer.rs` can hang indefinitely, blocking the consumer.
  - **Action:** Wrap critical `fetch_all` calls in `tokio::time::timeout`.

- **Fix RwLock held during I/O (NEW)** — ✅ `handle_end` now snapshots assembler state, drops the map/Mutex guards before annex imports or DB writes, and reinserts the Arc on retries (`cargo nextest run -p sinex-ingestd material_assembler::tests::buffered_slice_is_removed_and_returned`, run `7c02139f-d90e-49f5-b16b-b69e08d8e92d`).
  - **File:** `material_assembler.rs`
  - **Issue:** Async RwLock held across file operations blocks all material processing.
  - **Action:** Load necessary data, drop lock, perform I/O, re-acquire if needed.

## 2. High Priority (Weeks 2-3)

### 2.1 Architecture & Refactoring

        1. ✅ `StageAsYouGoContext` now always publishes begin/slice/end through `AcquisitionManager`, and the legacy `PgPool` / blob-ingest fallback path was removed entirely.
        2. ✅ `AcquisitionManager` now carries metadata on begin/end; ingestd receives the same JSON the satellite would have written.
        3. ✅ `SourceMaterialRepository::register_external_in_flight` lets ingestd create/update `raw.source_material_registry` rows using the ULID minted at the edge; `MaterialAssembler` registers records on begin and finalizes them (including merged metadata + ledger writes) on end.
        4. ✅ Every satellite/test that constructs `StageAsYouGoContext` now chains `.with_acquisition_manager(...)` (or injects one via `from_sender`), and the obsolete DB-required test was deleted so JetStream is the only supported path.
        5. ✅ Added the `stage_as_you_go_pipeline_end_to_end` integration test (`cargo nextest run -p sinex-satellite-sdk --test stage_as_you_go_integration`) which boots ingestd + JetStream, runs a log processor, and asserts ingestd persists the source material/event records.

### 2.3 Operational Gaps

- **Implement `reset_checkpoint()` (NEW)** — ✅ Deleting a processor/consumer triple uses `ProcessorName`/`ConsumerGroup`/`ConsumerName` identities and clears the optional KV entry; covered by `cargo nextest run -p sinex-satellite-sdk checkpoint_history_stats_and_reset` (run `dac966cb-fdfd-4bf7-becf-01184ca9470a`).
  - **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

- **Implement `get_checkpoint_stats()` (NEW)** — ✅ Stats now summarize `core.processor_checkpoints` rows for the processor/consumer pair and drive the same regression run above.
  - **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

- **Expose checkpoint history (NEW)** — ✅ `CheckpointManager::get_checkpoint_history(limit)` fetches recent rows for the processor/group/consumer triple (descending `updated_at`), enforces sane default limits, and surfaces negative counters as checkpoint errors (also verified by run `dac966cb-fdfd-4bf7-becf-01184ca9470a`).
  - **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

- **Fix Shutdown Polling (NEW)** — ✅ `LifecycleManager::shutdown_future` now clones a `tokio::sync::watch` receiver so shutdown notifications propagate instantly (no 100 ms sleep loop) and signals/Ctrl+C send through the channel (`cargo nextest run -p sinex-satellite-sdk lifecycle::tests::shutdown_future_notifies_without_polling`, run `61b444c1-2bf7-4cd1-bad0-39f37af1dd17`).
  - **File:** `crate/lib/sinex-satellite-sdk/src/lifecycle.rs`
  - **Action:** Replace 100ms polling loop with `tokio::sync::watch` channel for immediate shutdown.
  - **Snippet:**

        ```rust
        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        // In shutdown handler
        shutdown_tx.send(true)?;
        // In processing loop
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            result = process_next() => { ... }
        }
        ```

## 4. Active Implementation Initiatives

1. **Schema Pipeline Unification**: ✅ Complete. Rust types are source of truth.

2. **Documentation Alignment**: ✅ Complete. Docs reflect JetStream-only world.

4. **Processor Model Cleanup**: ✅ Complete. HotlogAutomaton removed.

## Core Architecture & Control Plane

43. **Annex path safety & symlink lookup hardening** — ✅ *BlobManager enforces validated paths*
    - **Files:** `crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs`, `crate/lib/sinex-satellite-sdk/src/annex/path_validator.rs`.
    - **Status:** `ingest_file` now accepts raw `&str` inputs and validates/sanitizes them via `validate_and_convert_path`, `ingest_from_bytes` writes through `create_secure_temp_path`, temporary files are removed explicitly, and path traversal coverage lives under `crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs`.

44. **Native messaging auth not fully exercised** — ✅
    - **Status:** `native_messaging.rs` enforces allowlists via `SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS` (optional shared secrets) and emits structured `native_messaging.auth` logs for both allow/deny decisions. The regression tests `native_messaging_rejects_untrusted_extensions`, `native_messaging_accepts_trusted_extension_with_secret`, and `native_messaging_rejects_missing_secret` (`crate/core/sinex-gateway/tests/native_messaging_auth_test.rs`) cover the real manifest/payload flow, and `cargo nextest run -p sinex-gateway` (run `18f2e461-b4f2-485a-b0bb-387324e8065d`) confirms the suite.

45. **Gateway insecure bypass remains enabled** — ✅
    - **Status:** `guard_tcp_auth` rejects any TCP bind when `GatewayAuth` is disabled (`crate/core/sinex-gateway/src/rpc_server.rs:545-569`), so `SINEX_GATEWAY_ALLOW_INSECURE=1` only applies to Unix-socket bindings. Tests `tcp_binding_disallows_insecure_mode`, `gateway_auth_blocks_missing_token`, and `gateway_auth_accepts_bearer_header` exercise the guardrails as part of `cargo nextest run -p sinex-gateway` (run `18f2e461-b4f2-485a-b0bb-387324e8065d`).

48. **RPC transport security for TCP bindings is missing** — ✅
    - **Status:** `rpc_server.rs` now refuses to bind TCP unless `SINEX_GATEWAY_TLS_CERT`/`SINEX_GATEWAY_TLS_KEY` are set, wraps the listener in `tokio-rustls`, and calls `guard_tcp_auth` so `SINEX_GATEWAY_ALLOW_INSECURE` is only permitted on Unix sockets. Optional client CA support enables mTLS. Tests `tcp_binding_disallows_insecure_mode`, `tls_paths_must_be_set_for_tcp`, and `gateway_auth_blocks_missing_token` cover the guardrails, and `cargo nextest run -p sinex-gateway` (run `18f2e461-b4f2-485a-b0bb-387324e8065d`) passed the suite.

54. **Paths still passed as raw strings** — ✅
    - **Status:** `crate/lib/sinex-satellite-sdk/src/annex/path_validator.rs:6-48` now exposes a `VerifiedPath` newtype; `BlobManager::ingest_file` accepts only `&VerifiedPath` and callers must parse/validate upfront (`crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs:163-270`). The security regression tests under `crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs` construct VerifiedPath instances before ingestion. Locally, `cargo nextest run -p sinex-satellite-sdk --test security` still fails to compile because the shared Postgres instance lacks the TimescaleDB extension needed for sqlx offline metadata; once TimescaleDB is available the suite can be rerun to capture a run ID.

58. **Blob manager security patch is unmerged** — ✅ *Patch merged, standalone file removed*
    - **Files:** `crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs`, `path_validator.rs`.
    - **Status:** The documented fixes now live in BlobManager (`ingest_file` validates string paths, `ingest_from_bytes` relies on secure temp files), path traversal tests remain in `tests/security/path_validation_test.rs`, and `secure_blob_manager_patch.rs` has been deleted.

61. **Gateway insecure bypass remains a production foot-gun** — ✅
    - **Status:** `GatewayAuth::from_env` now refuses to start without `SINEX_RPC_TOKEN` unless `SINEX_GATEWAY_ALLOW_INSECURE=1`, and `guard_tcp_auth` (`crate/core/sinex-gateway/src/rpc_server.rs:151-214, 531-560`) rejects TCP bindings when auth is disabled, forcing insecure mode to Unix sockets only. The regression tests `gateway_auth_blocks_missing_token` / `gateway_auth_accepts_bearer_header` cover the enforcement.
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, docs.
    - **Steps:** Remove or hard-gate `SINEX_GATEWAY_ALLOW_INSECURE=1` to localhost/dev; add TLS/mTLS config for TCP bindings (see item 48).
    - **Tests:** Integration that asserts TCP startup without TLS/auth fails; dev-mode allows only loopback.

64. **Stage-as-You-Go still writes directly to Postgres** — ✅ *JetStream-only path enforced*
    - **Files:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`, `acquisition_manager.rs`.
    - **Status:** Satellites publish begin/slice/end exclusively via `AcquisitionManager`, ingestd registers/finalizes the material rows, and the `stage_as_you_go_pipeline_end_to_end` integration test (`cargo nextest run -p sinex-satellite-sdk --test stage_as_you_go_integration`) guards the flow.

## Gateway Hardening

1. **Require explicit TCP opt-in and authentication for JSON-RPC** — ✅ *Completed via GatewayAuth enforcement*
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `docs/current/architecture/UserInteraction_And_Query_Architecture.md`, CLI.
    - **Status:** `sinex-gateway` now refuses to start unless `SINEX_RPC_TOKEN` (or `SINEX_RPC_TOKEN_FILE`) is provided. Every request must present `Authorization: Bearer <token>` (or `X-Sinex-Rpc-Token`). CLI commands accept `--rpc-token` and automatically attach the header; tests `gateway_auth_blocks_missing_token`, `gateway_auth_accepts_bearer_header`, and `gateway_auth_accepts_custom_header` cover the new flow. `SINEX_GATEWAY_ALLOW_INSECURE=1` remains as the explicit dev/test escape hatch.

2. **Enforce rate limiting and payload caps on RPC** — ✅ *Guards wired via tower middleware*
   - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `crate/core/sinex-gateway/docs/rpc_server.md`.
   - **Status:** Router is wrapped in `LoadShed + ConcurrencyLimit + Timeout + RequestBodyLimit` with env-tunable knobs (`SINEX_GATEWAY_MAX_CONCURRENCY`, `SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`, `SINEX_GATEWAY_MAX_BODY_BYTES`). Tests `concurrency_limit_returns_429`, `timeout_layer_returns_504`, and `body_limit_returns_413` confirm each guard.

   - **Steps:** ✅ Enforce allowlists via `SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS`, optionally requiring per-extension secrets. Structured logging now emits `native_messaging.auth` events for every allow/deny decision so operators can audit failures.
   - **Tests:** `native_messaging_rejects_untrusted_extensions`, `native_messaging_accepts_trusted_extension_with_secret`, and `native_messaging_rejects_missing_secret` in `crate/core/sinex-gateway/tests/native_messaging_auth_test.rs`.

## Content / Blob Pipeline

4. **Keep gateway out of the blob/event write path** — ✅ *Direct DB writes removed; follow-up re-publish TBD*
   - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `sinex-satellite-sdk/src/annex/blob_manager.rs`, gateway tests.
   - **Steps:** Gateway no longer drains BlobManager events into `EventRepository`/JetStream. BlobManager now accepts an optional event sink so satellites continue emitting via Stage-as-You-Go while gateway/storage helpers can disable emissions entirely. Follow-up: reintroduce JetStream publishing for CLI uploads once the command bus exists.
   - **Tests:** `content_store_blob_does_not_insert_events` (`crate/core/sinex-gateway/tests/blob_route_security_test.rs`) asserts the RPC surface does not mutate `core.events`. Future work should add a positive replay/path test once blob uploads are routed through ingestd.

5. **Migrate `sinex-document-ingestor` off sensd** — ✅ *Completed via direct ingestion pipeline*
   - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`, `.sqlx` artifacts, docs.
   - **Steps:** swap the `MaterialSlice` stub + `raw.sensor_jobs` polling for the SDK’s `AcquisitionManager`, stage-as-you-go ingestion, and JetStream slices; delete legacy SQL.
   - **Status:** `DocumentProcessor` now ingests files directly and emits `document.ingested` events without touching `raw.sensor_jobs`; `document_processor_emits_events_for_targets` (`crate/satellites/sinex-document-ingestor/tests/direct_ingestion_test.rs`) covers the behavior.

6. **Fix NULL material IDs in document job monitor** — ✅ *Completed via ULID parsing fix*
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`, associated tests.
    - **Status:** `monitor_jobs` now parses the JSON `source_material_id` field into a real `Ulid`, logs malformed configs, and the regressions `monitor_jobs_null_material_id`, `document_jobs_compare_ulids`, and `document_jobs_metadata` pass.

7. **Stream document data directly to annex** — ✅ *Document ingestor no longer buffers entire files*
   - **Status:** `DocumentProcessor::ingest_target` only inspects metadata + a 4 KiB sniff window to detect encoding, streams blobs via `BlobManager`, and enforces `max_document_size` before allocating memory. Oversized files are skipped with a warning, and `document_processor_emits_events_for_targets` continues to pass using the leaner path.

## System Satellite

8. **Wire real watchers into `SystemProcessor`** — ✅ *watchers start + surface readiness*
   - **Files:** `crate/satellites/sinex-system-satellite/src/unified_processor.rs`, `dbus_watcher.rs`, `journal_watcher.rs`, `udev_watcher.rs`, `systemd_watcher.rs`.
   - **Status:** `SystemProcessor::initialize` now seeds watcher handles and `start_*_stream` spawns the real async tasks (falling back to simulated events on error). The regression `system_processor_still_lacks_watchers` passes under `cargo nextest run -p sinex-system-satellite --test system_processor_watchers`, proving the processor exposes every watcher as ready before scans run.

9. **Add integration tests for each watcher** — ✅ *regressions wired through watcher suite*
   - **Files:** watcher modules + `crate/satellites/sinex-system-satellite/tests/system_processor_watchers.rs`.
   - **Status:** The watcher suite now includes per-source tests (`dbus_watcher_should_emit_signal_events`, `journal_watcher_should_emit_entry_events`, `udev_watcher_should_emit_device_events`, `systemd_watcher_should_emit_unit_events`). They execute in dry-run mode by default and become fully end-to-end when `SINEX_NATIVE_SYSTEM_TESTS=1`. Command: `cargo nextest run -p sinex-system-satellite --test system_processor_watchers`.

## Observability & Heartbeats

10. **Emit heartbeats for all processor modes** — ✅ *Completed via `command_requires_heartbeat` expansion*
    - **Status:** `command_requires_heartbeat` now returns `true` for service/scan/explore commands, the CLI macro spawns a `HeartbeatEmitter` for each mode, and the regression tests `scan_mode_emits_heartbeats` and `explore_mode_emits_heartbeats` verify the behavior.

11. **Improve heartbeat metrics (CPU/memory/lag)** — ✅ *Completed via `heartbeat emitter CPU + status refresh`*
    - **Status:** Heartbeat emitter now derives CPU usage from `getrusage`, keeps per-mode error rolling totals before reset, and the regression suite `heartbeat_metrics_regression` passes (CPU > 0, status transitions to `ProcessStatus::Failed` after repeated errors).

12. **Make process heartbeat status strongly typed** — ✅ *Completed in `ProcessStatus enum + heartbeat wiring`*
    - **Status:** `ProcessHeartbeatPayload` now uses the new `ProcessStatus` enum (`Healthy|Degraded|Failed`), `HeartbeatEmitter` emits typed statuses, and `process_status_test` verifies unknown strings are rejected.

## Schema Tooling

14. **Implement schema compatibility validation** — ✅ *Completed via `sinex-schema validate` diffing*
    - **Files:** `crate/lib/sinex-core/src/types/bin/sinex-schema.rs`.
    - **Status:** `sinex-schema validate <from> <to>` now loads the referenced schema JSON, reports missing required fields/type regressions/enum removals, and exits non-zero when any incompatibilities are found. Unit tests `detect_missing_required_fields` / `detect_enum_regressions` cover the comparator.

## Testing Coverage

15. **Restore BlobManager integration tests** — ✅ *git-annex harness + verification restored*
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/blob_manager_integration.rs`, annex modules.
    - **Status:** `blob_manager_deduplicates_content`, `blob_manager_round_trips_content`, and `blob_manager_detects_corruption_on_retrieve` now run deterministically against disposable git-annex repos. `GitAnnex::get_content` auto-detects keys vs. paths, `contentlocation` resolves storage paths, retrieval verifies SHA256/BLAKE3 digests, and tampering flips files to writable before corrupting. `cargo nextest run -p sinex-satellite-sdk --test blob_manager_integration` passes (`00c4a642… → c2f6ea18…` history recorded in CI notes).

16. **Re-enable blob path validation regression test** — ✅ *percent-encoded traversal blocked*
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/security.rs`, `crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs`, `crate/lib/sinex-core/src/types/validation/core.rs`.
    - **Status:** `validate_path` now rejects percent-encoded `..` segments before annex gets involved, and the regression `blob_manager_rejects_percent_encoded_traversal` (wired via `tests/security.rs`) passes under `cargo nextest run -p sinex-satellite-sdk --test security`.

17. **Uncomment schema property/integration tests** — ✅ *property suite re-enabled*
    - **Files:** `crate/lib/sinex-core/tests/property/schema_property_test.rs`.
    - **Status:** The async property harness runs under Nextest and `schema_registry_should_drive_json_validation` now proves that DB-registered schemas drive `EventValidator`. Command: `cargo nextest run -p sinex-core --test property_tests`.

## Additional Priorities

18. **Deprecate `raw.sensor_jobs` / sensd schema** — ✅ *Completed via canonical schema rewrite*
    - **Files:** `crate/lib/sinex-schema/src/schema/sensd.rs`, residual `.sqlx` caches, docs referencing sensd.
    - **Steps:** drop the tables in the squashed migration (or gate them behind a feature), scrub `.sqlx` artifacts, and rewrite any docs/tools still referencing sensd workflows.
    - **Status:** The squashed migration no longer creates `raw.sensor_jobs` / `raw.sensor_states`, `ensure_required_extensions` skips unavailable extensions cleanly, and the dev database was rebuilt (`cargo run -p sinex-schema -- up`) to verify the tables are gone.

21. **Structured DLQ metrics and tooling** — ✅ *Completed via `exo dlq metrics`*
    - **Files:** `cli/exo.py`, `tests/cli_missing_commands.rs`.
    - **Status:** The new `exo dlq metrics` command surfaces backlog summaries, per-category counts, and top offending automata over a configurable window; `exo_dlq_metrics_command_reports_stats` now passes.

23. **Heartbeat-driven alerting for satellites** — ✅ *Completed via heartbeat alert sink plumbing*
    - **Files:** `sinex-satellite-sdk/src/heartbeat.rs`, `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`.
    - **Status:** Heartbeat emitter now logs structured `process.degraded` / `process.failed` entries (with deduplicated transitions) and the regression tests `heartbeat_emits_degraded_alert_on_error_spike` / `heartbeat_emits_failed_alert_only_on_transition` (`crate/lib/sinex-satellite-sdk/tests/heartbeat_metrics_regression.rs`) cover the behavior.

24. **Gateway CLI teardown awareness** — ✅ *Completed via RPC error guidance helper*
    - **Files:** `cli/exo.py`, `crate/core/sinex-gateway/src/rpc_server.rs`.
    - **Status:** `handle_rpc_error` now surfaces tailored hints for 401/429 (auth tokens vs. rate-limit/`--use-db` guidance) and the coverage suite `test_query_surfaces_rate_limit_guidance` / `test_query_prompts_for_auth_on_unauthorized` (`cli/tests/test_cli_error_guidance.py`) passes.

27. **DLQ / confirmation CLI commands** — ✅ *Implemented CLI wiring*
    - **Files:** `cli/exo.py` (DLQ group enhancements + new `confirmations` group).
    - **Status:** `exo dlq list`/`purge` short-circuit gracefully when the database is unavailable, and `exo confirmations tail` surfaces recent confirmation events (with fallback messaging when `DATABASE_URL` isn’t set). Tests `test_dlq_list_command_exists`, `test_dlq_purge_command_exists`, and `test_confirmations_tail_command_exists` now pass.

28. **Remove dead sensd stubs from satellites** — ✅ *Completed by removing the sensd schema and DocumentProcessor sensd hooks*
    - **Status:** `sinex-document-ingestor` now ingests files directly and the sensd schema/table definitions (`raw.sensor_jobs/raw.sensor_states`) have been dropped.

## SQL Ergonomics Sweep

33. **Remove remaining SeaQuery call sites (outside schema/migration code)** — ✅ *Completed in `Range-aware replays and cascade repository refactor` follow-up*
    - **Status:** `seaquery_helpers.rs` modules/tests were removed and `repositories_common` now builds SQL via `format!`; only schema/migration crates retain SeaQuery.
    - **Regression Test:** `cargo check` (via `cargo xtask check`) ensures no `sea_query` references remain under `sinex-core` outside migrations; add `rg "sea_query" crate/lib/sinex-core` CI guard if desired.

34. **Sweep for aliased IDs (`SELECT id AS foo_id`) and align with schema names** — ✅ *Verified*
    - **Status:** Workspace `rg " AS [A-Za-z0-9_]+_id"` (excluding sqlx macro aliases like `"id!: ..."`) returns no business logic hits; search previously-flagged services (search, analytics) now bind `id` directly.
    - **Ongoing guard:** keep the search command in CI lint docs to ensure future aliases don’t regress.

35. **Adopt shared fixture constants across remaining test suites** — ✅ *Done*
    - **Status:** `rg "repo-test"` / `rg "query.safety"` only match `sinex-test-utils/src/constants.rs`; `integration_tests`, `type_safety_test`, and other suites import the constants via the prelude.
    - **Guard:** keep the `rg` check noted here so future literals get caught early.

## Legacy Cleanup & Provenance

38. **SDK `JobManager` still operates on `raw.sensor_jobs` / `raw.sensor_states`** — ✅ *Removed alongside the sensd schema*
    - **Status:** The legacy `JobManager` and sensor executors were deleted, and the core migration no longer creates `raw.sensor_jobs` / `raw.sensor_states`.

41. **Document jobs don’t carry material metadata** — ✅ *Completed via `document capture metadata plumbing`*
    - **Status:** `DocumentProcessor::insert_document_capture_job_with_metadata` now records a `source_material_id` for every job, `submit_document_job` uses it, and `document_job_records_material_id` verifies the field is persisted.

43. **Gateway blob endpoints lack auth/size quotas** — ✅ *RPC token plus blob quota enforced*
    - **Files:** `crate/core/sinex-gateway/src/handlers.rs`, `sinex-services/src/content.rs`.
    - **Status:** JSON-RPC requires `SINEX_RPC_TOKEN`, and `handle_store_blob` now enforces `SINEX_GATEWAY_MAX_BLOB_BYTES` (default 5 MiB) before decoding payloads. The regression `blob_routes_should_enforce_auth_and_quota` ensures oversized uploads error before reaching git-annex.

45. **Document job monitor compares ULIDs to file paths, so jobs never retire** — ✅ *Completed via `document capture metadata plumbing`*
    - **Status:** `monitor_jobs` now selects `config->>'source_material_id'`, filters on jobs that carry the field, and compares against event payloads. The regression `document_monitor_detects_completed_job` passes once a matching `document.ingested` event exists.

46. **Large clipboard captures are silently dropped** — ✅ *Completed via `clipboard annex ingestion`*
    - **Status:** Clipboard watcher now initializes a `BlobManager`, ingests oversized payloads into git-annex, annotates metadata with blob references, and the regression test `clipboard_large_content_is_persisted` passes.

49. **Stage-as-You-Go contexts still require direct Postgres access from satellites** — ✅ *edge mode no longer needs `DATABASE_URL`*
    - **Files:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`, `acquisition_manager.rs`, any satellite constructing `StageAsYouGoContext`.
    - **Status:** Stage-as-you-go contexts only need a JetStream connection: begin/slice/end are published via `AcquisitionManager`, ingestd performs the DB writes, and the NATS-only integration test (`cargo nextest run -p sinex-satellite-sdk --test stage_as_you_go_integration`) covers the flow.

50. **JobManager never marks jobs as running/completed** — ✅ *Completed via `sensor job status normalization`*
    - **Status:** `raw.sensor_jobs` now permits the expanded lifecycle (`active`, `paused`, `running`, `completed`, `failed`, `retired`), `JobManager::update_job_status` preserves the requested state, and cleanup logic only tracks jobs that remain `active|paused|running`. Tests `job_manager_updates_status_properly` and `sensor_job_status_transitions` cover the new states.

51. **Satellites still insert source material/ledger rows, racing ingestd** — ✅ *ingestd is the sole writer*
    - **Files:** `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs`, `stage_as_you_go.rs`.
    - **Status:** Satellites emit begin/slice/end via JetStream and ingestd’s `MaterialAssembler` handles registration/finalization, eliminating duplicate ledger inserts. Covered by `stage_as_you_go_pipeline_end_to_end`.

99. **Crash-recovery tests for material acquisition** — ✅
    - **Status:** `MaterialAssembler::handle_end` now aborts if the DB pool is already closed, assembler state directories are created eagerly, blob repo operations log structured errors, and the restart/concurrency suites explicitly quiesce background ingestd tasks before restarts. The JetStream-only acquisition path is guarded by `cargo nextest run -p sinex-satellite-sdk -E "binary(material_acquisition)"` (latest run `5c0cbb15-ae88-470a-91d9-426a779c94b1`).
    - **Coverage:** `material_acquisition_restart_recovery` simulates mid-flight restarts, `material_acquisition_concurrent_sessions_isolated` polls for completion via `WaitHelpers`, and `material_acquisition_out_of_order_slices` ensures orphan slices are reconciled. These suites collectively cover early/mid/finalization crashes plus concurrent acquisitions on the JetStream-only writer model.

## 4. CI & Tooling Hygiene

### Tasks

- ✅ `.github/workflows/ci.yml` / `sqlx-cache.yml` / `sqlx-check.yml` run migrations from `crate/lib/sinex-schema` and all Postgres images are pinned to `timescale/timescaledb:2.15.2-pg16`.
