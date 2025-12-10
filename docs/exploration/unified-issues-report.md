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

**Recent Fixes (current branch)**

- Material assembler panic paths hardened (removed `expect` and return errors).
- Material assembler state map now uses per-material locks so concurrent materials no longer serialize on a global lock.
- Ingestion shutdown now waits for background tasks before closing DB pools.
- Native messaging secret comparison uses constant-time equality.
- Systemd hardening applied to ingestd, gateway, and satellite units (ProtectSystem strict, PrivateTmp, NoNewPrivileges, AF restriction).
- git-annex add surfaces disk-full/permission/corruption errors; path validation tests now reject symlinks and cover Unicode paths.

## 1. Critical & Immediate Actions (Week 1)

**Goal:** Stabilize production, prevent data loss, and close security vulnerabilities.

### 1.1 Data Integrity & Corruption

- **Fix subnano precision loss (NEW-C1)**
  - **Context:** `events.rs` stores nanoseconds in `i16` columns but retrieves them as full nanoseconds, losing 99.99% of precision.
  - **File:** `crate/lib/sinex-core/src/db/repositories/events.rs`
  - **Action:** Audit `ts_orig_subnano` usage. Ensure storage and retrieval logic matches.
- **Standardize lock ID endianness (NEW-C2)**
  - **Context:** `distributed_locking.rs` uses BigEndian while `state_machine.rs` uses LittleEndian for `i64` conversions.
  - **Action:** Pick one standard (BigEndian recommended for network/DB sortability) and apply consistently.
- **Remove dangerous UNIQUE indexes (NEW-C3, NEW-C4)**
  - **File:** `crate/lib/sinex-schema/src/schema/entities.rs`
  - **Action:** Remove `ix_entities_type` (prevents >1 entity per type) and entity relation unique indexes (prevents graph edges).
  - **Migration:** Create a new migration to drop these indexes immediately.

### 1.2 Production Stability (Crashes/Panics)

- **Fix panics in hot paths (NEW-C5, C6, C7)**
  - **Files:** `material_assembler.rs:474` (unwrap on buffer), `events.rs:1051` (unwrap in loop), `terminal_satellite.rs` (expects).
  - **Action:** Replace `.expect()`/`.unwrap()` with proper `Result` propagation and error logging.
- **Prevent config integer overflow (NEW-C8)**
  - **File:** `rpc_dispatcher.rs:289`
  - **Action:** Validate config values at load time; return error instead of panicking at runtime.

### 1.3 Security & Hardening

- **Apply systemd security hardening to all services (NEW)**
  - **Context:** Currently only `preflight-verification.nix` is hardened. Ingestd, Gateway, and Satellites run exposed.
  - **Files:** `nixos/modules/*.nix`
  - **Snippet:**

        ```nix
        serviceConfig = {
            ProtectSystem = "strict";
            ProtectHome = true;
            PrivateTmp = true;
            NoNewPrivileges = true;
            ProtectKernelTunables = true;
            ProtectControlGroups = true;
            RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
            LockPersonality = true;
        };
        ```

- **Add SIGTERM handler to ingestd (NEW)**
  - **File:** `crate/core/sinex-ingestd/src/main.rs`
  - **Issue:** ingestd only catches `SIGINT`. systemd uses `SIGTERM` for stop, causing unclean shutdowns.
  - **Action:** Use `tokio::signal::unix::SignalKind` to catch both.
  - **Snippet:**

        ```rust
        // Replace ctrl_c() with:
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        ```

- **Merge Blob Manager security patch (TODO #58)**
  - **Files:** `blob_manager.rs`, `secure_blob_manager_patch.rs`
  - **Action:** Fold security fixes (path traversal, symlink guards) into main file and delete the patch.
- **Harden Annex path safety (TODO #43)**
  - **Action:** Introduce `VerifiedPath` type to prevent raw string usage in filesystem operations.

### 1.4 Database & Concurrency

- **Add database query timeouts (NEW)**
  - **Context:** Queries in `jetstream_consumer.rs` can hang indefinitely, blocking the consumer.
  - **Action:** Wrap critical `fetch_all` calls in `tokio::time::timeout`.
- **Fix RwLock held during I/O (NEW)**
  - **File:** `material_assembler.rs`
  - **Issue:** Async RwLock held across file operations blocks all material processing.
  - **Action:** Load necessary data, drop lock, perform I/O, re-acquire if needed.

---

## 2. High Priority (Weeks 2-3)

**Goal:** Close architecture gaps, restore disabled tests, and complete the JetStream migration.

### 2.1 Architecture & Refactoring

- **Complete Stage-as-You-Go JetStream Migration (TODO #49, 51, 64, 88)**
  - **Context:** Satellites still write directly to `raw.source_material_registry` and `raw.temporal_ledger`, causing race conditions with ingestd.
  - **Action:**
        1. Remove `PgPool` dependency from `StageAsYouGoContext`.
        2. Emit `MaterialSlice` events via JetStream.
        3. Let ingestd's `MaterialAssembler` be the *single writer* to the database.
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

- **Implement `reset_checkpoint()` (NEW)**
  - **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`
  - **Snippet:**

        ```rust
        pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
            self.pool.checkpoints().delete(
                &ProcessorName::new(&self.processor_name),
                &ConsumerGroup::new(&self.consumer_group),
                &ConsumerName::new(&self.consumer_name),
            ).await?;
            info!(processor = %self.processor_name, "Checkpoint reset");
            Ok(())
        }
        ```

- **Implement `get_checkpoint_stats()` (NEW)**
  - **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`
  - **Action:** Implement stats retrieval (currently returns empty/zero stats).
- **Fix Shutdown Polling (NEW)**
  - **File:** `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs`
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

---

## 4. Active Implementation Initiatives

*Tracked from `implementation-plan.md`*

1. **Schema Pipeline Unification**: ✅ Complete. Rust types are source of truth.
2. **Documentation Alignment**: ✅ Complete. Docs reflect JetStream-only world.
3. **Channel Hygiene**: 🔄 In Progress. Audit satellites for unbounded channels.
4. **Processor Model Cleanup**: ✅ Complete. HotlogAutomaton removed.
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

43. **Annex path safety & symlink lookup hardening**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs`, `secure_blob_manager_patch.rs`.
    - **Steps:** Integrate the secure path patch (reject traversal/symlink tricks), replace brittle `find_symlink_path` assumptions with annex queries, and extend validation to cover percent-encoded traversal.
    - **Tests:** Extend `path_validation_test`/blob manager tests with traversal and symlink escape cases; ensure failures are recorded instead of served.

44. **Native messaging auth not fully exercised**
    - **Files:** `crate/core/sinex-gateway/src/native_messaging.rs`.
    - **Steps:** Add end-to-end tests with a fake browser extension manifest to prove allow/deny semantics beyond unit auth checks; ensure logging isn’t the only guard.
    - **Tests:** Integration test that simulates real native messaging payloads and asserts rejections/acceptance per configured IDs/secrets.

45. **Gateway insecure bypass remains enabled**
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, docs.
    - **Steps:** Gate or remove `SINEX_GATEWAY_ALLOW_INSECURE=1` in production profiles; document dev-only usage and add a test that fails when the env is set in non-dev mode.
    - **Tests:** Integration test that asserts RPC startup fails without a token unless explicitly in dev/insecure mode.

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

48. **RPC transport security for TCP bindings is missing**
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, NixOS module defaults, docs/current/architecture/security-architecture.md.
    - **Steps:** Require TLS/mTLS when the gateway binds to TCP (Unix socket remains default); disallow `SINEX_GATEWAY_ALLOW_INSECURE=1` outside dev; add cert/key options (agenix-delivered) and enforce token + TLS for any non-localhost binding.
    - **Tests:** Integration test that TCP startup fails without TLS; test that with cert/key the server accepts TLS and rejects unauthenticated clients.

49. **Browser activity capture is missing**
    - **Files:** new browser extension + gateway/native messaging bridge, ingest pipeline.
    - **Steps:** Implement the browser extension event source per `docs/roadmap/features/browser-extension.md`: capture URLs/titles/dom summaries with explicit opt-in; publish via native messaging → JetStream. Update native messaging auth to cover this path.
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

54. **Paths still passed as raw strings**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/annex/path_validator.rs`, `blob_manager.rs`, related callers.
    - **Steps:** Introduce a `VerifiedPath` newtype with a private constructor that enforces validation; update blob/path consumers to accept `VerifiedPath` instead of raw strings/PathBuf.
    - **Tests:** Extend path validation/security tests to require `VerifiedPath` and ensure traversal/symlink cases are rejected at compile-time API boundaries.

55. **Stateful automata lack sharding/affinity**
    - **Files:** stateful automata (analytics, session-aware processors), routing helpers.
    - **Steps:** Add a `Shardable` trait + consistent hashing router for JetStream subjects to guarantee per-key ordering/affinity; adopt in stateful processors.
    - **Tests:** Integration test that events with the same shard key always hit the same worker and preserve order under parallel workers.

56. **Retry/idempotency not encoded in types**
    - **Files:** retry helpers, satellite/ingestd operations.
    - **Steps:** Add marker traits (e.g., `Idempotent`) for operations eligible for automatic retry; enforce via retry wrappers.
    - **Tests:** Unit tests that non-idempotent ops are refused by retry helpers; positive test for idempotent ops.

57. **Units/size/times use raw integers**
    - **Files:** config structs and validation for timeouts/size limits in ingestd/satellites.
    - **Steps:** Introduce small newtypes for bytes/durations in new/updated configs to prevent unit mixups; adopt in validation boundaries (not a wholesale rewrite).
    - **Tests:** Config parsing tests that catch unit mixups; compile-time type checks in affected modules.

58. **Blob manager security patch is unmerged**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/annex/secure_blob_manager_patch.rs`, `blob_manager.rs`.
    - **Steps:** Fold the security fixes from `secure_blob_manager_patch.rs` into `blob_manager.rs` (path validation, symlink/traversal guards), then delete the patch file.
    - **Tests:** Extend blob/path validation tests to cover the patched behavior; ensure regression in `blob_manager_detects_corruption_on_retrieve` and path traversal tests stay green.

59. **Satellites still require direct DB access (violates edge isolation)**
    - **Files:** `crate/lib/sinex-satellite-sdk` (checkpoint manager, Stage-as-You-Go), `crate/core/sinex-ingestd`.
    - **Steps:** Move checkpoints to NATS KV/stream, route stage-as-you-go/material writes exclusively via JetStream/ingestd, and remove PgPool dependencies from satellites. Align with the edge-mode TODO to enforce NATS-only satellites.
    - **Tests:** Integration run of a satellite with no DATABASE_URL (NATS-only) that still succeeds; ensure duplicate ledger insert races disappear.

60. **Events repository is a God module**
    - **Files:** `crate/lib/sinex-core/src/db/repositories/events.rs`.
    - **Steps:** Split into writer/reader/analytics modules; keep cascade/helpers isolated. Reduce cognitive load and surface narrower traits for callers.
    - **Tests:** Ensure existing tests still pass; add smoke tests for the separated modules if needed.

61. **Gateway insecure bypass remains a production foot-gun**
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, docs.
    - **Steps:** Remove or hard-gate `SINEX_GATEWAY_ALLOW_INSECURE=1` to localhost/dev; add TLS/mTLS config for TCP bindings (see item 48).
    - **Tests:** Integration that asserts TCP startup without TLS/auth fails; dev-mode allows only loopback.

62. **Syslog/journal watcher shells out to journalctl**
    - **Files:** `crate/satellites/sinex-system-satellite/src/journal_watcher.rs`.
    - **Steps:** Replace `journalctl` subprocess parsing with a native journal API (e.g., sd-journal bindings) to reduce brittleness and improve performance.
    - **Tests:** Integration test with journal fixtures via native API; ensure existing watcher tests still pass.

63. **COUNT(*) used for event counts**
    - **Files:** `crate/lib/sinex-core/src/db/repositories/events.rs` (count_all, stats).
    - **Steps:** Replace exact `COUNT(*)` in dashboards/stats with estimates or a maintained counter to avoid full scans at scale.
    - **Tests:** Unit/integration test that the new count path returns reasonable estimates and doesn’t block on large tables.

64. **Stage-as-You-Go still writes directly to Postgres**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`, `acquisition_manager.rs`.
    - **Steps:** Remove direct inserts into `raw.source_material_registry`/`raw.temporal_ledger` from satellites; emit begin/slice/end via JetStream and let ingestd’s MaterialAssembler own persistence.
    - **Tests:** Regression `jetstream_material_ingest_conflicts_with_satellite_inserts` should be fixed; new test to confirm NATS-only path succeeds without DB.

## Gateway Hardening

1. **Require explicit TCP opt-in and authentication for JSON-RPC** — ✅ *Completed via GatewayAuth enforcement*
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `docs/current/architecture/UserInteraction_And_Query_Architecture.md`, CLI.
    - **Status:** `sinex-gateway` now refuses to start unless `SINEX_RPC_TOKEN` (or `SINEX_RPC_TOKEN_FILE`) is provided. Every request must present `Authorization: Bearer <token>` (or `X-Sinex-Rpc-Token`). CLI commands accept `--rpc-token` and automatically attach the header; tests `gateway_auth_blocks_missing_token`, `gateway_auth_accepts_bearer_header`, and `gateway_auth_accepts_custom_header` cover the new flow. `SINEX_GATEWAY_ALLOW_INSECURE=1` remains as the explicit dev/test escape hatch.

2. **Enforce rate limiting and payload caps on RPC** — ✅ *Guards wired via tower middleware*
   - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `crate/core/sinex-gateway/docs/rpc_server.md`.
   - **Status:** Router is wrapped in `LoadShed + ConcurrencyLimit + Timeout + RequestBodyLimit` with env-tunable knobs (`SINEX_GATEWAY_MAX_CONCURRENCY`, `SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`, `SINEX_GATEWAY_MAX_BODY_BYTES`). Tests `concurrency_limit_returns_429`, `timeout_layer_returns_504`, and `body_limit_returns_413` confirm each guard.

3. **Validate native-messaging origins**
   - **Files:** `crate/core/sinex-gateway/src/native_messaging.rs`, `docs/native_messaging.md`.
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

8. **Wire real watchers into `SystemProcessor`**
   - **Files:** `crate/satellites/sinex-system-satellite/src/unified_processor.rs`, `dbus_watcher.rs`, `journal_watcher.rs`, `udev_watcher.rs`, `systemd_watcher.rs`.
   - **Steps:** instantiate the watchers in `initialize`, store handles, and start their async loops in `start_continuous_monitoring`; ensure they emit events via `EventEmitter`.
   - **Tests:** fail-first Nextest case that asserts watchers remain `None` today (e.g., `system_processor_emits_no_watchers`), then replace with positive assertions once wiring exists.
   - **Status:** `system_processor_still_lacks_watchers` (`crate/satellites/sinex-system-satellite/tests/system_processor_watchers.rs`) now fails (once `libdbus-1` is available) because the watcher snapshot still reports every watcher as `None`.

9. **Add integration tests for each watcher**
   - **Files:** watcher modules + new tests under `crate/satellites/sinex-system-satellite/tests/`.
   - **Steps:** use mocks/fakes (e.g., a stub D-Bus bus, journalctl with fixtures) to assert payload parsing and event emission; ensure tests cover failure paths.
   - **Tests:** once real watcher wiring exists, add per-watcher integration tests using fakes (journal fixtures, stub D-Bus, etc.); avoid placeholder tests until those APIs are ready.
   - **Status:** `dbus_watcher_should_emit_signal_events`, `journal_watcher_should_emit_entry_events`, `udev_watcher_should_emit_device_events`, and `systemd_watcher_should_emit_unit_events` (all in `crate/satellites/sinex-system-satellite/tests/system_processor_watchers.rs`) now fail because the processor never wires or emits from the real watcher loops.

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

15. **Restore BlobManager integration tests**
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/integration/blob_manager_test.rs`, annex-related modules.
    - **Steps:** ensure the annex-backed harness runs deterministically (git-annex available, temp repos drained) and re-enable the dedupe/corruption/large-file tests that currently assume sensd ingestion.
    - **Tests:** bring back the existing integration tests (`dedupe`, corruption, large file`) targeting the real`BlobManager`; they should fail until blob verification + annex plumbing behave under JetStream.
    - **Status:** `blob_manager_detects_corruption_on_retrieve` (`crate/lib/sinex-satellite-sdk/tests/integration/blob_manager_test.rs`) now fails because `retrieve_content` happily serves mutated annex files instead of verifying their hashes or erroring.

16. **Re-enable blob path validation regression test**
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs`.
    - **Steps:** once task 15 provides a usable BlobManager, finish the regression test to assert safe/dangerous paths.
    - **Tests:** piggyback on the restored BlobManager integration tests—once the mock exists, re-activate the regression that feeds dangerous paths and expect a failure.
    - **Status:** `blob_manager_rejects_percent_encoded_traversal` (`crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs`) now fails because percent-encoded parent traversals still pass `validate_path`, allowing ingestion attempts instead of being rejected up front.

17. **Uncomment schema property/integration tests**
    - **Files:** `crate/lib/sinex-core/tests/property/schema_property_test.rs`.
    - **Steps:** extend the `#[sinex_test]` macro (or move to sync contexts) so proptest + async works, then restore the commented suites.
    - **Tests:** once the `#[sinex_test]` macro supports async property tests, re-enable the commented suites; skip adding a fail-first placeholder today.
    - **Status:** `schema_registry_should_drive_json_validation` (`crate/lib/sinex-core/tests/property/schema_property_test.rs`) now fails because registering an event schema does not influence `validate_json`, proving the property/integration path is still disabled.

## Additional Priorities

18. **Deprecate `raw.sensor_jobs` / sensd schema** — ✅ *Completed via canonical schema rewrite*
    - **Files:** `crate/lib/sinex-schema/src/schema/sensd.rs`, residual `.sqlx` caches, docs referencing sensd.
    - **Steps:** drop the tables in the squashed migration (or gate them behind a feature), scrub `.sqlx` artifacts, and rewrite any docs/tools still referencing sensd workflows.
    - **Status:** The squashed migration no longer creates `raw.sensor_jobs` / `raw.sensor_states`, `ensure_required_extensions` skips unavailable extensions cleanly, and the dev database was rebuilt (`cargo run -p sinex-schema -- up`) to verify the tables are gone.

19. **Document ingestor job metadata**
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`.
    - **Steps:** when submitting jobs (or emitting events), include the actual material ULID and path metadata so downstream components do not rely on parsing `target_uri`.
    - **Tests:** once metadata is carried through, add an integration test that exercises `submit_document_job` + `process_material` and asserts emitted `document.ingested` events contain the ULID and path fields explicitly.

20. **Replay control bus resilience**
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `crate/core/sinex-gateway/src/replay_control`.
    - **Steps:** implement exponential backoff + monitoring when `spawn_replay_control` fails instead of silent warn-and-disable; expose health info to the gateway CLI.
    - **Tests:** integration test that currently shows the replay client missing when NATS is down; expect failure until retries/metrics exist.
    - **Status:** `service_container_should_fail_when_replay_control_unavailable` (`crate/core/sinex-gateway/tests/replay_control_resilience_test.rs`) now fails because `ServiceContainer::new` still returns `Ok` with `replay_control=None` when NATS connections error instead of surfacing the failure.

21. **Structured DLQ metrics and tooling** — ✅ *Completed via `exo dlq metrics`*
    - **Files:** `cli/exo.py`, `tests/cli_missing_commands.rs`.
    - **Status:** The new `exo dlq metrics` command surfaces backlog summaries, per-category counts, and top offending automata over a configurable window; `exo_dlq_metrics_command_reports_stats` now passes.

22. **Gateway performance isolation**
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `sinex-services`.
    - **Steps:** refactor long-running queries (analytics/search) to async tasks or chunked pagination so one RPC cannot hog the shared DB pool.
    - **Tests:** after the async refactor, add a stress test (or benchmark harness) that fires multiple expensive queries concurrently and ensures throughput improves; no useful fail-first coverage is practical before the refactor.
    - **Status:** `analytics_queries_block_each_other_with_single_connection` (`crate/lib/sinex-services/tests/analytics_service_test.rs`) now fails because two analytics queries against a single-connection pool block each other, demonstrating the lack of workload isolation.

23. **Heartbeat-driven alerting for satellites** — ✅ *Completed via heartbeat alert sink plumbing*
    - **Files:** `sinex-satellite-sdk/src/heartbeat.rs`, `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`.
    - **Status:** Heartbeat emitter now logs structured `process.degraded` / `process.failed` entries (with deduplicated transitions) and the regression tests `heartbeat_emits_degraded_alert_on_error_spike` / `heartbeat_emits_failed_alert_only_on_transition` (`crate/lib/sinex-satellite-sdk/tests/heartbeat_metrics_regression.rs`) cover the behavior.

24. **Gateway CLI teardown awareness** — ✅ *Completed via RPC error guidance helper*
    - **Files:** `cli/exo.py`, `crate/core/sinex-gateway/src/rpc_server.rs`.
    - **Status:** `handle_rpc_error` now surfaces tailored hints for 401/429 (auth tokens vs. rate-limit/`--use-db` guidance) and the coverage suite `test_query_surfaces_rate_limit_guidance` / `test_query_prompts_for_auth_on_unauthorized` (`cli/tests/test_cli_error_guidance.py`) passes.

25. **Watcher teardown and restart handling**
    - **Files:** `dbus_watcher.rs`, `journal_watcher.rs`, `systemd_watcher.rs`, `udev_watcher.rs`.
    - **Steps:** add explicit shutdown signals to stop spawned tasks, and ensure the unified processor can restart watchers on reconfiguration.
    - **Status:** `processors_should_stop_background_tasks_on_shutdown` (`crate/lib/sinex-satellite-sdk/tests/processor_shutdown_leak_test.rs`) now fails because the default `StatefulStreamProcessor::shutdown` leaves spawned tasks running forever.

26. **Gateway structured logging + tracing context**
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`.
    - **Steps:** introduce request IDs, user/session tags, and propagate them into service-layer logs for auditability.
    - **Tests:** when request IDs are wired, add an integration test that issues an RPC call with a tracing subscriber configured to capture events and asserts the resulting log contains the propagated `request_id`.
    - **Status:** `rpc_responses_include_request_id_header` (`crate/core/sinex-gateway/src/rpc_server.rs`) now fails because the router still responds without any `x-request-id` header or structured trace context.

27. **DLQ / confirmation CLI commands** — ✅ *Implemented CLI wiring*
    - **Files:** `cli/exo.py` (DLQ group enhancements + new `confirmations` group).
    - **Status:** `exo dlq list`/`purge` short-circuit gracefully when the database is unavailable, and `exo confirmations tail` surfaces recent confirmation events (with fallback messaging when `DATABASE_URL` isn’t set). Tests `test_dlq_list_command_exists`, `test_dlq_purge_command_exists`, and `test_confirmations_tail_command_exists` now pass.

28. **Remove dead sensd stubs from satellites** — ✅ *Completed by removing the sensd schema and DocumentProcessor sensd hooks*
    - **Status:** `sinex-document-ingestor` now ingests files directly and the sensd schema/table definitions (`raw.sensor_jobs/raw.sensor_states`) have been dropped.

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
    - **Files:** `docs/testing-priorities-and-roadmap.md`.
    - **Steps:** fold the new gateway/system tasks into that roadmap so engineers know the order of operations; ensures the plan stays in sync with this TODO file.
    - **Tests:** manual verification.

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

36. **Document job monitor leaks tasks and never retires jobs**
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs` (`monitor_jobs`, `scan`).
    - **Steps:** retain the spawned JoinHandle (or run the monitor inside `ProcessorCommand::Service`), wire it to a shutdown signal, and update job status/material IDs as work completes. Remove the `NULL::ulid` placeholder and set `status='retired'` once events are emitted so jobs are not reprocessed endlessly.
    - **Tests:** add fail-first async test `document_monitor_leaks_job_loop` under `crate/satellites/sinex-document-ingestor/tests/` that asserts `tokio::spawn` count increases per scan (current behaviour) and that job statuses remain `active`. After the fix, the monitor should shut down cleanly and rows should transition to `retired`.
    - **Status:** `document_monitor_leaks_job_loop` now fails because the monitor keeps looping forever and leaves `raw.sensor_jobs.status = 'active'` despite emitting events.

37. **Desktop satellite still depends on sensd/DB connectivity**
    - **Files:** `crate/satellites/sinex-desktop-satellite/src/unified_processor.rs`.
    - **Steps:** remove the sensd job-submission remnants, instantiate real watchers (or AcquisitionManager-driven sensors), and emit events via JetStream instead of dropping metadata into Postgres.
    - **Tests:** `desktop_processor_emits_clipboard_events` (`crate/satellites/sinex-desktop-satellite/src/unified_processor.rs`) now fails because snapshot scans still don't emit any clipboard/window events.

38. **SDK `JobManager` still operates on `raw.sensor_jobs` / `raw.sensor_states`** — ✅ *Removed alongside the sensd schema*
    - **Status:** The legacy `JobManager` and sensor executors were deleted, and the core migration no longer creates `raw.sensor_jobs` / `raw.sensor_states`.

39. **Replay planner bypasses ingestion invariants (DB target)**
    - **Files:** `cli/replay_planner.py`.
    - **Steps:** stop inserting directly into `core.events` (which fails due to generated `ts_ingest` and missing provenance). Route through ingestd or stage-as-you-go so provenance and schema checks pass.
    - **Tests:** `test_replay_planner_database_target_errors` (`cli/tests/test_replay_planner.py`) now fails because the planner still attempts direct Postgres writes.

40. **Replay planner NATS target is unimplemented**
    - **Files:** same file as task 39.
    - **Steps:** implement publishing to `sinex.control.replay` with operation IDs in message headers.
    - **Tests:** `test_replay_planner_nats_target_publishes` (`cli/tests/test_replay_planner.py`) now fails because the NATS branch remains a stub.

41. **Document jobs don’t carry material metadata** — ✅ *Completed via `document capture metadata plumbing`*
    - **Status:** `DocumentProcessor::insert_document_capture_job_with_metadata` now records a `source_material_id` for every job, `submit_document_job` uses it, and `document_job_records_material_id` verifies the field is persisted.

42. **Watcher tasks never shut down**
    - **Files:** `sinex-system-satellite` watchers, desktop watchers.
    - **Steps:** add cancellation handles so `ProcessorRunner::shutdown` stops each spawned `tokio::spawn` loop.
    - **Tests:** fail-first integration test `system_watchers_stop_on_shutdown`; today watchers run forever after shutdown.
    - **Status:** `processor_runner_triggers_processor_shutdown` (`crate/lib/sinex-processor-runtime/tests/processor_runner.rs`) now fails because `ProcessorRunner` never calls `StatefulStreamProcessor::shutdown` when handling service-mode shutdowns, so background watcher tasks keep running.

43. **Gateway blob endpoints lack auth/size quotas** — ✅ *RPC token plus blob quota enforced*
    - **Files:** `crate/core/sinex-gateway/src/handlers.rs`, `sinex-services/src/content.rs`.
    - **Status:** JSON-RPC requires `SINEX_RPC_TOKEN`, and `handle_store_blob` now enforces `SINEX_GATEWAY_MAX_BLOB_BYTES` (default 5 MiB) before decoding payloads. The regression `blob_routes_should_enforce_auth_and_quota` ensures oversized uploads error before reaching git-annex.

44. **Desktop clipboard/window watchers still write directly to Postgres tables**
    - **Files:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs` and related modules.
    - **Steps:** replace raw `INSERT INTO raw.source_material_registry/raw.temporal_ledger` calls with AcquisitionManager + JetStream writes so the satellite no longer requires `DATABASE_URL`.
    - **Tests:** `desktop_clipboard_requires_database_pool` (unit test inside `clipboard.rs`) now fails because `store_clipboard_source_material` returns `None` when `db_pool` is absent.

45. **Document job monitor compares ULIDs to file paths, so jobs never retire** — ✅ *Completed via `document capture metadata plumbing`*
    - **Status:** `monitor_jobs` now selects `config->>'source_material_id'`, filters on jobs that carry the field, and compares against event payloads. The regression `document_monitor_detects_completed_job` passes once a matching `document.ingested` event exists.

46. **Large clipboard captures are silently dropped** — ✅ *Completed via `clipboard annex ingestion`*
    - **Status:** Clipboard watcher now initializes a `BlobManager`, ingests oversized payloads into git-annex, annotates metadata with blob references, and the regression test `clipboard_large_content_is_persisted` passes.

47. **System satellite emits events with invalid provenance references**
    - **Files:** `crate/satellites/sinex-system-satellite/src/dbus_watcher.rs`, `journal_watcher.rs`, `systemd_watcher.rs`, `udev_watcher.rs`.
    - **Steps:** replace the hard-coded `system_bootstrap_id` calls to `Provenance::from_synthesis_safe` with real provenance (e.g., material provenance via AcquisitionManager or actual parent events). If a bootstrap ULID is required, persist the corresponding event during startup so parent IDs exist.
    - **Status:** `system_processor_still_uses_synthetic_provenance` (`crate/satellites/sinex-system-satellite/tests/system_processor_watchers.rs`) now fails because snapshot scans continue to emit synthesis provenance instead of real material-backed IDs.

48. **Terminal history watcher re-reads entire history file each poll**
    - **Files:** `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs` (`HistoryWatcherContext::monitor`).
    - **Steps:** replace `fs::read_to_string` with incremental tailing (seek from saved offset, read chunks) so large history files don’t get reloaded every interval.
    - **Tests:** new unit test `terminal_watcher_tails_incrementally` that currently fails because memory usage scales linearly with file size per poll.

49. **Stage-as-You-Go contexts still require direct Postgres access from satellites**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`, `acquisition_manager.rs`, any satellite constructing `StageAsYouGoContext`.
    - **Steps:** remove the `PgPool` dependency from StageAsYouGo/AcquisitionManager so satellites publish begin/slice/end purely via JetStream and let ingestd persist source materials/ledger entries. Satellites should run with only NATS + annex, no `DATABASE_URL`.
    - **Tests:** add integration test `satellite_runs_without_database_url` (e.g., terminal processor) which currently panics when `runtime.db_pool()` is missing due to StageAsYouGo registering materials directly in `raw.source_material_registry`.
    - **Status:** `jetstream_material_ingest_conflicts_with_satellite_inserts` and `stage_as_you_go_context_should_not_require_live_database` (`crate/lib/sinex-satellite-sdk/tests/stage_as_you_go_requires_db.rs`) now fail because Stage-as-You-Go still writes ledger rows itself *and* assumes a live Postgres pool instead of emitting JetStream material slices.

50. **JobManager never marks jobs as running/completed** — ✅ *Completed via `sensor job status normalization`*
    - **Status:** `raw.sensor_jobs` now permits the expanded lifecycle (`active`, `paused`, `running`, `completed`, `failed`, `retired`), `JobManager::update_job_status` preserves the requested state, and cleanup logic only tracks jobs that remain `active|paused|running`. Tests `job_manager_updates_status_properly` and `sensor_job_status_transitions` cover the new states.

51. **Satellites still insert source material/ledger rows, racing ingestd**
    - **Files:** `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs`, `stage_as_you_go.rs`.
    - **Steps:** stop writing directly to `raw.source_material_registry` and `raw.temporal_ledger` from satellites. In the JetStream architecture ingestd’s `MaterialAssembler` is the single writer; the current duplicate inserts hit the unique constraint `uk_temporal_ledger_material_offset` when ingestd replays the same material. Emit begin/slice/end only via JetStream and let ingestd persist the rows.
    - **Tests:** integration test `jetstream_material_ingest_conflicts_with_satellite_inserts` that currently reproduces a duplicate-key violation when both the satellite and ingestd try to insert the same `(source_material_id, offset_start)`.
    - **Status:** `jetstream_material_ingest_conflicts_with_satellite_inserts` (`crate/lib/sinex-satellite-sdk/tests/stage_as_you_go_requires_db.rs`) now fails because Stage-as-You-Go writes the ledger row before ingestd runs, causing a duplicate-key error when the ingest service attempts the same insert.

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
    - **Files:** `docs/` tree (`README.md`, `way.md`, `way_2.md`, `JETSTREAM_MIGRATION_STATUS.md`, `IMPLEMENTATION_PROGRESS.md`, testing docs, misc analyses).
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

99. **Crash-recovery tests for material acquisition**
    - **Files:** `crate/lib/sinex-satellite-sdk` acquisition path, material assembler.
    - **Steps:** Add adversarial tests simulating satellite crashes at stages (early, mid, finalization) and concurrent acquisition, verifying registry/ledger state and checkpoint recovery; ensure tests are compatible with the JetStream-only single-writer model.
    - **Tests:** New crash-recovery suite covering early/mid/finalization crashes, orphan detection, checkpoint recovery, and concurrent acquisition.

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

# Appendix B: Architecture Deep Dive (Restored from Deep Dive Findings)

## Cross-Cutting Concerns

### Idempotency Patterns

Idempotency is achieved through a **three-layer defense** across the system:

#### 1. NATS Message Deduplication

All satellites use `Nats-Msg-Id` headers for publisher-side deduplication:

```rust
// crate/lib/sinex-satellite-sdk/src/nats_publisher.rs
let msg_id = format!("{}:{{"{}"}}", satellite_id, event.id);
headers.insert("Nats-Msg-Id", msg_id);
```

JetStream maintains a deduplication window (default 2 minutes) to reject duplicate message IDs.

#### 2. Database-Level Idempotency

All event inserts use `ON CONFLICT DO NOTHING`:

```rust
// crate/core/sinex-ingestd/src/jetstream_consumer.rs:741
builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid as \"id!\"");
```

This ensures duplicate ULID insertions are silently ignored, not errored.

#### 3. Confirmation Stream Compaction

The `sinex.events.confirmations` stream uses `max_msgs_per_subject: 1`:

```rust
// Configuration ensures only the latest confirmation per subject is retained
StreamConfig {
    max_msgs_per_subject: 1,  // Compacts to latest confirmation
    ...
}
```

This prevents automata from seeing duplicate confirmations for the same event.

#### Assessment: **CONSISTENT** ✅

Idempotency is uniformly implemented across all layers. The system achieves exactly-once semantics through this layered approach.

---

### Backpressure Mechanisms

Backpressure is coordinated across four layers:

#### 1. Gateway Layer

```rust
// crate/core/sinex-gateway/src/rpc_server.rs
ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(30)))
    .layer(ConcurrencyLimitLayer::new(100))
    .layer(RateLimitLayer::new(100, Duration::from_secs(1)))
```

- **Concurrency limit**: 100 concurrent requests
- **Timeout**: 30 seconds per request
- **Rate limit**: 100 requests/second

#### 2. JetStream Consumer Layer

```rust
// crate/core/sinex-ingestd/src/jetstream_consumer.rs
ConsumerConfig {
    max_ack_pending: 100,      // Flow control
    ack_wait: Duration::from_secs(30),
    max_deliver: 10,           // Retry limit before DLQ
    ...
}
```

**Note**: `max_ack_pending` is currently hardcoded, not configurable.

#### 3. Database Pool Layer

```rust
// Connection pool configuration
PgPoolOptions::new()
    .max_connections(10)
    .connect_timeout(Duration::from_secs(30))
```

#### 4. Internal Channel Bounds

```rust
// Typical bounded channel pattern
let (tx, rx) = tokio::sync::mpsc::channel(100);
```

#### Assessment: **MOSTLY CONSISTENT** ⚠️

Backpressure is well-coordinated, but there's a configuration mismatch:
- Config allows `batch_size: 1000` but consumer pulls only 100 messages
- `max_ack_pending` is hardcoded and should be configurable

---

### Graceful Shutdown

#### Signal Handling Patterns

**sinex-ingestd** (partial):

```rust
// Only catches SIGINT, missing SIGTERM
tokio::signal::ctrl_c().await?;
```

**Satellites** (complete):

```rust
// Catches both signals
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint = signal(SignalKind::interrupt())?;

tokio::select! {
    _ = sigterm.recv() => { /* shutdown */ }
    _ = sigint.recv() => { /* shutdown */ }
}
```

#### Shutdown Sequence

1. Signal received
2. Cancellation token triggered
3. In-flight messages completed (or NAK'd for redelivery)
4. Checkpoint saved to database
5. Connections closed

#### Polling-Based Shutdown Detection

```rust
// crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs:778-782
tokio::select! {
    _ = async {
        while !self.should_stop() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    } => { /* shutdown */ }
    result = self.run_ingestor_startup_sequence() => { /* completed */ }
}
```

**Issue**: 100ms polling introduces up to 100ms shutdown latency.

#### Assessment: **INCONSISTENT** ❌

- **ingestd only catches SIGINT** - systemd sends SIGTERM by default
- 100ms polling for shutdown is inefficient; should use channels or events
- Checkpoint saving happens, but `reset_checkpoint()` is NOT IMPLEMENTED

---

### Configuration Precedence

#### Loading Order

All services use Figment for configuration with clear precedence:

```rust
// Typical pattern across all services
Figment::new()
    .merge(Toml::file("config.toml"))       // 1. Config file (lowest)
    .merge(Env::prefixed("SINEX_"))         // 2. Environment variables
    .merge(Serialized::defaults(&cli_args)) // 3. CLI args (highest)
```

#### Environment Variable Prefixes

| Service | Prefix | Example |
|---------|--------|---------|
| Gateway | `SINEX_` | `SINEX_RPC_PORT` |
| Ingestd | `INGESTD_` | `INGESTD_BATCH_SIZE` |
| Satellites | `SATELLITE_` | `SATELLITE_POLL_INTERVAL` |

**Issue**: Inconsistent prefixes across services.

#### Secret Injection

```nix
# nixos/modules/secrets.nix
environment.SINEX_DB_PASSWORD = config.sops.secrets.db-password.path;
```

Secrets are injected via environment variables pointing to agenix-managed paths.

#### Assessment: **MOSTLY CONSISTENT** ⚠️

- Clear precedence (file → env → CLI)
- Inconsistent prefix naming conventions
- Secret handling is properly externalized

---

## Critical Path Analysis

### Ingestion Hot Path

**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`

#### Message Flow

```
NATS JetStream
    │
    ▼ pull_batch(100)
┌─────────────────────┐
│   process_batch()   │ ← Lines 334-647
│   ├── Deserialize   │
│   ├── Validate      │
│   ├── Parse ULID    │
│   └── Build batch   │
└─────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ persist_batch_optimized()   │ ← Lines 687-753
│ └── Multi-row INSERT        │
│     ON CONFLICT DO NOTHING  │
└─────────────────────────────┘
    │
    ▼ AFTER commit
┌─────────────────────────────┐
│ publish_confirmations()     │ ← Lines 598-605
│ └── To sinex.events.{id}    │
└─────────────────────────────┘
    │
    ▼
┌─────────────────────┐
│      ack_all()      │
└─────────────────────┘
```

#### Key Configuration

```rust
// Consumer configuration
ConsumerConfig {
    deliver_policy: DeliverPolicy::All,
    ack_policy: AckPolicy::Explicit,
    ack_wait: Duration::from_secs(30),
    max_deliver: 10,           // After 10 failures → DLQ
    max_ack_pending: 100,      // Flow control
    filter_subject: "sinex.events.*".to_string(),
}
```

#### Batch Processing

```rust
// Lines 362-380: Pull up to 100 messages with 5s timeout
let messages = consumer
    .fetch()
    .max_messages(100)
    .expires(Duration::from_secs(5))
    .messages()
    .await?;
```

#### Critical Invariant: Confirmations After Commit

```rust
// Lines 598-605: Order matters for exactly-once
// 1. DB transaction commits
// 2. THEN confirmations published
// 3. THEN messages ACK'd

// If we crash after commit but before ACK:
// - Messages redeliver (idempotent insert)
// - Confirmations republish (compacted stream)
// Result: No duplicates, no lost events
```

---

### Provenance Enforcement

Provenance enforces **audit trail integrity** via an XOR constraint: every event must have EITHER material provenance (external source) OR synthesis provenance (derived from other events), but never both or neither.

#### Application-Level Validation

```rust
// jetstream_consumer.rs:482-521
fn validate_provenance(raw_event: &RawEvent) -> Result<PreparedProvenance> {
    match (&raw_event.material_id, &raw_event.source_event_ids) {
        // Material provenance (from external source)
        (Some(material_id), None) => Ok(PreparedProvenance::Material {
            material_id: material_id.clone(),
            byte_offset_start: raw_event.byte_offset_start,
            byte_offset_end: raw_event.byte_offset_end,
        }),

        // Synthesis provenance (derived from other events)
        (None, Some(source_ids)) => Ok(PreparedProvenance::Synthesis {
            source_event_ids: source_ids.clone(),
        }),

        // XOR violation - both present
        (Some(_), Some(_)) => Err(anyhow!("Event has both material and synthesis provenance")),

        // Neither present - default to self-referential
        (None, None) => {
            warn!(event_id = %raw_event.id, "Event missing provenance; assuming self-referential");
            Ok(PreparedProvenance::Synthesis {
                source_event_ids: vec![raw_event.id.as_uuid()],
            })
        }
    }
}
```

#### Database-Level Constraint

```sql
-- From schema migrations
ALTER TABLE raw.events ADD CONSTRAINT provenance_xor CHECK (
    (material_id IS NOT NULL AND source_event_ids IS NULL) OR
    (material_id IS NULL AND source_event_ids IS NOT NULL)
);
```

#### Default Self-Referential Provenance

When neither provenance type is provided, the system defaults to self-referential synthesis (event is its own source). This is a **recovery mechanism**, not the intended path.

---

### Checkpoint Lifecycle

**File**: `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

#### CheckpointState Structure

```rust
// Lines 91-107
pub struct CheckpointState {
    /// Unified checkpoint data (External/Internal/Stream/Timestamp)
    pub checkpoint: Checkpoint,

    /// Total number of messages/events processed
    pub processed_count: u64,

    /// Last activity timestamp
    pub last_activity: chrono::DateTime<chrono::Utc>,

    /// Processor-specific state data
    pub data: Option<serde_json::Value>,

    /// Checkpoint version (for schema evolution)
    pub version: u32,  // Currently v2
}
```

#### Checkpoint Variants

```rust
pub enum Checkpoint {
    None,                              // Initial state
    Internal { event_id: Ulid, ... },  // Automata (event ULID)
    External { position: u64, ... },   // Ingestors (file offset, etc.)
    Stream { message_id: String, ... }, // NATS message ID
    Timestamp { at: DateTime, ... },   // Time-based processing
}
```

#### Load with Migration

```rust
// Lines 282-368
pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
    let row = self.pool.checkpoints().get_by_processor(...).await?;

    if let Some(row) = row {
        if row.checkpoint_data.is_some() {
            // Version 2+: Deserialize unified format
            let checkpoint: Checkpoint = serde_json::from_value(data)?;
            return Ok(CheckpointState { checkpoint, ... });
        } else {
            // Version 1: Migrate legacy format
            warn!("Migrating legacy checkpoint format");
            let legacy = LegacyCheckpointState { ... };
            let unified = CheckpointState::from(legacy);
            self.save_checkpoint(&unified).await?;
            return Ok(unified);
        }
    }

    // No checkpoint found - start fresh
    Ok(CheckpointState::default())
}
```

#### Save with Atomic Upsert

```rust
// Lines 387-435
pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
    let checkpoint_data = serde_json::to_value(&state.checkpoint)?;

    self.pool.checkpoints().upsert(
        CheckpointIdentity { processor, consumer_group, consumer_name },
        last_processed_id,
        processed_count,
        Some(checkpoint_data),
    ).await?;
}
```

#### NOT IMPLEMENTED Functions

```rust
// Lines 458-474: Reset checkpoint
pub async fn reset_checkpoint(&self) -> SatelliteResult<()> {
    warn!("Reset checkpoint not implemented in new API");
    Ok(())  // No-op!
}

// Lines 477-486: Get checkpoint stats
pub async fn get_checkpoint_stats(&self) -> SatelliteResult<CheckpointStats> {
    Ok(CheckpointStats {
        total_checkpoints: 0,
        max_processed: 0,
        last_update: None,
        first_checkpoint: None,
    })  // Returns empty stats!
}
```

---

### Three-Phase Startup

**File**: `crate/lib/sinex-satellite-sdk/src/runtime/stream/mod.rs`

#### Phase Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTOR STARTUP                         │
├─────────────────────────────────────────────────────────────┤
│  Phase 1: SNAPSHOT                                          │
│  └── Capture current state of external system               │
│      (if supports_snapshot capability)                      │
├─────────────────────────────────────────────────────────────┤
│  Phase 2: GAP-FILL                                          │
│  └── Process historical data since last checkpoint          │
│      (if supports_historical capability)                    │
├─────────────────────────────────────────────────────────────┤
│  Phase 3: CONTINUOUS                                        │
│  └── Real-time event streaming                              │
│      (if supports_continuous capability)                    │
└─────────────────────────────────────────────────────────────┘
```

#### Implementation

```rust
// Lines 609-668
async fn run_ingestor_startup_sequence(&mut self) -> SatelliteResult<()> {
    let capabilities = self.processor.capabilities();

    // Phase 1: Snapshot (if supported)
    if capabilities.supports_snapshot {
        info!("Phase 1: Starting snapshot capture");
        let checkpoint = self.checkpoint_manager.load_checkpoint().await?;
        self.processor.scan_snapshot(checkpoint).await?;
    }

    // Phase 2: Gap-filling (if supported and needed)
    if capabilities.supports_historical {
        info!("Phase 2: Starting historical gap-fill");
        let checkpoint = self.checkpoint_manager.load_checkpoint().await?;
        let report = self.processor.scan_historical(checkpoint).await?;
        self.checkpoint_manager.save_checkpoint(&report.checkpoint).await?;
    }

    // Phase 3: Continuous processing
    if capabilities.supports_continuous {
        info!("Phase 3: Starting continuous processing");
        self.run_continuous_processing().await?;
    }

    Ok(())
}
```

#### Capability-Driven Behavior

```rust
pub struct ProcessorCapabilities {
    pub supports_snapshot: bool,     // Can capture point-in-time state
    pub supports_historical: bool,   // Can backfill from checkpoint
    pub supports_continuous: bool,   // Can stream real-time events
    pub requires_confirmation: bool, // Needs DB confirmation before processing
}
```

Different satellites implement different capability sets:
- **File ingestor**: snapshot + historical + continuous
- **Desktop events**: continuous only
- **Health automaton**: continuous + requires_confirmation

---

## NixOS Deployment Audit

### Module Completeness

#### Available Modules (10 total)

| Module | Purpose | Config Options |
|--------|---------|----------------|
| `default.nix` | Service orchestration | enable, users, groups |
| `ingestd.nix` | Event ingestion daemon | batch_size, workers, nats_url |
| `gateway.nix` | HTTP/RPC gateway | port, auth, rate_limits |
| `nats.nix` | NATS JetStream server | jetstream, clustering |
| `blob-storage.nix` | Binary artifact storage | path, max_size |
| `satellite-services.nix` | Satellite systemd units | per-satellite config |
| `preflight-verification.nix` | Startup gates | health_checks, timeouts |
| `database.nix` | PostgreSQL + TimescaleDB | extensions, pools |
| `secrets.nix` | Agenix secret management | paths, permissions |
| `monitoring.nix` | Prometheus/Grafana | metrics, dashboards |

#### Option Coverage Assessment

Most production-critical options are exposed, but some are missing:
- `max_ack_pending` not configurable (hardcoded)
- `shutdown_timeout` not exposed
- Individual satellite enable/disable flags

### Service Orchestration

#### Startup Order

```nix
# Defined via systemd dependencies
postgresql.service
    └── nats.service
        └── sinex-ingestd.service
            └── sinex-gateway.service
                └── satellite-*.service
```

#### Dependency Declaration

```nix
# satellite-services.nix
systemd.services."satellite-${name}" = {
    after = [ "network.target" "nats.service" "sinex-ingestd.service" ];
    requires = [ "nats.service" ];
    wantedBy = [ "multi-user.target" ];
};
```

#### Health Checks

```nix
# preflight-verification.nix
ExecStartPre = [
    "${pkgs.bash}/bin/bash -c 'until pg_isready; do sleep 1; done'"
    "${pkgs.bash}/bin/bash -c 'until nats-server --help; do sleep 1; done'"
];
```

### Failure Recovery

#### Restart Policies

```nix
# Standard across all services
systemd.services.sinex-ingestd = {
    serviceConfig = {
        Restart = "on-failure";
        RestartSec = "5s";
        StartLimitBurst = 3;
        StartLimitIntervalSec = "60s";
    };
};
```

#### Preflight Gates

```nix
# preflight-verification.nix
systemd.services.sinex-preflight = {
    serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
    };
    script = ''
        # Verify all dependencies before main services start
        pg_isready -h localhost
        nats-server --check
        # Additional health checks...
    '';
};
```

### Security Hardening

#### CRITICAL FINDING: Missing Hardening ❌

**Only the preflight service has security hardening:**

```nix
# preflight-verification.nix (ONLY SERVICE WITH HARDENING)
serviceConfig = {
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    NoNewPrivileges = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectControlGroups = true;
    RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    MemoryDenyWriteExecute = true;
    LockPersonality = true;
};
```

**Production services (ingestd, gateway, satellites) have ZERO hardening:**

```nix
# satellite-services.nix (NO HARDENING)
serviceConfig = {
    ExecStart = "${satellite}/bin/${name}";
    Restart = "on-failure";
    # NO security directives!
};
```

#### Secret Management

```nix
# secrets.nix
sops.secrets = {
    "sinex/db-password" = {
        owner = "sinex";
        group = "sinex";
        mode = "0400";
    };
};
```

Secrets are properly managed via agenix with appropriate permissions.

---

## Patterns Summary

### Consistent Patterns

| Pattern | Implementation | Assessment |
|---------|---------------|------------|
| **Idempotency** | NATS Msg-Id + ON CONFLICT + compaction | ✅ Excellent |
| **ULID Keys** | All entities use time-ordered ULIDs | ✅ Consistent |
| **Provenance XOR** | App + DB dual-layer enforcement | ✅ Robust |
| **Figment Config** | File → Env → CLI precedence | ✅ Clear |
| **Checkpoint Format** | Unified v2 with migration | ✅ Forwards-compatible |
| **Confirmation Flow** | Always AFTER DB commit | ✅ Exactly-once safe |
| **Secret Handling** | Externalized via agenix | ✅ Secure |

### Inconsistent/Missing Patterns

| Pattern | Issue | Impact |
|---------|-------|--------|
| **Signal Handling** | ingestd only catches SIGINT, not SIGTERM | 🔴 High - systemd shutdown may not work |
| **Security Hardening** | Zero hardening on production services | 🔴 Critical - attack surface exposed |
| **Shutdown Detection** | 100ms polling instead of channels | 🟡 Medium - latency/CPU waste |
| **Config Prefixes** | SINEX_vs INGESTD_ vs SATELLITE_ | 🟡 Medium - confusing |
| **Checkpoint Reset** | `reset_checkpoint()` not implemented | 🟡 Medium - ops gap |
| **Checkpoint Stats** | `get_checkpoint_stats()` returns empty | 🟡 Medium - observability gap |
| **max_ack_pending** | Hardcoded, not configurable | 🟡 Medium - tuning limitation |
| **Batch Size Mismatch** | Config allows 1000, consumer pulls 100 | 🟢 Low - misleading config |

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

**Decision:** All current docs must reflect the JetStream-only ingestion described in `docs/way.md`. Transactional outbox references move to historical sections.

### Tasks

- [x] Sweep docs for “transactional outbox,” “sensd,” or “gRPC ingestion” references and either delete or mark as historical context. (Historical banners added to the remaining references in reports/TODO files.)
- [x] Add short banners to historical docs (e.g., `/docs/historical/**`) to clarify their status.
- [x] Update `README.md` and satellite guides to mention JetStream confirmations, DLQ subjects, and the confirmation-aware automaton flow.

**Exit criteria:** No current doc suggests that the outbox or sensd are active components; contributors find a single, coherent story that matches `docs/way.md`.

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

- ✅ `.github/workflows/ci.yml` / `sqlx-cache.yml` / `sqlx-check.yml` run migrations from `crate/lib/sinex-schema` and all Postgres images are pinned to `timescale/timescaledb:2.15.2-pg16`.
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

**Decision:** Resolve the issues listed in `docs/ANALYSIS_INDEX.md` (sensd tense, broken links, missing status markers).

### Tasks

- [x] Fix tense/temporal markers in `project-target-state.md`.
- [x] Repair or remove references to deleted files (`docs/plan_v3.txt`, `docs/TARGET_final.md`, etc.).
- [x] Add current-phase indicators to `way.md` or replace with an explicit “Completed” note.
- [x] Add “Last Verified” stamps to canonical docs (only after we have automated verification baked into the workflow). (Added to `docs/current/README.md`, architecture set, provenance, and security posture.)

**Exit criteria:** `ANALYSIS_INDEX.md` items are checked off and the file reflects the updated status.

---

*This report is the canonical backlog. Delete items as they are completed.*

```
