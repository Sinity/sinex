# Sinex TODO Tracker

> **Historical context:** Some tasks mention sensd or gRPC ingestion from the pre-JetStream era. They remain for archival clarity; consult `docs/way.md` for the current JetStream-only design.

Authoritative backlog for the gaps identified during the recent codebase survey. Each task lists the owning files, concrete steps, and the validation expected (including fail-first tests).

> **Fail-first guarantee:** Every open task below cites at least one Nextest case or regression that currently fails. If a listed test turns green without the corresponding fix, update this tracker immediately so we don't lose signal.

## Core Architecture & Control Plane (New Findings 2025-12-02)

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
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, NixOS module defaults, docs/architecture/security-architecture.md.  
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
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `docs/architecture/UserInteraction_And_Query_Architecture.md`, CLI.  
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
    - **Tests:** bring back the existing integration tests (`dedupe`, corruption, large file`) targeting the real `BlobManager`; they should fail until blob verification + annex plumbing behave under JetStream.  
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
    - **Files:** `sinex-satellite-sdk/src/heartbeat.rs`, `docs/architecture/SystemOperations_And_Integrity_Architecture.md`.  
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
    - **Regression Test:** `cargo check` (via `devenv tasks run dev:check`) ensures no `sea_query` references remain under `sinex-core` outside migrations; add `rg "sea_query" crate/lib/sinex-core` CI guard if desired.

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
