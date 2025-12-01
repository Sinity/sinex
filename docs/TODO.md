# Sinex TODO Tracker

> **Historical context:** Some tasks mention sensd or gRPC ingestion from the pre-JetStream era. They remain for archival clarity; consult `docs/way.md` for the current JetStream-only design.

Authoritative backlog for the gaps identified during the recent codebase survey. Each task lists the owning files, concrete steps, and the validation expected (including fail-first tests).

> **Fail-first guarantee:** Every open task below cites at least one Nextest case or regression that currently fails. If a listed test turns green without the corresponding fix, update this tracker immediately so we don't lose signal.

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
