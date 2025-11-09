# Sinex TODO Tracker

Authoritative backlog for the gaps identified during the recent codebase survey. Each task lists the owning files, concrete steps, and the validation expected (including fail-first tests).

## Gateway Hardening

1. **Require explicit TCP opt-in and authentication for JSON-RPC**  
   - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`, `docs/architecture/UserInteraction_And_Query_Architecture.md`.  
   - **Steps:** gate TCP binding behind a `--tcp-listen` flag; inject mandatory auth (mTLS or signed tokens) into both RPC entrypoints; surface misconfiguration errors early.  
   - **Tests:** add axum-based integration tests that (a) fail today because unauthenticated TCP requests succeed, then pass once auth is enforced.

2. **Enforce rate limiting and payload caps on RPC**  
   - **Files:** same as task 1 plus `crate/core/sinex-gateway/doc/rpc_server.md`.  
   - **Steps:** wrap the Router in `tower::limit::ConcurrencyLimit`, `tower::timeout::Timeout`, and request-body size guards; expose config knobs in CLI/env.  
   - **Tests:** add integration tests that currently hang/accept huge payloads; they should fail (timeout or 413 missing) before middleware lands.

3. **Validate native-messaging origins**  
   - **Files:** `crate/core/sinex-gateway/src/native_messaging.rs`, `doc/native_messaging.md`.  
   - **Steps:** extend the handshake to demand an extension ID/secret, reject unknown IDs, and log attempts.  
   - **Tests:** scripted stdin/stdout harness that sends spoofed origins—should currently succeed, fail once validation is in place.

## Content / Blob Pipeline

4. **Publish blob manager events instead of discarding them**  
   - **Files:** `crate/core/sinex-gateway/src/service_container.rs`.  
   - **Steps:** replace the “drain and log” task with a JetStream publisher or in-process handler that forwards `blob.ingested` / `blob.verified` events to consumers.  
   - **Tests:** add unit test proving events hit the publisher; fails today because channel is never observed.

5. **Migrate `sinex-document-ingestor` off sensd**  
   - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`, `.sqlx` artifacts, docs.  
   - **Steps:** swap the `MaterialSlice` stub + `raw.sensor_jobs` polling for the SDK’s `AcquisitionManager`, stage-as-you-go ingestion, and JetStream slices; delete legacy SQL.  
   - **Tests:** new integration test that spawns a fake AcquisitionManager stream; currently impossible (no implementation) so mark as expected failure until migration lands.

6. **Fix NULL material IDs in document job monitor**  
   - **Files:** same file as task 5 (lines 520-561).  
   - **Steps:** join against the ledger or carry the material ULID in `target_uri`; only process jobs once the ULID is known.  
   - **Tests:** unit test around `monitor_jobs` that fails today because `material_id` is `None`.

7. **Stream document data directly to annex**  
   - **Files:** `process_material` in `sinex-document-ingestor`.  
   - **Steps:** use streaming readers/writers or annex pipes instead of buffering entire documents; abort once `max_document_size` is exceeded.  
   - **Tests:** memory-usage regression (ingesting a >1GB fixture) that currently OOMs—expect failure before streaming rewrite.

## System Satellite

8. **Wire real watchers into `SystemProcessor`**  
   - **Files:** `crate/satellites/sinex-system-satellite/src/unified_processor.rs`, `dbus_watcher.rs`, `journal_watcher.rs`, `udev_watcher.rs`, `systemd_watcher.rs`.  
   - **Steps:** instantiate the watchers in `initialize`, store handles, and start their async loops in `start_continuous_monitoring`; ensure they emit events via `EventEmitter`.  
   - **Tests:** fail-first Nextest case that asserts watchers remain `None` today (e.g., `system_processor_emits_no_watchers`), then replace with positive assertions once wiring exists.

9. **Add integration tests for each watcher**  
   - **Files:** watcher modules + new tests under `crate/satellites/sinex-system-satellite/tests/`.  
   - **Steps:** use mocks/fakes (e.g., a stub D-Bus bus, journalctl with fixtures) to assert payload parsing and event emission; ensure tests cover failure paths.  
   - **Tests:** new cases that currently panic/skip because watchers never start.

## Observability & Heartbeats

10. **Emit heartbeats for all processor modes**  
    - **Files:** `crate/lib/sinex-processor-runtime/src/cli.rs`.  
    - **Steps:** move `HeartbeatEmitter` spawning so `service`, `scan`, and `explore` all register periodic beats; ensure tasks shut down gracefully on command completion.  
    - **Tests:** CLI test harness that records stdout for heartbeat JSON; fails now because no heartbeat appears outside `service` mode.

11. **Improve heartbeat metrics (CPU/memory/lag)**  
    - **Files:** `crate/lib/sinex-satellite-sdk/src/heartbeat.rs`.  
    - **Steps:** integrate `sysinfo` or `/proc` parsing for actual CPU%, memory, JetStream lag, and last-error; add platform guards.  
    - **Tests:** unit tests using fixed `/proc/self/status` fixtures; currently impossible because parser always returns zero.

12. **Make process heartbeat status strongly typed**  
    - **Files:** `crate/lib/sinex-core/src/types/events/payloads/process.rs`.  
    - **Steps:** introduce `ProcessStatus` enum (`Healthy|Degraded|Failed`) with serde integration, schema docs, and database constraints.  
    - **Tests:** compile-time check ensures invalid strings no longer compile; integration test verifying DB constraint rejects unknown status (fails today because column accepts anything).

## Security & Encryption

13. **Enable pgsodium and encrypt sensitive columns**  
    - **Files:** squashed migration (`crate/lib/sinex-schema/src/migrations/...`), `nixos/modules/secrets-management.md`.  
    - **Steps:** install `pgsodium`, generate/ingest master key via agenix, wrap `core.events.payload`, blob metadata, DLQ entries with `pgsodium.crypto_aead_*`.  
    - **Tests:** migration test that currently fails because the extension is missing; after change, verify encrypt/decrypt round-trip.

## Schema Tooling

14. **Implement schema compatibility validation**  
    - **Files:** `crate/lib/sinex-core/src/types/bin/sinex-schema.rs`.  
    - **Steps:** load the two schema versions, diff required fields/types/enums, and record the results; expose non-zero exit on breaking changes.  
    - **Tests:** CLI integration test that compares intentionally incompatible schemas; fails today because the command just logs a warning.

## Testing Coverage

15. **Restore BlobManager integration tests**  
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/integration/blob_manager_test.rs`, annex-related modules.  
    - **Steps:** add a lightweight `IngestClient` mock or feature-flag to remove the dependency, then re-enable the dedupe/corruption/large-file tests.  
    - **Tests:** previously skipped cases should run and fail today; mark them `#[should_panic]` until the mock exists.

16. **Re-enable blob path validation regression test**  
    - **Files:** `crate/lib/sinex-satellite-sdk/tests/security/path_validation_test.rs`.  
    - **Steps:** once task 15 provides a usable BlobManager, finish the regression test to assert safe/dangerous paths.  
    - **Tests:** the skipped portion should fail prior to the BlobManager fix because it returns `Ok(())` prematurely.

17. **Uncomment schema property/integration tests**  
    - **Files:** `crate/lib/sinex-core/tests/property/schema_property_test.rs`.  
    - **Steps:** extend the `#[sinex_test]` macro (or move to sync contexts) so proptest + async works, then restore the commented suites.  
    - **Tests:** ensure the resurrected tests fail with the current harness limitations and pass after the macro support lands.


## Additional Priorities

18. **Deprecate `raw.sensor_jobs` / sensd schema**  
    - **Files:** `crate/lib/sinex-schema/src/schema/sensd.rs`, residual `.sqlx` caches, docs referencing sensd.  
    - **Steps:** drop the tables in the squashed migration (or gate them behind a feature), scrub `.sqlx` artifacts, and rewrite any docs/tools still referencing sensd workflows.  
    - **Tests:** migration test that fails now because tables still exist; schema diff ensures removal is deliberate.

19. **Document ingestor job metadata**  
    - **Files:** `crate/satellites/sinex-document-ingestor/src/lib.rs`.  
    - **Steps:** when submitting jobs (or emitting events), include the actual material ULID and path metadata so downstream components do not rely on parsing `target_uri`.  
    - **Tests:** unit test verifying metadata is populated; currently impossible because we only store `file://path`.

20. **Replay control bus resilience**  
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `crate/core/sinex-gateway/src/replay_control`.  
    - **Steps:** implement exponential backoff + monitoring when `spawn_replay_control` fails instead of silent warn-and-disable; expose health info to the gateway CLI.  
    - **Tests:** integration test that currently shows the replay client missing when NATS is down; expect failure until retries/metrics exist.

21. **Structured DLQ metrics and tooling**  
    - **Files:** `crate/core/sinex-ingestd/src/material_assembler.rs` (DLQ publish), `docs/architecture/Core_Architecture.md`.  
    - **Steps:** emit metrics/logs for DLQ insert/delete, provide a CLI command to inspect DLQ contents, and wire alerts for sustained backlog.  
    - **Tests:** fail-first CLI test demonstrating no DLQ inspection command exists.

22. **Gateway performance isolation**  
    - **Files:** `crate/core/sinex-gateway/src/service_container.rs`, `sinex-services`.  
    - **Steps:** refactor long-running queries (analytics/search) to async tasks or chunked pagination so one RPC cannot hog the shared DB pool.  
    - **Tests:** stress test that fires multiple queries; currently they run sequentially and block.

23. **Heartbeat-driven alerting for satellites**  
    - **Files:** `sinex-satellite-sdk/src/heartbeat.rs`, `docs/architecture/SystemOperations_And_Integrity_Architecture.md`.  
    - **Steps:** define thresholds and log/emit `process.degraded` or `process.failed` events when heartbeat error counts exceed tolerances; integrate with NixOS monitoring rules.  
    - **Tests:** new heartbeat unit test injecting synthetic error counts; fails now because status stays "healthy" regardless.

24. **Gateway CLI teardown awareness**  
    - **Files:** `cli/exo.py`, `crate/core/sinex-gateway/src/rpc_server.rs`.  
    - **Steps:** ensure the CLI handles 401/429 gracefully (prompting for `--use-db` or auth), and add integration tests verifying error messages (currently CLI suggests `--use-db` even when rate limited).  
    - **Tests:** CLI tests that expect specific guidance; fail today because generic errors bubble up.

25. **Watcher teardown and restart handling**  
    - **Files:** `dbus_watcher.rs`, `journal_watcher.rs`, `systemd_watcher.rs`, `udev_watcher.rs`.  
    - **Steps:** add explicit shutdown signals to stop spawned tasks, and ensure the unified processor can restart watchers on reconfiguration.  
    - **Tests:** harness that cancels the processor and asserts watchers exit; fails now because tasks run forever/

26. **Gateway structured logging + tracing context**  
    - **Files:** `crate/core/sinex-gateway/src/rpc_server.rs`.  
    - **Steps:** introduce request IDs, user/session tags, and propagate them into service-layer logs for auditability.  
    - **Tests:** tracing subscriber test verifying logs contain IDs; fails currently because logs lack correlation IDs.

27. **DLQ / confirmation CLI commands**  
    - **Files:** `cli/exo.py` (new subcommands).  
    - **Steps:** add `exo dlq list/purge` and `exo confirmations tail` commands to inspect health from the CLI, backed by DB queries or JetStream.  
    - **Tests:** CLI integration tests; currently no commands exist, so tests will fail.

28. **Remove dead sensd stubs from satellites**  
    - **Files:** `crate/satellites/*` modules still containing `MaterialSlice` stubs, commented sensd references.  
    - **Steps:** delete the stubs after AcquisitionManager is adopted (task 5) and update documentation to state JetStream-only operation.  
    - **Tests:** `cargo check` should fail if stubs remain referenced, ensuring we actually remove them.

29. **Replay automation coverage**  
    - **Files:** `crate/lib/sinex-processor-runtime/src/lib.rs` (replay module), `crate/lib/sinex-services/src/analytics.rs`.  
    - **Steps:** add integration tests for the replay control lifecycle (create → preview → approve → execute) using the gateway RPC dispatch; verify error paths and cancellation.  
    - **Tests:** currently absent; new tests should fail because the RPC handler’s error messages are not validated.

30. **Gateway secret management via agenix**  
    - **Files:** `nixos/modules/secrets-management.md`, `nixos/modules/default.nix`.  
    - **Steps:** ensure gateway-related secrets (tokens, TLS certs) are provisioned through agenix instead of raw env vars; document rotation.  
    - **Tests:** NixOS VM test verifying services refuse to start when secrets missing; fails now because they happily read env defaults.

31. **Better documentation surfacing for watchers**  
    - **Files:** `crate/satellites/sinex-system-satellite/doc/README.md`, workspace docs.  
    - **Steps:** explain how each watcher works, configuration knobs, and failure behavior; currently the README doesn’t mention the real implementations, leading to confusion.  
    - **Tests:** documentation lint or manual review (no automated failure today), but include this task so we update the docs alongside code.

32. **Upgrade plan for gateway/test infra**  
    - **Files:** `docs/testing-priorities-and-roadmap.md`.  
    - **Steps:** fold the new gateway/system tasks into that roadmap so engineers know the order of operations; ensures the plan stays in sync with this TODO file.  
    - **Tests:** manual verification.

## SQL Ergonomics Sweep

33. **Remove remaining SeaQuery call sites (outside schema/migration code)** — ✅ *Completed in `Range-aware replays and cascade repository refactor` follow-up*  
    - **Status:** `seaquery_helpers.rs` modules/tests were removed and `repositories_common` now builds SQL via `format!`; only schema/migration crates retain SeaQuery.  
    - **Regression Test:** `cargo check` / `just check` ensures no `sea_query` references remain under `sinex-core` outside migrations; add `rg "sea_query" crate/lib/sinex-core` CI guard if desired.

34. **Sweep for aliased IDs (`SELECT id AS foo_id`) and align with schema names**  
    - **Files:** workspace-wide search across `*.rs`, `*.sql`, and `.sqlx` definitions.  
    - **Steps:** replace ad-hoc aliases with direct column names (relying on struct field renames or `FromRow` annotations); regenerate `.sqlx` cache.  
    - **Tests:** `cargo check`/`just check` should fail before the sweep wherever structs expect aliased names.

35. **Adopt shared fixture constants across remaining test suites**  
    - **Files:** `crate/lib/sinex-test-utils/src/constants.rs`, all tests still hard-coding sources/types (search for `"repo-test"`, `"query.safety"`, etc.).  
    - **Steps:** expand the constants module as needed and update downstream tests to import via the prelude; consider a lint/CI check that flags the old literals.  
    - **Tests:** grep/CI step proving the old strings no longer appear; as interim guard, add a test that fails if the constants aren’t used in representative suites.
