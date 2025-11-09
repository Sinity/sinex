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

