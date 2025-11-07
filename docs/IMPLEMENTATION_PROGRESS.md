# JetStream Refactor Implementation Progress

## Completed Work

### Phase 1: JetStream Integration Tests — COMPLETE

**Accomplished:**
1. Added NATS infrastructure to `TestContext`:
   - `with_nats()` method to enable ephemeral NATS server
   - `nats_client()` to get NATS client
   - `env()` to get `SinexEnvironment`
   - `nats_url()` to get server URL

2. Fixed JetStream integration tests:
   - Removed `#[ignore]` attributes
   - Updated tests to use `TestContext::new().await?.with_nats().await?`
   - Tests use direct `JetStreamConsumer` instead of full `IngestService`
   - Added stream bootstrapping before publishing

3. Files modified:
   - `sinex-test-utils/src/test_context.rs` - Added NATS support
   - `sinex-ingestd/tests/jetstream_consumer_test.rs` - Fixed tests

**Notes:**
- Tests run against ephemeral NATS via `TestContext::with_nats()`.
- Long-running integration suites remain `#[ignore]` to avoid CI flakiness.

### Existing Implementation (Already Complete)

From codebase review:
- ✅ JetStream consumer with batch insert (`jetstream_consumer.rs`)
- ✅ Material assembler for source material slices (`material_assembler.rs`)
- ✅ Confirmation publishing (`jetstream_consumer.rs:388-413`)
- ✅ DLQ handling (`jetstream_consumer.rs:416-437`)
- ✅ `NatsPublisher` in SDK (`nats_publisher.rs`)
- ✅ `AcquisitionManager` for material capture (`acquisition_manager.rs`)
- ✅ All satellites support `--nats-url` flag for direct JetStream publishing

## Remaining Work

- Promote replay tooling onto `sinex.control.*` subjects (CLI + gateway integration).
- Complete JetStream migrations for analytics/search automata.
- Harden annex integration for environments without local git-annex (better mocks for tests).

## Architecture Status

The codebase is in a **JetStream-first state**:
- ingestd runs exclusively on JetStream streams with material assembler persistence and confirmation fan-out.
- Satellite SDK exposes only NATS transports; gRPC client/helpers have been removed.
- Stage-as-You-Go and AcquisitionManager ship with restart-safe annex integration and ledger writes.
- Replay/control-plane work is staged but needs `sinex.control.*` subjects to replace the remaining TODOs.
