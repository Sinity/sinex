# JetStream Migration Status

## Completed

### Phase 1 - Events Backbone (DONE)
✅ JetStream consumer: pulls from events.raw.*, persists to DB
✅ Confirmation publishing: events.confirmations.* after DB commit
✅ UNNEST batch insert optimization (≥5K events/sec target)
✅ DLQ routing for validation failures
✅ Stream bootstrap (events_raw, events_confirmations, events_dlq)
✅ EventTransport abstraction (Grpc + Nats variants)
✅ NatsPublisher with double-await pattern
✅ --nats-url CLI flag across all satellites
✅ NixOS satellite configuration uses NATS by default

### Phase 3 - Source Material Slices (DONE)
✅ MaterialAssembler fully implemented and integrated into ingestd
✅ Three separate consumers (begin, slices, end)
✅ Out-of-order slice handling with buffering
✅ Hash verification
✅ Temp file management and git-annex integration

### Phase 5 - Cleanup (IN PROGRESS)
✅ sensd crate deleted (crate/core/sinex-sensd)
✅ sensd removed from workspace Cargo.toml
✅ sensd integration modules deleted from satellites
⚠️ Satellites need MaterialSlice migration to AcquisitionManager
   - fs-watcher, document-ingestor, desktop, terminal have stub MaterialSlice types
   - Full migration to AcquisitionManager needed (Phase 6 work)

## Remaining Work

### Phase 2 - Confirmation-Aware Consumption (OPTIONAL - For JetStream-consuming automata)
❌ StreamProcessorRunner confirmation buffering - only needed when automata switch from DB polling to JetStream subscription
❌ Automaton migration to JetStream subscription (currently query DB directly, which still works)

### Phase 6 - Satellite Material Capture Migration
❌ Migrate fs-watcher to use AcquisitionManager
❌ Migrate document-ingestor to use AcquisitionManager
❌ Migrate terminal-satellite to use AcquisitionManager
❌ Migrate desktop-satellite to use AcquisitionManager
❌ Remove MaterialSlice stubs once migration complete

### Testing
❌ Write E2E integration test (satellite → NATS → ingestd → DB → confirmation)
❌ Write comprehensive events consumer integration tests
❌ Property tests for idempotency, ordering, hash integrity

### Documentation
❌ Update way.md to mark completed phases
❌ Remove remaining sensd references from docs

## Next Steps (Priority Order)

1. **Implement Confirmation Publishing in ingestd**
   - File: `crate/core/sinex-ingestd/src/jetstream_consumer.rs`
   - After batch INSERT succeeds, publish to `events.confirmations.<event_id>`
   - Message: `{event_id, persisted: true, ts_ingest}`

2. **Implement Confirmation-Aware Buffering**
   - File: `crate/lib/sinex-satellite-sdk/src/stream_processor.rs`
   - Add `provisional_events` buffer in StreamProcessorRunner
   - Subscribe to `events.confirmations.*` 
   - Deliver events to processor only after confirmation

3. **Remove sensd Entirely**
   - Delete `crate/core/sinex-sensd/*`
   - Remove sensd integration modules from satellites
   - Update Cargo workspace dependencies
   - Remove JobManager (likely pointless per user)

4. **Write Integration Tests**
   - Test: satellite publishes → ingestd persists → confirmation emitted
   - Test: automaton receives only confirmed events
   - Test: confirmation latency < 5s (per docs/way.md)

5. **Update Documentation**
   - Remove all sensd references
   - Document confirmation-aware processing
   - Update architecture diagrams

## Current Architecture State

**Working:** Satellites → NATS JetStream (with JetStream ack)
**Not Working:** Confirmation fan-out after DB commit
**Missing:** Confirmation-aware consumption in automata

