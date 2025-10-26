# JetStream Migration Status

## Completed
✅ Test compilation errors fixed (131/136 tests passing)
✅ EventTransport abstraction (Grpc + Nats variants)
✅ StreamProcessorContext updated for EventTransport
✅ ProcessorCli --nats-url flag added
✅ NixOS satellite configuration updated for NATS
✅ NatsPublisher awaits JetStream PublishAck (double-await pattern)
✅ All satellites configured to use --nats-url by default

## Critical Issues Found

### 1. Confirmation Architecture NOT Implemented
The current implementation only waits for JetStream PublishAck (publish confirmation),
but does NOT implement the post-commit confirmation flow from docs/way.md:

**Required Flow:**
```
Satellite → events.raw.* → ingestd consumer → Postgres (commit) → 
events.confirmations.<event_id> → Automata/consumers
```

**What's Missing:**
- ingestd events consumer does NOT publish to events.confirmations after DB commit
- StreamProcessorRunner does NOT buffer provisional events awaiting confirmation
- No confirmation stream configured in JetStream bootstrap

### 2. sensd Removed ✅
Entire crate deleted, removed from workspace, integration modules removed.

### 3. Integration Tests Missing
No E2E test for: publish → ingestd persist → confirmation → consumer receives

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

