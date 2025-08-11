# Sinex Architectural Cancer Analysis

## Executive Summary

This document captures a comprehensive architectural audit of the Sinex codebase, identifying violations of the canonical architecture, areas of technical debt, and opportunities for improvement. The audit was conducted through systematic analysis of the codebase against the canonical architecture defined in `docs/TARGET_canonical.md`.

## Methodology

The audit employed multiple specialized analyses examining:
- Single-writer principle enforcement
- Post-commit publish guarantees
- Provenance tracking patterns
- Unified processor model compliance
- Schema redundancy and orphaned features
- Database constraint enforcement
- Test infrastructure patterns

## Critical Findings

### 1. Missing sensd Schema Definitions

**Status**: CRITICAL GAP

The `sensd` service references two tables that have no schema definitions:
- `raw.sensor_jobs` - for managing sensor job lifecycle
- `raw.temporal_ledger` - for tracking byte-level capture timing

**Evidence**:
- Code in `crate/core/sinex-sensd/src/job_manager.rs` queries these tables
- `crate/satellites/sinex-fs-watcher/src/sensd_integration.rs` attempts to insert records
- No schema files exist in `crate/lib/sinex-migrations/src/schema/`
- No migrations create these tables

**Impact**: The sensd service will fail on startup when it attempts to access non-existent tables.

**Required Action**: Create schema files defining these tables, then create migrations that use the schemas.

### 2. Event Construction and Provenance Patterns

**Status**: CRITICAL MISUNDERSTANDING - REQUIRES ARCHITECTURAL CORRECTION

According to the canonical architecture, the provenance model is strictly binary (XOR):
1. **External provenance** (source_material_id, anchor_byte) - ALL first-order events from captured data
2. **Internal provenance** (source_event_ids) - ALL synthesized events from other events

**Critical Correction**: 
The (NULL, NULL) state is INVALID according to the canonical architecture. The document states:
- "Source Material is Ground Truth" - ALL raw observations must be captured as Source Material
- Even system events like `system.heartbeat` should reference Source Material (e.g., from a system metrics collection stream)
- The XOR constraint MUST be enforced: "Every event in `core.events` **MUST** have either external provenance OR internal provenance, but **NEVER** both, and **NEVER** neither"

**Architectural Violation**:
- Current code creates events with no provenance (both fields NULL)
- This violates Invariant #3: "Dual-Layer Provenance (XOR)"
- ALL first-order events MUST be tied to Source Material captured by sensd

**Required Fix**:
- All "raw observation" events must be captured through sensd first
- sensd creates Source Material entries for system observations (process lists, metrics, etc.)
- Ingestors then create events with proper external provenance to this material
- No event should ever have (NULL, NULL) provenance

**ts_orig Nullability Issue**:
- Currently nullable but this was unintended
- ALL events should have timestamps
- Consider making ts_orig NOT NULL in future migration

### 3. Test Infrastructure Wrapper Methods

**Status**: MINOR CLEANUP

Three unnecessary wrapper methods in `TestContext`:

1. **`insert_event()` (lines 114-120)**:
   - Wraps `pool.events().insert()` with event tracking
   - Tests should use: `ctx.pool.events().insert(event).await?`

2. **`assert_event_exists()` (lines 134-140)**:
   - Wraps `pool.events().exists_by_id()` with basic error message
   - Tests should use: `assert!(ctx.pool.events().exists_by_id(&id).await?)`

3. **`test_event_count()` (lines 143-145)**:
   - Wraps `pool.events().count_all()` and swallows errors with `unwrap_or(0)`
   - Tests should use: `ctx.pool.events().count_all().await?`

### 4. Validation Cache Schema Mismatch

**Status**: BUG

The `sinex_schemas.validation_cache` table has a schema mismatch:
- Table expects: `(payload_hash, schema_id)` as key
- Function uses: `(event_id, schema_id)` in insert
- This will cause runtime failures when validation caching is enabled

## Validated Architecture Patterns

### 1. Post-Commit Publish Implementation

**Status**: PARTIALLY IMPLEMENTED

The ingestion service correctly applies the transactional outbox pattern for events it receives, but the overall system still permits bypass paths:
- ingestd writes events and outbox entries in the same transaction; a background task publishes to NATS post-commit.
- The Satellite SDK still supports direct NATS publishing (and some CLI flows default to it), which breaks the single-writer and post-commit guarantees when used.
- Some content paths (BlobManager) emit events directly to the DB, bypassing ingestd entirely.

**Evidence**:
- `IngestService::batch_write_to_db()` atomically writes to both tables; `process_outbox()` runs periodically.
- `crate/lib/sinex-satellite-sdk/src/event_processor.rs` includes `NatsPublisher` path and defaults.
- `crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs` inserts `RawEvent` via repositories.

### 2. Archive-on-Delete Pattern

**Status**: CORRECTLY IMPLEMENTED

The `audit.archived_events` table serves a legitimate purpose:
- Preserves complete event data when deleted from `core.events`
- Maintains audit trail with who/when/why metadata at the row level (per canonical target: `archived_by`, `archive_reason`, `superseded_by_event_id` alongside `operation_id`). Operation narratives remain in `operations_log`.
- Supports "rebuildability via replay" principle
- NOT redundant with `operations_log` (which tracks operational metadata)

### 3. Caching Infrastructure

**Status**: LEGITIMATE BUT NEEDS BUG FIX

Two distinct caching tables serve different purposes:
- `validation_cache`: Caches expensive JSON schema validation results
- `embedding_cache`: Caches expensive AI embedding API calls

These are NOT parallel systems but domain-specific optimizations.

### 4. Preflight Service Design

**Status**: ARCHITECTURALLY CORRECT

The preflight tool's direct database access is justified:
- Runs BEFORE ingestd starts (service dependency ordering)
- Functions as system verification, not a satellite
- `process.heartbeat` events are appropriate for health reporting
- Alternative tables (operations_log, processor_manifests) don't fit the use case

### 5. StageAsYouGoProcessor Pattern

**Status**: LEGITIMATE HELPER PATTERN

This is NOT a violation of the unified processor model:
- Optional helper trait for real-time provenance tracking
- Complements (doesn't replace) StatefulStreamProcessor
- All processors still implement the required unified interface
- Provides `StageAsYouGoContext` for processors needing in-flight material registration

## Architectural Strengths

### 1. Unified Processor Model

All satellites correctly use the `processor_main!` macro and implement `StatefulStreamProcessor`. The architecture successfully unifies ingestors and automata under a single interface with:
- Unified checkpoints supporting both external and internal positions
- Three time horizons (Snapshot, Historical, Continuous)
- Consistent CLI structure across all processors

### 2. Event Type System

The three-tier type hierarchy provides excellent type safety:
- `Event<T: EventPayload>` - compile-time type safety
- `RawEvent` - runtime flexibility with JSON payloads
- `EventRecord` - database representation

### 3. Repository Pattern

Clean separation between domain models and database access:
- Repositories handle all SQL interactions
- Domain models remain database-agnostic
- Test infrastructure correctly exposes `pool` for direct repository access

## Misconceptions Corrected

### 1. NATS Migration Status

The SDK currently supports both gRPC→ingestd and direct NATS publishing, and some CLI flows default to NATS. This creates a potential violation of Invariant #1 (single-writer) and Invariant #2 (post-commit) when the direct NATS path is used. Action: default to gRPC and feature-gate or remove direct NATS publish for event creation.

### 2. Duplicate State Tracking

Initial audit claimed `archived_events` duplicates `operations_log`. Analysis showed:
- These serve completely different purposes
- `archived_events`: Stores complete deleted event data
- `operations_log`: Tracks operational workflows and metadata
- They are complementary, not redundant

### 3. Parallel Processor Implementations

Initial concern about `StageAsYouGoProcessor` creating a parallel system. Investigation revealed it's an optional helper, not a replacement. The unified model is largely adopted, but remnants of NATS-centric processing paths remain in the SDK; ensure run loops are unified under `StatefulStreamProcessor` and outputs go through ingestd.

## Additional Critical Corrections

### A. Database Constraint Accuracy

- Provenance XOR: The current migration’s CHECK permits the invalid `(source_material_id IS NULL AND source_event_ids IS NULL)` state. Tighten to exact XOR per canonical target.
- Idempotency index: The unique index includes `id` (`(source_material_id, anchor_byte, id)`), defeating idempotency. Replace with a partial unique index on `(source_material_id, anchor_byte)` where both are non-null.

### B. Ingest Path Bypass via BlobManager

`BlobManager` directly inserts `RawEvent` via repositories, bypassing ingestd’s outbox/publish pipeline. Route these through ingestd’s gRPC client to preserve invariants.

### C. Missing sensd DDL (Temporal Ledger)

`sensd` uses `raw.temporal_ledger`, but no corresponding DDL exists in migrations. Add the table and append-only trigger with recommended indexes per canonical target.

### D. Environment Namespacing

`SINEX_ENVIRONMENT`-scoped namespacing for DB names, NATS subjects, sockets, and paths is not yet implemented. Add a central helper and thread through services and SDK.

### E. Gateway RPC Path Mismatch

Gateway serves JSON-RPC at `/rpc`, while the CLI defaults to posting to the base URL. Either set `SINEX_RPC_URL` to include `/rpc` by default or have gateway accept `/` for compatibility.

### 4. Provenance Model Understanding

Initial misunderstanding: Believed (NULL, NULL) provenance was valid for "raw observation" events.

Corrected understanding from canonical architecture:
- The XOR constraint is absolute - NEVER (NULL, NULL) allowed
- ALL first-order events must reference Source Material
- Even system observations (heartbeats, process lists) must be captured as Source Material first
- The architecture requires: "Source Material is Ground Truth"

## Implementation Gaps

### 1. sensd Service (20% Complete)

**What exists**:
- Job manager framework
- Material stream abstractions
- Sensor type definitions

**What's missing**:
- Database schema definitions
- gRPC server implementation  
- MaterialSliceStream data loading
- Integration with satellites

### 2. Event Provenance Safety

**Current state**:
- Default constructors create events with no provenance
- No compile-time enforcement of provenance requirements
- Runtime validation only at database level

**Improvements needed**:
- Factory methods for different event types
- Compile-time safety for synthesis events
- Better documentation of provenance requirements

## Recommendations

### Immediate Actions

1. **Create sensd schema files**:
   - Define `sensor_jobs` and `temporal_ledger` in schema directory
   - Create migration using these schemas
   - Complete MaterialSliceStream implementation

2. **Fix validation_cache bug**:
   - Update function to use `payload_hash` instead of `event_id`
   - Add tests for validation caching

3. **Consider ts_orig constraint**:
   - Evaluate making ts_orig NOT NULL
   - Ensure all event creation paths set appropriate timestamps

### Medium Priority

1. **Remove test wrapper methods**:
   - Delete unnecessary wrappers in TestContext
   - Update tests to use repository methods directly

2. **Improve event construction safety**:
   - Add factory methods for different event categories
   - Consider builder pattern with required fields

3. **Document provenance patterns**:
   - Clarify when (NULL, NULL) provenance is appropriate
   - Document the three-tier event hierarchy

### Long Term

1. **Complete sensd implementation**:
   - Implement gRPC server
   - Refactor satellites to use MaterialSliceStream
   - Remove direct I/O from satellites

2. **Enhance type safety**:
   - Consider state machines for event construction
   - Add compile-time provenance enforcement where possible

## Conclusion

The Sinex architecture is fundamentally sound with excellent separation of concerns, proper transactional guarantees, and a well-designed event model. However, there is one critical architectural violation that needs correction:

1. **Critical violation**: Events with (NULL, NULL) provenance violate the canonical architecture
2. **Incomplete features** (sensd) blocking proper provenance implementation
3. **Minor bugs** (validation_cache) that are easily fixed
4. **Documentation gaps** about design intentions

The provenance violation is serious - it undermines the "Source Material is Ground Truth" principle. ALL events must have provenance, either external (to Source Material) or internal (to parent events). The fix requires completing sensd so that all raw observations are first captured as Source Material, then converted to events with proper external provenance.

## Appendix: Key Architectural Invariants

From the canonical architecture, current compliance status:

1. **Single-Writer Ingest**: ❌ SDK allows direct NATS and some code paths bypass ingestd (BlobManager)
2. **Post-Commit Publish**: ⚠️ Ingestd enforces it, but bypass paths exist
3. **Dual-Layer Provenance**: ❌ CHECK too permissive; (NULL, NULL) allowed and used
4. **Archive-on-Delete**: ✅ Trigger moves deleted events to audit table with required context
5. **Unified Processor Model**: ⚠️ Largely adopted; remove remaining NATS-direct output paths

The critical violation is Invariant #3 - the system currently allows events without any provenance, which violates the fundamental "Source Material is Ground Truth" principle. This must be fixed by:
1. Completing sensd to capture all raw observations as Source Material
2. Ensuring all first-order events reference their Source Material
3. Enforcing the XOR constraint to prevent (NULL, NULL) states

## Critical Event Emission Bypass Issue

### F. State Change Events Bypass Ingestd

**Status**: CRITICAL ARCHITECTURAL VIOLATION

**Problem**: The event emission implementation for state changes directly inserts events into `core.events`, completely bypassing ingestd:

```rust
async fn emit_state_change_event_tx(
    &self,
    tx: &mut Transaction<'_, Postgres>,
    event: RawEvent,
) -> DbResult<RawEvent> {
    let event_repo = EventRepository::new(self.pool);
    event_repo.insert_with_tx(tx, event).await  // BYPASSES INGESTD!
}
```

**Violations**:
- Bypasses schema validation and sanitization
- Bypasses telemetry and monitoring
- Bypasses NATS publication (events invisible to message bus)
- Violates Single-Writer Ingest invariant (#1)
- Violates Post-Commit Publish invariant (#2)

**Evidence**:
- `/realm/project/sinex/crate/lib/sinex-core/src/db/repositories/state.rs` - `emit_state_change_event_tx()` directly inserts
- Checkpoint operations emit `checkpoint.save_intent` and `checkpoint.saved` events
- Schema lifecycle changes emit `schema.status_changed` events
- Processor status changes emit `processor.status_changed` events
- All bypass the entire ingestd pipeline

**Required Fix**:
1. **Option A**: Remove event emission for internal state changes entirely
2. **Option B**: Route through ingestd's gRPC interface like all other events
3. **Option C**: Use separate `internal.state_events` table for internal tracking

This is a critical violation that defeats the purpose of having ingestd as the central coordinator and single point of truth for event ingestion.

## Additional Critical Architectural Violations

### G. Direct Event Insertion in Distributed Locking

**Status**: CRITICAL ARCHITECTURAL VIOLATION

**Problem**: The distributed locking module directly inserts events into the database, completely bypassing ingestd:

```rust
// In crate/lib/sinex-core/src/db/distributed_locking.rs
async fn record_leadership(&self, pool: &DbPool) -> CoreResult<()> {
    // ...
    let event_repo = EventRepository::new(pool);
    event_repo
        .insert_with_tx(&mut tx, leadership_intent_event)
        .await  // BYPASSES INGESTD!
        .map_err(SinexError::from)?;
    // ...
}
```

**Evidence**:
- Lines 268-270: Leadership intent events inserted directly
- Lines 298-301: Leadership acquired events inserted directly  
- Lines 337-340: Heartbeat intent events inserted directly
- Lines 364-367: Heartbeat updated events inserted directly

**Violations**:
- Bypasses Single-Writer Ingest invariant (#1)
- Bypasses Post-Commit Publish invariant (#2)
- No schema validation via pg_jsonschema
- No NATS publication (events invisible to subscribers)
- No telemetry or monitoring

### H. SDK Default to Direct NATS Publishing

**Status**: ARCHITECTURAL VIOLATION (PARTIAL)

**Problem**: While the SDK has gRPC client support, it still allows direct NATS publishing which bypasses ingestd:

```rust
// In crate/lib/sinex-satellite-sdk/src/grpc_client.rs
// Good: Proper gRPC client implementation exists
pub async fn ingest_event(&mut self, event: &RawEvent) -> SatelliteResult<String>

// But SDK still supports direct NATS path elsewhere
```

**Required Fixes**:
1. Remove all direct database event insertion paths
2. Remove or feature-gate direct NATS publishing in SDK
3. Route ALL events through ingestd's gRPC interface
4. Ensure distributed locking events go through proper channels
