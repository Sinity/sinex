# Ingestd Service Analysis - Area 07

**Analyzed Files:** 15 files  
**Focus:** Core ingestd service implementation - the single writer to core.events

## Executive Summary

The ingestd service shows generally solid architecture but contains **several high priority issues** that violate the single-writer pattern and could lead to data loss. The most serious problem is **incomplete provenance handling** which violates the XOR invariant required by the canonical architecture.

**Key Findings:**
- 🔴 **HIGH**: Incomplete provenance handling violating XOR invariant
- 🟡 **MEDIUM**: Missing critical database constraints for safety
- 🟡 **MEDIUM**: Inefficient batch insert implementation
- 🟡 **MEDIUM**: Schema validation gaps and performance issues

---

## Critical Issues


### ISSUE #1: Incomplete Provenance Handling
**Location:** `src/service.rs:737-752`  
**Category:** Architecture  
**Severity:** HIGH

**Description:**
The batch insert only handles Material provenance, completely ignoring Internal provenance (source_event_ids). This violates the XOR provenance invariant required by the canonical architecture.

**Evidence:**
```rust
// Only handles Material provenance case
let (source_event_ids_opt, source_material_id, offset_start, offset_end, anchor_byte) =
    match event.provenance {
        Provenance::Material { id, anchor_byte, offset_start, offset_end, .. } => (
            None,
            Some(ulid_to_uuid(*id.as_ulid())),
            offset_start,
            offset_end,
            anchor_byte,
        ),
        // Missing: Provenance::Internal case
    };
```

**Impact:**
- Events with internal provenance (from automata) will fail to insert or have null provenance
- Violation of XOR provenance constraint in canonical architecture
- Data corruption as derivation chains are broken
- Automata cannot properly record their source events

**Suggested Fix:**
1. Add complete pattern matching for `Provenance::Internal` case
2. Implement proper handling of `source_event_ids` arrays
3. Add validation to ensure XOR constraint is maintained
4. Add tests for both provenance types

**Dependencies:**
Must be coordinated with automata that generate internal provenance events.

---

### ISSUE #2: Missing Database Constraints
**Location:** Schema vs Implementation gap  
**Category:** Architecture  
**Severity:** MEDIUM

**Description:**
The implementation doesn't enforce critical architectural invariants that should be database constraints according to TARGET_canonical.md.

**Evidence:**
Target canonical architecture specifies:
- XOR constraint: `(material_id IS NOT NULL AND source_event_ids IS NULL) OR (material_id IS NULL AND source_event_ids IS NOT NULL)`
- Idempotency constraint: `UNIQUE(material_id, anchor_byte) WHERE material_id IS NOT NULL`

But current schema lacks these constraints.

**Impact:**
- No database-level protection against invariant violations
- Possible duplicate events from replay operations
- Invalid provenance states can be inserted
- System relies solely on application-level validation

**Suggested Fix:**
1. Add migration to create XOR constraint on core.events
2. Add unique index on (material_id, anchor_byte) where material_id is not null
3. Add trigger functions for invariant enforcement
4. Update validation logic to rely on database constraints as safety net

**Dependencies:**
Requires migration coordination and testing against existing data.

---

## High Priority Issues

### ISSUE #3: Inefficient Batch Insert Pattern
**Location:** `src/service.rs:784-821`  
**Category:** Performance  
**Severity:** MEDIUM

**Description:**
The UNNEST-based batch insert is inefficient due to complex type conversions and large parameter binding. The current approach creates 17 separate arrays for batch binding, which is memory-intensive and complex.

**Evidence:**
```rust
// Creates 17 separate vectors for UNNEST binding
let mut event_ids = Vec::with_capacity(event_count);
let mut sources = Vec::with_capacity(event_count);
// ... 15 more vectors
```

**Impact:**
- High memory usage for large batches
- Complex error handling and debugging
- Maintenance burden with many parallel arrays
- Possible performance degradation under load

**Suggested Fix:**
1. Consider using PostgreSQL's COPY protocol for bulk inserts
2. Implement streaming insert for very large batches
3. Add benchmarks to measure actual performance impact
4. Consider breaking into smaller sub-batches for memory efficiency

**Dependencies:**
Performance testing framework to validate improvements.

---

### ISSUE #4: Schema Validation Performance Issues
**Location:** `src/validator.rs:145-174, 432-450`  
**Category:** Performance  
**Severity:** MEDIUM

**Description:**
Schema loading and validation patterns have performance and reliability issues. The validator loads all schemas on startup and periodically reloads, but lacks proper error handling for reload failures.

**Evidence:**
```rust
// Reload can fail silently
if let Err(e) = validator_guard.reload_schemas(&pool).await {
    warn!("Failed to reload schemas: {}", e);
}
```

**Impact:**
- Schema validation may become stale after reload failures
- No metrics on validation performance
- Potential memory leaks from schema cache growth
- Silent degradation of validation quality

**Suggested Fix:**
1. Add circuit breaker pattern for schema reload failures
2. Implement metrics for validation performance and cache size
3. Add max cache size limits and LRU eviction
4. Provide fallback validation strategies when schemas are unavailable

**Dependencies:**
Telemetry system integration for metrics collection.

---

## Medium Priority Issues

### ISSUE #5: Missing Provenance Metadata Fields
**Location:** `src/service.rs:685-697`  
**Category:** Completeness  
**Severity:** MEDIUM

**Description:**
The batch insert doesn't populate several provenance-related fields that are defined in the schema, particularly `offset_kind` and proper handling of `processor_manifest_id`.

**Evidence:**
```rust
// Hard-coded as None instead of proper values
.bind(&vec![None::<i32>; events.len()]) // processor_manifest_id
// offset_kind is not properly set based on material type
```

**Impact:**
- Loss of important provenance metadata
- Queries that rely on these fields will return incomplete results
- Replay operations may not have sufficient information for reprocessing
- Audit trails are incomplete

**Suggested Fix:**
1. Populate `offset_kind` based on material type
2. Implement `processor_manifest_id` lookup and assignment
3. Add tests to verify all provenance fields are properly populated
4. Document which fields are optional vs required

**Dependencies:**
Integration with processor manifest system from satellite SDK.

---

### ISSUE #6: Subject Cache Memory Leak Potential
**Location:** `src/service.rs:64-111`  
**Category:** Quality  
**Severity:** MEDIUM

**Description:**
The SubjectCache grows unbounded and lacks any cleanup mechanism. In long-running deployments with many different source/event_type combinations, this could lead to memory exhaustion.

**Evidence:**
```rust
pub struct SubjectCache {
    cache: Mutex<AHashMap<(String, String), Arc<String>>>,
}
// No eviction policy, no size limits, no cleanup
```

**Impact:**
- Gradual memory leak in long-running processes
- No visibility into cache performance or hit rates
- Potential for memory exhaustion with dynamic event types
- No way to clear stale cache entries

**Suggested Fix:**
1. Implement LRU eviction with configurable max size
2. Add cache statistics (hit rate, size, memory usage)
3. Add periodic cleanup of unused entries
4. Consider using a proper caching library with TTL support

**Dependencies:**
Cache statistics integration with telemetry system.

---

### ISSUE #7: Incomplete Error Context in gRPC Layer
**Location:** `src/service.rs:956-1018`  
**Category:** Quality  
**Severity:** MEDIUM

**Description:**
The gRPC service implementation lacks proper error context and correlation IDs, making debugging difficult in production environments.

**Evidence:**
```rust
// Generic error responses without context
IngestResponse {
    success: false,
    error: Some(format!("Event conversion failed: {}", e)),
    event_id: None,
}
```

**Impact:**
- Difficult to trace errors in production
- No correlation between client requests and server logs
- Limited debugging information for satellite developers
- Poor observability for operational monitoring

**Suggested Fix:**
1. Add request correlation IDs to all gRPC operations
2. Include more specific error codes and categories
3. Add structured logging with request context
4. Implement distributed tracing integration

**Dependencies:**
Integration with distributed tracing system.

---

## Testing Gaps

### ISSUE #8: Missing Integration Tests for Outbox Pattern
**Location:** Test coverage gap  
**Category:** Testing  
**Severity:** MEDIUM

**Description:**
No integration tests verify the complete transactional outbox pattern from event insert through NATS publication and cleanup.

**Evidence:**
Integration test file `ingestd_grpc_test.rs` only tests gRPC patterns, not the actual outbox processing pipeline.

**Impact:**
- Critical path not validated in CI
- Potential for silent failures in production
- No verification of post-commit publish guarantee
- Regression risk when modifying outbox logic

**Suggested Fix:**
1. Add integration test that verifies complete outbox cycle
2. Test failure scenarios (NATS unavailable, partial failures)
3. Verify exactly-once delivery guarantees
4. Add performance tests for batch processing

**Dependencies:**
Test infrastructure that can spin up NATS for integration testing.

---

## Architecture Compliance

The ingestd service demonstrates good adherence to most architectural patterns:

✅ **Single-writer pattern**: Correctly positioned as sole writer to core.events  
✅ **Transactional outbox**: Pattern implementation is correctly implemented  
✅ **Batch processing**: Efficient batching of database operations  
✅ **Schema validation**: Comprehensive validation framework  
✅ **Environment isolation**: Proper SINEX_ENVIRONMENT scoping  

✅ **Post-commit publish**: Correctly implemented with proper table name  
❌ **Provenance XOR**: Incomplete implementation for internal provenance  
❌ **Database constraints**: Missing critical invariant enforcement  

## Recommendations

1. **High Priority**: Complete provenance handling (#1) to enable automata integration  
2. **Medium Priority**: Add missing database constraints (#2) for data integrity
3. **Ongoing**: Implement comprehensive integration tests for outbox pattern (#8)
4. **Performance**: Profile and optimize batch insert patterns (#3) under realistic load

The ingestd service is architecturally sound but needs these fixes to fulfill its role as the reliable single writer in the Sinex system.

## DONE

### ISSUE #1: Outbox Table Name Mismatch (FIXED)
**Location:** `src/service.rs:494, 548, 826`  
**Category:** Architecture  
**Severity:** CRITICAL

**Description:**
The code referenced `core.outbox` but the schema defines `core.transactional_outbox`. This caused the transactional outbox pattern to fail completely at runtime.

**Evidence:**
```sql
-- Schema definition (DDL.sql)
CREATE TABLE IF NOT EXISTS core.transactional_outbox (...)

-- Code usage (service.rs:494) - FIXED
"SELECT id, event_id, destination as subject, payload, created_at FROM core.transactional_outbox"

-- Code usage (service.rs:826) - FIXED
"INSERT INTO core.transactional_outbox (id, event_id, destination, payload) VALUES (...)"
```

**Fix Applied:**
Updated all SQL queries in `service.rs` to use the correct table name `core.transactional_outbox` instead of `core.outbox`. This ensures the transactional outbox pattern works correctly for post-commit NATS publishing.