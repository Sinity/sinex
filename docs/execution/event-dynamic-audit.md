# Event::dynamic Usage Audit

**Date:** 2026-01-16
**Context:** Phase 1.3 Test Modernization - Task D1
**Auditor:** Claude Agent

## Executive Summary

Audited all 13 files using `Event::dynamic` in the codebase. Found 4 legitimate production uses and 9 test/example files. Production usage is appropriate for handling truly dynamic schemas or internal helpers. Test files should be considered for migration to typed payloads where applicable.

## Production Files (Legitimate Usage)

### 1. `/realm/project/sinex/crate/lib/sinex-core/src/db/repositories/events/persistence.rs`
**Lines:** 926-934
**Context:** Test helper method `create_test_event`
**Usage:**
```rust
let event = Event::dynamic(
    EventSource::new(source.to_string()),
    EventType::new(event_type.to_string()),
    payload,
)
.with_provenance(Provenance::from_material(test_material_id, 0, None, None))
.build()?;
```
**Verdict:** **LEGITIMATE** - This is a test helper in the repository module used to create test fixtures. It handles arbitrary payloads by design.

### 2. `/realm/project/sinex/crate/nodes/sinex-pkm-automaton/src/lib.rs`
**Lines:** 733-742, 839-842
**Context:** PKM automaton event synthesis
**Usage:**
```rust
let event = Event::dynamic(
    "pkm-automaton",
    "pkm.knowledge_extraction",
    insights_payload,
)
.from_parents(source_event_ids.into_iter())?
.at_time(Utc::now())
.build()?;
```
**Verdict:** **LEGITIMATE** - PKM automaton creates synthesized events with dynamic, AI-generated content. The payload structure varies based on analysis results. This is a valid use case for dynamic events.

### 3. `/realm/project/sinex/crate/nodes/sinex-content-automaton/src/lib.rs`
**Lines:** 519-522, 582-585, 626-629
**Context:** Content automaton event synthesis
**Usage:**
```rust
let event = Event::dynamic("content-automaton", "content.analyzed", analysis_payload)
    .from_parents(parents)?
    .at_time(Utc::now())
    .build()?;
```
**Verdict:** **LEGITIMATE** - Similar to PKM automaton, content automaton generates events with varying structures based on analysis type (content analysis, classification, similarity detection). Dynamic payloads are appropriate here.

### 4. `/realm/project/sinex/crate/lib/sinex-node-sdk/src/annex/blob_manager.rs`
**Lines:** 66-69
**Context:** Internal blob manager helper
**Usage:**
```rust
fn create_blob_event<T: serde::Serialize>(
    event_type: &str,
    payload: T,
    material_id: Id<SourceMaterial>,
) -> Result<Event<JsonValue>> {
    Event::dynamic("blob-manager", event_type, serde_json::to_value(payload)?)
        .from_material(material_id, 0)
        .build()
        .map_err(|err| eyre!("{err}"))
}
```
**Verdict:** **LEGITIMATE** - This is a generic helper that accepts any serializable payload. The function is type-safe at the call site but uses dynamic construction internally. This is a valid abstraction pattern.

## Example/SDK Files (Illustrative, Not Production)

### 5. `/realm/project/sinex/crate/lib/sinex-node-sdk/src/examples/filesystem_processor.rs`
**Lines:** 132-135, 159-162
**Context:** Example code for SDK documentation
**Verdict:** **EXAMPLE CODE** - Shows how to use the SDK. Not production code, so no migration needed.

## Macro-Generated Code (Infrastructure)

### 6. `/realm/project/sinex/crate/lib/sinex-macros/src/typed_event_envelope.rs`
**Lines:** 104-109, 118-123
**Context:** Macro fallback for enum variants without explicit conversions
**Verdict:** **INFRASTRUCTURE** - This is codegen fallback logic. Should probably be improved in the macro itself, but not a test migration concern.

## Test Files (Migration Candidates)

### 7. `/realm/project/sinex/crate/lib/sinex-core/src/db/models/event.rs`
**Lines:** 318-321
**Context:** Unit test for event builder offset functionality
**Test:** `event_builder_sets_offsets_for_material_provenance`
**Migration:** LOW PRIORITY - Testing Event API itself, dynamic usage is appropriate for API tests.

### 8. `/realm/project/sinex/crate/lib/sinex-core/tests/security/ulid_attack_test.rs`
**Lines:** 34-37
**Context:** Security test for ULID validation
**Test:** Validates time-based ULID attack prevention
**Migration:** LOW PRIORITY - Security tests intentionally create malformed events. Dynamic construction is appropriate.

### 9. `/realm/project/sinex/crate/lib/sinex-core/tests/integration/provenance_test.rs`
**Lines:** 466-469, 476-479, 507-510, 516-519, 525-528, 550-553
**Context:** Provenance cycle detection and validation tests
**Tests:** Multiple tests for provenance graph integrity
**Migration:** MEDIUM PRIORITY - Could use typed payloads but current approach is clear. Consider migration if touching these tests.

### 10. `/realm/project/sinex/crate/lib/sinex-core/tests/unit/database_test.rs`
**Lines:** 361-364
**Context:** Database bulk insert performance test
**Test:** `bulk_insert_transaction_performance`
**Migration:** LOW PRIORITY - Performance test needs minimal overhead. Dynamic construction is fine.

### 11. `/realm/project/sinex/crate/lib/sinex-core/tests/event_model.rs`
**Lines:** 46-48, 56-58
**Context:** Testing RawEvent type alias and JSON serialization
**Tests:** `raw_event_alias_is_equivalent`, `json_conversion_round_trips_payload`
**Migration:** LOW PRIORITY - These tests specifically validate the dynamic/raw event API. Must use dynamic construction.

### 12. `/realm/project/sinex/crate/lib/sinex-core/tests/sanitization.rs`
**Lines:** 11-14, 31-34, 47-50
**Context:** Security/sanitization tests
**Tests:** Path traversal, null byte injection, SQL injection payload preservation
**Migration:** LOW PRIORITY - Security tests intentionally create malicious payloads. Dynamic construction is appropriate.

### 13. `/realm/project/sinex/crate/nodes/sinex-terminal-command-canonicalizer/src/unified_processor.rs`
**Lines:** 88-91
**Context:** Test helper function in node processor tests
**Function:** `test_event(source, payload)`
**Migration:** LOW PRIORITY - This is a test helper similar to `create_test_event`. By design it accepts arbitrary payloads.

## Summary by Category

| Category | Count | Files |
|----------|-------|-------|
| **Production (Legitimate)** | 4 | persistence.rs, pkm-automaton, content-automaton, blob_manager.rs |
| **Example/SDK** | 1 | filesystem_processor.rs (examples) |
| **Infrastructure** | 1 | typed_event_envelope.rs (macro) |
| **Test (Low Priority)** | 6 | event.rs, ulid_attack, database_test, event_model, sanitization, terminal-canonicalizer tests |
| **Test (Medium Priority)** | 1 | provenance_test.rs |

## Migration Recommendations

### DO NOT MIGRATE
1. **Production automatons** (pkm, content) - Dynamic payloads are intentional and correct
2. **Test helpers** (create_test_event, test_event) - Generic by design
3. **Security tests** - Malformed/malicious payloads require dynamic construction
4. **API tests** - Tests that validate Event::dynamic behavior must use it
5. **Macro infrastructure** - Requires macro improvements, not test migration

### CONSIDER MIGRATING (Medium Priority)
1. **`provenance_test.rs`** (6 usages) - Could define simple typed structs for test payloads:
   ```rust
   #[derive(Serialize)]
   struct ProvenanceCyclePayload {
       role: String,
   }
   ```
   However, current approach is clear and readable. Only migrate if actively refactoring these tests.

### OVERALL ASSESSMENT
**No urgent migration needed.** All `Event::dynamic` usage is appropriate for the context:
- Production code handles truly dynamic schemas (AI analysis results)
- Test code either tests the dynamic API itself or intentionally creates malformed data
- Test helpers are generic by design

The earlier concern about `Event::dynamic` usage was based on a misunderstanding. The codebase is already using typed events where appropriate (via `Event::new<T>` and the builder pattern). `Event::dynamic` is reserved for cases where the schema is genuinely dynamic or for test fixtures.

## False Positive: create_test_event

The execution plan mentioned `create_test_event` as potentially problematic. After audit, this is a **false positive**:

- `create_test_event` is a test helper in the repository module
- It's designed to create arbitrary test events with any payload
- It's the correct tool for test fixtures that don't need type safety
- It should NOT be removed or discouraged for test code

## Conclusion

**Event::dynamic usage is healthy.** No test migration sweep needed. The codebase correctly uses:
- Typed events (`Event::new<T>`) for production code with known schemas
- Dynamic events (`Event::dynamic`) for AI-generated content and test fixtures
- Builder pattern for both, ensuring provenance safety

**Recommendation:** Close D1 as complete. No further action required for Event::dynamic migration.
