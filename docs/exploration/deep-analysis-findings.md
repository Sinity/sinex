# Deep Analysis Findings

This document covers performance, data integrity, code duplication, and database schema analysis.

---

## Executive Summary

| Category | Critical | High | Medium | Low | Total |
|----------|----------|------|--------|-----|-------|
| Performance | 2 | 4 | 8 | 5 | 19 |
| Data Integrity | 3 | 4 | 2 | 1 | 10 |
| Code Duplication | - | - | - | - | 600+ LOC |
| Database Schema | 2 | 4 | 6 | 3 | 15 |

---

## 1. Performance Issues

### Critical Performance Issues

#### PERF-C1: Payload Cloning in Hot Path
**File**: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:719-737`

```rust
for (idx, prepared) in batch.iter().enumerate() {
    builder.push_bind(prepared.raw.payload.clone());     // Clones entire JSON
    builder.push_bind(prepared.source_event_ids.clone()); // Clones Vec<Uuid>
    builder.push_bind(prepared.associated_blob_ids.clone());
}
```

**Impact**: For 100-event batches, clones 100 JSON payloads (potentially MBs of data) + 200 vectors. This is the hottest path in the system.

**Fix**: Use references if sqlx supports `&JsonValue`, or restructure to avoid cloning.

---

#### PERF-C2: Repeated String Allocations in Search Filters
**File**: `crate/lib/sinex-core/src/db/repositories/events.rs:791-798`

```rust
let values: Vec<String> = sources.iter().map(|s| s.as_str().to_string()).collect();
let values: Vec<String> = event_types.iter().map(|t| t.as_str().to_string()).collect();
```

**Impact**: Every search with multiple filters allocates N strings unnecessarily.

**Fix**: Use `Vec<&str>` instead of `Vec<String>`.

---

### High Performance Issues

#### PERF-H1: UUID Conversion Vec Allocations (6 instances)
**Files**: `events.rs:448,452,890,894,1060,1064`

```rust
let source_event_uuids = source_event_ids
    .as_ref()
    .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
```

**Impact**: Creates intermediate `Vec` for every event insertion.

---

#### PERF-H2: Confirmation String Operations
**File**: `jetstream_consumer.rs:757-769`

```rust
let event_id_str = event_id.to_string();
let confirmation = Confirmation { event_id: event_id_str.clone(), ... };
let subject = format!("{}{}", prefix, event_id_str);
```

**Impact**: Multiple string allocations per confirmed event.

---

#### PERF-H3: Dynamic Query Building Allocations
**File**: `events.rs:1631-1653`

```rust
let mut query_parts = vec!["DELETE FROM core.events WHERE 1=1".to_string()];
query_parts.push(format!(" AND source = ${}", bind_index));
let query_sql = query_parts.join("");
```

**Impact**: Multiple allocations + join for query construction.

---

#### PERF-H4: Hasher Clone for Finalization
**File**: `material_assembler.rs:791`

```rust
let computed_hash = state.hasher.clone().finalize().to_hex().to_string();
```

**Impact**: Clones Blake3 hasher state (contains full hash computation state).

---

### Performance Summary Table

| Issue | File | Line | Hot Path? | Fix Effort |
|-------|------|------|-----------|------------|
| Payload clone | jetstream_consumer.rs | 719 | YES | Medium |
| String alloc in search | events.rs | 791 | YES | Low |
| UUID Vec alloc | events.rs | 448+ | YES | Medium |
| Confirmation strings | jetstream_consumer.rs | 757 | YES | Low |
| Query builder | events.rs | 1631 | No | Low |
| Hasher clone | material_assembler.rs | 791 | No | Low |
| ILIKE format | events.rs | 827 | YES | Low |

---

## 2. Data Integrity Issues

### Critical Data Integrity Issues

#### INT-C1: Provenance anchor_byte No Negative Validation
**File**: `crate/lib/sinex-core/src/db/models/event.rs:138-152`

```rust
pub fn from_material(
    id: impl Into<Id<SourceMaterial>>,
    anchor_byte: i64,  // No validation that anchor_byte >= 0!
    offset_start: Option<i64>,
    offset_end: Option<i64>,
) -> Self
```

**Impact**: Negative anchor_byte values could bypass uniqueness constraints and corrupt provenance chain.

---

#### INT-C2: Replay State Machine Race Condition
**File**: `crate/lib/sinex-core/src/db/replay/state_machine.rs:396-437`

```rust
pub async fn update_preview(&self, operation_id: Ulid, preview: Value) -> Result<()> {
    let row = sqlx::query!(
        r#"SELECT preview_summary FROM core.operations_log WHERE id::uuid = $1::uuid"#,
        // NO FOR UPDATE - race condition!
    )
```

**Race Scenario**:
1. Thread A reads state = "Planning"
2. Thread B reads state = "Planning"
3. Thread A transitions to "Previewed"
4. Thread B updates without validating state → invalid transition

**Fix**: Add `FOR UPDATE` like `transition_with_tx()` uses.

---

#### INT-C3: Checkpoint Monotonicity Not Enforced
**File**: `state_machine.rs:548-584`

```rust
pub async fn update_checkpoint(&self, operation_id: Ulid, checkpoint: &ReplayCheckpoint) {
    meta.checkpoint = checkpoint.clone();  // No validation!
}
```

**Missing validations**:
- `processed_events` should never decrease
- `processed_events <= total_events`
- `last_event_id` should progress monotonically

---

### High Data Integrity Issues

#### INT-H1: Outcome-Finished State Constraint Gap
**File**: `state_machine.rs:366-371`

Only "Completed" state sets outcome. "Failed" and "Cancelled" states don't set outcome, creating inconsistency.

**Missing invariant**: `finished_at IS NOT NULL ⟹ outcome IS NOT NULL`

---

#### INT-H2: Material Uniqueness Enforcement Gap
**File**: `event.rs:100-107`

Database has unique constraint on `(source_material_id, anchor_byte)` but no application-level pre-check. Concurrent inserts can violate constraint.

---

#### INT-H3: EventRecord Provenance Reconstruction Fragility
**File**: `events.rs:82-157`

Complex 6-way match for provenance reconstruction. Logic is correct but fragile - easy to introduce gaps during refactoring.

---

#### INT-H4: Checkpoint Consumer Group Collision
**File**: `checkpoints.rs:113-120`

```rust
let consumer_group = checkpoint.consumer_group
    .unwrap_or_else(|| ConsumerGroup::new("default"));
let consumer_name = checkpoint.consumer_name
    .unwrap_or_else(|| ConsumerName::new("default"));
```

**Impact**: Multiple processors could unknowingly share checkpoint identity `("default", "default")`.

---

### Data Integrity Summary

| Issue | Type | Severity | Impact |
|-------|------|----------|--------|
| anchor_byte validation | Type Safety | Critical | Provenance corruption |
| update_preview race | Concurrency | Critical | State machine corruption |
| Checkpoint monotonicity | Invariant | Critical | Progress tracking unreliable |
| Outcome-finished gap | Invariant | High | Query inconsistency |
| Material uniqueness | Constraint | High | Idempotency violation |
| Provenance reconstruction | Logic | High | Future bug risk |
| Checkpoint collision | Default | Medium | Silent data merging |

---

## 3. Code Duplication Analysis

### Summary Statistics

| Category | Instances | LOC Duplicated | Fix Effort |
|----------|-----------|----------------|------------|
| Processor constructors | 7 satellites | 50 | Low |
| Error handling wrappers | 13+ locations | 30 | Low |
| JetStream consumer setup | 2 exact copies | 52 | Low |
| StatefulStreamProcessor impls | 10 satellites | 600+ | High |
| Event emission pattern | 8+ locations | 160+ | Medium |
| Automaton base logic | 5 automatons | 140+ per pair | Medium |
| Repository CRUD | 11 repositories | 70+ | Low |

### Critical Duplications

#### DUP-1: JetStream Consumer Setup (Exact Copy)
**Files**:
- `sinex-analytics-automaton/src/lib.rs:158-210`
- `sinex-health-aggregator/src/lib.rs:159-211`

**52 lines of identical code** with only config name changes.

```rust
async fn ensure_consumer(&mut self) -> SatelliteResult<()> {
    // Identical 52-line method
}
```

**Fix**: Extract to `sinex-satellite-sdk` utility function.

---

#### DUP-2: StatefulStreamProcessor Trait Implementations
**All 10 satellites** implement identical:
- `new()` - identical initialization (~8 lines each)
- `runtime()` - identical error handling (~5 lines each)
- `config()` - identical (~3 lines each)
- `initialize()` - nearly identical setup (~20 lines each)
- `shutdown()` - identical cleanup (~6 lines each)

**Total**: 600+ lines of duplicated boilerplate

**Fix**: Create `ProcessorBase<C>` struct with common implementations.

---

#### DUP-3: Event Emission Pattern
**8+ locations** across satellites:

```rust
let provenance = Provenance::Material {
    id: Id::from_ulid(material_id),
    anchor_byte: 0,
    offset_start: Some(0),
    offset_end: Some(total_bytes),
    offset_kind: OffsetKind::Byte,
};
let event = CoreEvent::create(source, event_type, payload, provenance);
ctx.stage_context.emit_event_with_provenance(...).await?;
```

**Fix**: Create `MaterialEventBuilder` helper.

---

#### DUP-4: Confirmed Event Handler (Exact Copy)
**Files**:
- `sinex-analytics-automaton/src/lib.rs:870-895`
- `sinex-health-aggregator/src/lib.rs:727-752`

**25 lines identical** - `ChannelConfirmedEventHandler` struct and impl.

**Fix**: Move to SDK.

---

### Duplication by Satellite Pair

| Satellite A | Satellite B | Shared Code | LOC |
|-------------|-------------|-------------|-----|
| analytics-automaton | health-aggregator | ensure_consumer, event handler | 77 |
| fs-watcher | terminal-satellite | material capture, WatchContext | 40 |
| All satellites | All satellites | trait impls, constructors | 600+ |

### Recommended Abstractions

1. **`#[derive(ProcessorBuilder)]`** - Generate constructor boilerplate
2. **`ProcessorBase<C>`** - Common trait method implementations
3. **`MaterialEventBuilder`** - Event emission helper
4. **`setup_jetstream_consumer()`** - Extracted utility
5. **`ConfigValidator` trait** - Unified config validation
6. **`#[derive(Repository)]`** - Repository CRUD boilerplate

---

## 4. Database Schema Issues

### Critical Database Issues

#### DB-C1: UNIQUE Index Prevents Multiple Entities Per Type
**File**: `entities.rs:165-169`

```rust
Index::create()
    .unique()
    .name("ix_entities_type")
    .table(Self::table_iden())
    .col(Entities::EntityType)  // Only ONE entity per type allowed!
```

**Impact**: System can only store ONE person, ONE file, etc. Fatal data model corruption.

**Fix**: Remove this index immediately.

---

#### DB-C2: Entity Relations UNIQUE Prevents Graph Structure
**File**: `entities.rs:338-370`

```rust
// These prevent normal graph relationships:
Index::create().unique().name("ix_entity_relations_from_type")
    .col(EntityRelations::FromEntityId)
    .col(EntityRelations::RelationType)  // One outgoing rel per type!

Index::create().unique().name("ix_entity_relations_to_type")
    .col(EntityRelations::ToEntityId)
    .col(EntityRelations::RelationType)  // One incoming rel per type!
```

**Impact**: A person can only "manage" ONE project. Knowledge graph unusable.

**Fix**: Remove both indexes.

---

### High Database Issues

#### DB-H1: ValidationCache Missing Indexes
**File**: `sinex_schemas.rs:420-467`

Only primary key exists. Missing indexes for:
- `schema_id` - find all validations for a schema
- `event_id` - find all validations for an event
- `validated_at` - cleanup old entries

---

#### DB-H2: Temporal Ledger Missing DESC Index
**File**: `temporal_ledger.rs:92-118`

Time-range queries on `ts_capture` need DESC index for efficient range scans.

---

#### DB-H3: ProcessorCheckpoints Missing Activity Index
**File**: `processors.rs:138-147`

No index on `last_activity` - stale checkpoint detection requires full table scan.

---

#### DB-H4: Outbox Missing Processing Status Index
**File**: `outbox.rs:151-171`

No index for "processing" status - recovery of stalled messages requires full scan.

---

### Medium Database Issues

#### DB-M1: TimescaleDB Configuration Suboptimal
**File**: `events.rs:151-154`

- No explicit chunk interval set
- No compression configured
- No retention policy defined
- No continuous aggregates

---

#### DB-M2: Entities Merged Logic Not Enforced
**File**: `entities.rs:126-131`

`is_merged` and `merged_into_id` can be inconsistent - no CHECK constraint.

---

#### DB-M3: Missing Event Query Pattern Indexes

Common query pattern not optimally indexed:
```sql
SELECT * FROM core.events
WHERE event_type = ? AND ts_orig BETWEEN ? AND ?
```

---

### Database Summary

| Issue | Location | Severity | Impact |
|-------|----------|----------|--------|
| Entity type UNIQUE | entities.rs:165 | CRITICAL | Only 1 entity per type |
| Relations UNIQUE | entities.rs:338 | CRITICAL | Graph unusable |
| ValidationCache indexes | sinex_schemas.rs | HIGH | Full table scans |
| Temporal ledger DESC | temporal_ledger.rs | HIGH | Slow time queries |
| Checkpoints activity | processors.rs | MEDIUM | Slow stale detection |
| TimescaleDB config | events.rs | MEDIUM | Storage inefficiency |
| Merged constraint | entities.rs | MEDIUM | Data inconsistency |

---

## Recommended Priority Order

### Immediate (Before Production)

1. **Remove `ix_entities_type` unique index** - Fatal data model bug
2. **Remove entity relations unique indexes** - Graph unusable
3. **Add `FOR UPDATE` to `update_preview()`** - Race condition
4. **Add anchor_byte validation** - Provenance corruption risk
5. **Fix payload cloning in hot path** - Major perf impact

### Week 1

1. Add missing ValidationCache indexes
2. Add checkpoint monotonicity validation
3. Extract JetStream consumer setup (52-line duplicate)
4. Fix string allocations in search filters
5. Add ProcessorCheckpoints activity index

### Week 2

1. Create `ProcessorBase<C>` abstraction (600+ LOC)
2. Add temporal ledger DESC index
3. Configure TimescaleDB (compression, retention)
4. Add entities merged CHECK constraint
5. Create `MaterialEventBuilder` helper

### Ongoing

1. Standardize satellite implementations
2. Extract remaining duplications
3. Add missing query pattern indexes
4. Clean up repository boilerplate

---

## Files Most Needing Attention

| File | Issues | Categories |
|------|--------|------------|
| `entities.rs` | 3 critical | Database schema |
| `jetstream_consumer.rs` | 4 | Performance |
| `events.rs` | 6 | Performance, DB |
| `state_machine.rs` | 3 | Data integrity |
| `lib.rs` (analytics) | 3 | Duplication |
| `lib.rs` (health) | 3 | Duplication |
| All satellites | 10 | Duplication |

---

*Generated from deep codebase analysis, December 2024*
