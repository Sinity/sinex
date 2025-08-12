# Event Model Implementation Plan

## Current State Analysis

### Database Schema Issues
1. **Missing anchor_byte**: The EventRecord struct doesn't have anchor_byte field, but code references it (compilation error waiting to happen)
2. **Separate provenance fields**: source_event_ids and source_material_id are separate columns instead of a unified provenance structure
3. **Missing offset fields**: source_material_offset_start/end referenced but not in EventRecord

### Code Structure Issues
1. **Optional provenance**: Allows invalid (NULL, NULL) state
2. **Redundant anchor_byte**: Should be part of Material provenance
3. **Optional ts_orig**: Events always happen at some time
4. **Too many builder methods**: For what should be required fields

## Implementation Steps

### Step 1: Fix EventRecord Definition
```rust
// crate/lib/sinex-migrations/src/schema/records/event.rs
#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,  // Will become NOT NULL later
    pub source: String,
    pub event_type: String,
    pub host: String,  // MISSING!
    pub payload: JsonValue,
    pub payload_schema_id: Option<uuid::Uuid>,
    pub processed_at: Option<DateTime<Utc>>,
    
    // Provenance fields (will be consolidated later)
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub source_material_offset_start: Option<i64>,  // MISSING!
    pub source_material_offset_end: Option<i64>,    // MISSING!
    pub anchor_byte: Option<i64>,                   // MISSING!
    
    // Other fields
    pub ingestor_version: Option<String>,  // MISSING!
    pub processor_name: Option<String>,
    pub processor_version: Option<String>,
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
    pub event_cluster_id: Option<uuid::Uuid>,
}
```

### Step 2: Add Missing Columns Migration
```sql
-- Migration: Add missing event columns
ALTER TABLE core.events 
ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT 'unknown',
ADD COLUMN IF NOT EXISTS source_material_offset_start BIGINT,
ADD COLUMN IF NOT EXISTS source_material_offset_end BIGINT,
ADD COLUMN IF NOT EXISTS anchor_byte BIGINT,
ADD COLUMN IF NOT EXISTS ingestor_version TEXT;

-- Update default for host after adding
ALTER TABLE core.events 
ALTER COLUMN host DROP DEFAULT;
```

### Step 3: Create New Event Model (Backward Compatible)
```rust
// crate/lib/sinex-core/src/db/models/event_v2.rs

/// Clean event structure with required provenance
pub struct Event {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    pub ts_orig: Timestamp,  // REQUIRED
    pub host: HostName,
    pub provenance: Provenance,  // REQUIRED
    // Optional metadata
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

/// Provenance with anchor_byte in the right place
pub enum Provenance {
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        offset_kind: OffsetKind,
    },
    Synthesis {
        source_event_ids: NonEmpty<Id<Event>>,  // At least one parent!
        operation_id: Option<Id<Operation>>,
    },
}

/// Ensure at least one parent event
pub struct NonEmpty<T> {
    first: T,
    rest: Vec<T>,
}

impl Event {
    /// Only valid constructors that ensure provenance
    pub fn from_material(
        source: EventSource,
        event_type: EventType,
        payload: JsonValue,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            id: None,
            source,
            event_type,
            payload,
            ts_orig,
            host: get_hostname(),
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
    
    pub fn from_synthesis(
        source: EventSource,
        event_type: EventType,
        payload: JsonValue,
        ts_orig: Timestamp,
        parent_ids: NonEmpty<Id<Event>>,
    ) -> Self {
        Self {
            id: None,
            source,
            event_type,
            payload,
            ts_orig,
            host: get_hostname(),
            provenance: Provenance::Synthesis {
                source_event_ids: parent_ids,
                operation_id: get_current_operation_id(),
            },
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
}
```

### Step 4: Migration Path for Existing Code

#### Phase 1: Add Compatibility Layer
```rust
// Keep old RawEvent but mark deprecated
#[deprecated(since = "0.3.0", note = "Use Event instead")]
pub type RawEvent = OldRawEvent;

impl From<Event> for OldRawEvent {
    fn from(e: Event) -> Self {
        // Convert new to old for backward compat
    }
}

impl TryFrom<OldRawEvent> for Event {
    type Error = InvalidEventError;
    
    fn try_from(old: OldRawEvent) -> Result<Self, Self::Error> {
        // Validate provenance exists
        let provenance = old.provenance
            .ok_or(InvalidEventError::MissingProvenance)?;
        
        // Validate ts_orig exists
        let ts_orig = old.ts_orig
            .ok_or(InvalidEventError::MissingTimestamp)?;
            
        // Convert
        Ok(Event { /* ... */ })
    }
}
```

#### Phase 2: Update Repositories
```rust
impl EventRepository {
    /// New method using clean model
    pub async fn insert_v2(&self, event: &Event) -> DbResult<Event> {
        // Decompose provenance for current DB schema
        let (source_event_ids, material_id, offset_start, offset_end, anchor_byte) = 
            match &event.provenance {
                Provenance::Material { id, anchor_byte, offset_start, offset_end, .. } => {
                    (None, Some(id), *offset_start, *offset_end, Some(*anchor_byte))
                }
                Provenance::Synthesis { source_event_ids, .. } => {
                    (Some(source_event_ids.to_vec()), None, None, None, None)
                }
            };
        
        // Insert with all fields
        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload, ts_orig,
                source_event_ids, source_material_id, 
                source_material_offset_start, source_material_offset_end,
                anchor_byte, ingestor_version, payload_schema_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING *
            "#,
            // ... values
        )
        .fetch_one(&self.pool)
        .await?;
        
        Ok(record.to_event_v2())
    }
}
```

### Step 5: Database Schema Evolution

#### Migration 1: Add CHECK constraint
```sql
-- Enforce XOR provenance (allows transition period)
ALTER TABLE core.events
ADD CONSTRAINT events_provenance_xor_soft CHECK (
    -- Allow old records with neither (temporarily)
    (source_material_id IS NULL AND source_event_ids IS NULL)
    OR
    -- Material provenance
    (source_material_id IS NOT NULL AND source_event_ids IS NULL)
    OR
    -- Synthesis provenance
    (source_material_id IS NULL AND source_event_ids IS NOT NULL AND array_length(source_event_ids, 1) > 0)
);
```

#### Migration 2: Consolidate to JSONB (future)
```sql
-- After all code migrated, consolidate provenance
ALTER TABLE core.events
ADD COLUMN provenance JSONB;

-- Migrate data
UPDATE core.events
SET provenance = 
    CASE 
        WHEN source_material_id IS NOT NULL THEN
            jsonb_build_object(
                'type', 'material',
                'material_id', source_material_id,
                'anchor_byte', anchor_byte,
                'offset_start', source_material_offset_start,
                'offset_end', source_material_offset_end
            )
        WHEN source_event_ids IS NOT NULL THEN
            jsonb_build_object(
                'type', 'synthesis',
                'source_event_ids', source_event_ids
            )
    END;

-- Make required
ALTER TABLE core.events
ALTER COLUMN provenance SET NOT NULL,
ALTER COLUMN ts_orig SET NOT NULL;

-- Drop old columns
ALTER TABLE core.events
DROP COLUMN source_material_id,
DROP COLUMN source_material_offset_start,
DROP COLUMN source_material_offset_end,
DROP COLUMN anchor_byte,
DROP COLUMN source_event_ids;
```

### Step 6: Update Schema Definition
```rust
// crate/lib/sinex-migrations/src/schema/core_events.rs
#[derive(Iden)]
pub enum Events {
    Table,
    Id,
    CreatedAt,
    UpdatedAt,
    TsOrig,  // Will be NOT NULL
    Source,
    EventType,
    Host,  // Add this
    Payload,
    Provenance,  // New consolidated field
    IngestorVersion,
    PayloadSchemaId,
    ProcessorName,
    ProcessorVersion,
    AssociatedBlobIds,
    EventClusterId,
}
```

## Testing Strategy

### Unit Tests
```rust
#[test]
fn event_requires_provenance() {
    // Should not compile:
    // let event = Event { provenance: None, ... };
    
    // Should compile:
    let event = Event::from_material(/* ... */);
}

#[test]
fn provenance_xor_enforced() {
    // Cannot have both material and synthesis provenance
    // (enforced by enum)
}
```

### Integration Tests
```rust
#[test]
async fn database_rejects_invalid_events() {
    let event_without_provenance = /* ... */;
    let result = repo.insert(event_without_provenance).await;
    assert!(result.is_err());
}
```

## Rollout Plan

### Week 1: Preparation
- [ ] Add missing columns to database
- [ ] Fix EventRecord definition
- [ ] Create Event v2 model
- [ ] Add compatibility layer

### Week 2: Migration
- [ ] Update satellites to use new constructors
- [ ] Update tests
- [ ] Add validation in ingestd

### Week 3: Enforcement
- [ ] Add database CHECK constraint
- [ ] Remove deprecated constructors
- [ ] Monitor for violations

### Week 4: Cleanup
- [ ] Consolidate provenance to JSONB
- [ ] Make ts_orig NOT NULL
- [ ] Remove old columns

## Success Metrics

1. **Zero invalid events**: No events with missing provenance
2. **Cleaner API**: Fewer methods, clearer intent
3. **Better performance**: Fewer NULL checks, better indexes
4. **Type safety**: Invalid states unrepresentable

## Risk Mitigation

1. **Backward compatibility**: Keep old API during transition
2. **Gradual rollout**: Add constraints progressively
3. **Monitoring**: Track constraint violations
4. **Rollback plan**: Keep old columns until fully migrated