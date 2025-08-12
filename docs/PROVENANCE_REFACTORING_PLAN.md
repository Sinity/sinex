# Provenance Refactoring Plan

## Current Problems

1. **anchor_byte is in the wrong place** - it's a field on Event instead of inside Material provenance
2. **provenance is Optional** - allows invalid (NULL, NULL) state
3. **Missing columns in database** - host, anchor_byte, offsets, ingestor_version aren't in schema
4. **CHECK constraint is wrong** - allows (NULL, NULL) which violates architecture

## Simple Fix: Just Fix It

### Step 1: Fix the Provenance Enum

```rust
// crate/lib/sinex-core/src/db/models/event.rs

pub enum Provenance {
    /// Event derived from source material
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,  // MOVE HERE from Event
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
    /// Event synthesized from other events  
    Synthesis {
        source_event_ids: Vec<Id<RawEvent>>,  
    },
}

pub struct RawEvent {
    pub id: Option<Id<RawEvent>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    pub ts_orig: Option<Timestamp>,  // Keep nullable for now
    pub host: HostName,
    pub provenance: Provenance,  // NOT OPTIONAL
    // pub anchor_byte: Option<i64>,  // DELETE THIS
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}
```

### Step 2: Add Missing Schema Columns

```rust
// crate/lib/sinex-migrations/src/schema/core_events.rs

#[derive(Iden)]
pub enum Events {
    Table,
    Id,
    CreatedAt,
    UpdatedAt,
    TsOrig,
    Source,
    EventType,
    Host,  // ADD THIS
    Payload,
    PayloadSchemaId,
    ProcessedAt,
    SourceEventIds,
    SourceMaterialId,
    SourceMaterialOffsetStart,  // ADD THIS
    SourceMaterialOffsetEnd,    // ADD THIS
    AnchorByte,                 // ADD THIS
    IngestorVersion,            // ADD THIS
    ProcessorName,
    ProcessorVersion,
    AssociatedBlobIds,
    EventClusterId,
}
```

### Step 3: Fix EventRecord

```rust
// crate/lib/sinex-migrations/src/schema/records/event.rs

#[derive(Debug, Clone, FromRow)]
pub struct EventRecord {
    pub id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub source: String,
    pub event_type: String,
    pub host: String,  // ADD THIS
    pub payload: JsonValue,
    pub payload_schema_id: Option<uuid::Uuid>,
    pub processed_at: Option<DateTime<Utc>>,
    pub source_event_ids: Option<Vec<uuid::Uuid>>,
    pub source_material_id: Option<uuid::Uuid>,
    pub source_material_offset_start: Option<i64>,  // ADD THIS
    pub source_material_offset_end: Option<i64>,    // ADD THIS
    pub anchor_byte: Option<i64>,                   // ADD THIS
    pub ingestor_version: Option<String>,           // ADD THIS
    pub processor_name: Option<String>,
    pub processor_version: Option<String>,
    pub associated_blob_ids: Option<Vec<uuid::Uuid>>,
    pub event_cluster_id: Option<uuid::Uuid>,
}
```

### Step 4: Create Migration to Add Columns

```sql
-- New migration file
ALTER TABLE core.events 
ADD COLUMN host TEXT NOT NULL DEFAULT 'unknown',
ADD COLUMN source_material_offset_start BIGINT,
ADD COLUMN source_material_offset_end BIGINT,
ADD COLUMN anchor_byte BIGINT,
ADD COLUMN ingestor_version TEXT;

-- Fix the CHECK constraint
ALTER TABLE core.events
DROP CONSTRAINT IF EXISTS events_provenance_xor;

ALTER TABLE core.events
ADD CONSTRAINT events_provenance_xor CHECK (
    (source_material_id IS NOT NULL AND source_event_ids IS NULL)
    OR
    (source_material_id IS NULL AND source_event_ids IS NOT NULL)
    -- NO THIRD OPTION! Must have provenance
);
```

### Step 5: Fix Event Construction

```rust
impl RawEvent {
    /// No more default constructor that creates invalid events
    pub fn from_material(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,  // Caller should set this
            host: get_hostname(),
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte,
                offset_start: None,
                offset_end: None,
            },
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
    
    pub fn from_synthesis(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        parent_ids: Vec<Id<RawEvent>>,
    ) -> Self {
        assert!(!parent_ids.is_empty(), "Must have at least one parent");
        
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,
            host: get_hostname(),
            provenance: Provenance::Synthesis { source_event_ids: parent_ids },
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
}
```

### Step 6: Fix Repository Code

```rust
// crate/lib/sinex-core/src/db/repositories/events.rs

fn extract_provenance(
    provenance: &Provenance,
) -> (
    Option<Vec<uuid::Uuid>>, // source_event_ids
    Option<uuid::Uuid>,      // source_material_id
    Option<i64>,             // offset_start
    Option<i64>,             // offset_end
    Option<i64>,             // anchor_byte
) {
    match provenance {
        Provenance::Material { id, anchor_byte, offset_start, offset_end } => {
            (None, Some(id.to_uuid()), *offset_start, *offset_end, Some(*anchor_byte))
        }
        Provenance::Synthesis { source_event_ids } => {
            let uuids = source_event_ids.iter().map(|id| id.to_uuid()).collect();
            (Some(uuids), None, None, None, None)
        }
    }
}

impl EventRecord {
    pub fn to_raw_event(self) -> RawEvent {
        let provenance = match (self.source_event_ids, self.source_material_id, self.anchor_byte) {
            (Some(ids), None, None) if !ids.is_empty() => {
                Provenance::Synthesis {
                    source_event_ids: ids.into_iter().map(Id::from_uuid).collect(),
                }
            }
            (None, Some(mat_id), Some(anchor)) => {
                Provenance::Material {
                    id: Id::from_uuid(mat_id),
                    anchor_byte: anchor,
                    offset_start: self.source_material_offset_start,
                    offset_end: self.source_material_offset_end,
                }
            }
            _ => panic!("Invalid provenance in database! This shouldn't happen with CHECK constraint"),
        };
        
        RawEvent {
            id: Some(Id::from_uuid(self.id)),
            source: self.source.into(),
            event_type: self.event_type.into(),
            host: self.host.into(),
            payload: self.payload,
            ts_orig: self.ts_orig,
            provenance,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id.map(uuid_to_ulid),
            associated_blob_ids: self.associated_blob_ids.map(|ids| ids.into_iter().map(uuid_to_ulid).collect()),
        }
    }
}
```

### Step 7: Update All Event Creation Sites

Search and replace all:
- `RawEvent::new()` -> Use `from_material()` or `from_synthesis()`
- `Event::new()` -> Use `from_material()` or `from_synthesis()`
- `.with_provenance()` -> Delete, provenance is required in constructor
- `.with_anchor_byte()` -> Delete, it's part of Material provenance now

## That's It

No "migration path", no "backwards compatibility", no "gradual rollout". Just:

1. Fix the types
2. Add the missing columns
3. Fix the constraint
4. Update the code
5. Run tests
6. Fix what breaks

The system isn't in production. There's no data to preserve. Just fix it.

## Commands

```bash
# 1. Create migration
echo "CREATE MIGRATION" > crate/lib/sinex-migrations/src/m20250812_fix_events_schema.rs

# 2. Run migration
just migrate

# 3. Fix compilation errors
cargo check --workspace 2>&1 | grep "error\["

# 4. Run tests
just test

# 5. Fix what breaks
```