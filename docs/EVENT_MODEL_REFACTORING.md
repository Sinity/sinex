# Event Model Refactoring Plan

## Current Problems

1. **`anchor_byte` is a separate field** when it's actually part of Material provenance
2. **`provenance` is Optional** allowing invalid states to exist
3. **`ts_orig` is Optional** but events always happen at some time
4. **Constructor allows invalid intermediate states** (can create event without provenance)
5. **Too many builder methods** for what should be required fields

## Proposed Clean Design

### Core Event Structure

```rust
pub struct RawEvent {
    /// Event ID - None when creating, Some when from DB
    pub id: Option<Id<RawEvent>>,
    
    /// REQUIRED: Event source
    pub source: EventSource,
    
    /// REQUIRED: Event type
    pub event_type: EventType,
    
    /// REQUIRED: Event payload
    pub payload: JsonValue,
    
    /// REQUIRED: When the event actually occurred
    pub ts_orig: Timestamp,
    
    /// REQUIRED: Where the event was generated
    pub host: HostName,
    
    /// REQUIRED: Event provenance (XOR enforced by enum)
    pub provenance: Provenance,
    
    // Optional metadata
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

pub enum Provenance {
    /// Event derived from source material
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,  // MOVED HERE where it belongs!
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        offset_kind: OffsetKind,  // byte, line, record, etc.
    },
    /// Event synthesized from other events
    Synthesis {
        source_event_ids: Vec<Id<RawEvent>>,
        operation_id: Option<Id<Operation>>,  // For tracking replay operations
    },
}
```

### Clean Constructors (No Invalid States!)

```rust
impl RawEvent {
    /// Create a first-order event from material
    pub fn from_material(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
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
    
    /// Create a synthesized event from other events
    pub fn from_synthesis(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
        ts_orig: Timestamp,
        source_event_ids: Vec<Id<RawEvent>>,
    ) -> Self {
        Self {
            id: None,
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig,
            host: get_hostname(),
            provenance: Provenance::Synthesis {
                source_event_ids,
                operation_id: get_current_operation_id(),
            },
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
    
    /// Only builder methods for OPTIONAL fields
    pub fn with_schema(mut self, schema_id: Ulid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }
    
    pub fn with_blobs(mut self, blob_ids: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(blob_ids);
        self
    }
}
```

### Simplified Typed Events

```rust
/// Do we even need Event<T>? Consider if it adds value or just complexity.
/// Option 1: Keep it simple
pub struct Event<T: EventPayload> {
    pub id: Option<Id<Event<T>>>,
    pub payload: T,
    pub ts_orig: Timestamp,
    pub host: HostName,
    pub provenance: Provenance,
    // Metadata
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl<T: EventPayload> Event<T> {
    pub fn from_material(
        payload: T,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            id: None,
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
            payload_schema_id: None,
            associated_blob_ids: None,
        }
    }
}

/// Option 2: Just use RawEvent everywhere and have typed helpers
impl RawEvent {
    pub fn typed_payload<T: EventPayload>(&self) -> Result<T, SinexError> {
        serde_json::from_value(self.payload.clone())
            .map_err(|e| SinexError::PayloadDeserialization(e))
    }
}
```

## Database Migration

```sql
-- Move anchor_byte into the provenance structure
ALTER TABLE core.events 
DROP COLUMN anchor_byte;

-- Store provenance as JSONB with proper structure
ALTER TABLE core.events
ADD COLUMN provenance JSONB NOT NULL;

-- Add check constraint for XOR
ALTER TABLE core.events
ADD CONSTRAINT provenance_xor CHECK (
    (provenance->>'type' = 'material' AND provenance->'material_id' IS NOT NULL)
    OR 
    (provenance->>'type' = 'synthesis' AND provenance->'source_event_ids' IS NOT NULL)
);

-- Make ts_orig required
ALTER TABLE core.events
ALTER COLUMN ts_orig SET NOT NULL;
```

## Benefits of This Design

1. **Impossible to create invalid events** - constructors require all necessary data
2. **Cleaner data model** - anchor_byte is where it belongs
3. **Type safety** - Provenance enum enforces XOR at compile time
4. **Simpler API** - fewer methods, clearer intent
5. **Better ergonomics** - create valid events in one call

## Migration Path

### Phase 1: Add New API (Backward Compatible)
1. Add new constructors alongside old ones
2. Mark old constructors as deprecated
3. Update documentation

### Phase 2: Update Callsites
1. Migrate all code to use new constructors
2. Remove uses of `.with_provenance()`, `.with_anchor_byte()`, etc.

### Phase 3: Remove Old API
1. Remove deprecated constructors
2. Make fields non-optional
3. Update database schema

## Alternative Considerations

### Should Event<T> exist at all?

**Pros of Event<T>:**
- Type safety for homogeneous processing
- Compile-time payload validation
- Cleaner code when working with specific event types

**Cons of Event<T>:**
- Duplicates RawEvent structure
- Conversion overhead
- Complexity for marginal benefit

**Alternative:** Just use RawEvent with typed helper methods:
```rust
impl RawEvent {
    pub fn parse_as<T: EventPayload>(&self) -> Result<T, SinexError> {
        T::from_raw_event(self)
    }
}
```

### Should we go further with type states?

Could use phantom types to encode provenance type:
```rust
pub struct Event<P: ProvenanceType> {
    // ...
    _provenance: PhantomData<P>,
}

pub struct MaterialProvenance;
pub struct SynthesisProvenance;

impl Event<MaterialProvenance> {
    // Methods specific to material events
}

impl Event<SynthesisProvenance> {
    // Methods specific to synthesis events
}
```

But this might be over-engineering for our needs.

## Summary

The key insight is: **Make invalid states unrepresentable**. By making provenance and ts_orig required, moving anchor_byte where it belongs, and providing only constructors that create valid events, we eliminate entire classes of bugs at compile time.