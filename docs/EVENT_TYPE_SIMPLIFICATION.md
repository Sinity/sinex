# Event Type System Simplification

## Current Complexity

We have three event representations:
1. **Event<T>** - Strongly typed payload
2. **RawEvent** - JSON payload
3. **EventRecord** - Database row

This creates conversion overhead and maintenance burden.

## Analysis: Do We Need Event<T>?

### Current Usage Pattern
```rust
// Creating typed event
let payload = FileCreatedPayload { path: "/foo", size: 100 };
let event = Event::new(payload);

// Converting to RawEvent for storage
let raw: RawEvent = event.into();

// Converting back from database
let typed: Event<FileCreatedPayload> = Event::try_from(raw)?;
```

### Problems with Event<T>

1. **Duplication**: Same fields as RawEvent
2. **Conversion overhead**: Serialize/deserialize on every conversion
3. **Limited benefit**: Type safety only during creation, lost immediately
4. **Complexity**: Two parallel APIs to maintain

### What Event<T> Provides

1. **Compile-time payload validation** - But only at creation
2. **Auto-derived source/event_type** - Could be done differently
3. **Type-safe processing** - But only if you know the type ahead

## Simpler Alternative: Typed Helpers

### Option 1: Typed Constructors on RawEvent
```rust
impl RawEvent {
    /// Create file system event with typed payload
    pub fn file_created(
        path: impl AsRef<Path>,
        size: u64,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        let payload = json!({
            "path": path.as_ref().display().to_string(),
            "size": size,
        });
        
        Self::material(
            "fs-watcher",
            "file.created",
            payload,
            ts_orig,
            material_id,
            anchor_byte,
        )
    }
}
```

### Option 2: Payload Traits
```rust
pub trait EventPayload: Serialize + DeserializeOwned {
    const SOURCE: &'static str;
    const EVENT_TYPE: &'static str;
    
    fn to_raw_event(
        self,
        ts_orig: Timestamp,
        provenance: Provenance,
    ) -> RawEvent {
        RawEvent {
            source: Self::SOURCE.into(),
            event_type: Self::EVENT_TYPE.into(),
            payload: serde_json::to_value(self).unwrap(),
            ts_orig,
            provenance,
            // ... other fields
        }
    }
    
    fn from_raw_event(event: &RawEvent) -> Result<Self, Error> {
        if event.event_type.as_str() != Self::EVENT_TYPE {
            return Err(Error::WrongEventType);
        }
        serde_json::from_value(event.payload.clone())
            .map_err(Error::Deserialization)
    }
}

// Usage
let payload = FileCreatedPayload { path: "/foo", size: 100 };
let event = payload.to_raw_event(ts_orig, provenance);

// Later
let payload: FileCreatedPayload = FileCreatedPayload::from_raw_event(&event)?;
```

### Option 3: Module-Based Organization
```rust
pub mod events {
    pub mod filesystem {
        use super::*;
        
        pub fn file_created(
            path: &Path,
            size: u64,
            material: MaterialRef,
        ) -> RawEvent {
            RawEvent::material(
                "fs-watcher",
                "file.created",
                json!({ "path": path, "size": size }),
                Utc::now(),
                material.id,
                material.current_offset,
            )
        }
        
        pub fn parse_file_created(event: &RawEvent) -> Result<FileInfo> {
            if event.event_type != "file.created" {
                return Err(Error::WrongType);
            }
            // Parse payload
        }
    }
}

// Usage
let event = events::filesystem::file_created(path, size, material);
```

## Recommendation: Remove Event<T>

### Why Remove It?

1. **Minimal benefit**: Type safety is immediately lost after creation
2. **Maintenance burden**: Duplicate structure to maintain
3. **Conversion overhead**: Constant serialization/deserialization
4. **Simpler is better**: One event type is easier to understand

### Migration Path

1. **Phase 1**: Add typed helpers to RawEvent
```rust
impl RawEvent {
    pub fn from_payload<T: EventPayload>(
        payload: T,
        ts_orig: Timestamp,
        provenance: Provenance,
    ) -> Self {
        Self {
            source: T::SOURCE.into(),
            event_type: T::EVENT_TYPE.into(),
            payload: serde_json::to_value(payload).unwrap(),
            ts_orig,
            provenance,
            // ...
        }
    }
    
    pub fn payload<T: EventPayload>(&self) -> Result<T> {
        T::from_raw_event(self)
    }
}
```

2. **Phase 2**: Update satellites
```rust
// Before
let event = Event::new(payload).with_provenance(prov);

// After  
let event = RawEvent::from_payload(payload, ts_orig, prov);
```

3. **Phase 3**: Remove Event<T>
```rust
#[deprecated(note = "Use RawEvent::from_payload()")]
pub struct Event<T> { /* ... */ }
```

## Final Simplified Structure

```rust
/// The ONE event type
pub struct Event {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    pub ts_orig: Timestamp,
    pub host: HostName,
    pub provenance: Provenance,
    // Optional metadata
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

/// Provenance (with anchor_byte in the right place)
pub enum Provenance {
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
    Synthesis {
        source_event_ids: Vec<Id<Event>>,
        operation_id: Option<Id<Operation>>,
    },
}

/// Typed payload helpers
pub trait EventPayload: Serialize + DeserializeOwned {
    const SOURCE: &'static str;
    const EVENT_TYPE: &'static str;
}

impl Event {
    /// Material event with typed payload
    pub fn material_typed<T: EventPayload>(
        payload: T,
        ts_orig: Timestamp,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Self {
            source: T::SOURCE.into(),
            event_type: T::EVENT_TYPE.into(),
            payload: serde_json::to_value(payload).unwrap(),
            ts_orig,
            host: get_hostname(),
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte,
                offset_start: None,
                offset_end: None,
            },
            // ...
        }
    }
    
    /// Extract typed payload
    pub fn payload<T: EventPayload>(&self) -> Result<T> {
        if self.event_type.as_str() != T::EVENT_TYPE {
            return Err(Error::WrongEventType);
        }
        serde_json::from_value(self.payload.clone())
            .map_err(Error::Deserialization)
    }
}
```

## Benefits of Simplification

1. **One event type**: No confusion about which to use
2. **Clear provenance**: Required field, proper structure
3. **Type helpers available**: When you need them
4. **Less code**: No duplicate structures
5. **Better performance**: No unnecessary conversions

## Summary

Event<T> adds complexity without proportional benefit. A single Event type with typed helpers provides the same capabilities with less code and clearer semantics. The type safety of Event<T> is illusory since it's immediately lost when stored or transmitted.