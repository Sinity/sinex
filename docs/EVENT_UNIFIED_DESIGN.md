# Unified Event Design: Merging RawEvent and Event<T>

## The Insight

You're right - homogeneous collections are the common case for actual processing. We want type safety when working with events, but need flexibility for storage/transmission. Can we have both in a single type?

## Design Option 1: Generic Event with Type Erasure

```rust
pub struct Event<T = JsonValue> {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: T,
    pub ts_orig: Option<Timestamp>,
    pub host: HostName,
    pub provenance: Provenance,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

// Type aliases for convenience
pub type RawEvent = Event<JsonValue>;
pub type TypedEvent<T> = Event<T>;

// Now it's the SAME struct!
impl<T: EventPayload> Event<T> {
    pub fn from_material(
        payload: T,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        // ...
    }
    
    // Convert typed to raw (type erasure)
    pub fn to_raw(self) -> RawEvent 
    where 
        T: Serialize 
    {
        Event {
            payload: serde_json::to_value(self.payload).unwrap(),
            // ... copy other fields
        }
    }
}

impl RawEvent {
    // Try to convert raw to typed
    pub fn to_typed<T: DeserializeOwned>(self) -> Result<Event<T>> {
        Ok(Event {
            payload: serde_json::from_value(self.payload)?,
            // ... copy other fields
        })
    }
}
```

## Design Option 2: Enum-Based Payload

```rust
pub struct Event {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: Payload,  // <-- Enum instead of generic
    pub ts_orig: Option<Timestamp>,
    pub host: HostName,
    pub provenance: Provenance,
    // ...
}

pub enum Payload {
    // Known types (fast, zero-copy access)
    FileCreated(FileCreatedPayload),
    CommandExecuted(CommandExecutedPayload),
    TerminalOutput(TerminalOutputPayload),
    // ... more typed variants
    
    // Fallback for unknown types
    Json(JsonValue),
}

impl Event {
    // Type-safe construction
    pub fn file_created(
        path: PathBuf,
        size: u64,
        material_id: Id<SourceMaterial>,
        anchor_byte: i64,
    ) -> Self {
        Event {
            payload: Payload::FileCreated(FileCreatedPayload { path, size }),
            // ...
        }
    }
    
    // Pattern matching for processing
    pub fn process(&self) {
        match &self.payload {
            Payload::FileCreated(p) => {
                // Direct typed access
                println!("File: {}", p.path.display());
            }
            Payload::CommandExecuted(p) => {
                println!("Command: {}", p.command);
            }
            Payload::Json(j) => {
                // Unknown type, handle generically
            }
        }
    }
}
```

## Design Option 3: Trait-Based Unification

```rust
// The single Event type
pub struct Event {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: Box<dyn EventPayload>,  // Trait object
    pub ts_orig: Option<Timestamp>,
    pub host: HostName,
    pub provenance: Provenance,
    // ...
}

pub trait EventPayload: Send + Sync {
    fn as_json(&self) -> JsonValue;
    fn as_any(&self) -> &dyn Any;
    fn event_type(&self) -> &'static str;
    fn source(&self) -> &'static str;
}

// Blanket implementation for all payload types
impl<T> EventPayload for T 
where 
    T: Serialize + Send + Sync + Any + 'static,
    T: HasEventType + HasSource,
{
    fn as_json(&self) -> JsonValue {
        serde_json::to_value(self).unwrap()
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn event_type(&self) -> &'static str {
        T::EVENT_TYPE
    }
    
    fn source(&self) -> &'static str {
        T::SOURCE
    }
}

impl Event {
    // Downcast to specific type when needed
    pub fn payload_as<T: EventPayload + 'static>(&self) -> Option<&T> {
        self.payload.as_any().downcast_ref::<T>()
    }
    
    // Work with typed payloads
    pub fn from_payload<T: EventPayload>(
        payload: T,
        provenance: Provenance,
    ) -> Self {
        Event {
            source: payload.source().into(),
            event_type: payload.event_type().into(),
            payload: Box::new(payload),
            provenance,
            // ...
        }
    }
}
```

## Design Option 4: Zero-Cost Abstraction with PhantomData

```rust
use std::marker::PhantomData;

pub struct Event<T = ()> {
    pub id: Option<Id<Event>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,  // Always JSON internally
    pub ts_orig: Option<Timestamp>,
    pub host: HostName,
    pub provenance: Provenance,
    // ...
    _phantom: PhantomData<T>,
}

// Type aliases
pub type RawEvent = Event<()>;
pub type TypedEvent<T> = Event<T>;

impl<T: EventPayload> Event<T> {
    // Construct with type safety
    pub fn new(payload: T, provenance: Provenance) -> Self {
        Event {
            source: T::SOURCE.into(),
            event_type: T::EVENT_TYPE.into(),
            payload: serde_json::to_value(payload).unwrap(),
            provenance,
            _phantom: PhantomData,
            // ...
        }
    }
    
    // Access payload with type safety (lazy deserialization)
    pub fn typed_payload(&self) -> Result<T> {
        serde_json::from_value(self.payload.clone())
    }
    
    // Zero-cost type erasure
    pub fn to_raw(self) -> RawEvent {
        Event {
            payload: self.payload,
            _phantom: PhantomData,
            // ... other fields unchanged
        }
    }
}

impl RawEvent {
    // Type assertion (zero-cost at runtime)
    pub fn assume_type<T>(self) -> Event<T> {
        Event {
            payload: self.payload,
            _phantom: PhantomData,
            // ... other fields unchanged
        }
    }
}
```

## My Recommendation: Option 1 (Generic with Default)

```rust
pub struct Event<T = JsonValue> {
    // ... fields
    pub payload: T,
}
```

This gives us:
- **Single type**: No duplicate structures
- **Type safety when wanted**: `Event<FileCreatedPayload>`
- **Flexibility when needed**: `Event<JsonValue>` (or just `Event`)
- **Natural conversions**: `to_raw()` and `to_typed()`
- **Familiar pattern**: Similar to how `Vec<T>` works

### Usage Examples

```rust
// Creating typed events
let file_event = Event::from_material(
    FileCreatedPayload { path, size },
    material_id,
    anchor_byte,
);

// Processing homogeneous collections (your common case!)
fn process_files(events: Vec<Event<FileCreatedPayload>>) {
    for event in events {
        // Direct typed access
        archive_file(&event.payload.path).await?;
    }
}

// Storage/transmission
async fn store_event<T: Serialize>(event: Event<T>) {
    let raw = event.to_raw();  // Convert to Event<JsonValue>
    db.insert(raw).await?;
}

// Retrieval with type recovery
async fn get_file_events() -> Vec<Event<FileCreatedPayload>> {
    let raw_events = db.query("SELECT * FROM events WHERE event_type = 'file.created'").await?;
    raw_events.into_iter()
        .filter_map(|e| e.to_typed().ok())
        .collect()
}

// Mixed processing when needed
fn process_mixed(events: Vec<Event>) {  // Default to JsonValue
    for event in events {
        match event.event_type.as_str() {
            "file.created" => {
                if let Ok(typed) = event.to_typed::<FileCreatedPayload>() {
                    process_file(typed);
                }
            }
            // ...
        }
    }
}
```

## Benefits of Unification

1. **No duplication**: Single Event struct
2. **Type safety by default**: Use typed variants for processing
3. **Flexibility when needed**: RawEvent is just `Event<JsonValue>`
4. **Natural conversions**: Type erasure and recovery are explicit
5. **Homogeneous collections work**: `Vec<Event<T>>` for your common case
6. **Progressive enhancement**: Start with raw, add types as needed

## The Key Insight

You're right that homogeneous processing is the common case. By making Event generic with a default type parameter, we get:
- Type safety for the 80% case (processing specific event types)
- Flexibility for the 20% case (storage, transmission, mixed processing)
- No duplicate code or parallel hierarchies

This design acknowledges that events spend most of their "active" time being processed as typed entities, even if they spend most of their "lifetime" serialized as JSON.