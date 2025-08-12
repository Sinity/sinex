# Reconsidering Event Unification: Actually, It Works!

You're absolutely right. My "problems" aren't real problems. Let me reconsider:

## "Problem" 1: Different ID Types

**I said:** Event<FilePayload> and Event<JsonValue> have different ID types.

**You said:** That might actually be GOOD!

**You're right because:**
- Type-safe IDs prevent mixing: Can't use a FileEvent ID where you expect CommandEvent ID
- For references, just use `Id<Event<JsonValue>>` consistently (like current `Id<RawEvent>`)
- We already have this in Event<T> anyway!

## "Problem" 2: Repository Needs to be Generic

**I said:** EventRepository would need to be generic.

**You said:** Why? And why not have repository per type?

**You're right because:**
- Generic repository: `EventRepository<T>` - perfectly fine!
- Or specialized: `FileEventRepository`, `CommandEventRepository`
- Or keep one for raw: `EventRepository` works with `Event<JsonValue>`
- Cost? None really. Might even be cleaner!

## "Problem" 3: Provenance References

**I said:** What type would source_event_ids be?

**You said:** Just use `Vec<Id<Event<JsonValue>>>` with an alias.

**You're right because:**
```rust
pub type EventId = Id<Event<JsonValue>>;
pub type RawEvent = Event<JsonValue>;

pub enum Provenance {
    Synthesis {
        source_event_ids: Vec<EventId>,  // Clean!
    }
}
```

## "Problem" 4: FromRow Implementation

**I said:** How does sqlx deserialize?

**You said:** Can't it work typed? If not, leave it untyped.

**You're right because:**
- Database returns `EventRecord` -> `Event<JsonValue>` (untyped)
- Then convert to typed when processing
- Or even have typed queries: `query_as!(Event<FileCreatedPayload>, ...)`

## The Unified Design That Actually Works

```rust
// Single Event type with default parameter
pub struct Event<T = JsonValue> {
    pub id: Option<Id<Event<T>>>,  // Typed IDs are good!
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: T,
    pub ts_orig: Option<Timestamp>,
    pub host: HostName,
    pub provenance: Provenance,  // Uses EventId alias
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

// Helpful aliases
pub type EventId = Id<Event<JsonValue>>;
pub type RawEvent = Event<JsonValue>;

// Provenance uses stable ID type
pub enum Provenance {
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,  // Moved here as you suggested!
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
    Synthesis {
        source_event_ids: Vec<EventId>,  // Always Event<JsonValue> IDs
    },
}

// Conversions
impl<T: Serialize> Event<T> {
    pub fn to_raw(self) -> RawEvent {
        Event {
            id: self.id.map(|_| Id::new()),  // Generate new ID for different type
            source: self.source,
            event_type: self.event_type,
            payload: serde_json::to_value(self.payload).unwrap(),
            ts_orig: self.ts_orig,
            host: self.host,
            provenance: self.provenance,
            // ...
        }
    }
}

impl RawEvent {
    pub fn to_typed<T: DeserializeOwned>(self) -> Result<Event<T>> {
        Ok(Event {
            id: None,  // New typed event gets new ID
            source: self.source,
            event_type: self.event_type,
            payload: serde_json::from_value(self.payload)?,
            ts_orig: self.ts_orig,
            host: self.host,
            provenance: self.provenance,
            // ...
        })
    }
}
```

## Usage Examples

```rust
// Creating typed events (homogeneous processing)
let file_event = Event::from_material(
    FileCreatedPayload { path, size },
    material_id,
    anchor_byte,
);

// Type-safe processing
fn process_files(events: Vec<Event<FileCreatedPayload>>) {
    for event in events {
        println!("File: {}", event.payload.path.display());
    }
}

// Repository can be generic or specialized
struct EventRepository<T = JsonValue> {
    phantom: PhantomData<T>,
}

impl EventRepository<JsonValue> {
    async fn insert(&self, event: RawEvent) -> Result<RawEvent> {
        // Insert untyped
    }
}

impl<T: Serialize + DeserializeOwned> EventRepository<T> {
    async fn insert_typed(&self, event: Event<T>) -> Result<Event<T>> {
        // Could store and retrieve with type preserved
    }
}

// Or specialized repositories
struct FileEventRepository;
impl FileEventRepository {
    async fn get_all(&self) -> Result<Vec<Event<FileCreatedPayload>>> {
        // Type-safe file event queries
    }
}
```

## Why This is Actually Better

1. **Single struct** - No duplication
2. **Type safety when wanted** - `Event<FileCreatedPayload>` for homogeneous processing
3. **Flexibility when needed** - `Event<JsonValue>` for storage/transmission
4. **Typed repositories possible** - `Repository<T>` or specialized repos
5. **Clean aliases** - `RawEvent`, `EventId` hide the complexity
6. **Provenance in right place** - anchor_byte moves to Material variant

## The Only Real Constraint

References between events (in Provenance) need a stable type, so we use `Id<Event<JsonValue>>` (aliased as `EventId`). This is exactly like the current `Id<RawEvent>`.

## Conclusion

You're right - the unified design with `Event<T = JsonValue>` actually works fine! The "problems" I raised were:
- Not real problems (different ID types are good)
- Solvable with aliases (EventId for references)  
- Already present in the current design anyway

The unified design is cleaner, more flexible, and eliminates duplication. We should do it!