# The Case for Removing Event<T>

## Current State: Three Event Types

We currently have:
1. **Event<T: EventPayload>** - Strongly typed payload, compile-time safety
2. **RawEvent** - JSON payload, runtime flexibility  
3. **EventRecord** - Database row representation

## The Core Problem: Type Safety is an Illusion

### Where Type Safety Exists
```rust
// Only here, at creation:
let payload = FileCreatedPayload { path: "/foo", size: 100 };
let event = Event::<FileCreatedPayload>::new(payload);
```

### Where Type Safety is Lost (Everywhere Else)
```rust
// 1. Immediately lost when sending to ingestd
let proto_event = event.to_proto();  // -> Serialized to JSON

// 2. Lost in database
INSERT INTO events (payload) VALUES ($1::jsonb)  // -> JSONB column

// 3. Lost over network
grpc_client.ingest_event(proto_event);  // -> Protobuf with JSON string

// 4. Lost when reading back
let record = query!("SELECT * FROM events");
let event = record.to_raw_event();  // -> RawEvent with JsonValue

// 5. Lost in NATS
publisher.publish(serde_json::to_string(&event));  // -> JSON string
```

## The Actual Lifecycle of an Event

```
Event<T> (0.01% of lifetime)
    ↓ (immediate conversion)
RawEvent (0.1% of lifetime)  
    ↓ (serialization)
JSON/Protobuf (99.89% of lifetime in DB, network, queues)
    ↓ (deserialization)
RawEvent (for processing)
    ↓ (maybe conversion if you know the type)
Event<T> (rarely - only if processor knows exact type)
```

## Why Event<T> Doesn't Help

### 1. You Almost Never Know the Type at Compile Time

```rust
// This is what we want to write:
async fn process_events(pool: &PgPool) {
    let events = get_recent_events(pool).await?;
    for event in events {
        match event {
            Event::<FileCreatedPayload>(e) => handle_file_created(e.payload),
            Event::<CommandExecutedPayload>(e) => handle_command(e.payload),
            // ... 50+ more event types
        }
    }
}

// But this is impossible! Events come from DB as RawEvent
// The actual code:
async fn process_events(pool: &PgPool) {
    let events = get_recent_events(pool).await?;
    for event in events {
        match event.event_type.as_str() {
            "file.created" => {
                // We're doing runtime type checking anyway!
                let payload: FileCreatedPayload = serde_json::from_value(event.payload)?;
                handle_file_created(payload);
            }
            "command.executed" => {
                let payload: CommandExecutedPayload = serde_json::from_value(event.payload)?;
                handle_command(payload);
            }
            // ...
        }
    }
}
```

### 2. Heterogeneous Processing is the Norm

```rust
// Automata process multiple event types:
impl StatefulStreamProcessor for TerminalCanonicalizer {
    async fn process_event(&mut self, event: RawEvent) -> Result<Vec<RawEvent>> {
        // Has to handle terminal.command, terminal.output, session.started, etc.
        // Event<T> doesn't help here - we need runtime dispatch
    }
}
```

### 3. The Conversion Overhead is Wasteful

```rust
// Current flow with Event<T>:
let payload = FileCreatedPayload { ... };
let event = Event::new(payload);              // Allocation 1
let raw = event.into();                       // Serialize to JSON (Allocation 2)
let proto = raw.to_proto();                   // Re-serialize (Allocation 3)
// Send to ingestd...
// In database: deserialize proto -> RawEvent -> serialize to JSONB
// Reading back: deserialize JSONB -> RawEvent
// If you want typed: deserialize JSON -> FileCreatedPayload (Allocation 4)

// Simpler flow without Event<T>:
let payload = FileCreatedPayload { ... };
let event = RawEvent::from_payload(payload);  // Direct to JSON (Allocation 1)
let proto = event.to_proto();                 // Serialize once (Allocation 2)
// Send to ingestd...
// Reading back: deserialize JSONB -> RawEvent
// If you want typed: deserialize JSON -> FileCreatedPayload (Allocation 3)
```

### 4. It Duplicates Everything

```rust
// Current: Two parallel structures
pub struct Event<T> {
    pub id: Option<Id<Event<T>>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: T,  // Only difference
    pub ts_orig: OptionalTimestamp,
    pub host: HostName,
    pub provenance: Option<Provenance>,
    pub anchor_byte: Option<i64>,
    // ... more fields
}

pub struct RawEvent {
    pub id: Option<Id<RawEvent>>,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,  // Only difference
    pub ts_orig: OptionalTimestamp,
    pub host: HostName,
    pub provenance: Option<Provenance>,
    pub anchor_byte: Option<i64>,
    // ... more fields
}

// Every method has to be implemented twice
// Every change has to be made in two places
// Conversion logic has to be maintained
```

## What We Actually Need

### Pattern 1: Typed Creation Helpers
```rust
impl RawEvent {
    pub fn file_created(
        path: PathBuf,
        size: u64,
        material: &MaterialRef,
    ) -> Self {
        Self::from_material(
            "fs-watcher",
            "file.created",
            json!({ "path": path, "size": size }),
            Utc::now(),
            material.id,
            material.offset,
        )
    }
}
```

### Pattern 2: Typed Extraction
```rust
impl RawEvent {
    pub fn as_file_created(&self) -> Result<FileCreatedPayload> {
        if self.event_type != "file.created" {
            return Err(WrongEventType);
        }
        serde_json::from_value(self.payload.clone())
    }
}
```

### Pattern 3: Payload Traits (If You Want Types)
```rust
trait EventPayload: Serialize + DeserializeOwned {
    const SOURCE: &'static str;
    const EVENT_TYPE: &'static str;
}

impl RawEvent {
    pub fn from_payload<T: EventPayload>(
        payload: T,
        provenance: Provenance,
    ) -> Self {
        Self {
            source: T::SOURCE.into(),
            event_type: T::EVENT_TYPE.into(),
            payload: serde_json::to_value(payload).unwrap(),
            // ...
        }
    }
    
    pub fn extract_payload<T: EventPayload>(&self) -> Result<T> {
        if self.event_type.as_str() != T::EVENT_TYPE {
            return Err(WrongType);
        }
        serde_json::from_value(self.payload.clone())
    }
}
```

## Real-World Evidence

### Look at Our Actual Processors

```rust
// Do any of them use Event<T>? Let's check...

// terminal_canonicalizer.rs
async fn process_event(&mut self, event: RawEvent) -> Result<Vec<RawEvent>>

// health_aggregator.rs  
async fn process_event(&mut self, event: RawEvent) -> Result<Vec<RawEvent>>

// fs_watcher.rs
async fn process_event(&mut self, event: RawEvent) -> Result<Vec<RawEvent>>

// ALL processors use RawEvent!
```

### Database Queries Never Return Event<T>

```rust
// What we can't do:
let typed_events: Vec<Event<FileCreatedPayload>> = 
    sqlx::query_as!(Event<FileCreatedPayload>, "SELECT * FROM events WHERE event_type = 'file.created'")
    .fetch_all(pool).await?;
// This is impossible - sqlx doesn't know about Event<T>

// What we actually do:
let events: Vec<EventRecord> = 
    sqlx::query_as!(EventRecord, "SELECT * FROM events WHERE event_type = 'file.created'")
    .fetch_all(pool).await?;
let raw_events: Vec<RawEvent> = events.into_iter().map(|r| r.to_raw_event()).collect();
// Then maybe convert to typed if needed
```

## The Verdict: Event<T> Should Go

### Why Remove It

1. **False promise**: Suggests type safety that doesn't exist in practice
2. **Complexity burden**: Two parallel types, conversion overhead, maintenance cost
3. **Not used**: Our actual code uses RawEvent everywhere
4. **Performance cost**: Extra allocations and conversions
5. **Conceptual confusion**: Makes people think events are typed when they're really JSON

### What We Gain by Removing It

1. **Simplicity**: One event type, clear mental model
2. **Honesty**: Events are JSON documents with metadata - that's the reality
3. **Performance**: Fewer conversions, less allocation
4. **Maintainability**: One structure to update, one set of methods
5. **Flexibility**: Typed helpers where useful, JSON everywhere else

### Migration is Trivial

```rust
// Before:
let event = Event::new(FileCreatedPayload { path, size })
    .with_provenance(provenance);

// After:
let event = RawEvent::from_payload(
    FileCreatedPayload { path, size },
    provenance
);

// Or even simpler:
let event = RawEvent::file_created(path, size, material_ref);
```

## Conclusion

Event<T> is a well-intentioned abstraction that doesn't match reality. Events in Sinex are JSON documents with provenance - they're stored as JSON, transmitted as JSON, and processed as JSON. The brief moment of type safety at creation doesn't justify the complexity of maintaining a parallel type system.

Remove Event<T>. Use RawEvent everywhere. Add typed helpers where they provide value. This matches how the system actually works and removes unnecessary complexity.