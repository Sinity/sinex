# Why Event<T = JsonValue> Might Not Work

## The ID Type Parameter Problem

This is likely the killer:

```rust
pub struct Event<T = JsonValue> {
    pub id: Option<Id<Event<???>>>,  // What goes here?
    // ...
}
```

### Problem 1: Self-Referential Type Parameter

```rust
// This doesn't work:
pub struct Event<T = JsonValue> {
    pub id: Option<Id<Event<T>>>,  // Now Event<FilePayload> has different ID type than Event<JsonValue>
    // ...
}

// This means:
let file_event: Event<FileCreatedPayload> = ...;
let raw_event: Event<JsonValue> = ...;

// These have DIFFERENT ID types!
file_event.id  // Option<Id<Event<FileCreatedPayload>>>
raw_event.id   // Option<Id<Event<JsonValue>>>

// So you can't do:
if file_event.id == raw_event.id {  // Type error!
```

### Problem 2: Provenance Breaks

```rust
pub enum Provenance {
    Material { ... },
    Synthesis {
        source_event_ids: Vec<Id<Event<???>>>,  // What type here?
    },
}

// If we use Vec<Id<Event<JsonValue>>>:
let typed_event: Event<FileCreatedPayload> = ...;
let parent_id = typed_event.id;  // Id<Event<FileCreatedPayload>>

// Can't put it in provenance:
let child = Event {
    provenance: Provenance::Synthesis {
        source_event_ids: vec![parent_id],  // Type error! Expected Id<Event<JsonValue>>
    },
    // ...
};
```

### Problem 3: Database Queries Return Wrong Type

```rust
// Database always returns Event<JsonValue> (it doesn't know types)
let events: Vec<Event<JsonValue>> = db.query("SELECT * FROM events").await?;

// To get typed, you'd convert:
let typed: Event<FileCreatedPayload> = events[0].to_typed()?;

// But now typed.id has different type than events[0].id!
// They're the same event but with incompatible ID types
```

## The Repository Problem

```rust
impl EventRepository {
    // Which signature?
    
    // Option 1: Generic repository
    async fn insert<T>(&self, event: Event<T>) -> Result<Event<T>> {
        // But DB returns Event<JsonValue>, not Event<T>
        // Would need to serialize then deserialize - wasteful
    }
    
    // Option 2: Always use JsonValue
    async fn insert(&self, event: Event<JsonValue>) -> Result<Event<JsonValue>> {
        // Forces conversion before every insert
        // Loses type information
    }
    
    // Option 3: Duplicate methods (ugh)
    async fn insert_raw(&self, event: Event<JsonValue>) -> Result<Event<JsonValue>>
    async fn insert_typed<T>(&self, event: Event<T>) -> Result<Event<JsonValue>>
}
```

## The Type Coercion Problem

```rust
// Can't treat Event<T> as Event<JsonValue> even though T can serialize to JSON
fn process_any_event(event: Event<JsonValue>) { ... }

let typed = Event::<FileCreatedPayload>::new(...);
process_any_event(typed);  // ERROR: expected Event<JsonValue>, found Event<FileCreatedPayload>

// Would need explicit conversion every time:
process_any_event(typed.to_raw());  // Works but annoying
```

## The Collection Problem

```rust
// Can't mix types in collections
let mut events: Vec<Event<???>> = vec![];

let file_event = Event::<FileCreatedPayload>::new(...);
let command_event = Event::<CommandPayload>::new(...);

events.push(file_event);     // What type is events?
events.push(command_event);  // Can't push different T

// Would need:
let mut events: Vec<Event<JsonValue>> = vec![];
events.push(file_event.to_raw());
events.push(command_event.to_raw());
// But then lost type info
```

## Why Separate Types Actually Make Sense

The current design with separate `RawEvent` and `Event<T>` actually solves these problems:

```rust
// Clear, consistent ID type
pub struct RawEvent {
    pub id: Option<Id<RawEvent>>,  // Always the same type
    // ...
}

pub struct Event<T> {
    pub id: Option<Id<Event<T>>>,  // Or could use Id<RawEvent> 
    // ...
}

// Provenance is clear
pub enum Provenance {
    Synthesis {
        source_event_ids: Vec<Id<RawEvent>>,  // Always RawEvent IDs
    },
}

// Clear conversion boundary
impl<T> From<Event<T>> for RawEvent { ... }  // Explicit type erasure
impl<T> TryFrom<RawEvent> for Event<T> { ... }  // Explicit type recovery
```

## The Real Problem: Reference Identity

Events need stable identities that persist across type conversions. If the ID type changes with the payload type, you lose referential integrity.

Consider:
1. Event created as `Event<FileCreatedPayload>` with `Id<Event<FileCreatedPayload>>`
2. Stored in DB (converted to JSON)
3. Retrieved as `Event<JsonValue>` with `Id<Event<JsonValue>>`
4. Another event references it via `source_event_ids`
5. Which ID type do we use in the reference?

## Possible Solutions (All Problematic)

### Solution 1: Type-Erased IDs
```rust
pub struct EventId(Ulid);  // Not parameterized

pub struct Event<T> {
    pub id: Option<EventId>,  // Same for all T
    // ...
}
```
But loses type safety - can accidentally use wrong IDs.

### Solution 2: Phantom Type IDs
```rust
pub struct Event<T> {
    pub id: Option<Id<Event<()>>>,  // Always unit type
    // ...
}
```
Weird and confusing.

### Solution 3: Associated Type
```rust
trait EventLike {
    type Payload;
    type IdType;
}
```
Complex and doesn't solve the conversion problem.

## Conclusion

The unified `Event<T = JsonValue>` design breaks down because:

1. **ID types become inconsistent** across payload types
2. **Provenance can't reference events** with different payload types  
3. **Database round-trips break type identity**
4. **Collections can't be heterogeneous**

The current separation of `RawEvent` (for storage/transmission/references) and `Event<T>` (for type-safe construction/processing) actually makes sense. It clearly separates:

- **Identity domain**: RawEvent with stable IDs
- **Type safety domain**: Event<T> for construction and processing
- **Explicit conversion boundary**: Makes type erasure visible

This is probably why you didn't go with the unified design originally!