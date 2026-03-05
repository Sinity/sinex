# Type-Safe Units & Identifiers

Sinex utilizes a rigorous system of type-safe units and identifiers to eliminate semantic errors and ensure mathematical correctness across the codebase.

## Unit Newtypes

To prevent the accidental mixing of different physical or logical quantities (e.g., adding bytes to seconds), the system wraps primitive numeric types in domain-specific newtypes.

### Time & Duration
- **Seconds**, **Milliseconds**, **Microseconds**, **Nanoseconds**: Prevent unit confusion in timeouts and intervals.
- **Conversion Safety**: Methods like `as_duration()` provide safe boundaries for interacting with standard Rust library types (`std::time::Duration`).

### Data & Storage
- **Bytes**, **Kilobytes**, **Megabytes**, **Gigabytes**: Ensure consistency in memory limits, file sizes, and storage calculations.
- **Validation**: Types like `Bytes` often include a `MAX` constant and a `validate()` method to enforce system limits (e.g., maximum event payload size).

### System & Counting
- **ExitCode**: Encodes POSIX exit code semantics, including success checks and signal number extraction.
- **ProcessId**, **UnixUid**, **UnixGid**: Prevent accidental mixing of PIDs, UIDs, and GIDs.
- **EventCount**, **LineCount**: Provide saturating arithmetic (`saturating_add`) to prevent overflow during aggregation.

## Type-Safe Identifiers (`Id<T>`)

All system identifiers use the `Id<T>` wrapper, which is a phantom-typed UUIDv7 (Universally Unique Lexicographically Sortable Identifier).

### Benefits of UUIDv7 IDs
- **Ordering**: UUIDv7 IDs are roughly chronologically ordered, which optimizes database index performance and simplifies time-range queries.
- **Collision Resistance**: Provides the uniqueness guarantees of UUIDs while remaining sortable.

### Phantom Typing (`T`)
The `Id<T>` wrapper is parameterized by a marker type `T`, ensuring that IDs from different domains are not interchangeable at compile-time.

```rust
let event_id: Id<Event> = ...;
let material_id: Id<SourceMaterial> = ...;

// This will fail to compile:
process_material(event_id);
```

### Common Identifiers
- `Id<Event>`: Primary identifier for all events.
- `Id<SourceMaterial>`: Used for provenance and raw data registry.
- `Id<Blob>`: Identifies binary objects in git-annex.
- `Id<Entity>`: Identifies nodes in the Knowledge Graph.

## Serialization & Interop

- **Transparent Serialization**: Units and IDs use `#[serde(transparent)]` or custom implementations to ensure they serialize to raw primitives (strings or numbers) over the wire, matching NATS and PostgreSQL formats.
- **SQLx Integration**: The `Id<T>` type is natively integrated with `sqlx`, allowing it to be stored as a standard PostgreSQL `UUID` while retaining its typed nature in the Rust application layer.
