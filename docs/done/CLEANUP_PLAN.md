# Cleanup Plan: Streamlining Schema Evolution

Based on the implemented features and identified redundancies, here's the plan to simplify while keeping the seamless experience.

## Phase 1: Remove Redundant Components

### 1.1 Remove Phantom Type Complexity

- Delete `Version<MAJOR, MINOR, PATCH>` phantom type
- Delete `TypedVersioned<T, V>` wrapper
- Delete `VersionedType<T, MAJOR, MINOR, PATCH>` struct
- Delete `VersionCompatible` trait
- Keep only the `VERSION: &'static str` const

### 1.2 Remove DynEventPayload

- Delete `DynEventPayload` trait
- Delete `DynPayloadBox` struct
- These are redundant with the type-safe approach

### 1.3 Simplify Event Deserialization

Instead of `fetch_typed`, add methods to Event itself:

```rust
impl Event {
    /// Extract the payload with automatic version migration
    pub fn payload<T: EventPayload + DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        // If we have schema info, use version-aware deserialization
        if let Some(schema_id) = self.payload_schema_id {
            // Get version from schema registry (cached)
            let version = get_schema_version(schema_id)?;
            T::try_from_legacy(self.payload.clone(), &version)
        } else {
            // Direct deserialization for unversioned events
            serde_json::from_value(self.payload.clone())
        }
    }
    
    /// Try to extract payload as a specific type
    pub fn try_payload<T: EventPayload + DeserializeOwned>(&self) -> Option<T> {
        self.payload().ok()
    }
}
```

Then queries become more natural:

```rust
// Instead of fetch_typed
let events = repo.search(filters).await?;
for event in events {
    let payload: FileCreatedV3 = event.payload()?; // Automatic migration!
    // use payload...
}

// Or with filtering
let file_events: Vec<FileCreatedV3> = repo
    .search(filters)
    .await?
    .into_iter()
    .filter_map(|e| e.try_payload())
    .collect();
```

Note: Event creation already exists via `Event::from(payload)` - no need to add a new method!

### 1.4 Update EventRepository

Remove `fetch_typed` and `fetch_payloads` methods - they're no longer needed.

## Phase 2: Clarify Blanket Implementations

Add documentation to clarify that blanket impls are for deserialization support:

```rust
/// Blanket implementations for common wrapper types.
/// 
/// These implementations allow EventPayload types to be wrapped in
/// standard containers while maintaining version migration support.
/// 
/// IMPORTANT: These do NOT create new event types. They inherit the
/// source/event_type/version from the inner type and are used only
/// during deserialization to handle structural variations.
/// 
/// Example: An Option<FileCreated> is used when deserializing events
/// that might have missing payloads, not to create a new event type.
```

## Summary of Changes

### Keep (Essential for Seamlessness)

1. ✅ `try_from_legacy` in EventPayload trait
2. ✅ Blanket implementations for Option, Vec, etc.
3. ✅ `evolves_from` macro attribute
4. ✅ Multi-version schema cache
5. ✅ `VersionEvolution` trait
6. ✅ Compile-time version validation in macro

### Remove (Redundant)

1. ❌ Phantom type versions (Version<>, TypedVersioned<>, etc.)
2. ❌ DynEventPayload and DynPayloadBox
3. ❌ fetch_typed and fetch_payloads methods
4. ❌ Complex const functions for version compatibility

### Add (Better Integration)

1. ✅ `Event::payload<T>()` method for type-safe extraction with version migration
2. ✅ Clear documentation on blanket impl purpose

This approach:

- Keeps all the seamless version migration
- Removes redundant abstractions
- Makes Event the central type for payload handling
- Feels more natural and integrated with Rust patterns

## Additional Simplifications Discovered

### 1. Use Standard `From` Trait Instead of `VersionEvolution`

```rust
// Instead of custom VersionEvolution trait:
impl From<FileCreatedV2> for FileCreatedV3 {
    fn from(prev: FileCreatedV2) -> Self {
        Self {
            id: prev.id.parse().unwrap_or(0),
            path: prev.path,
        }
    }
}

// try_from_legacy can use it:
fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError> {
    match version {
        "2.0.0" => {
            let v2: FileCreatedV2 = serde_json::from_value(value)
                .map_err(|e| SinexError::serialization(e.to_string()))?;
            Ok(v2.into()) // Uses standard From!
        }
        _ => serde_json::from_value(value)
            .map_err(|e| SinexError::serialization(e.to_string()))
    }
}
```

### 2. Remove `EventPayloadCollection` Trait

The blanket implementations already make iterators work:

```rust
// This already works without a special trait:
let payloads: Result<Vec<FileCreated>, _> = values
    .into_iter()
    .map(|v| FileCreated::try_from_legacy(v, version))
    .collect();
```

### 3. Simplify `wrapped_payload!` Macro

Instead of creating new event types, use type aliases:

```rust
// For semantic clarity without breaking the event model:
type OptionalFileCreated = Option<FileCreated>;
type FileCreatedBatch = Vec<FileCreated>;
```

### 4. Use SinexError Throughout

Change `try_from_legacy` to return `Result<Self, SinexError>`:

```rust
fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError> {
    // Now errors are properly typed
    serde_json::from_value(value)
        .map_err(|e| SinexError::serialization(format!("Failed to deserialize {}: {}", version, e)))
}
```

## Final Architecture

The simplified system would have:

1. **EventPayload trait** with VERSION const and try_from_legacy method
2. **Blanket implementations** for common wrappers (Option, Vec, etc.)
3. **Standard From trait** for version migrations
4. **Event methods** for payload extraction and construction
5. **Multi-version schema support** in the validator
6. **CI validation** for schema compatibility

This is more idiomatic Rust while keeping all the "magic" that makes version evolution seamless.

## Additional Streamlining Opportunities Discovered

### 1. Schema Cache Usage in Event::payload<T>()

When implementing the new `Event::payload<T>()` method, it should use the validator's schema cache instead of querying the database for version information. This eliminates redundant database queries.

### 2. Consolidate Provenance Fields (Recommended)

The Event struct currently has separate fields for provenance that could be consolidated:

```rust
// Current: Multiple fields
source_event_ids: Option<Vec<Ulid>>,
source_material_id: Option<Ulid>,
source_material_offset_start: Option<i64>,
source_material_offset_end: Option<i64>,

// Better: Single field using existing Provenance enum
provenance: Provenance,  // Not Optional - either Events or Material
```

This would:

- Automatically enforce the XOR constraint (either event IDs or material, not both)
- Simplify the Event struct
- Make the `with_provenance()` method more natural

Note: This requires careful handling of sqlx FromRow/ToRow implementations to map database fields to/from the Provenance enum. It won't really work at all without migrating to sea-query-migration based migrations, and generally sea-query defined schema. But we will be making such transition, so. Just don't attempt to implement this point if it's not yet done, it is deferred.

### 3. Remove Unused EventPayloadCollection

The trait is defined but never used - confirms it should be removed.

### 4. Consistent Error Handling

Replace error wrapping patterns like:

```rust
.map_err(|e| db_error(
    sqlx::Error::Protocol(format!("Failed to deserialize payload: {}", e)),
    "deserialize payload"
))?
```

With direct SinexError usage:

```rust
.map_err(|e| SinexError::serialization(format!("Failed to deserialize payload: {}", e)))?
```

### 5. Leverage Schema Content Hash

The validator loads `content_hash` but doesn't use it. Use it to detect schema changes and invalidate cache.

### 6. Clarify impl_version_evolution! Macro Usage

The macro exists and works well:

```rust
impl_version_evolution!(FileCreatedV2, FileCreatedV1, |old| {
    FileCreatedV2 {
        path: old.path,
        size: old.size,
        // new fields...
    }
});
```

This generates both `From<FileCreatedV1> for FileCreatedV2` and the VersionEvolution impl. Since we're removing VersionEvolution trait in favor of just From, this macro becomes not useful and should be removed.

