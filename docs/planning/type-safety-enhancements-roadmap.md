# Roadmap: Type-Safety Enhancements

This document outlines a roadmap for future refactoring work focused on improving type safety and robustness, primarily within the `sinex-types` and `sinex-core` crates. The proposals are based on the "Parse, don't validate" principle and are currently unimplemented or partially implemented.

## 1. Type-Safe Configuration Parsing

*   **Status:** Not Implemented
*   **Goal:** Replace opaque, string-based configuration for stream processors with strongly-typed, per-processor configuration structs, parsed once at the system boundary.

### Proposed Enhancement

```rust
trait StatefulStreamProcessor {
    // Add a dedicated, deserializable config type to each processor.
    type Config: for<'de> Deserialize<'de> + Default;
}

// Example of initialization at the system boundary:
let config: T::Config = serde_json::from_str(&config_str)?;
processor.initialize(context, config).await?;
```

### Benefits
-   **Robustness:** Eliminates runtime parsing errors within processor logic.
-   **Clarity:** Configuration structure is explicit and validated at compile time.
-   **Maintainability:** Simplifies processor initialization and removes boilerplate.

## 2. State Machine Patterns with Enums

*   **Status:** Partially Implemented
*   **Goal:** Incrementally refactor logic that relies on boolean flags or simple status enums to use enums with associated state data, making invalid states unrepresentable in the type system.

### Pattern Example

```rust
// Instead of multiple booleans like `is_finalized`, `is_archived`.
enum MaterialState {
    InFlight { started_at: DateTime<Utc> },
    Finalized { blob_id: Ulid, hash: Blake3Hash },
    Archived { archive_path: SanitizedPath },
}
```

### Benefits
-   **Correctness:** Invalid state combinations cannot be created.
-   **Clarity:** The type system enforces valid state transitions.
-   **Exhaustiveness:** The compiler forces all possible states to be handled.

## 3. Generic Payloads with Marker Traits

*   **Status:** Not Implemented
*   **Goal:** Enable the creation of generic, reusable algorithms that can operate on abstract categories of events or data sources, verified at compile time.

### Concept

```rust
// Define marker traits to represent capabilities.
trait HistoricalDataSource {}
trait RealtimeDataSource {}

// Generic function that requires a specific capability.
fn process_historical<P>(processor: P)
where P: StatefulStreamProcessor + HistoricalDataSource
```

### Benefits
-   **Genericity:** Reduces code duplication by allowing for more abstract processors.
-   **Compile-Time Guarantees:** Ensures that processors are only used with compatible data sources.
-   **Explicit Contracts:** Makes the capabilities and requirements of components clear.

## 4. Advanced Event Payload Trait System

*   **Status:** Not Implemented
*   **Goal:** Evolve the `EventPayload` trait to standardize common patterns like builders, validation, and the declaration of entity relationships.

### 4.1. `PayloadExt` Trait for Builders and Validation

*   **Concept:** A unified extension trait for all payloads.
    ```rust
    trait PayloadExt: EventPayload + PayloadBuilder + TestablePayload {
        fn validate(&self) -> Result<(), ValidationError>;
        fn sanitize(&mut self);
    }
    ```
*   **Benefits:** Provides a consistent API for creating, testing, and validating all payload types.

### 4.2. `EventPayload` Provenance for Entity Relationships

*   **Concept:** Add associated types to `EventPayload` to declare relationships between events and data entities.
    ```rust
    trait EventPayload {
        type PrimaryEntity: Entity;
        fn primary_entity_id(&self) -> Option<String>;
        type SecondaryEntities: EntityList = ();
    }
    ```
*   **Benefits:** Enables powerful features like automated knowledge graph extraction and type-safe provenance tracking.
