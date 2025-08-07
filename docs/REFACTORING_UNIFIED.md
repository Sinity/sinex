# Sinex Types Enhancement Research & Implementation Notes

## Overview

This document captures research findings and implementation decisions for enhancing the `sinex-types` crate based on the architectural discussions in `docs/refac.md`. The enhancements follow the "Parse, don't validate" principle to create a more type-safe, maintainable system.

## Completed Enhancements

### 1. Domain Newtypes (✅ Implemented)

**New Types Added:**
- **Path Types:** `SanitizedPath`, `RelativePath`, `AbsoluteUri`
- **Hash Types:** `Blake3Hash`, `Sha256Hash` (with validation)
- **Semantic Identifiers:** `ServiceName`, `JobId`, `AnnexKey`, `NatsSubject`

**Key Features:**
- Validation at construction time via `FromStr`
- Invalid states unrepresentable
- Full SQLx integration
- Comprehensive test coverage

### 2. EventEnvelope Enum (✅ Implemented)

**Achievement:**
- Type-safe enum with 97+ event payload variants
- Exhaustive pattern matching replaces string comparisons
- `Event::to_envelope()` method for seamless conversion
- Forward compatibility via `Unknown` variant

**Impact:**
- Eliminates runtime deserialization errors
- Compiler-enforced complete event handling
- Significant improvement in automaton code clarity

## Research Findings

### PayloadExt Trait Pattern

**Context:** Standardizing builder patterns and test constructors across payload types.

**Proposed Design:**
```rust
trait PayloadBuilder: Sized {
    type Builder;
    fn builder() -> Self::Builder;
}

trait TestablePayload: Sized {
    fn test_default() -> Self;
    fn with_test_data() -> Self;
}

trait PayloadExt: EventPayload + PayloadBuilder + TestablePayload {
    fn validate(&self) -> Result<(), ValidationError>;
    fn sanitize(&mut self);
}
```

**Benefits:**
- Consistent API across all payload types
- Generic test helpers
- Better IDE discoverability

**Recommendation:** Defer implementation until after core enhancements. Would require significant refactoring of existing payloads.

### EventPayload Provenance Refinements

**Context:** Adding associated types to EventPayload for entity relationship declaration.

**Proposed Enhancement:**
```rust
trait EventPayload {
    type PrimaryEntity: Entity;
    fn primary_entity_id(&self) -> Option<String>;
    type SecondaryEntities: EntityList = ();
}
```

**Benefits:**
- Automated knowledge graph extraction
- Type-safe entity relationships
- Declarative provenance tracking

**Challenges:**
- Requires comprehensive entity type hierarchy
- Complex for multi-entity events
- May over-constrain processing flexibility

**Recommendation:** Consider for v2.0 after establishing entity model foundation.

### State Machines with Enums

**Pattern Example:**
```rust
enum MaterialState {
    InFlight { started_at: DateTime<Utc> },
    Finalized { blob_id: Ulid, hash: Blake3Hash },
    Archived { archive_path: SanitizedPath },
}
```

**Benefits:**
- Invalid states unrepresentable
- Type-safe state transitions
- Exhaustive pattern matching

**Application Areas:**
- Job processing lifecycle
- Connection management
- Event processing stages

**Recommendation:** Adopt incrementally as we refactor boolean-based state tracking.

### Generic Payloads with Marker Traits

**Concept:**
```rust
trait HistoricalDataSource {}
trait RealtimeDataSource {}

fn process_historical<P>(processor: P) 
where P: StatefulStreamProcessor + HistoricalDataSource 
```

**Benefits:**
- Compile-time capability checking
- Generic algorithms
- Explicit contracts

**Recommendation:** Excellent for new components. Consider gradual migration.

## Configuration Parsing Improvements

### Current State
- `ProcessorCli` accepts opaque `Option<String>` config
- Multiple parsing stages with potential failures
- Generic `HashMap<String, Value>` intermediates

### Proposed Enhancement
```rust
trait StatefulStreamProcessor {
    type Config: for<'de> Deserialize<'de> + Default;
}

// Parse once at boundary
let config: T::Config = serde_json::from_str(&config_str)?;
processor.initialize(context, config).await?;
```

### Benefits
- Single parsing point at system boundary
- Type-safe configuration objects
- Compile-time validation of config structure

## Tracing Integration Alternative to #[with_context]

### Current: Custom #[with_context] Macro
```rust
#[with_context(operation = "file_read")]
fn read_file() -> Result<String, SinexError> {
    // Automatic error enrichment
}
```

### Alternative: tracing::instrument
```rust
#[tracing::instrument(name = "file_read", skip(self))]
fn read_file(&self) -> Result<String> {
    // Span-based context
}
```

### Comparison

**#[with_context] Advantages:**
- Zero overhead on success path
- Structured key-value context
- Automatic function/module capture

**tracing::instrument Advantages:**
- Industry standard
- Richer context (full trace)
- Unified with logging

**Recommendation:** Implement both for comparison. Let performance benchmarks guide final decision.

## Implementation Priority

1. **High Priority (Immediate):**
   - ✅ Domain newtypes
   - ✅ EventEnvelope enum
   - Configuration parsing improvements
   - Tracing integration comparison

2. **Medium Priority (Next Sprint):**
   - State machine patterns for existing types
   - Basic marker traits for capabilities

3. **Low Priority (Future):**
   - PayloadExt trait system
   - EventPayload provenance refinements
   - Full generic payload system

## Testing Strategy

- Unit tests for all new types
- Property-based testing for validation logic
- Integration tests for Event ↔ EventEnvelope conversion
- Benchmark comparisons for #[with_context] vs tracing

## Migration Path

1. New code uses enhanced types immediately
2. Gradual refactoring of existing code
3. Deprecation warnings for old patterns
4. Complete migration in major version bump

## Success Metrics

- Reduction in runtime parsing errors
- Decrease in type-confusion bugs
- Improved automaton code clarity
- Faster development of new event processors
- Better compiler-assisted refactoring