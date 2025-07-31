# Sinex Architecture Redux - Target State Specification

## Core Architecture

### Data Access Layer

**Repository Pattern with Hybrid Approach**
- All database access through repository pattern via `DbPoolExt` trait
- Direct sqlx for static, performance-critical queries
- SeaQuery for dynamic query building
- NO custom query builders or string concatenation

**Type System**
- Strongly-typed ID types (EventId, CheckpointId, etc.) using custom macro
- Domain string types using `Cow<'static, str>` pattern with `from_static` for constants
- All paths use `Utf8Path`/`Utf8PathBuf` from camino
- Unified `Event` struct (no NewEvent/RawEvent split)

**Schema Management**
- Database schema defined in code using SeaQuery
- sea-orm-migration for type-safe migrations
- All tables implement `TableDef` trait
- Schema as single source of truth

### Infrastructure Stack

**Builder Pattern**: All builders use `bon` derive macros
**Retry Logic**: `tokio-retry` for all retry operations
**Validation**: `validator` crate with derive macros
**Configuration**: `figment` for layered configuration
**Error Context**: `serde_path_to_error` + `color-eyre`
**Message Bus**: NATS JetStream (no Redis)

### Testing Infrastructure

**Framework**: `rstest` for fixtures and parameterized tests
**Snapshots**: `insta` for complex output validation
**Tracing**: `tracing-test` for test logging
**Assertions**: `similar-asserts` for better diffs

### Type Safety Guarantees

- No string-based ID mixing possible
- No non-UTF8 path errors
- Compile-time validation for event types and sources
- Type-safe SQL query building

## Domain Model

### Event System
```rust
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct Event {
    #[builder(skip)]
    pub id: Option<Ulid>,  // None = new, Some = persisted
    
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    
    #[builder(skip)]
    pub ts_ingest: DateTime<Utc>,
    
    #[builder(default)]
    pub ts_orig: Option<DateTime<Utc>>,
    
    #[builder(default = HostName::current())]
    pub host: HostName,
    
    // Provenance (XOR: either events or material)
    pub source_event_ids: Option<Vec<Ulid>>,
    pub source_material_id: Option<Ulid>,
    // ... other fields
}
```

### Repository Access
```rust
// Extension trait pattern
pub trait DbPoolExt {
    fn events(&self) -> EventRepository;
    fn checkpoints(&self) -> CheckpointRepository;
    fn source_materials(&self) -> SourceMaterialRepository;
    fn knowledge_graph(&self) -> KnowledgeGraphRepository;
    fn state(&self) -> StateRepository;
}

// Usage
let event = pool.events().get_by_id(event_id).await?;
```

### Constants
```rust
pub mod sources {
    pub const FILESYSTEM: EventSource = EventSource::from_static("fs");
    pub const TERMINAL: EventSource = EventSource::from_static("terminal");
    pub const DESKTOP: EventSource = EventSource::from_static("desktop");
    pub const SYSTEM: EventSource = EventSource::from_static("system");
}

pub mod event_types {
    pub mod filesystem {
        pub const FILE_CREATED: EventType = EventType::from_static("file.created");
        pub const FILE_MODIFIED: EventType = EventType::from_static("file.modified");
        pub const FILE_DELETED: EventType = EventType::from_static("file.deleted");
    }
}
```

## Satellite Architecture

**Communication**: gRPC between satellites and ingestd
**Message Bus**: NATS JetStream for event distribution
**State Management**: `StatefulStreamProcessor` interface
**Service Model**: Independent systemd services

## Development Principles

1. **Replace, Don't Wrap**: Use third-party libraries directly
2. **Type Safety Everywhere**: Compile-time guarantees over runtime checks
3. **Single Source of Truth**: SeaQuery schemas define database structure
4. **Zero String SQL**: All queries through sqlx macros or SeaQuery
5. **Test Reality**: TestContext uses production repositories
6. **Domain Innovation**: Preserve ULID-first design and provenance model

## Performance Optimizations

- `jemalloc` as global allocator
- `ahash` for HashMap performance
- `Arc<String>` in hot paths to avoid cloning
- Batch operations for bulk processing
- Connection pooling with configurable limits

## Observability

- OpenTelemetry for distributed tracing
- Structured logging with tracing
- Metrics collection at repository layer
- Error context with stack traces

## Security

- No SQL string concatenation
- Input validation at domain boundaries
- Type-safe query building
- Audit trail for sensitive operations

## Migration Path

1. Repository pattern replaces custom query builders
2. NATS replaces Redis (direct cutover, no parallel systems)
3. bon replaces manual builders
4. Standard libraries replace custom utilities
5. Delete obsolete modules immediately (no compatibility shims)

## Success Metrics

- 30-50% reduction in custom infrastructure code
- Zero string-based ID errors possible
- All queries type-safe at compile time
- Test execution under 2 minutes
- No performance regression from refactoring