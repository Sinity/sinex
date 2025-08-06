# Sinex Architecture Refactoring - Remaining Work

**Last Updated**: 2025-01-04
**Status**: Core Refactoring Complete ✅  
**Completed Work**: See [REFACTORING_STATUS.md](./REFACTORING_STATUS.md)

## 🎉 All Refactoring Tasks Completed!

**Note**: The Sinex refactoring is now COMPLETE. All planned tasks have been successfully implemented.

### ✅ Completed Tasks (2025-01-05)

**Medium Priority - DONE:**
1. ~~**ahash Expansion**~~ ✅ COMPLETED - 831 HashMap/HashSet migrated across 134+ files
2. ~~**Event Payload Builders**~~ ✅ COMPLETED - All payload types have test convenience methods
3. ~~**Test Migration**~~ ✅ COMPLETED - Key test files migrated to rstest/insta with parameterization

**Low Priority - DONE:**
1. ~~**bon::Builder Expansion**~~ ✅ COMPLETED - Config structs converted, ~100 lines removed
2. ~~**color-eyre Integration**~~ ✅ COMPLETED - All 15 binaries + 107 files migrated from anyhow
3. ~~**Complete SeaQuery Migration**~~ ✅ COMPLETED - Dynamic SQL replaced with type-safe builders
4. ~~**Complete camino Migration**~~ ✅ COMPLETED - All files migrated to Utf8Path/Utf8PathBuf
5. ~~**Remove Redis Dependencies**~~ ✅ COMPLETED - All 4 automata use NATS, Redis fully removed

## Executive Summary

The Sinex refactoring replaces custom infrastructure with battle-tested Rust libraries while preserving domain innovations: ULID-first design, provenance tracking, and event sourcing.

**Key Changes:**

- Generic `Id<T>` replacing EventId, CheckpointId, etc.
- Repository pattern replacing custom query builders
- Crate consolidation: ~40 → ~30 crates
- NATS JetStream replacing Redis
- Modern test stack: rstest, insta, tracing-test

## Core Principles & Philosophy

### 1. Replace, Don't Wrap

Direct usage of third-party libraries without abstraction layers. Custom wrappers add complexity without value.

### 2. Type Safety Everywhere

Compile-time guarantees eliminate entire classes of runtime errors. Every string has a type, every ID is generic, every path is UTF-8 safe.

### 3. Errors are Features

Breaking changes during refactoring are opportunities to improve. No backwards compatibility shims or migration periods.

### 4. Locality

Code lives where it's used. Macros in their logical crates, types near their consumers, no artificial separation.

### 5. Simplification Through Standards

One way to do things. Standard libraries over custom implementations. Community patterns over novel approaches.

### 6. Aggressive Renaming

Default to shorter names. `id` not `event_id` unless ambiguous. Have a reason NOT to rename.

### 7. Domain Innovation Preservation

Keep what makes Sinex unique: ULID-first design, provenance tracking, event sourcing, personal data OS vision.

## Target Architecture

### Data Access Layer

#### Repository Pattern

```rust
pub trait DbPoolExt {
    fn events(&self) -> EventRepository;
    fn checkpoints(&self) -> CheckpointRepository;
    fn source_materials(&self) -> SourceMaterialRepository;
    fn knowledge_graph(&self) -> KnowledgeGraphRepository;
    fn state(&self) -> StateRepository;
}

// Usage: Ergonomic, discoverable, type-safe
let event = pool.events().get_by_id(id).await?;
```

#### Hybrid Query Approach

- **Static queries**: Direct sqlx macros for compile-time verification
- **Dynamic queries**: SeaQuery for runtime construction
- **Security**: Zero string concatenation, all queries parameterized
- **Performance**: Prepared statements, connection pooling

#### Type System Excellence

```rust
// Generic ID type with phantom type parameter
pub struct Id<T>(Ulid, PhantomData<T>);

// Domain strings with const support
pub struct EventSource(Cow<'static, str>);

impl EventSource {
    pub const fn from_static(s: &'static str) -> Self {
        Self(Cow::Borrowed(s))
    }
}

// UTF-8 safe paths everywhere
use camino::{Utf8Path, Utf8PathBuf};

// SeaQuery ULID helpers for clean query building
pub trait SeaQueryUlidExt {
    fn eq_ulid(self, id: impl AsUlid) -> SimpleExpr;
    fn in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr;
}

// ULID must be converted to UUID for sqlx transport
impl From<Id<T>> for Uuid {
    fn from(id: Id<T>) -> Self {
        id.0.into()  // ULID → UUID conversion
    }
}
```

### Infrastructure Stack

#### Builder Pattern

```rust
#[derive(bon::Builder)]
pub struct Event {
    #[builder(skip)]
    pub id: Option<Id<Event>>,
    
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    
    #[builder(default = HostName::current())]
    pub host: HostName,
    
    #[builder(default)]
    pub ts_orig: Option<DateTime<Utc>>,
}
```

#### Error Handling

- **Context**: Standard error types with thiserror
- **Path errors**: `serde_path_to_error` for JSON parsing
- **Validation**: `validator` derive macros
- **Propagation**: `?` operator with context methods

#### Message Bus Architecture

```rust
// NATS JetStream configuration
pub struct NatsConfig {
    pub servers: Vec<String>,
    pub stream_name: String,
    pub consumer_name: String,
}

// Subject pattern: events.<source>.<event_type>
// Example: events.filesystem.file_created
```

### Testing Infrastructure

#### Modern Test Stack

- **Fixtures**: `rstest` for parameterized tests and shared fixtures
- **Snapshots**: `insta` for complex output validation
- **Tracing**: `tracing-test` for test-time logging
- **Assertions**: `similar-asserts` for readable diffs
- **Property**: `proptest` for randomized testing

#### Test Philosophy

```rust
// TestContext uses REAL repositories, not mocks
impl TestContext {
    pub fn repos(&self) -> &impl DbPoolExt {
        &self.pool // Direct access to production code
    }
}
```

### Performance Optimizations

#### Memory Management

- **Allocator**: `mimalloc` for improved performance (jemalloc blocked on NixOS)
- **Hashing**: `ahash` for faster HashMap/HashSet operations
- **Strings**: `Arc<String>` in hot paths to avoid cloning
- **Collections**: `SmallVec` for small, stack-allocated vectors

#### Concurrency

- **Parallelism**: `rayon` for CPU-bound operations
- **Async**: Tokio with careful task spawning
- **Channels**: Bounded channels to prevent memory bloat
- **Batching**: Process events in configurable batches
- **Bulk Operations**: Batch inserts for high-throughput scenarios
- **Connection Pooling**: Configurable pool limits for database connections

### Observability Strategy

#### Comprehensive Monitoring

- **Distributed Tracing**: OpenTelemetry for cross-service request tracking
- **Structured Logging**: `tracing` crate for hierarchical, contextual logs
- **Metrics Collection**: Repository-layer instrumentation for database operations
- **Error Context**: Rich stack traces with `color-eyre` integration
- **Performance Tracking**: Automatic latency histograms for key operations

## Implementation Status

### ✅ Completed Components

See [REFACTORING_STATUS.md](./REFACTORING_STATUS.md) for details on all completed components.

### ⏳ Remaining Tasks

#### Test Infrastructure Migration
- [ ] Migrate existing tests to use rstest fixtures
- [ ] Add insta snapshot tests throughout codebase
- [ ] Increase property test coverage (currently ~11%, target >50%)

#### Performance Enhancements
- [ ] **Expand ahash usage** beyond 2 files (validator.rs, stream_manager.rs)
- [ ] Expand Arc<String> usage to more hot paths (event types, sources, etc.)
- [ ] Zero-copy serialization with rkyv
- [ ] io_uring integration via tokio-uring

#### Builder Pattern Extension
- [ ] Migrate manual builders to bon::Builder where appropriate
- [ ] Add builder methods to event payload types for test convenience

#### Path Safety
- [ ] Complete camino migration (10+ files still use std::path)

#### Schema Management
- [ ] Complete SeaQuery for ALL dynamic queries
- [ ] Implement TableDef trait for all tables

#### Optional Cleanup
- [ ] Remove Redis dependencies completely (may keep for legacy support)
- [ ] Expand color-eyre usage throughout codebase

## Migration Patterns & Examples

### Remaining Migration Work

#### Complete SeaQuery Migration
Many queries still use raw SQL or custom builders. Need to migrate to SeaQuery:

```rust
// Current: Raw SQL in many places
let query = "SELECT * FROM events WHERE source = $1";

// Target: SeaQuery everywhere for type safety
let query = Query::select()
    .from(Events::Table)
    .columns([Events::All])
    .where_col(Events::Source, Expr::eq(source))
    .build_sqlx(PostgresQueryBuilder);
```

### Event Creation Evolution

#### Phase 1: Current State (Orphan Rule Workaround)

```rust
// Can't implement From<T> due to orphan rule
let event = Event::from_payload(FileCreatedPayload {
    path: "/tmp/test.txt".into(),
    size: 1024,
})?; // Returns Result<Event, SinexError>
```

#### Phase 2: Future State (After Crate Merge)

```rust
// Direct From implementation
let event = Event::from(FileCreatedPayload {
    path: "/tmp/test.txt".into(),
    size: 1024,
}); // Returns Event directly
```

### Builder Pattern Extension

Many structs still use manual builders that should migrate to bon:

```rust
// Current: Manual builders in test-utils and elsewhere
pub struct EventBuilder<'ctx> {
    context: &'ctx TestContext,
    // manual implementation...
}

// Target: bon::Builder everywhere
#[derive(bon::Builder)]
pub struct TestEvent {
    #[builder(default)]
    pub source: EventSource,
    // ...
}
```

### NATS Migration Pattern

#### Before: Redis Streams

```rust
// Publishing to Redis
let stream_key = format!("events:{}", source);
redis_client
    .xadd(&stream_key, "*", &[("data", json)])
    .await?;

// Consuming from Redis
let messages = redis_client
    .xread(&["events:*"], &["$"])
    .await?;
```

#### After: NATS JetStream

```rust
// Publishing to NATS with subject hierarchy
let subject = format!("events.{}.{}", source, event_type);
jetstream
    .publish(subject, event_data)
    .await?;

// Consuming with durable consumer
let consumer = jetstream
    .consumer("events", "my-processor")
    .await?;
```

## Technical Decisions & Rationale

### Why Generic IDs?

**Decision**: `Id<T>` instead of `EventId`, `CheckpointId`, etc.

**Rationale**:

1. **Type Safety**: Can't accidentally pass EventId where CheckpointId expected
2. **Code Reduction**: One implementation for all ID types
3. **Consistency**: Same API for all entities
4. **Future Proof**: Easy to add new entity types

### Why Repository Pattern?

**Decision**: Extension trait on PgPool instead of repository structs

**Rationale**:

1. **Discoverability**: `pool.events()` autocompletes all repositories
2. **Ergonomics**: No need to pass repositories around
3. **Testing**: Single pool to mock/replace
4. **Flexibility**: Easy to add new repositories

### Why NATS over Redis?

**Decision**: Replace Redis Streams with NATS JetStream

**Rationale**:

1. **Purpose Built**: NATS designed for messaging, Redis for caching
2. **Features**: Built-in persistence, replay, deduplication
3. **Performance**: Better throughput for event streaming
4. **Simplicity**: One less infrastructure component

### Why bon for Builders?

**Decision**: bon derive macros over manual builders

**Rationale**:

1. **Maintenance**: 90% less code to maintain
2. **Features**: Automatic Into conversions, default handling
3. **Consistency**: Same builder API everywhere
4. **Type Safety**: Compile-time builder validation

### Why Cow<'static, str> for Domain Types?

**Decision**: Cow for EventSource, EventType, etc.

**Rationale**:

1. **Const Support**: Can define compile-time constants
2. **Performance**: No allocation for static strings
3. **Flexibility**: Still supports dynamic strings
4. **Memory**: Shared static strings across application

## Known Issues & Mitigation

### Issue 1: NATS Migration Incomplete

**Problem**: Some services still use Redis, migration in progress

**Current Status**:
- sinex-ingestd migrated to NATS
- Satellites still use Redis
- Automata still use Redis

**Next Steps**:
- Migrate satellite SDK to NATS
- Update all satellites
- Remove Redis dependencies

### Issue 2: Event::from Orphan Rule

**Problem**: Can't implement `From<T>` for Event after moving to sinex-db

**Current Mitigation**:
- `Event::from_payload()` returns `Result<Event, SinexError>`

**Long-term Solution**:
- Merge sinex-types into sinex-db
- Restore `From<T>` implementation

**Migration Plan When Crates Are Merged**:

1. **Implement From Trait**:
```rust
impl<T: EventPayload> From<T> for Event {
    fn from(payload: T) -> Self {
        // Restore lost functionality:
        // - Schema ID lookup from registry
        // - Ingestor version from CARGO_PKG_VERSION  
        // - Proper timestamp handling
        // - Other EventPayload trait functionality
    }
}
```

2. **Automated Code Updates**:
```bash
# Basic conversions
Event::from_payload\((.*?)\)\? → Event::from($1)
Event::from_payload\((.*?)\)\.ok\(\) → Some(Event::from($1))

# Remove unnecessary imports
Remove: use sinex_types::error::SinexError;
(where no longer needed)
```

3. **Manual Updates**:
   - Functions that return Result just for Event creation can return Event directly
   - Update tests that explicitly check Result types
   - Update Event documentation to remove from_payload references

4. **Example Transformations**:
```rust
// Simple case
let event = Event::from_payload(FileCreatedPayload { ... })?;
// Becomes
let event = Event::from(FileCreatedPayload { ... });

// Option case
return Event::from_payload(SystemdUnitStatusPayload { ... }).ok();
// Becomes
return Some(Event::from(SystemdUnitStatusPayload { ... }));

// Method signature (if appropriate)
async fn create_clipboard_event(&self, content: &ClipboardContent) -> Result<Event, SinexError>
// Becomes
async fn create_clipboard_event(&self, content: &ClipboardContent) -> Event
```

## Future Enhancements

### High-Priority Libraries

#### 1. nix - System API Access (Rust crate for system calls)

Replace shell commands with direct system calls for massive performance gains:

```rust
// Current: Spawning processes (slow, error-prone)
let output = Command::new("journalctl").args(&["-f", "-o", "json"]).output()?;

// Future: Direct API (orders of magnitude faster)
use nix::sys::journal;
let journal = journal::Journal::open(journal::JournalFiles::System)?;
```

Concrete benefits for Sinex satellites:
- **sinex-system-satellite**: Direct systemd journal API instead of parsing journalctl output
- **sinex-fs-watcher**: Use inotify directly instead of polling or external tools
- **Process monitoring**: Direct access to /proc without spawning `ps` commands
- **Better error handling**: Real error codes instead of parsing stderr
- **No binary dependencies**: Works even if journalctl/dbus-monitor aren't installed

#### 2. tantivy - Full-Text Search

Enable "Google for your personal data" with a proper search engine:

```rust
// Build a search index over all event payloads
let mut index = Index::create_in_dir(&path, schema)?;
let mut writer = index.writer(50_000_000)?;

// Index events with faceted search
writer.add_document(doc!(
    body => event.payload.to_string(),
    source => event.source.as_str(),
    event_type => event.event_type.as_str(),
    timestamp => event.ts_ingest.timestamp(),
))?;

// Search with typo tolerance and relevance scoring
let query = QueryParser::for_index(&index, vec![body])
    .parse_query("fuzzy search for 'complie error'")?;
```

Key advantages over PostgreSQL full-text search:
- **BM25 relevance scoring**: Better ranking than PostgreSQL's ts_rank
- **Faceted search**: Filter by event type/source/time efficiently
- **Fuzzy matching**: Handles typos automatically
- **Near-instant indexing**: No VACUUM or maintenance overhead
- **Complex queries**: Phrase search, wildcards, proximity search

#### 3. polars - DataFrames

Transform Sinex into a powerful analytics platform:

```rust
// Load events into DataFrame for complex analysis
let df = DataFrame::read_parquet("events.parquet")?;

// Analyze command patterns by hour of day
let hourly_patterns = df
    .lazy()
    .with_column(col("ts_ingest").dt().hour().alias("hour"))
    .filter(col("source").eq(lit("terminal")))
    .groupby(["hour", "payload.command"])
    .agg([col("id").count().alias("count")])
    .sort("count", SortOptions::default().with_descending(true))
    .collect()?;

// Window functions for session analysis
let sessions = df
    .lazy()
    .sort("ts_ingest")
    .with_column(
        (col("ts_ingest") - col("ts_ingest").shift(1))
            .gt(lit(Duration::minutes(30)))
            .cumsum()
            .alias("session_id")
    )
    .groupby(["session_id"])
    .agg([
        col("ts_ingest").min().alias("session_start"),
        col("ts_ingest").max().alias("session_end"),
        col("id").count().alias("event_count"),
    ])
    .collect()?;
```

Perfect for:
- **Command frequency analysis**: Which commands do I use most at different times?
- **File access patterns**: Which files do I edit together?
- **Cross-correlation**: When I use vim, what git commands follow?
- **Activity reports**: Daily/weekly summaries with statistics
- **Anomaly detection**: Unusual patterns in your behavior

#### 4. OpenTelemetry - Observability

Current sinex-telemetry provides:
- Prometheus metrics with auto-instrumentation macros
- Telemetry events stored as Sinex events
- Hybrid real-time + historical approach

OpenTelemetry could replace this with:
- Unified metrics/traces/logs API
- Standard exporters (Prometheus, Jaeger, etc.)
- Context propagation across service boundaries
- Less custom code to maintain

Benefit: Remove custom telemetry implementation, use industry standard.

### Medium-Priority Enhancements

#### 1. tokio-uring - io_uring Support

Extreme I/O performance on Linux for Sinex's I/O-heavy operations:

```rust
// Current: Standard tokio file operations
let mut file = tokio::fs::File::open(path).await?;
let mut buf = vec![0; 1024];
file.read(&mut buf).await?;

// With tokio-uring: Zero-copy, batched operations
let file = uring_fs::File::open(path).await?;
let buf = file.read_at(0, 1024).await?;  // Zero-copy read

// Batch multiple operations
let ring = IoUring::new(256)?;
for path in paths {
    ring.submit_read(path)?;  // Submit without waiting
}
ring.submit_and_wait_all()?;  // Single syscall for all reads
```

Perfect for:
- **sinex-fs-watcher**: Monitor thousands of files with minimal overhead
- **ingestd**: Handle high-throughput event ingestion
- **Blob storage**: Future zero-copy blob operations
- **Batch processing**: Submit hundreds of I/O operations in one syscall

#### 2. ratatui - Terminal UI for Dev Tool

Immediate use case: Sinex development environment manager
- Replace justfile commands with interactive TUI
- One-stop shop for:
  - Database operations (reset, setup, migrations)
  - Service management (start/stop satellites)
  - Schema generation and validation
  - Log viewing with filtering
- CLI/TUI hybrid for both human and AI use
- Start simple: just wrap existing commands
- Expand: add monitoring, health checks, etc.

#### 3. console-subscriber - Async Debugging

Tokio runtime visibility:

- Task inspection
- Deadlock detection
- Performance profiling

### Extended Type Safety

Beyond the core domain types, the Cow<'static, str> pattern can be extended to more concepts:

```rust
// Commands and shells
define_string_type!(CommandText);
define_string_type!(ShellName);
define_string_type!(WorkingDirectory);

// Patterns and regexes  
define_string_type!(GlobPattern);
define_string_type!(RegexPattern);

// Network types
define_string_type!(Hostname);
define_string_type!(IpAddress);

// Git types
define_string_type!(CommitHash);
define_string_type!(BranchName);
define_string_type!(RemoteName);

// Validation on construction
impl CommandText {
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Err("Command cannot be empty".to_string());
        }
        Ok(())
    }
}
```

Benefits:
- **Self-documenting code**: Types explain intent
- **Compile-time safety**: Can't mix CommandText with BranchName
- **Const support**: Define compile-time constants for common values
- **Zero runtime cost**: Cow<'static, str> optimizes away

### Performance Optimizations

#### Memory Allocator

Using mimalloc is literally a 2-line change with immediate benefits:

```rust
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Benefits for long-running Sinex services:
- Better performance under concurrent load (multiple satellites)
- Reduced memory fragmentation
- Better memory usage reporting
- ~10-25% performance improvement in allocation-heavy code

### Advanced Optimizations

See [ROADMAP.md](./ROADMAP.md) for far-future optimizations including:
- pgrx for PostgreSQL extensions
- roaring-rs for compressed bitmaps  
- zerocopy for binary serialization
- Machine learning integration (linfa for clustering, candle for embeddings)
- Advanced visualization (egui for GUI, bevy for 3D knowledge graphs)

These require demonstrated bottlenecks before consideration.

## Success Metrics & Validation

**Achieved metrics have been documented in [REFACTORING_STATUS.md](./REFACTORING_STATUS.md)**

### Outstanding Goals

#### Test Framework Adoption
- **Target**: Modern test infrastructure throughout
- **Current**: Infrastructure ready but tests not migrated
- **Path**: Migrate existing tests to rstest/insta

#### ahash Performance Optimization
- **Target**: Replace all HashMap/HashSet with ahash versions
- **Current**: Only 2 files use ahash
- **Path**: Systematic replacement with conditional compilation for JsonSchema compatibility

#### Complete Path Safety
- **Target**: All file paths use Utf8Path/Utf8PathBuf
- **Current**: 20+ files migrated, 10+ remain
- **Path**: Complete systematic migration

## Implementation Priorities

### 1. Test Infrastructure Migration
Infrastructure is ready but tests need migration:

```rust
// Current: Standard test patterns
#[tokio::test]
async fn test_something() {
    let ctx = TestContext::new().await;
    // ...
}

// Target: Enhanced with rstest features
#[sinex_test]  // NEVER replace with #[rstest]
async fn test_something(
    ctx: TestContext,
    #[case("fs", "/tmp/file.txt")]
    #[case("terminal", "ls -la")]
    test_data: (&str, &str)
) {
    // rstest's #[case] works seamlessly with #[sinex_test]
    ctx.snapshot_event(&event);  // insta deeply integrated
}
```

### 2. ahash Performance Expansion
Currently only in 2 files, needs systematic adoption:

```rust
// Add conditional compilation for JsonSchema compatibility
#[cfg(not(feature = "json-schema"))]
type HashMap<K, V> = ahash::AHashMap<K, V>;
#[cfg(feature = "json-schema")]
type HashMap<K, V> = std::collections::HashMap<K, V>;
```

### 3. Complete camino Migration
Many files still use std::path:

```rust
// Current: std::path
use std::path::{Path, PathBuf};

// Target: camino
use camino::{Utf8Path, Utf8PathBuf};
```

