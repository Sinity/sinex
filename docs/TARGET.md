# Sinex Target Architecture

{Codeword: kangaroo}

## Data Access Layer

### Repository Pattern

```rust
// repositories/mod.rs
pub trait DbPoolExt {
    fn events(&self) -> EventRepository;
    fn checkpoints(&self) -> CheckpointRepository;
    fn source_materials(&self) -> SourceMaterialRepository;
    fn knowledge_graph(&self) -> KnowledgeGraphRepository;
    fn state(&self) -> StateRepository;
}

impl DbPoolExt for PgPool {
    fn events(&self) -> EventRepository {
        EventRepository { pool: self }
    }
    
    fn checkpoints(&self) -> CheckpointRepository {
        CheckpointRepository { pool: self }
    }
    // ... etc
}

// repositories/events.rs
use sinex_macros::Repository;

#[derive(Repository)]
pub struct EventRepository;

// The macro generates only the boilerplate:
// pub struct EventRepository<'a> { pool: &'a PgPool }
// impl<'a> EventRepository<'a> { 
//     pub fn new(pool: &'a PgPool) -> Self { Self { pool } }
// }
// Query methods are hand-written in the impl block below

impl<'a> EventRepository<'a> {
    pub async fn insert(&self, mut event: Event) -> DbResult<Event> {
        // Validate provenance XOR rule
        let has_events = event.source_event_ids.as_ref().map_or(false, |v| !v.is_empty());
        let has_material = event.source_material_id.is_some();
        
        if has_events && has_material {
            return Err(DbError::Validation("Event cannot have both source event IDs and material ID".into()));
        }
        if !has_events && !has_material {
            return Err(DbError::Validation("Event must have provenance (either source events or material)".into()));
        }
        
        let id = EventId::new();
        let ts_ingest = Utc::now();
        
        let result = sqlx::query_as!(
            EventRecord,
            r#"INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_ingest, ts_orig, ingestor_version,
                source_event_ids, source_material_id,
                source_material_offset_start, source_material_offset_end
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8, $9::uuid[], $10::uuid, $11, $12
            ) RETURNING *"#,
            ulid_to_uuid(id.as_ulid()),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            ts_ingest,
            event.ts_orig,
            event.ingestor_version,
            event.source_event_ids.as_ref().map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>()),
            event.source_material_id.as_ref().map(|id| id.as_uuid()),
            event.source_material_offset_start,
            event.source_material_offset_end
        )
        .fetch_one(self.pool)
        .await?;
        
        Ok(result.into_domain())
    }
    
    pub async fn get_by_id(&self, id: EventId) -> DbResult<Option<Event>> {
        // implementation
    }
}

// Usage anywhere
let event = pool.events().insert(new_event).await?;
let checkpoint = pool.checkpoints().get_latest(processor_name).await?;
```

### Query Implementation

```rust
// Static queries with sqlx
pub async fn get_by_id(&self, id: EventId) -> DbResult<Option<RawEvent>> {
    sqlx::query_as!(
        RawEvent,
        r#"SELECT 
            event_id as "id: Ulid",
            source as "source!",
            event_type as "event_type!",
            ts_ingest as "ts_ingest!",
            payload as "payload!"
        FROM core.events 
        WHERE event_id = $1::uuid"#,
        ulid_to_uuid(id.as_ulid())
    )
    .fetch_optional(self.pool)
    .await
    .map_err(|e| db_error(e, "get event by id"))
}

// Dynamic queries with SeaQuery
pub async fn search(&self, filters: EventSearchFilters) -> DbResult<Vec<RawEvent>> {
    let mut query = Query::select()
        .from(Events::Table)
        .columns([Events::EventId, Events::Source, Events::EventType])
        .to_owned();
        
    if let Some(source) = &filters.source {
        query = query.and_where(Expr::col(Events::Source).eq(source.as_str()));
    }
    
    let (sql, values) = query.build_sqlx(PostgresQueryBuilder);
    sqlx::query_as_with::<_, RawEvent, _>(&sql, values)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "search events"))
}
```

### SeaQuery ULID Helpers

```rust
pub trait SeaQueryUlidExt {
    fn eq_ulid(self, id: impl AsUlid) -> SimpleExpr;
    fn in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr;
}

impl SeaQueryUlidExt for Expr {
    fn eq_ulid(self, id: impl AsUlid) -> SimpleExpr {
        self.eq(ulid_to_uuid(id.as_ulid()))
    }
    
    fn in_ulids(self, ids: impl IntoUlidArray) -> SimpleExpr {
        let uuids: Vec<Uuid> = ids.into_ulid_array()
            .into_iter()
            .map(|id| ulid_to_uuid(id.as_ulid()))
            .collect();
        self.is_in(uuids)
    }
}

// Clean usage without manual conversion
let query = Query::select()
    .from(Events::Table)
    .and_where(Expr::col(Events::EventId).eq_ulid(event_id))
    .and_where(Expr::col(Events::SourceEventIds).in_ulids(source_ids));
```

## Type System

### Strongly-Typed IDs

```rust
// Define ID types using macro to eliminate boilerplate
use sinex_macros::define_id_type;

define_id_type!(EventId);
define_id_type!(CheckpointId);
define_id_type!(MaterialId);
define_id_type!(BlobId);
define_id_type!(SessionId);
define_id_type!(OperationId);
define_id_type!(AnnotationId);
define_id_type!(ProcessorId);

// The macro generates for each type:
// - Struct definition: pub struct EventId(Ulid);
// - Common traits: Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize
// - Methods: new(), as_ulid(), from_string(), to_string()
// - sqlx implementations: Decode, Encode, Type
// - Display and FromStr implementations
```

### ID Type Macro Implementation

```rust
// In sinex-macros/src/lib.rs
#[macro_export]
macro_rules! define_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(ulid::Ulid);

        impl $name {
            pub fn new() -> Self { Self(ulid::Ulid::new()) }
            pub fn as_ulid(&self) -> &ulid::Ulid { &self.0 }
            pub fn from_string(s: &str) -> Result<Self, ulid::DecodeError> {
                Ok(Self(ulid::Ulid::from_string(s)?))
            }
        }
        
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
        
        // sqlx implementations...
    };
}
```

### Domain String Types

```rust
pub struct EventSource(Cow<'static, str>);

impl EventSource {
    pub const fn from_static(s: &'static str) -> Self {
        Self(Cow::Borrowed(s))
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Constants are now defined on payload types via EventPayload trait
pub trait EventPayload: Serialize + JsonSchema + Send + Sync + 'static {
    const SOURCE: EventSource;
    const EVENT_TYPE: EventType;
}

// Example payload with derive macro
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.created")]
pub struct FileCreatedPayload {
    pub path: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

// Usage: FileCreatedPayload::SOURCE and FileCreatedPayload::EVENT_TYPE
```

## Event Model

### Unified Event Structure

```rust
// Single Event type for both creation and retrieval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: Option<EventId>, // None when creating, Some when from DB
    pub ts_ingest: Option<DateTime<Utc>>, // None when creating, Some when from DB
    
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: HostName,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<SchemaId>,
    
    // Provenance fields (XOR rule enforced)
    pub source_event_ids: Option<Vec<EventId>>,
    pub source_material_id: Option<MaterialId>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    // ... other fields
}

// Event creation patterns
impl Event {
    /// Create from strongly-typed payload
    pub fn from<P: EventPayload>(payload: P) -> Self {
        Event {
            id: None,
            ts_ingest: None,
            source: P::SOURCE,
            event_type: P::EVENT_TYPE,
            payload: serde_json::to_value(payload).expect("EventPayload must serialize"),
            ts_orig: None,
            host: HostName::current(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            // ... all other fields None
        }
    }
    
    /// Builder for schemaless/external events only
    pub fn schemaless() -> EventBuilder {
        EventBuilder::default()
    }
    
    /// Fluent setters for common fields
    pub fn with_ts_orig(mut self, ts: Option<DateTime<Utc>>) -> Self {
        self.ts_orig = ts;
        self
    }
    
    pub fn with_provenance(mut self, provenance: impl Into<Provenance>) -> Self {
        // Enforces XOR rule between source_event_ids and source_material_id
        match provenance.into() {
            Provenance::Events(ids) => {
                self.source_event_ids = Some(ids);
                self.source_material_id = None;
            }
            Provenance::Material { id, offset_start, offset_end } => {
                self.source_event_ids = None;
                self.source_material_id = Some(id);
                self.source_material_offset_start = offset_start;
                self.source_material_offset_end = offset_end;
            }
        }
        self
    }
}

// Provenance enum for XOR enforcement
pub enum Provenance {
    Events(Vec<EventId>),
    Material {
        id: MaterialId,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    },
}
```

## Infrastructure Libraries

### Dependencies in Action

```toml
[dependencies]
sqlx = { version = "0.7", features = ["postgres", "uuid", "time", "json"] }
sea-query = { version = "0.31", features = ["postgres", "with-uuid"] }
sea-query-binder = { version = "0.6", features = ["sqlx-postgres"] }
bon = "2.0"
tokio-retry = "0.3"
tracing = "0.1"
figment = { version = "0.10", features = ["toml", "env"] }
validator = { version = "0.18", features = ["derive"] }
thiserror = "1.0"
async-nats = "0.35"
```

```rust
// How each is used
use sqlx::query_as;              // Static queries
use sea_query::{Query, Expr};     // Dynamic queries  
use bon::Builder;                 // Derive builders
use tokio_retry::Retry;           // Retry logic
use tracing::{info, instrument};  // Structured logging
use figment::Figment;             // Config loading
use validator::Validate;          // Input validation
use thiserror::Error;             // Error derives
```

### Message Bus

- **NATS JetStream**: Unified message bus for event distribution
- **async-nats**: Official client with automatic reconnection
- **Stream configuration**: Persistent streams with retention policies

## Error Handling

```rust
// Domain-specific error types with automatic conversion
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Database query failed: {0}")]
    Query(#[from] sqlx::Error),
    
    #[error("Validation failed: {0}")]
    Validation(String),
    
    #[error("Transaction failed: {0}")]
    Transaction(String),
}

pub type DbResult<T> = Result<T, DbError>;

// Service layer uses ServiceError with automatic DbError conversion
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Database(#[from] DbError),
    // ... other variants
}
```

## Testing Patterns

```rust
// Async test with database
#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> anyhow::Result<()> {
    let event = Event::from(FileCreatedPayload {
        path: "/tmp/test.txt".to_string(),
        size: 1024,
        created_at: Utc::now(),
        permissions: Some(0o644),
    })
    .with_provenance(Provenance::Material { 
        id: MaterialId::new(), 
        offset_start: Some(0),
        offset_end: Some(1024),
    });
    
    let persisted = ctx.pool.events().insert(event).await?;
    assert_eq!(persisted.source, FileCreatedPayload::SOURCE);
    assert_eq!(persisted.event_type, FileCreatedPayload::EVENT_TYPE);
    Ok(())
}

// Property test
#[proptest]
fn test_event_provenance_xor(
    #[strategy(event_ids())] event_ids: Vec<EventId>,
    #[strategy(material_id())] material_id: MaterialId,
) {
    let event = Event::from(TestPayload::default());
    
    // Cannot have both
    let invalid = event.clone()
        .with_provenance(Provenance::Events(event_ids.clone()))
        .with_provenance(Provenance::Material { id: material_id, offset_start: None, offset_end: None });
    
    prop_assert!(invalid.source_event_ids.is_none() || invalid.source_material_id.is_none());
}

// Test organization
test/
├── unit/              # Pure logic tests
├── integration/       # Database tests  
├── system/           # End-to-end flows
└── property/         # Randomized testing
```

## Module Organization

```
crate/sinex-db/src/
├── repositories/
│   ├── mod.rs         # DbPoolExt trait + re-exports
│   ├── events.rs      # EventRepository implementation
│   ├── checkpoints.rs # CheckpointRepository implementation
│   └── ...
├── db_schema/         # SeaQuery table definitions
├── query_helpers/     # ULID conversion utilities
├── conversions/       # DB record → domain type conversions
├── error.rs          # Domain error types
└── lib.rs            # Public API with prelude

crate/sinex-events/src/
├── event.rs          # Core Event struct and Provenance enum
├── payloads/
│   ├── mod.rs        # Re-export all payloads
│   ├── filesystem.rs # FileCreatedPayload, FileModifiedPayload, etc.
│   ├── shell.rs      # CommandExecutedPayload, ShellHistoryPayload, etc.
│   ├── clipboard.rs  # ClipboardCopiedPayload, ClipboardSelectedPayload
│   ├── window.rs     # WindowCreatedPayload, WindowFocusedPayload
│   ├── process.rs    # ProcessStartedPayload, ProcessHeartbeatPayload
│   ├── system.rs     # SystemBootPayload, DeviceConnectedPayload
│   └── telemetry.rs  # EventsProcessedPayload, SystemResourcesPayload
└── lib.rs            # Public API with EventPayload trait

crate/sinex-macros/src/
├── lib.rs            # Macro definitions
├── id_type.rs        # define_id_type! macro
├── repository.rs     # Repository derive macro
├── event_payload.rs  # EventPayload derive macro
└── from_sqlx.rs      # FromSqlx derive macro
```

### DB Conversion Pattern

```rust
// conversions/events.rs
use sinex_macros::FromSqlx;

#[derive(sqlx::FromRow)]
struct EventRecord {
    id: Uuid,
    source: String,
    event_type: String,
    // ... all fields as DB types
}

#[derive(FromSqlx)]
#[from_sqlx(record = "EventRecord")]
impl Event {
    #[from_sqlx(id, via = "EventId::from_uuid")]
    #[from_sqlx(source, via = "EventSource::from_string")]
    #[from_sqlx(event_type, via = "EventType::from_string")]
    // Macro generates the conversion implementation
}

// Or manually:
impl EventRecord {
    fn into_domain(self) -> Event {
        Event {
            id: Some(EventId::from_uuid(self.id)),
            source: EventSource::from_string(self.source),
            event_type: EventType::from_string(self.event_type),
            // ...
        }
    }
}
```

### Prelude Pattern

```rust
// sinex-db/src/lib.rs
pub mod prelude {
    pub use crate::repositories::DbPoolExt;
    pub use crate::error::{DbError, DbResult};
    pub use sinex_core_types::ids::{
        EventId, CheckpointId, MaterialId, BlobId,
    };
    pub use sinex_core_types::domain::{
        EventSource, EventType, HostName,
    };
}

// sinex-events/src/lib.rs
pub use event::{Event, EventPayload, Provenance};
pub use payloads::*; // Re-export all payload types

// Any consuming module just needs:
use sinex_db::prelude::*;
use sinex_events::*;

// Then can immediately use typed payloads:
let event = Event::from(FileCreatedPayload {
    path: "/tmp/test.txt".to_string(),
    size: 1024,
    created_at: Utc::now(),
    permissions: Some(0o644),
})
.with_provenance(material_id);

pool.events().insert(event).await?;
```

## Architecture Patterns

### Query Patterns

```rust
// Static: sqlx for known queries
let event = sqlx::query_as!(Event, "SELECT * FROM events WHERE id = $1", id)
    .fetch_one(&pool).await?;

// Dynamic: SeaQuery for runtime composition
let query = Query::select().from(Events::Table)
    .and_where(Expr::col(Events::Source).eq(source));
```

### Type Conversion Pattern

```rust
// Automatic at boundaries
.where_eq("event_id", ulid_to_uuid(event_id))
.eq_ulid(event_id)  // Helper does conversion
```

### Repository Access Pattern

```rust
// Always through extension trait
pool.events().insert(event).await?
pool.checkpoints().get_latest(name).await?
```

### Retry Pattern

```rust
use tokio_retry::{Retry, strategy::ExponentialBackoff};

Retry::spawn(
    ExponentialBackoff::from_millis(100).take(5),
    || async { /* operation */ }
).await?
```

### Event Creation Pattern

```rust
// Strongly-typed events (99% of cases)
let event = Event::from(FileCreatedPayload {
    path: "/tmp/test.txt".to_string(),
    size: 1024,
    created_at: Utc::now(),
    permissions: Some(0o644),
})
.with_ts_orig(Some(file_metadata.modified()))
.with_provenance(source_material_id);

// Schemaless events (only for truly external/unknown events)
let event = Event::schemaless()
    .source("external-api")
    .event_type("webhook.received")
    .payload(raw_json)
    .build()
    .with_provenance(material_id);
```

## Database Migration System

### SeaQuery-Based Migrations

```rust
// migrations/mod.rs
use sea_query_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m00000000000000_enable_extensions::Migration),
            Box::new(m00000000000001_create_schemas::Migration),
            Box::new(m00000000000002_create_core_tables::Migration),
            // ...
        ]
    }
}

// migrations/m00000000000002_create_core_tables.rs
use sea_query::{*, postgres::extension::*};

pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Use shared schema definitions
        let table = Events::create_table()
            .if_not_exists()
            .col(ColumnDef::new(Events::EventId).custom("ULID").not_null().primary_key())
            .col(ColumnDef::new(Events::TsIngest).timestamp_with_time_zone().not_null()
                .extra("GENERATED ALWAYS AS (event_id::timestamp) STORED"))
            .col(ColumnDef::new(Events::Source).text().not_null())
            .col(ColumnDef::new(Events::EventType).text().not_null())
            .col(ColumnDef::new(Events::Payload).json().not_null())
            .to_owned();
            
        manager.create_table(table).await?;
        
        // Create indexes using type-safe definitions
        manager.create_index(
            Index::create()
                .name("idx_events_source_ts")
                .table(Events::Table)
                .col(Events::Source)
                .col(Events::TsIngest.desc())
                .to_owned()
        ).await?;
        
        Ok(())
    }
    
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.drop_table(Table::drop().table(Events::Table).to_owned()).await
    }
}

// schema_migrations.rs - integrated into migration system
impl Events {
    pub fn create_table() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("core"), Alias::new("events")))
            .if_not_exists()
            // ... column definitions
    }
    
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .name("idx_events_source_ts")
                .table(Events::Table)
                .col(Events::Source)
                .col(Events::TsIngest.desc())
                .to_owned(),
            // ... other indexes
        ]
    }
}
```

### ULID/UUID Conversion Pattern

```rust
// ULID must be converted to UUID for sqlx (required, not optional)
impl EventRepository {
    pub async fn get(&self, id: EventId) -> DbResult<Option<Event>> {
        // Convert ULID to UUID for sqlx transport
        sqlx::query_as!(
            EventRecord,
            r#"SELECT * FROM core.events WHERE event_id = $1::uuid"#,
            id.as_uuid() // Required conversion for sqlx
        )
        .fetch_optional(self.pool)
        .await?
        .map(|r| r.into_domain())
        .transpose()
    }
    
    pub async fn get_by_ids(&self, ids: &[EventId]) -> DbResult<Vec<Event>> {
        // Convert ULID array to UUID array for sqlx
        let uuids: Vec<Uuid> = ids.iter().map(|id| id.as_uuid()).collect();
        
        sqlx::query_as!(
            EventRecord,
            r#"SELECT * FROM core.events WHERE event_id = ANY($1::uuid[])"#,
            &uuids
        )
        .fetch_all(self.pool)
        .await?
        .into_iter()
        .map(|r| r.into_domain())
        .collect::<Result<Vec<_>, _>>()
    }
}
```
