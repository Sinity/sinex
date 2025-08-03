# Event System Refactoring Supplement

This document supplements the main EVENT_SYSTEM_ARCHITECTURE.md with concrete implementation details based on discussions with multiple LLMs and current codebase analysis.

## Core Design Decisions

### 1. Fluent Struct Pattern (No Half-Measures)

Event becomes its own builder - no separate builder types:

```rust
// Strongly-typed events (internal sources)
let event = Event::from(FileCreatedPayload { ... })
    .with_ts_orig(Some(historical_time))
    .with_provenance(Provenance::from_events([event_a.id, event_b.id]));

// Schemaless events (external sources only)
let event = Event::schemaless()
    .source("EXTERNAL")
    .event_type("UNKNOWN")
    .payload(json_value)
    .build();
```

**NO**: EventFactory, TypedEventBuilder, Event::builder()  
**YES**: Event::from() for typed, Event::schemaless() for untyped

### 2. Event Constants Through Payload Types

Payload types serve as the source of truth for event metadata:

```rust
// Each payload type defines its source and event type
impl EventPayload for FileCreatedPayload {
    const SOURCE: EventSource = EventSource::from_static("fs-watcher");
    const EVENT_TYPE: EventType = EventType::from_static("file.created");
}

// Usage:
if event.source == FileCreatedPayload::SOURCE { ... }
if event.event_type == FileCreatedPayload::EVENT_TYPE { ... }
```

Benefits:
- Single source of truth for event metadata
- Event types inherently bound to their sources
- No separate constants modules needed

### 3. EventPayload Trait

```rust
pub trait EventPayload: Serialize + JsonSchema + Send + Sync + 'static {
    const SOURCE: EventSource;
    const EVENT_TYPE: EventType;
    // Schema name is derived as "{SOURCE}.{EVENT_TYPE}"
    // Schema version is determined by registry based on actual schema changes
}
```

### 4. Provenance as XOR Rule

Events must have EITHER source_event_ids OR source_material_id, never both:

```rust
pub enum Provenance {
    Events(Vec<EventId>),
    Material {
        id: MaterialId,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    }
}

impl Event {
    pub fn with_provenance(mut self, provenance: impl Into<Provenance>) -> Self {
        // Clear any existing provenance to enforce XOR
        self.source_event_ids = None;
        self.source_material_id = None;
        self.source_material_offset_start = None;
        self.source_material_offset_end = None;
        
        match provenance.into() {
            Provenance::Events(ids) => self.source_event_ids = Some(ids),
            Provenance::Material { id, offset_start, offset_end } => {
                self.source_material_id = Some(id);
                self.source_material_offset_start = offset_start;
                self.source_material_offset_end = offset_end;
            }
        }
        self
    }
}
```

### 5. Schema Versioning

Schema versioning is handled by the registry, not hardcoded:

- Schema name is derived: `"{SOURCE}.{EVENT_TYPE}"`
- Version is determined by comparing actual schema changes
- Directory structure: `/schemas/v1/source/event_type.json`
- Database tracks `schema_name` and `schema_version` separately

## Implementation Plan

### Phase 1: Core Implementation

#### 1.1 Event Struct

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<EventId>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ingest: Option<DateTime<Utc>>,
    
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: JsonValue,
    
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: HostName,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<SchemaId>,
    
    // Provenance fields (XOR enforced)
    pub source_event_ids: Option<Vec<EventId>>,
    pub source_material_id: Option<MaterialId>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,
    pub associated_blob_ids: Option<Vec<BlobId>>,
}
```

#### 1.2 Implement Fluent Methods

```rust
impl Event {
    /// Create from typed payload
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
            payload_schema_id: None, // TODO: Look up from registry using P::SOURCE and P::EVENT_TYPE
            source_event_ids: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            anchor_byte: None,
            associated_blob_ids: None,
        }
    }
    
    /// Builder for schemaless events
    pub fn schemaless() -> EventBuilder {
        EventBuilder::default()
    }
    
    // Fluent setters (consuming self)
    pub fn with_ts_orig(mut self, ts: Option<DateTime<Utc>>) -> Self {
        self.ts_orig = ts;
        self
    }
    
    pub fn with_provenance(mut self, provenance: impl Into<Provenance>) -> Self {
        // Clear any existing provenance to enforce XOR
        self.source_event_ids = None;
        self.source_material_id = None;
        self.source_material_offset_start = None;
        self.source_material_offset_end = None;
        
        match provenance.into() {
            Provenance::Events(ids) => self.source_event_ids = Some(ids),
            Provenance::Material { id, offset_start, offset_end } => {
                self.source_material_id = Some(id);
                self.source_material_offset_start = offset_start;
                self.source_material_offset_end = offset_end;
            }
        }
        self
    }
    
    // ... other with_* methods
}
```

#### 1.3 EventRecord Handling

Keep EventRecord for now as a single point of boilerplate:

```rust
#[derive(sqlx::FromRow)]
struct EventRecord {
    id: Uuid,
    ts_ingest: DateTime<Utc>,
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: Option<DateTime<Utc>>,
    host: String,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Uuid>,
    source_event_ids: Option<Vec<Uuid>>,
    source_material_id: Option<Uuid>,
    // ... other fields
}

impl From<EventRecord> for Event {
    fn from(rec: EventRecord) -> Self {
        Event {
            id: Some(EventId::from_uuid(rec.id)),
            ts_ingest: Some(rec.ts_ingest),
            source: EventSource::from_string(rec.source),
            event_type: EventType::from_string(rec.event_type),
            // ... convert all fields
        }
    }
}
```

### Phase 2: Schema Integration

#### 2.1 Schema Registration at Startup

Since schemas are already versioned in the filesystem, load them at startup:

```rust
// In a startup function
pub async fn register_schemas(db: &Database) -> Result<()> {
    let schema_dir = Path::new("schemas/v1");
    
    // Walk the schema directory
    for entry in WalkDir::new(schema_dir) {
        let entry = entry?;
        if entry.path().extension() == Some("json") {
            let schema_content = fs::read_to_string(entry.path())?;
            let schema_json: Value = serde_json::from_str(&schema_content)?;
            
            // Extract schema name from path (e.g., "filesystem/file_created")
            let relative_path = entry.path().strip_prefix(schema_dir)?;
            let schema_name = relative_path.with_extension("").to_string_lossy();
            
            // Insert or update in database
            sqlx::query!(
                r#"
                INSERT INTO sinex_schemas.event_payload_schemas 
                    (schema_name, schema_version, schema_content, event_types, is_active)
                VALUES ($1, $2, $3, $4, true)
                ON CONFLICT (schema_name, schema_version) 
                DO UPDATE SET 
                    schema_content = EXCLUDED.schema_content,
                    updated_at = NOW()
                "#,
                schema_name.as_ref(),
                "v1", // Version from directory
                schema_json,
                &[schema_name.as_ref()], // For now, schema name == event type
            )
            .execute(db)
            .await?;
        }
    }
    Ok(())
}
```

#### 2.2 Build Script Integration

The current system generates schemas via a separate binary. We should migrate to build.rs for better integration:

```rust
// crate/sinex-events/build.rs
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // Only rebuild when payload definitions change
    println!("cargo:rerun-if-changed=src/payloads/");
    
    // Generate schemas when explicitly requested (development)
    if env::var("SINEX_GENERATE_SCHEMAS").is_ok() {
        generate_schemas();
    }
    
    // Always generate EventPayload trait implementations
    generate_event_payload_impls();
}

fn generate_schemas() {
    // Use derive macro registry to automatically discover all payloads
    let payloads = discover_payload_types();
    
    for payload in payloads {
        let schema = schemars::schema_for!(payload.type);
        let version = determine_schema_version(&payload.path, &schema);
        let output_path = format!("schemas/{}/{}.json", version, payload.path);
        
        fs::create_dir_all(Path::new(&output_path).parent().unwrap()).unwrap();
        fs::write(output_path, serde_json::to_string_pretty(&schema).unwrap()).unwrap();
    }
}

fn generate_event_payload_impls() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("event_payload_impls.rs");
    
    // Generate from derive macro registry
    let impls = generate_impls_from_registry();
    fs::write(dest_path, impls).unwrap();
}

fn determine_schema_version(path: &str, new_schema: &Schema) -> String {
    // Load existing schema if it exists
    let existing_path = format!("schemas/v1/{}.json", path);
    
    if let Ok(existing) = fs::read_to_string(&existing_path) {
        let existing_schema: Schema = serde_json::from_str(&existing).unwrap();
        
        if has_breaking_changes(&existing_schema, new_schema) {
            // Would need to increment version
            eprintln!("WARNING: Breaking schema change detected for {}", path);
            eprintln!("Consider incrementing schema version");
        }
    }
    
    "v1".to_string() // For now, always v1 until versioning strategy is implemented
}
```

#### 2.3 Derive Macro for Automatic Discovery

```rust
// In sinex-macros
#[proc_macro_derive(EventPayload, attributes(event_payload))]
pub fn derive_event_payload(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    
    // Parse attributes
    let attrs = parse_event_payload_attributes(&input.attrs);
    
    // Register this type for build script discovery
    register_payload_type(name, &attrs);
    
    // Generate EventPayload implementation
    let expanded = quote! {
        impl EventPayload for #name {
            const SOURCE: EventSource = #source;
            const EVENT_TYPE: EventType = #event_type;
        }
    };
    
    TokenStream::from(expanded)
}
```

#### 2.4 Payload Organization Strategy

Instead of keeping all payloads in `strongly_typed_events.rs`, organize by domain:

```
crate/sinex-events/src/
├── lib.rs
├── event.rs              # Core Event struct
├── constants.rs          # Re-export from sinex-core-types
├── payloads/
│   ├── mod.rs           # Re-export all payloads
│   ├── filesystem.rs    # Filesystem event payloads
│   ├── shell.rs         # Shell/terminal event payloads
│   ├── clipboard.rs     # Clipboard event payloads
│   ├── window.rs        # Window manager event payloads
│   ├── system.rs        # System event payloads
│   └── process.rs       # Process lifecycle payloads
└── strongly_typed_events.rs  # DEPRECATED: Keep temporarily for migration
```

Example payload file:

```rust
// payloads/filesystem.rs
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileCreatedPayload {
    pub path: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

impl EventPayload for FileCreatedPayload {
    const SOURCE: EventSource = EventSource::from_static("fs-watcher");
    const EVENT_TYPE: EventType = EventType::from_static("file.created");
}

// ... other filesystem payloads
```

## Key Implementation Notes

### Schema Versioning Rules

- **Adding optional fields**: Backward compatible
- **Adding required fields**: Breaking change
- **Removing fields**: Breaking change  
- **Changing field types**: Breaking change

### Provenance Validation

```rust
impl EventRepository {
    pub async fn insert(&self, event: Event) -> Result<Event> {
        // Validate provenance XOR rule
        let has_events = event.source_event_ids.as_ref().map_or(false, |v| !v.is_empty());
        let has_material = event.source_material_id.is_some();
        
        if has_events && has_material {
            return Err(DbError::Validation("Event cannot have both source types"));
        }
        if !has_events && !has_material {
            return Err(DbError::Validation("Event must have source provenance"));
        }
        
        // Generate ID and insert...
    }
}
```

## Success Criteria

- [x] Single, intuitive API for event creation (Event::from/schemaless)
- [x] Type safety for internal events (EventPayload trait)
- [x] Flexibility for external events (Event::schemaless with bon::Builder)
- [ ] Automatic schema assignment (TODO in code)
- [ ] Enforced provenance rules (no with_provenance method)
- [x] No duplicate builder code (using bon::Builder)
- [x] Automatic payload discovery via derive macros (basic implementation)
- [ ] Schema generation from Rust structs via build.rs
- [x] Domain-organized payload modules (payloads/ directory structure)


## Migration Status

### Current State (COMPLETED ✅)

The event system refactoring has been successfully completed:

1. **Event API**: Fully implemented and adopted
   - ✅ Event::from() implemented and works with typed payloads
   - ✅ Event::schemaless() implemented using bon::Builder
   - ✅ EventPayload trait fully implemented
   - ✅ with_provenance() IMPLEMENTED with XOR rule enforcement
   - ✅ Provenance enum IMPLEMENTED with convenient From traits
   - ✅ All satellites converted to use typed payloads where appropriate

2. **Payload Types**: Complete implementation
   - ✅ EventPayload trait defined in event.rs
   - ✅ Derive macro implemented and working (#[derive(EventPayload)])
   - ✅ Payloads organized by domain (filesystem.rs, shell.rs, window.rs, etc.)
   - ✅ Source-specific payloads (KittyCommandExecutedPayload vs AtuinCommandExecutedPayload)
   - ✅ Constants REMOVED from sources:: and types:: modules
   - ✅ All call sites converted to use typed payloads or appropriate Event::schemaless()

3. **Schema System**: Basic infrastructure (DEFERRED)
   - ❌ Build script exists but doesn't implement schema generation
   - ❌ Registry mechanism in macro writes to env vars but not integrated
   - ❌ No automatic schema file generation
   - ⚠️ payload_schema_id field exists but always None (TODO comment in code)

4. **Domain Objects**: Fully refactored
   - ✅ Applied same patterns to SourceMaterial, Checkpoint, Entity, EntityRelation
   - ✅ Semantic constructors with fluent methods
   - ✅ Removed all New* types, renamed to *Record pattern
   - ✅ Manual builder pattern chosen over bon::Builder for better UX

### Remaining Work (Low Priority/Deferred)

1. **Schema System** (Infrastructure improvement, not blocking):
   - Implement schema generation in build.rs
   - Connect registry from derive macro to build process
   - Implement schema lookup for payload_schema_id field
   - Design schema versioning strategy

2. **Provenance Validation** (Deferred until SDK usage):
   - Add validation in EventRepository to enforce XOR rule
   - Currently no events set provenance, so validation would break everything
   - Wait until SDK starts using provenance before adding validation

### Completed Work Summary

1. ✅ Provenance system fully implemented
2. ✅ All Event::schemaless() converted to typed payloads where appropriate  
3. ✅ Constants modules removed, using Payload::SOURCE/EVENT_TYPE
4. ✅ Domain objects refactored with consistent patterns
5. ✅ All "Enhanced" terminology removed
6. ✅ Test code updated for new patterns
7. ✅ Repository pattern used consistently
8. ✅ Event.id changed to Option<Ulid>

