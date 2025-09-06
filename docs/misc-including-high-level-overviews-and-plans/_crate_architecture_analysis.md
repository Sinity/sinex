# Sinex Crate Architecture Analysis

## Executive Summary

This document provides a comprehensive deep-dive analysis of the Sinex codebase architecture, examining the 41 crates in the workspace and their dependency relationships. The analysis reveals a sophisticated event-driven data capture system built on a unified "Deep Symmetry" architecture where both ingestors (event sources) and automata (event processors) implement the same `StatefulStreamProcessor` trait.

**Key Architectural Insights:**
- **Unified Processing Model**: All satellites use the same `scan(from: Checkpoint, until: TimeHorizon)` interface
- **Strong Type Safety**: ULID-based primary keys with careful UUID conversion for PostgreSQL compatibility  
- **Layered Dependency Structure**: Clear separation between foundation, core, service infrastructure, and implementation layers
- **Standardized Service Patterns**: The `processor_main!` macro generates consistent CLI patterns across all services
- **Evolution-Ready Design**: Evidence of migration from legacy patterns to the unified StatefulStreamProcessor approach

---

## 1. Crate Hierarchy & Dependencies

### 1.1 Complete Crate Inventory

The Sinex workspace contains **41 crates** organized in a clear dependency hierarchy:

#### Foundation Layer (No Internal Dependencies)
- `sinex-ulid` - ULID implementation with PostgreSQL UUID compatibility

#### Core Infrastructure Layer  
- `sinex-error` - Canonical error handling (depends on: ulid)
- `sinex-events` - Event type definitions and builders (depends on: ulid)
- `sinex-core-types` - Shared type definitions (depends on: ulid, events, validation)
- `sinex-validation` - Validation framework (depends on: ulid)

#### Service Infrastructure Layer
- `sinex-db` - Database abstraction with query builders (depends on: ulid, core-types, events, macros, validation, error)
- `sinex-satellite-sdk` - Unified SDK for all satellite services (depends on: core-types, core-utils, core-runtime, db, ulid, events, macros, metrics-lib, error)

#### Implementation Layer - Core Services
- `sinex-ingestd` - Central ingestion daemon (depends on: core-types, core-utils, db, ulid, events, satellite-sdk)
- `sinex-gateway` - API gateway service (depends on: core-types, core-utils, events, ulid, macros, metrics-lib)
- `sinex-preflight` - System validation service

#### Implementation Layer - Satellite Services (15 services)
**Event Source Satellites (Ingestors):**
- `sinex-fs-watcher` - Filesystem monitoring
- `sinex-terminal-satellite` - Terminal activity capture
- `sinex-desktop-satellite` - Desktop environment monitoring  
- `sinex-system-satellite` - System events (systemd, dbus, udev)

**Processing Satellites (Automata):**
- `sinex-terminal-command-canonicalizer` - Command normalization
- `sinex-analytics-automaton` - Analytics processing
- `sinex-content-automaton` - Content analysis
- `sinex-pkm-automaton` - Personal knowledge management
- `sinex-search-automaton` - Search indexing
- `sinex-health-aggregator` - System health monitoring
- `sinex-rpc-dispatcher` - RPC request routing

#### Supporting Infrastructure
- `sinex-macros` - Procedural macros for code generation
- `sinex-telemetry` - Metrics collection and export
- `sinex-test-macros` - Testing utilities
- `sinex-config` - Configuration management
- `sinex-services` - Shared service implementations
- `sinex-annex` - Large file management (git-annex integration)

### 1.2 Dependency DAG Analysis

The dependency graph forms a clean DAG with these key properties:

**Foundation → Core → Infrastructure → Implementation**

```
sinex-ulid (foundation)
    ↓
sinex-error, sinex-events (core types)
    ↓  
sinex-core-types, sinex-validation (core infrastructure)
    ↓
sinex-db, sinex-satellite-sdk (service infrastructure)
    ↓
[All 15+ satellite services] (implementation)
```

**Critical Dependencies:**
- All crates depend on `sinex-ulid` (directly or transitively)
- Most services depend on `sinex-satellite-sdk` for unified interfaces
- Database-accessing crates depend on `sinex-db` for type-safe queries
- Error handling flows through `sinex-error` consistently

**Architectural Benefits:**
- **Dependency Discipline**: No circular dependencies, clean layering
- **Shared Foundations**: ULID and error handling patterns are consistent
- **Service Uniformity**: All satellites share the same SDK and patterns
- **Isolated Changes**: Changes to core types propagate predictably

---

## 2. Core Infrastructure Crates

### 2.1 sinex-ulid: Foundation of Identity

**Purpose**: Provides ULID (Universally Unique Lexicographically Sortable Identifier) support with PostgreSQL compatibility.

**Key Features:**
- **Monotonic Generation**: Thread-safe ULID generation with ordering guarantees
- **PostgreSQL Integration**: Seamless conversion between ULID and UUID for database storage
- **Time Extraction**: Can extract timestamp components from ULIDs
- **SQLx Support**: Full support for PostgreSQL binding via UUID conversion

**Architecture Pattern:**
```rust
// ULID generation with monotonic ordering
let id = Ulid::new();

// PostgreSQL storage via UUID conversion  
let uuid = id.to_uuid(); // For database operations
let recovered = Ulid::from_uuid(uuid); // For application logic
```

**Dependencies**: None (foundation crate)
**Dependents**: All other crates (directly or transitively)

### 2.2 sinex-error: Canonical Error Handling

**Purpose**: Provides the unified error handling system used throughout Sinex.

**Key Features:**
- **Comprehensive Error Categories**: Database, IO, Validation, Configuration, etc.
- **Contextual Information**: Enrichment with operation context and metadata
- **Service Integration**: Conversion patterns for service-specific errors
- **Structured Logging**: Integration with tracing for error tracking

**Error Categories:**
```rust
pub enum CoreError {
    Database(String),
    Validation(String), 
    Configuration(String),
    Io(String),
    Service(String),
    // ... 15+ error types
}
```

**Dependencies**: sinex-ulid
**Used By**: All service crates for consistent error handling

### 2.3 sinex-events: Event Type System

**Purpose**: Defines the core event types and builders used throughout the system.

**Key Components:**
- **RawEvent**: The fundamental event structure stored in the database
- **Event Builders**: Type-safe builders for different event categories
- **Strongly Typed Events**: Typed wrappers for specific event payloads
- **Event Envelopes**: Unified interface for event processing

**Event Flow:**
```rust
// Event creation
let event = FilesystemEventBuilder::new()
    .file_created("/path/to/file")
    .build()?;

// Event processing  
match event.event_type.as_str() {
    event_types::file::CREATED => handle_file_created(event),
    // ... other event types
}
```

**Dependencies**: sinex-ulid, standard async/serde libraries
**Critical Role**: Defines the event contracts for the entire system

### 2.4 sinex-db: Database Abstraction Layer

**Purpose**: Provides type-safe database operations with automatic ULID/UUID conversion.

**Key Features:**
- **Query Builder Pattern**: Type-safe SQL query construction
- **ULID/UUID Conversion**: Automatic conversion at database boundaries
- **Operation Queries**: Standardized patterns for common database operations
- **Connection Pooling**: PostgreSQL connection pool management
- **Migration Support**: Database schema evolution support

**Query Pattern:**
```rust
// Type-safe query with automatic ULID/UUID conversion
let events = EventQueries::get_events_by_source(
    &db_pool,
    sources::FILESYSTEM,
    Some(limit)
).await?;
```

**Dependencies**: sinex-ulid, sinex-core-types, sinex-events, sinex-macros, sinex-validation, sinex-error
**Used By**: All services requiring database access

### 2.5 sinex-satellite-sdk: Unified Service Framework

**Purpose**: Provides the unified SDK that all satellite services use for consistent interfaces and behavior.

**Key Components:**
- **StatefulStreamProcessor Trait**: The core interface that unifies ingestors and automata
- **CLI Framework**: Standardized service/scan/explore command structure
- **Checkpoint Management**: State persistence across service restarts
- **gRPC/Redis Clients**: Communication with ingestd and message bus
- **Error Handling**: Service-specific error types with CoreError conversion

**Unified Interface:**
```rust
#[async_trait]
pub trait StatefulStreamProcessor: Send + Sync {
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon, 
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport>;
    
    fn processor_type(&self) -> ProcessorType; // Ingestor vs Automaton
    // ... other methods
}
```

**Dependencies**: 10+ core crates (most comprehensive dependency list)
**Used By**: All 15+ satellite services

---

## 3. Service Architecture

### 3.1 Service to Crate Mapping

The Sinex system produces **15+ binary services** from the crate workspace:

#### Core Infrastructure Services
- **sinex-ingestd**: Central event ingestion daemon
  - **Role**: Receives events via gRPC, validates against JSON schemas, persists to PostgreSQL
  - **Communication**: Unix Domain Socket server for satellites
  - **Storage**: PostgreSQL with TimescaleDB for time-series optimization

- **sinex-gateway**: API gateway and external interface  
  - **Role**: HTTP/WebSocket API, authentication, rate limiting
  - **Integration**: Native messaging for browser extensions

#### Ingestor Services (External → Internal Events)
- **sinex-fs-watcher**: Filesystem monitoring satellite
- **sinex-terminal-satellite**: Terminal activity capture
- **sinex-desktop-satellite**: Desktop environment monitoring
- **sinex-system-satellite**: System events (systemd, dbus, udev)

#### Automaton Services (Internal → Derived Events)
- **sinex-terminal-command-canonicalizer**: Command normalization automaton
- **sinex-analytics-automaton**: Analytics processing automaton
- **sinex-content-automaton**: Content analysis automaton  
- **sinex-pkm-automaton**: Personal knowledge management automaton
- **sinex-search-automaton**: Search indexing automaton

#### Support Services
- **sinex-health-aggregator**: System health monitoring
- **sinex-preflight**: System validation and readiness checks
- **sinex-rpc-dispatcher**: RPC request routing

### 3.2 Service Startup Patterns

All satellite services follow the unified startup pattern implemented via the `processor_main!` macro:

**Standard Service Entry Point:**
```rust
// Example: sinex-fs-watcher/src/main.rs
use sinex_fs_watcher::FilesystemProcessor;

// This macro generates the complete main() function with:
// - CLI parsing (service/scan/explore subcommands)
// - Logging setup
// - Service initialization  
// - Graceful shutdown handling
sinex_satellite_sdk::processor_main!(FilesystemProcessor);
```

**Generated CLI Structure:**
- `service` - Long-running service mode with 3-phase startup
- `scan` - One-off scanning operations
- `explore` - Interactive diagnostics and exploration

**3-Phase Service Startup:**
1. **Snapshot Phase**: Capture current system state (if supported)
2. **Gap-Fill Phase**: Process events since last checkpoint (if supported)  
3. **Continuous Phase**: Real-time event processing

### 3.3 Service Discovery & Communication

**Communication Patterns:**
- **Satellites → ingestd**: gRPC over Unix Domain Socket (`/run/sinex/ingest.sock`)
- **ingestd → PostgreSQL**: Direct SQL connection with connection pooling
- **ingestd → Redis**: Message bus for real-time event distribution
- **Automata → Redis**: Redis Streams for event consumption
- **External → gateway**: HTTP/WebSocket/Native Messaging

**Service Configuration:**
- **Environment-Based**: All configuration via environment variables
- **NixOS Integration**: Services configured declaratively via Nix modules
- **No File-Based Config**: Eliminates configuration file management complexity

---

## 4. Type Flow Through Layers

### 4.1 RawEvent Lifecycle

The `RawEvent` is the fundamental data structure that flows through the entire system:

**Creation (Ingestors):**
```rust
// 1. External event detected (file change, terminal command, etc.)
let raw_event = RawEventBuilder::new()
    .source(sources::FILESYSTEM)
    .event_type(event_types::file::CREATED)
    .payload(serde_json::json!({
        "path": "/path/to/file",
        "size": file_size
    }))
    .build()?;

// 2. Event validation
let validated = ValidationChain::new()
    .validate_json_schema(&event.payload)?;
```

**Ingestion (ingestd):**
```rust
// 3. gRPC transmission to ingestd
ingest_client.submit_events(vec![raw_event]).await?;

// 4. JSON schema validation in ingestd
validator.validate_event(&event).await?;

// 5. PostgreSQL storage with ULID→UUID conversion
let db_id = event.id.to_uuid(); // Convert for PostgreSQL
sqlx::query!(
    "INSERT INTO core.events (id, source, event_type, payload, ts_orig) VALUES ($1, $2, $3, $4, $5)",
    db_id, event.source, event.event_type, event.payload, event.ts_orig
).execute(&pool).await?;
```

**Processing (Automata):**
```rust
// 6. Redis Stream distribution
redis_client.stream_publish("sinex:events", &event).await?;

// 7. Automaton consumption with checkpoint tracking
let checkpoint = checkpoint_manager.load_checkpoint().await?;
let events = scan_events(checkpoint, TimeHorizon::Continuous).await?;

for event in events {
    let processed_result = process_event(event).await?;
    checkpoint_manager.save_checkpoint(event.id).await?;
}
```

### 4.2 Type Transformations at Boundaries

**ULID ↔ UUID Conversion:**
- **Application Layer**: Uses `Ulid` for time-ordering and type safety
- **Database Layer**: Converts to `UUID` via `.to_uuid()` for PostgreSQL compatibility
- **Recovery**: Converts back via `Ulid::from_uuid()` when loading from database

**JSON Schema Validation:**
- **Ingestion Point**: All event payloads validated against JSON schemas
- **Schema Registry**: Centralized schema definitions in `schemas/` directory
- **Evolution Support**: Versioned schemas for backward compatibility

**Serialization Points:**
- **gRPC Boundary**: Protocol buffer serialization for ingestor → ingestd communication
- **Redis Streams**: JSON serialization for event distribution
- **HTTP API**: REST/GraphQL serialization for external clients
- **Database Storage**: PostgreSQL JSONB for efficient payload storage

### 4.3 Error Propagation

Errors flow through the system via standardized conversion patterns:

**Error Flow:**
```rust
// Service-specific error
SatelliteError::Database(db_err) 
    ↓
// Conversion to core error
CoreError::Database(err_msg)
    ↓  
// Context enrichment via macro
ErrorContext::new("database_insert", module_path!())
    ↓
// Structured logging
tracing::error!(error = %err, context = %ctx, "Database operation failed");
```

---

## 5. Workspace Organization

### 5.1 Naming Conventions & Patterns

**Crate Naming:**
- `sinex-{component}` - Core infrastructure (db, events, error)
- `sinex-{service}-{type}` - Services (terminal-satellite, fs-watcher)  
- `sinex-{function}-automaton` - Processing automata (analytics, content, pkm)
- `sinex-{shared}-{category}` - Shared utilities (core-types, satellite-sdk)

**Directory Structure:**
- `crate/` - All Rust crates (36 workspace members)
- `cli/` - Python CLI tools (exo.py for querying)
- `schemas/` - JSON schema definitions (v1/, v2/ for versioning)
- `migrations/` - Database schema migrations
- `test/` - Comprehensive test suite (unit, integration, property, system)
- `nixos/` - NixOS service modules and configuration

**Feature Organization:**
- **Default Features**: Minimal core functionality
- **Optional Features**: `arbitrary` (testing), `metrics` (observability), `sqlx` (database)
- **Development Features**: Enhanced debugging and development tools

### 5.2 Build Optimization Strategies

**Workspace-Level Optimizations:**
- **Shared Dependencies**: Common dependencies defined in `workspace.dependencies`
- **Incremental Compilation**: Cargo workspace builds dependencies once
- **Feature Flags**: Optional functionality to reduce compilation time during development

**SQLX Offline Mode:**
- **Prepared Queries**: All SQL queries pre-validated and cached in `.sqlx/` directory
- **Nix Integration**: `.sqlx/` committed to git for reproducible Nix builds
- **Type Safety**: Compile-time SQL validation without database connection

**Binary Size Optimization:**
- **Shared Libraries**: Common functionality in shared crates reduces duplication
- **Feature-Gated Code**: Optional functionality behind feature flags
- **LTO (Link-Time Optimization)**: Enabled for release builds

### 5.3 Testing Architecture

**Test Categories:**
- **Unit Tests** (`test/unit/`): Core logic and utilities
- **Integration Tests** (`test/integration/`): Database, API, and system integration
- **Property Tests** (`test/property/`): QuickCheck-style testing with proptest
- **Adversarial Tests** (`test/adversarial/`): Chaos engineering and failure injection
- **System Tests** (`test/system/`): End-to-end workflow validation

**Test Infrastructure:**
- **Test Context**: `#[sinex_test]` macro provides database test fixtures
- **Factories**: Standardized test data generation
- **Mocks**: Comprehensive mock implementations for external services
- **VM Testing**: NixOS VM tests for deployment validation

---

## 6. Critical Architectural Patterns

### 6.1 Deep Symmetry: Unified Processing Model

The core architectural insight is the "Deep Symmetry" between ingestors and automata:

**Traditional Model:**
- Ingestors: Custom interfaces, ad-hoc processing
- Automata: Different interfaces, inconsistent patterns

**Unified Model:**
```rust
// BOTH ingestors and automata implement the same interface
impl StatefulStreamProcessor for FilesystemWatcher {
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) 
        -> SatelliteResult<ScanReport>
}

impl StatefulStreamProcessor for CommandCanonicalizer {
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) 
        -> SatelliteResult<ScanReport>
}
```

**Benefits:**
- **Operational Consistency**: Same CLI patterns, logging, monitoring
- **Testing Uniformity**: Same testing patterns for all satellites  
- **Development Efficiency**: Learn once, apply everywhere
- **Debugging Simplicity**: Consistent error patterns and logging

### 6.2 Service Generation via Macros

The `processor_main!` macro generates complete service binaries:

**Generated Components:**
- **CLI Parsing**: Standard service/scan/explore subcommands
- **Logging Setup**: Consistent logging configuration
- **Service Lifecycle**: 3-phase startup sequence
- **Error Handling**: Standardized error reporting
- **Graceful Shutdown**: Signal handling and cleanup

**Example Generated CLI:**
```bash
# Service mode (long-running)
sinex-fs-watcher service --dry-run

# Scan mode (one-off operation)  
sinex-fs-watcher scan --from=checkpoint.json --until=2024-01-01T12:00:00Z --targets=/home

# Explore mode (interactive diagnostics)
sinex-fs-watcher explore --coverage-analysis
```

### 6.3 Type-Safe Database Operations

The database layer provides compile-time type safety with runtime flexibility:

**Query Builder Pattern:**
```rust
// Type-safe query construction
let query = EventQueries::builder()
    .source(sources::FILESYSTEM)
    .event_types(&[event_types::file::CREATED, event_types::file::MODIFIED])
    .time_range(start_time..end_time)
    .limit(100)
    .build();

let events = query.execute(&db_pool).await?;
```

**ULID/UUID Boundary Management:**
```rust
// Application logic uses ULIDs
let event_id = Ulid::new();

// Database operations use UUIDs  
let db_result = sqlx::query!(
    "SELECT * FROM core.events WHERE id = $1",
    event_id.to_uuid()  // Automatic conversion
).fetch_one(&pool).await?;

// Recovery to ULID
let recovered_id = Ulid::from_uuid(db_result.id);
```

### 6.4 Evolution-Ready Architecture

The codebase shows evidence of ongoing architectural evolution:

**Legacy → Modern Migration:**
- Old `EventSource` trait → New `StatefulStreamProcessor` trait
- Custom CLI implementations → Generated `processor_main!` pattern
- Ad-hoc error handling → Unified `sinex-error` system
- Manual checkpointing → Automated `CheckpointManager`

**Forward Compatibility:**
- **Versioned Schemas**: Support for schema evolution (`v1/`, `v2/`)
- **Feature Flags**: Gradual rollout of new functionality
- **Trait Evolution**: Extension methods preserve backward compatibility
- **Configuration Migration**: Environment-based config eliminates file dependencies

---

## 7. Operational Characteristics

### 7.1 Service Deployment Patterns

**NixOS Integration:**
- **Declarative Services**: All services defined as NixOS modules
- **Atomic Upgrades**: Nix ensures consistent deployment across the entire system
- **Rollback Support**: Can rollback to previous system configurations
- **Development/Production Parity**: Same Nix flake builds both environments

**Service Dependencies:**
```nix
# Automatic service ordering and dependency management
services.sinex = {
  enable = true;
  ingestd.enable = true;           # Core daemon (started first)
  satellites.fs-watcher.enable = true;      # Depends on ingestd
  satellites.terminal.enable = true;        # Depends on ingestd  
  automata.command-canonicalizer.enable = true; # Depends on Redis
};
```

### 7.2 Data Persistence Strategy

**PostgreSQL + TimescaleDB:**
- **Event Storage**: Immutable events in `core.events` table
- **Time-Series Optimization**: TimescaleDB hypertables for time-based queries
- **Compression**: Automatic compression for historical data
- **Partitioning**: Time-based partitioning for scalability

**Redis Streams:**
- **Message Bus**: Real-time event distribution to automata
- **Consumer Groups**: Load balancing and fault tolerance
- **Checkpointing**: Progress tracking for resumable processing

**Git Annex Integration:**
- **Large File Storage**: Binary assets managed via git-annex
- **Content Deduplication**: Automatic deduplication of identical files
- **Distributed Storage**: Support for multiple storage backends

### 7.3 Observability & Monitoring

**Structured Logging:**
- **Tracing Integration**: All services use structured logging via tracing crate
- **Context Propagation**: Error context flows through the system
- **Performance Metrics**: Built-in metrics collection via `sinex-telemetry`

**Health Monitoring:**
- **Health Aggregator**: Dedicated service for system health monitoring
- **Heartbeat Mechanism**: Regular health checks from all satellites
- **Failure Detection**: Automated detection of service failures and restarts

---

## 8. Development & Maintenance Benefits

### 8.1 Developer Experience

**Consistency Across Services:**
- **Same Patterns**: All services follow identical development patterns
- **Unified Testing**: Same testing infrastructure for all components
- **Standard Debugging**: Consistent logging and error reporting
- **Predictable Behavior**: Same CLI patterns across all binaries

**Code Reuse:**
- **Shared SDK**: `sinex-satellite-sdk` provides 90% of service functionality
- **Common Infrastructure**: Database, error handling, validation patterns shared
- **Macro Generation**: Reduces boilerplate by generating standard service code

### 8.2 Maintenance Characteristics  

**Dependency Management:**
- **Clean DAG**: No circular dependencies simplify updates
- **Layered Architecture**: Changes propagate predictably through layers
- **Version Consistency**: Workspace-level dependency management

**Evolution Support:**
- **Backward Compatibility**: Careful trait evolution preserves existing implementations
- **Schema Versioning**: Database and event schema evolution support
- **Gradual Migration**: Feature flags enable gradual rollout of changes

**Testing Confidence:**
- **Comprehensive Coverage**: Unit, integration, property, and system tests
- **VM Testing**: Full deployment testing in NixOS VMs
- **Adversarial Testing**: Chaos engineering and failure injection

---

## Conclusion

The Sinex codebase demonstrates sophisticated software architecture with several noteworthy characteristics:

**Architectural Strengths:**
1. **Unified Abstractions**: The StatefulStreamProcessor trait unifies ingestors and automata under a single interface
2. **Strong Type Safety**: ULID-based identity with careful PostgreSQL integration
3. **Service Standardization**: Macro-generated services provide operational consistency
4. **Evolution-Friendly**: Architecture supports gradual migration and feature evolution
5. **Clear Dependencies**: Well-structured dependency DAG with proper layering

**Operational Excellence:**
1. **NixOS Integration**: Declarative deployment with atomic upgrades and rollback
2. **Comprehensive Testing**: Multi-layer testing strategy provides confidence
3. **Observability**: Structured logging and metrics throughout the system
4. **Developer Experience**: Consistent patterns reduce cognitive load

**Scale & Complexity Management:**
1. **41 Crates**: Successfully manages complexity through clear separation of concerns
2. **15+ Services**: Unified architecture scales to many satellite services
3. **Type Flow**: Clean data flow from external sources to PostgreSQL storage
4. **Error Propagation**: Standardized error handling across all components

This architecture demonstrates how careful abstraction and consistent patterns can manage complexity at scale while maintaining developer productivity and operational reliability. The "Deep Symmetry" concept of treating ingestors and automata uniformly is particularly innovative and provides significant operational benefits.

The codebase shows evidence of thoughtful evolution, with clear migration paths from legacy patterns to modern unified approaches, suggesting a mature approach to architecture evolution and technical debt management.