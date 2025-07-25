# Rustdoc Migration: Concrete Examples

This document provides concrete examples of how specific `/spec/` documents would translate to rustdoc format.

## Example 1: VISION.md → Crate Root Documentation

### Original (VISION.md excerpt)
```markdown
# Sinex Exocortex: The Sentient Archive - A Vision for Cognitive Sovereignty (v3.0)

## Foreword: The Imperative of Cognitive Sovereignty

We stand at a peculiar juncture in human history. Our digital tools grant us unprecedented access to information...
```

### Rustdoc Translation (in `crate/sinex-core/src/lib.rs`)
```rust
//! # Sinex: The Sentient Archive
//! 
//! > A comprehensive event-driven system for cognitive sovereignty through digital experience capture
//! 
//! ## Vision: The Imperative of Cognitive Sovereignty
//! 
//! We stand at a peculiar juncture in human history. Our digital tools grant us unprecedented 
//! access to information, yet this abundance often engenders a profound sense of fragmentation.
//! The lived texture of our daily experience—fleeting thoughts, crucial insights, decision 
//! context—is scattered across ephemeral applications and proprietary silos.
//! 
//! Sinex is a direct response to this crisis of digital amnesia. It's not another note-taking
//! app or productivity dashboard, but an **empowering digital environment for thought**: a 
//! persistent, universally capturing, and intelligently structured space that mirrors and 
//! augments the user's own mind.
//! 
//! ## Core Philosophy
//! 
//! ### The Exocortex Pledge
//! 
//! 1. **Capture Comprehensively**: Every potentially significant digital trace at highest fidelity
//! 2. **Structure Emergently**: Schemas evolve with needs; raw data remains inviolate
//! 3. **Empower Unconditionally**: User is absolute sovereign of their data
//! 4. **Evolve Transparently**: Living system co-evolving with user needs
//! 
//! ### Design Ethos
//! 
//! - **Universal Capture as Default**: If it can be instrumented, it should be
//! - **Emergent Structure**: Meaning discovered, not preordained
//! - **Sovereign User Agency**: Radical transparency, universal hackability
//! - **Continuous Context**: Events as parts of coherent sessions
//! - **Feedback-Driven Growth**: Friction becomes improvement signal
//! 
//! ## Implementation Status
//! 
//! Current implementation provides ~40-45% of envisioned capabilities:
//! 
//! | Component | Status | Coverage | 
//! |-----------|--------|----------|
//! | Satellite Architecture | ✅ Operational | 80% |
//! | Message Bus | ✅ Robust | 75% |
//! | Data Substrate | ✅ Mature | 70% |
//! | Event Sources | 🚧 Expanding | 50% |
//! | Automata | 🚧 Active | 40% |
//! 
//! ## Quick Start
//! 
//! ```rust
//! use sinex::{SatelliteBuilder, EventStream};
//! 
//! // Build a custom satellite
//! let satellite = SatelliteBuilder::new("my-source")
//!     .with_event_stream(EventStream::filesystem("/home/user"))
//!     .build()?;
//! 
//! // Run with automatic checkpointing
//! satellite.run().await?;
//! ```
//! 
//! ## Architecture Overview
//! 
//! Sinex uses a satellite constellation pattern where independent services capture
//! events and feed them into a central data substrate:
//! 
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        User Interfaces                           │
//! │         CLI (exo.py)    │    Future: Web UI    │   Neovim       │
//! └────────────────────────┬────────────────────────────────────────┘
//!                          │
//! ┌────────────────────────┴────────────────────────────────────────┐
//! │                      sinex-gateway                               │
//! │            API Gateway & Command/Response Handler                │
//! └────────────────────────┬────────────────────────────────────────┘
//!                          │
//! ┌────────────────────────┴────────────────────────────────────────┐
//! │                   Message Bus (Redis Streams)                    │
//! │      Real-time Event Distribution & Consumer Groups              │
//! └───┬────────────────────┴────────────────────────────────────┬───┘
//!     │                                                        │
//! ┌───▼──────────────────┐  ┌─────────────────────────────────▼───┐
//! │   Satellite Services  │  │        Data Substrate               │
//! │  Event Sources &      │  │   PostgreSQL + TimescaleDB          │
//! │  Automata             │  │   ULID Keys + JSON Schema           │
//! └───────────────────────┘  └─────────────────────────────────────┘
//! ```
//! 
//! ## Modules
//! 
//! - [`events`]: Event types and builders
//! - [`satellites`]: Satellite implementation framework
//! - [`substrate`]: Database and storage layer
//! - [`automata`]: Event processing and analysis
//! 
//! ## Further Reading
//! 
//! - [Architecture Guide](https://sinex.dev/architecture)
//! - [Satellite Development](https://sinex.dev/guides/satellites)
//! - [Contributing](https://sinex.dev/contributing)

#![doc(html_logo_url = "https://sinex.dev/logo.svg")]
#![doc(html_favicon_url = "https://sinex.dev/favicon.ico")]

pub mod events;
pub mod satellites;
pub mod substrate;
pub mod automata;
```

## Example 2: TIM → Type Documentation

### Original (TIM-EventSubstrateDDL.md excerpt)
```markdown
# TIM-EventSubstrateDDL: Core DDL for `core.events` and Foundational Schema Objects

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 95% (Core schema fully deployed, TimescaleDB integration operational)
**Dependencies**: PostgreSQL, pgx_ulid extension, TimescaleDB
**Blocks**: All event ingestion, promotion pipelines, AI processing
```

### Rustdoc Translation (in `crate/sinex-db/src/models/event.rs`)
```rust
/// Core event structure for the Sinex data substrate.
/// 
/// # Technical Implementation Module (TIM-EventSubstrateDDL)
/// 
/// **Maturity Level**: L4 - Implemented  
/// **Implementation**: 95% (Core schema fully deployed, TimescaleDB integration operational)  
/// **Dependencies**: PostgreSQL, pgx_ulid extension, TimescaleDB  
/// **Blocks**: All event ingestion, promotion pipelines, AI processing  
/// 
/// ## Overview
/// 
/// The `Event` type represents the fundamental unit of data in Sinex, mapping to the
/// `core.events` table in PostgreSQL. Events are immutable records that capture all
/// digital experiences with comprehensive metadata.
/// 
/// ## Database Schema
/// 
/// ```sql
/// CREATE TABLE IF NOT EXISTS core.events (
///     id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
///     source                  TEXT NOT NULL,
///     event_type              TEXT NOT NULL,
///     ts_ingest               TIMESTAMPTZ NOT NULL DEFAULT now(),
///     ts_orig                 TIMESTAMPTZ,
///     host                    TEXT NOT NULL,
///     ingestor_version        TEXT,
///     payload_schema_id       ULID NULLABLE REFERENCES sinex_schemas.event_payload_schemas(id),
///     payload                 JSONB NOT NULL
/// );
/// ```
/// 
/// ## Design Principles
/// 
/// 1. **Immutability**: Events are never modified after creation
/// 2. **Time-ordering**: ULID primary keys provide natural chronological sort
/// 3. **Schema validation**: JSON Schema ensures payload structure integrity
/// 4. **Provenance tracking**: Source event IDs enable full lineage reconstruction
/// 
/// ## Implementation Checklist
/// 
/// - [x] Database migrations
/// - [x] Core table structure (core.events)
/// - [x] Schema organization
/// - [x] Primary and performance indexes
/// - [x] ULID integration
/// - [x] TimescaleDB hypertable setup
/// - [x] Trigger functions
/// - [x] Documentation
/// - [ ] Retention policy automation
/// - [ ] Query optimization analysis
/// 
/// ## Performance Characteristics
/// 
/// With proper indexing and TimescaleDB chunking:
/// - Insert rate: 50,000+ events/second
/// - Query performance: <10ms for time-range queries
/// - Storage efficiency: ~200 bytes/event average
/// 
/// ## Examples
/// 
/// ### Creating an event
/// 
/// ```rust
/// use sinex_db::models::{Event, NewEvent};
/// use ulid::Ulid;
/// 
/// let event = NewEvent {
///     source: "filesystem".to_string(),
///     event_type: "file_created".to_string(),
///     host: hostname::get()?.to_string_lossy().to_string(),
///     payload: json!({
///         "path": "/home/user/document.txt",
///         "size": 1024,
///         "permissions": "644"
///     }),
///     ts_orig: Some(Utc::now()),
///     ingestor_version: Some("1.0.0".to_string()),
///     payload_schema_id: schema_registry.get_schema_id("filesystem.file_created")?,
/// };
/// 
/// let stored_event = event.insert(&mut conn).await?;
/// ```
/// 
/// ### Querying events
/// 
/// ```rust
/// use sinex_db::queries::EventQuery;
/// 
/// let recent_files = EventQuery::new()
///     .source("filesystem")
///     .event_type("file_created")
///     .after(Utc::now() - Duration::hours(1))
///     .limit(100)
///     .execute(&mut conn)
///     .await?;
/// ```
/// 
/// ## Migration Path
/// 
/// For systems migrating from v1 schema:
/// 1. Run migration 20250720000001_fix_events_default_and_hypertable.sql
/// 2. Update ULID generation to use pgx_ulid
/// 3. Rebuild indexes for optimal performance
/// 
/// ## See Also
/// 
/// - [`EventBuilder`]: Ergonomic event construction
/// - [`EventQuery`]: Flexible query interface
/// - [`SchemaRegistry`]: Payload validation
/// - [ADR-001](https://sinex.dev/adr/001-primary-keys): ULID key decision
#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = events)]
pub struct Event {
    /// Globally unique, time-ordered identifier (ULID)
    pub id: Ulid,
    
    /// Canonical source identifier (e.g., "desktop.clipboard", "terminal.kitty")
    pub source: String,
    
    /// Type within source domain (e.g., "copy", "paste", "command_executed")
    pub event_type: String,
    
    /// Ingestion timestamp (database server time, TimescaleDB partition key)
    pub ts_ingest: DateTime<Utc>,
    
    /// Original event timestamp from source system (best effort)
    pub ts_orig: Option<DateTime<Utc>>,
    
    /// Host machine identifier
    pub host: String,
    
    /// Version of ingestor that created this event
    pub ingestor_version: Option<String>,
    
    /// Reference to validated JSON Schema
    pub payload_schema_id: Option<Ulid>,
    
    /// Complete event data as JSON
    pub payload: Value,
}
```

## Example 3: ADR → Decision Documentation

### Original (ADR-001-PrimaryKeyStrategy.md excerpt)
```markdown
# ADR-001: Primary Key Strategy for Core Tables

*   **Status:** Implemented
*   **Date:** 2024-03-11 (Updated to reflect `pgx_ulid` adoption)
*   **Context & Problem Statement:**
    The Sinnix Exocortex requires a robust and efficient primary key strategy...
```

### Rustdoc Translation (in `crate/sinex-core-types/src/ids.rs`)
```rust
/// Primary key type for all Sinex entities.
/// 
/// # Architectural Decision Record (ADR-001)
/// 
/// **Status**: Implemented  
/// **Decision Date**: 2024-03-11  
/// **Implementation Date**: 2025-07-17  
/// 
/// ## Context & Problem Statement
/// 
/// Sinex requires a robust primary key strategy for high-volume, time-ordered data.
/// The strategy must address:
/// 
/// 1. **Index Efficiency**: Minimize B-tree bloat and fragmentation
/// 2. **Time-Ordering**: Keys should be naturally sortable by time
/// 3. **Global Uniqueness**: Support distributed generation
/// 4. **Performance**: Efficient generation and comparison
/// 5. **Developer Experience**: Good ecosystem support
/// 6. **Storage Size**: Reasonably compact
/// 
/// ## Decision
/// 
/// We use **ULIDs via the pgx_ulid PostgreSQL extension** for all primary keys.
/// 
/// ### Rationale
/// 
/// 1. **Best of Both Worlds**: Time-ordering benefits with native PostgreSQL support
/// 2. **Performance**: 30% faster generation than UUIDs in benchmarks
/// 3. **Rich Features**: Timestamp casting, monotonic generation
/// 4. **Binary Storage**: Efficient 16-byte storage (same as UUID)
/// 5. **Ecosystem Alignment**: pgx_ulid written in Rust aligns with our stack
/// 
/// ### Alternatives Considered
/// 
/// | Option | Pros | Cons | Decision |
/// |--------|------|------|----------|
/// | UUIDv4 | Standard, widely supported | Random = poor index locality | ❌ Rejected |
/// | UUIDv7 | Time-ordered, standard | Less mature ecosystem | ❌ Rejected |
/// | Custom ULID | No dependencies | Complex implementation | ❌ Rejected |
/// | pgx_ulid | All ULID benefits + native PG | External dependency | ✅ **Chosen** |
/// 
/// ## Implementation
/// 
/// ```sql
/// -- PostgreSQL side
/// CREATE EXTENSION pgx_ulid;
/// 
/// CREATE TABLE events (
///     id ULID PRIMARY KEY DEFAULT gen_ulid(),
///     -- ...
/// );
/// ```
/// 
/// ```rust
/// // Rust side
/// use ulid::Ulid;
/// 
/// let id = Ulid::new(); // Generates time-ordered ID
/// ```
/// 
/// ## Consequences
/// 
/// ### Positive
/// - Sequential inserts improve index performance
/// - Natural time-based partitioning
/// - Can extract timestamp from ID
/// - Sortable across distributed systems
/// 
/// ### Negative  
/// - Requires pgx_ulid extension installation
/// - 26-character string representation (vs 36 for UUID)
/// - Must handle in application code
/// 
/// ### Mitigation
/// - NixOS module handles extension installation
/// - Type wrappers provide ergonomic API
/// - Comprehensive documentation
/// 
/// ## Usage Examples
/// 
/// ```rust
/// use sinex_core_types::{EventId, Timestamp};
/// 
/// // Generate new ID
/// let id = EventId::new();
/// 
/// // Extract timestamp
/// let timestamp: Timestamp = id.timestamp();
/// 
/// // Compare chronologically
/// assert!(newer_id > older_id);
/// ```
/// 
/// ## See Also
/// 
/// - [`Ulid`]: The underlying ULID type
/// - [pgx_ulid documentation](https://github.com/pksunkara/pgx_ulid)
/// - [ULID specification](https://github.com/ulid/spec)
pub type EventId = Ulid;

/// Generate a new event ID with current timestamp.
/// 
/// This is a convenience wrapper around `Ulid::new()` that ensures
/// consistent ID generation throughout the codebase.
/// 
/// # Examples
/// 
/// ```rust
/// let id = sinex_core_types::generate_event_id();
/// println!("Generated ID: {}", id);
/// ```
#[inline]
pub fn generate_event_id() -> EventId {
    Ulid::new()
}
```

## Example 4: GLOSSARY.md → Searchable Module

### Original (GLOSSARY.md excerpt)
```markdown
# Sinex: Project Glossary

## A

*   **ADR (Architectural Decision Record):** A document that captures a single significant architectural decision...
*   **Agent:** A modular software component, often a systemd service...
```

### Rustdoc Translation (in `crate/sinex-core/src/glossary.rs`)
```rust
//! # Sinex Glossary
//! 
//! Comprehensive terminology reference for the Sinex system.
//! 
//! This module serves as a living glossary, with terms organized alphabetically
//! and cross-referenced to their implementations.

/// Architectural Decision Record (ADR)
/// 
/// A document capturing a significant architectural decision, including context,
/// rationale, and consequences. ADRs are embedded throughout the codebase at
/// decision points.
/// 
/// ## Examples in Codebase
/// 
/// - [`EventId`](crate::types::EventId): Documents ULID decision (ADR-001)
/// - [`StatefulStreamProcessor`](crate::processor::StatefulStreamProcessor): Unified interface (ADR-010)
/// - [`RedisStreams`](crate::bus::RedisStreams): Message bus choice (ADR-002)
/// 
/// ## Format
/// 
/// Each ADR includes:
/// - Status (Proposed, Accepted, Deprecated, Superseded)
/// - Context and problem statement
/// - Considered options with pros/cons
/// - Decision and rationale
/// - Consequences (positive and negative)
pub struct Adr;

/// Agent (Deprecated term, see Satellite)
/// 
/// Historical term for modular components. Modern Sinex uses "Satellite" for
/// services in the constellation architecture.
/// 
/// ## Migration
/// 
/// ```rust
/// // Old terminology
/// pub struct Agent { /* ... */ }
/// 
/// // New terminology
/// pub struct Satellite { /* ... */ }
/// ```
/// 
/// See [`Satellite`] for current implementation.
#[deprecated(since = "0.5.0", note = "Use Satellite terminology instead")]
pub struct Agent;

/// Satellite
/// 
/// An independent service in the Sinex constellation architecture. Satellites
/// can be event sources (capturing data) or automata (processing events).
/// 
/// ## Architecture
/// 
/// All satellites implement [`StatefulStreamProcessor`](crate::processor::StatefulStreamProcessor)
/// and run as systemd services orchestrated by NixOS.
/// 
/// ## Types
/// 
/// ### Event Source Satellites
/// - [`FilesystemSatellite`]: Monitor file system changes
/// - [`TerminalSatellite`]: Capture terminal activity
/// - [`ClipboardSatellite`]: Track clipboard operations
/// - [`SystemSatellite`]: System-level events
/// 
/// ### Automaton Satellites  
/// - [`HealthAggregator`]: System health analysis
/// - [`CommandCanonicalizer`]: Normalize shell commands
/// - [`ContentIndexer`]: Extract searchable content
/// 
/// ## Implementation
/// 
/// ```rust
/// use sinex_satellite_sdk::{Satellite, StatefulStreamProcessor};
/// 
/// struct CustomSatellite;
/// 
/// impl StatefulStreamProcessor for CustomSatellite {
///     // Implementation...
/// }
/// ```
pub struct Satellite;

/// ULID (Universally Unique Lexicographically Sortable Identifier)
/// 
/// Time-ordered identifiers used as primary keys throughout Sinex. ULIDs provide
/// natural chronological sorting while maintaining global uniqueness.
/// 
/// ## Structure
/// 
/// ```text
///  01AN4Z07BY      79KA1307SR9X4MV3
/// |----------|    |----------------|
///  Timestamp          Randomness
///    48bits             80bits
/// ```
/// 
/// ## Properties
/// 
/// - **Lexicographic sorting**: String representation sorts chronologically
/// - **Timestamp extraction**: First 48 bits encode milliseconds since epoch
/// - **Collision resistance**: 80 bits of randomness
/// - **Compact encoding**: 26-character Base32 representation
/// 
/// ## Usage
/// 
/// ```rust
/// use ulid::Ulid;
/// 
/// let id = Ulid::new();
/// let timestamp = id.timestamp_ms();
/// let string_repr = id.to_string(); // 26 characters
/// ```
/// 
/// ## Database Integration
/// 
/// Via pgx_ulid extension:
/// 
/// ```sql
/// CREATE TABLE events (
///     id ULID PRIMARY KEY DEFAULT gen_ulid()
/// );
/// ```
/// 
/// See [ADR-001](crate::types::EventId) for design rationale.
pub struct Ulid;

/// Generate glossary index for documentation.
/// 
/// This macro generates a searchable index of all glossary terms.
/// 
/// # Example
/// 
/// ```rust
/// glossary_index! {
///     ADR => "Architectural Decision Record",
///     Agent => "Deprecated, see Satellite", 
///     Satellite => "Independent service in constellation",
///     ULID => "Universally Unique Lexicographically Sortable ID",
///     // ... more terms
/// }
/// ```
#[macro_export]
macro_rules! glossary_index {
    ($($term:ident => $brief:expr),* $(,)?) => {
        /// Glossary index for quick reference.
        pub const GLOSSARY: &[(&str, &str)] = &[
            $(
                (stringify!($term), $brief),
            )*
        ];
    };
}
```

## Example 5: Architecture Module → Module Documentation

### Original (DataSubstrate_Architecture.md excerpt)
```markdown
# Sinex Data Substrate Architecture

The Data Substrate represents the foundational storage and structuring layer...
```

### Rustdoc Translation (in `crate/sinex-substrate/src/lib.rs`)
```rust
//! # Sinex Data Substrate
//! 
//! The foundational storage and structuring layer of the Sinex system.
//! 
//! ## Overview
//! 
//! The Data Substrate provides:
//! - Time-series optimized event storage
//! - Knowledge graph representation
//! - Artifact and blob management  
//! - Schema validation and evolution
//! - High-performance querying
//! 
//! ## Architecture Layers
//! 
//! ```text
//! ┌─────────────────────────────────────────┐
//! │          Query Interface                │
//! │    (SQL, GraphQL, Full-text Search)     │
//! ├─────────────────────────────────────────┤
//! │         Schema Management               │
//! │   (JSON Schema, Migrations, GitOps)     │
//! ├─────────────────────────────────────────┤
//! │          Core Data Model                │
//! │  (Events, Entities, Relations, Blobs)   │
//! ├─────────────────────────────────────────┤
//! │      PostgreSQL + Extensions            │
//! │ (TimescaleDB, pgvector, pg_jsonschema) │
//! └─────────────────────────────────────────┘
//! ```
//! 
//! ## Core Tables
//! 
//! ### Events (`core.events`)
//! 
//! The immutable event log capturing all system activity:
//! 
//! ```sql
//! CREATE TABLE core.events (
//!     id          ULID PRIMARY KEY,
//!     source      TEXT NOT NULL,
//!     event_type  TEXT NOT NULL,
//!     ts_ingest   TIMESTAMPTZ NOT NULL,
//!     ts_orig     TIMESTAMPTZ,
//!     payload     JSONB NOT NULL
//! );
//! ```
//! 
//! ### Knowledge Graph
//! 
//! Entities and their relationships:
//! 
//! ```sql
//! CREATE TABLE core.entities (
//!     id          ULID PRIMARY KEY,
//!     name        TEXT NOT NULL,
//!     entity_type TEXT NOT NULL
//! );
//! 
//! CREATE TABLE core.entity_relations (
//!     from_id     ULID REFERENCES entities(id),
//!     to_id       ULID REFERENCES entities(id),
//!     relation    TEXT NOT NULL,
//!     confidence  FLOAT
//! );
//! ```
//! 
//! ## Design Principles
//! 
//! 1. **Immutability First**: Events never change; derived data is regenerable
//! 2. **Time as First-Class**: All data is temporally anchored
//! 3. **Schema Evolution**: Backward-compatible changes without downtime
//! 4. **Provenance Tracking**: Complete audit trail for all data
//! 5. **Performance at Scale**: Optimized for billions of events
//! 
//! ## Usage Patterns
//! 
//! ### Event Ingestion
//! 
//! ```rust
//! use sinex_substrate::{EventStore, NewEvent};
//! 
//! let store = EventStore::connect(&db_url).await?;
//! 
//! let event = NewEvent::builder()
//!     .source("filesystem")
//!     .event_type("file_modified")
//!     .payload(json!({ "path": "/home/user/notes.md" }))
//!     .build();
//! 
//! store.insert_event(event).await?;
//! ```
//! 
//! ### Knowledge Graph Operations
//! 
//! ```rust
//! use sinex_substrate::{KnowledgeGraph, Entity, Relation};
//! 
//! let graph = KnowledgeGraph::new(&db_pool);
//! 
//! // Create entities
//! let person = Entity::new("John Doe", "person");
//! let project = Entity::new("Sinex", "project");
//! 
//! // Create relationship
//! let relation = Relation::new(&person, &project, "contributes_to")
//!     .with_confidence(0.95);
//! 
//! graph.add_entity(person).await?;
//! graph.add_entity(project).await?;
//! graph.add_relation(relation).await?;
//! ```
//! 
//! ### Time-Range Queries
//! 
//! ```rust
//! use sinex_substrate::queries::{EventQuery, TimeRange};
//! 
//! let events = EventQuery::new()
//!     .time_range(TimeRange::last_hour())
//!     .source("browser")
//!     .event_types(&["page_visit", "download"])
//!     .with_payload_path("$.domain", "github.com")
//!     .execute(&store)
//!     .await?;
//! ```
//! 
//! ## Performance Characteristics
//! 
//! With proper configuration:
//! 
//! | Operation | Throughput | Latency |
//! |-----------|------------|---------|
//! | Event Insert | 50K/sec | <1ms |
//! | Time-range Query | - | <10ms |
//! | Full-text Search | - | <50ms |
//! | Graph Traversal | - | <100ms |
//! 
//! ## Modules
//! 
//! - [`events`]: Event storage and querying
//! - [`knowledge`]: Knowledge graph operations
//! - [`schema`]: Schema management and validation
//! - [`migrations`]: Database migration framework
//! - [`metrics`]: Performance monitoring
//! 
//! ## See Also
//! 
//! - [TimescaleDB Configuration](crate::config::timescale)
//! - [Schema Evolution Guide](https://sinex.dev/guides/schema-evolution)
//! - [Performance Tuning](https://sinex.dev/guides/performance)

pub mod events;
pub mod knowledge;
pub mod schema;
pub mod migrations;
pub mod metrics;
```

## Summary

These examples demonstrate how different types of specification documents can be effectively translated to rustdoc:

1. **Vision/Philosophy** → Crate-level documentation with status tracking
2. **Technical Implementation Modules** → Rich type/struct documentation
3. **Architectural Decisions** → Inline decision documentation at implementation points
4. **Glossaries** → Searchable module with cross-references
5. **Architecture Modules** → Comprehensive module documentation

The key advantages of this approach:
- Documentation lives with code and stays synchronized
- Examples are compiler-checked
- Cross-references are automatic and validated
- IDE integration provides hover docs and go-to-definition
- Single source of truth for both code and architecture