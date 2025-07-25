//! # Sinex Glossary
//! 
//! Core terminology and concepts specific to the Sinex system.
//! 
//! This module provides searchable documentation for Sinex-specific terms,
//! with cross-references to their implementations throughout the codebase.

/// Satellite
/// 
/// An independent service in the Sinex constellation architecture. Satellites
/// can be event sources (capturing data) or automata (processing events).
/// 
/// All satellites implement [`StatefulStreamProcessor`](crate::stream_processor::StatefulStreamProcessor)
/// and run as systemd services orchestrated by NixOS.
/// 
/// ## Types
/// 
/// - **Event Source Satellites**: Capture domain-specific events
///   - `sinex-fs-watcher`: File system changes
///   - `sinex-terminal-satellite`: Terminal activity
///   - `sinex-desktop-satellite`: Desktop events (clipboard, window manager)
///   - `sinex-system-satellite`: System events
/// 
/// - **Automaton Satellites**: Process events to derive insights
///   - `sinex-health-automaton`: System health analysis
///   - `sinex-canonicalizer-automaton`: Command normalization
///   - Future: content indexer, pattern detector
/// 
/// ## Architecture
/// 
/// Satellites communicate via:
/// - gRPC with `sinex-ingestd` for event submission
/// - Redis Streams for real-time event consumption
/// - PostgreSQL for checkpoint persistence
pub struct Satellite;

/// Event
/// 
/// The atomic unit of data in Sinex. An immutable record stored in the
/// `core.events` table with comprehensive metadata and provenance tracking.
/// 
/// ## Structure
/// 
/// - `id`: ULID primary key (time-ordered, globally unique)
/// - `source`: Canonical identifier (e.g., "filesystem", "terminal.kitty")
/// - `event_type`: Type within source domain (e.g., "file_created", "command_executed")
/// - `ts_ingest`: Database ingestion timestamp (partitioning key)
/// - `ts_orig`: Original event timestamp from source
/// - `payload`: JSONB data with optional schema validation
/// 
/// ## Design Principles
/// 
/// - **Immutable**: Never modified after creation
/// - **Time-ordered**: ULID keys provide natural chronological sorting
/// - **Self-describing**: Payload schema tracked via `payload_schema_id`
/// - **Traceable**: Source event IDs enable full provenance chains
/// 
/// See [`RawEvent`](crate::RawEvent) for the Rust type.
pub struct Event;

/// StatefulStreamProcessor
/// 
/// The unified interface implemented by all Sinex satellites. Provides
/// consistent lifecycle management, checkpointing, and replay capabilities.
/// 
/// ## Core Method
/// 
/// ```rust,ignore
/// async fn scan(&mut self, from: Checkpoint, until: TimeHorizon) -> Result<ScanReport>
/// ```
/// 
/// ## Capabilities
/// 
/// - **Checkpoint-based recovery**: Resume from any point in time
/// - **Historical replay**: Reprocess past events with new logic
/// - **Unified monitoring**: Consistent heartbeats and metrics
/// - **Graceful shutdown**: Clean state persistence
/// 
/// This design enables both real-time processing and batch reprocessing
/// scenarios with the same code.
pub struct StatefulStreamProcessor;

/// Data Substrate
/// 
/// The foundational storage layer built on PostgreSQL with specialized extensions:
/// 
/// - **TimescaleDB**: Time-series optimization for events
/// - **pgx_ulid**: Time-ordered primary keys
/// - **pg_jsonschema**: Event payload validation
/// - **pgvector**: Future semantic search capabilities
/// 
/// ## Core Tables
/// 
/// - `core.events`: Immutable event log
/// - `core.automaton_checkpoints`: Processing state
/// - `raw.source_material_registry`: Original data preservation
/// - `sinex_schemas.processor_manifests`: Service metadata
/// - `sinex_schemas.event_payload_schemas`: Validation schemas
pub struct DataSubstrate;

/// Automaton
/// 
/// A satellite that processes events to derive insights, detect patterns,
/// or generate new events. Automata consume from Redis Streams and can
/// produce derived events back to the system.
/// 
/// ## Examples
/// 
/// - **Health Aggregator**: Monitors system health metrics
/// - **Command Canonicalizer**: Normalizes shell commands
/// - **Pattern Detector**: Identifies behavioral patterns
/// - **Content Indexer**: Extracts searchable content
/// 
/// Automata use the same [`StatefulStreamProcessor`] interface as
/// event sources, enabling unified operational management.
pub struct Automaton;

/// Checkpoint
/// 
/// Saved processing state enabling resumption after interruption. Contains:
/// 
/// - `processor_id`: Which satellite created it
/// - `checkpoint_id`: Unique identifier
/// - `checkpoint_time`: When created
/// - `checkpoint_data`: Processor-specific state (JSONB)
/// - `is_active`: Whether currently in use
/// 
/// Stored in `core.automaton_checkpoints` table. Enables:
/// - Crash recovery without data loss
/// - Planned maintenance with clean handoff
/// - Historical reprocessing from specific points
pub struct Checkpoint;

/// TimeHorizon
/// 
/// Specification for how far a processor should scan:
/// 
/// - `Historical(timestamp)`: Process up to specific time
/// - `Continuous`: Process indefinitely (real-time mode)
/// - `Snapshot`: Single point-in-time capture
/// 
/// Used in [`StatefulStreamProcessor::scan`] to control processing scope.
pub struct TimeHorizon;

/// ProcessorManifest
/// 
/// GitOps-driven metadata for satellites stored in `sinex_schemas.processor_manifests`:
/// 
/// - Processor capabilities and version
/// - Configuration schema
/// - Supported event types
/// - Resource requirements
/// 
/// Enables service discovery and compatibility checking.
pub struct ProcessorManifest;

/// SourceMaterialRegistry
/// 
/// Immutable ground truth preservation in `raw.source_material_registry`.
/// Links events to their original source data via `blob_id`, enabling:
/// 
/// - Audit trails to original data
/// - Reprocessing with updated logic
/// - Verification of transformations
/// 
/// Large content stored in git-annex, metadata in PostgreSQL.
pub struct SourceMaterialRegistry;

/// Exocortex
/// 
/// The overarching system concept - a "sentient archive" providing:
/// 
/// - Comprehensive digital experience capture
/// - Intelligent processing and analysis
/// - Powerful query capabilities
/// - Complete user sovereignty
/// 
/// The name combines "exo" (external) with "cortex" (brain), representing
/// an external cognitive augmentation system.
pub struct Exocortex;

/// ULID (in Sinex context)
/// 
/// Universally Unique Lexicographically Sortable Identifiers used as
/// primary keys throughout Sinex via the pgx_ulid PostgreSQL extension.
/// 
/// ## Why ULIDs?
/// 
/// - **Time-ordering**: Natural chronological sort without additional columns
/// - **Index efficiency**: Sequential inserts minimize B-tree fragmentation
/// - **Global uniqueness**: Safe for distributed generation
/// - **Performance**: 30% faster than UUID generation in benchmarks
/// 
/// See [ADR-001](crate::Ulid) for detailed rationale.
/// 
/// ## Usage
/// 
/// ```rust
/// use sinex_ulid::Ulid;
/// 
/// let event_id = Ulid::new();
/// let timestamp = event_id.timestamp_ms();
/// ```
pub struct UlidGlossary;

/// Living Document
/// 
/// A future Sinex component for dynamic, AI-augmented note-taking:
/// 
/// - Stream-of-consciousness capture
/// - Automatic structuring and linking
/// - Proactive AI assistance
/// - CRDT-based (Yjs) for real-time collaboration
/// 
/// Currently in planning phase. Will integrate with PKM features.
pub struct LivingDocument;

/// Friction-Driven Development
/// 
/// Sinex's development philosophy: prioritize features that alleviate
/// personally-felt pain points in daily workflow.
/// 
/// Instead of building speculatively, development focuses on:
/// - Actual daily frustrations
/// - Measurable time savings
/// - Workflow improvements with immediate benefit
/// 
/// This ensures the system evolves to serve real needs rather than
/// theoretical capabilities.
pub struct FrictionDrivenDevelopment;

/// Sentient Archive
/// 
/// The conceptual description of Sinex emphasizing its comprehensive
/// awareness of user context and capacity for intelligent assistance.
/// 
/// "Sentient" refers to:
/// - Awareness of temporal context
/// - Understanding of relationships between events
/// - Proactive pattern recognition
/// - Intelligent query capabilities
/// 
/// Not AGI, but rather deep integration with user's digital life.
pub struct SentientArchive;