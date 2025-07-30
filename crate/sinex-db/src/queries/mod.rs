//! Centralized query registry for all database operations
//!
//! This module provides a centralized location for all SQL queries used across the
//! Sinex codebase, organized by domain and with automatic ULID/UUID conversion.
//!
//! # Organization
//!
//! Queries are organized by domain:
//! - `events`: Raw event storage and retrieval
//! - `checkpoints`: Automaton checkpoint management
//! - `schemas`: Schema validation and metadata
//! - `operations`: System operations and health checks
//! - `processor_manifests`: Processor registration and management
//! - `verification`: Preflight and integration testing
//! - `knowledge_graph`: Entity and relation management
//!
//! # Usage
//!
//! ```rust
//! use sinex_db::queries::{EventQueries, CheckpointQueries};
//!
//! // Get an event by ID
//! let event = EventQueries::get_by_id(event_id).fetch_one(pool).await?;
//!
//! // Save a checkpoint
//! let checkpoint = CheckpointQueries::upsert_checkpoint(
//!     "my-processor",
//!     "default",
//!     "hostname-1234",
//!     &checkpoint_data
//! ).execute(pool).await?;
//! ```
//!
//! # Benefits
//!
//! - **Centralized**: All queries in one location
//! - **Type-safe**: Automatic ULID/UUID conversion
//! - **Consistent**: Uniform error handling and patterns
//! - **Maintainable**: Easy to update and refactor
//! - **Performant**: Prepared statements and connection pooling

pub mod annotations;
pub mod checkpoints;
pub mod events;
pub mod integrity;
pub mod knowledge_graph;
pub mod operations;
pub mod processor_manifests;
pub mod schemas;
pub mod source_material;
pub mod validation;
pub mod verification;

// Re-export query structs for easier access
pub use annotations::AnnotationQueries;
pub use checkpoints::CheckpointQueries;
pub use events::EventQueries;
pub use integrity::IntegrityQueries;
pub use knowledge_graph::KnowledgeGraphQueries;
pub use operations::OperationQueries;
pub use processor_manifests::ProcessorManifestQueries;
pub use schemas::SchemaQueries;
pub use source_material::SourceMaterialQueries;
pub use validation::ValidationQueries;
pub use verification::VerificationQueries;
