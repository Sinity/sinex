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
//! - `artifacts`: Blob and artifact management
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
pub mod artifacts;
pub mod checkpoints;
pub mod events;
pub mod operations;
pub mod schemas;

// Re-export query structs for easier access
pub use annotations::AnnotationQueries;
pub use artifacts::ArtifactQueries;
pub use checkpoints::CheckpointQueries;
pub use events::EventQueries;
pub use operations::OperationQueries;
pub use schemas::SchemaQueries;
