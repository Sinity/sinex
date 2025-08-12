//! Database schema definitions using SeaQuery
//!
//! This module provides type-safe schema definitions for all database tables
//! using SeaQuery's table definition API.

use sea_query::Alias;

// Import all schema modules
pub mod annotations;
pub mod blobs;
pub mod core_events;
pub mod embeddings;
pub mod entities;
pub mod event_relations;
pub mod knowledge_graph;
pub mod outbox;
pub mod processors;
pub mod records;
pub mod schemas;
pub mod sensor_jobs;
pub mod source_materials;
pub mod temporal_ledger;

// Re-export everything from modules
pub use annotations::*;
pub use blobs::*;
pub use core_events::*;
pub use embeddings::*;
pub use entities::*;
pub use event_relations::*;
pub use knowledge_graph::*;
pub use outbox::*;
pub use processors::*;
pub use records::*;
// Re-export key record types for backwards compatibility
pub use records::{BlobRecord, EventRecord, SourceMaterialRecord};
pub use schemas::*;
pub use sensor_jobs::*;
pub use source_materials::*;
pub use temporal_ledger::*;

/// Trait for table definitions that can be used with generic repository operations
pub trait TableDef: Copy + Clone {
    /// Get the table name
    fn table_name() -> &'static str;

    /// Get the schema name
    fn schema_name() -> &'static str;

    /// Get the primary key column name
    fn primary_key() -> &'static str;

    /// Get the full table identifier (schema.table)
    fn table_iden() -> (Alias, Alias) {
        (
            Alias::new(Self::schema_name()),
            Alias::new(Self::table_name()),
        )
    }
}
