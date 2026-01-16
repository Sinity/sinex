//! The Canonical Database Schema for the Sinex System.
//!
//! This module and its submodules provide the definitive, single source of truth for
//! the entire Sinex database schema. It uses `sea-query` to programmatically define
//! all tables, columns, indexes, and constraints, ensuring type-safety and
//! maintainability.

use sea_orm_migration::prelude::*;

// Define the core schema modules. Each file is responsible for a logical
// domain of the database.
pub mod annotations;
pub mod blobs;
pub mod embeddings;
pub mod entities;
pub mod events;
pub mod outbox;
pub mod processors;
pub mod sinex_schemas;
pub mod source_materials;
pub mod temporal_ledger;

// Re-export all schema definitions for easy access from migrations and repositories.
pub use annotations::*;
pub use blobs::*;
pub use embeddings::*;
pub use entities::*;
pub use events::*;
pub use outbox::*;
pub use processors::*;
pub use sinex_schemas::*;
pub use source_materials::*;
pub use temporal_ledger::*;

// Create a records submodule that re-exports all Record structs
pub mod records {
    pub use super::annotations::{EventAnnotationRecord, TagRecord};
    pub use super::blobs::BlobRecord;
    pub use super::embeddings::EmbeddingModelRecord;
    pub use super::entities::EntityRecord;
    pub use super::events::EventRecord;
    pub use super::outbox::OutboxRecord;
    pub use super::sinex_schemas::{EventPayloadSchemaRecord, ProcessorManifestRecord};
    pub use super::source_materials::SourceMaterialRecord;
    pub use super::temporal_ledger::TemporalLedgerRecord;
}

/// A unifying trait for all table schema definitions.
///
/// This trait provides a consistent interface for accessing fundamental schema
/// metadata (names, primary keys), enabling the creation of generic repository
/// functions and ensuring that all schema interactions are type-safe and
/// driven from this single source of truth.
pub trait TableDef: Copy + Clone {
    /// The name of the table in the database (e.g., "events").
    fn table_name() -> &'static str;

    /// The name of the schema the table belongs to (e.g., "core").
    fn schema_name() -> &'static str;

    /// The name of the primary key column for this table.
    fn primary_key() -> &'static str;

    /// Returns a `sea-query` compatible identifier for the table, including its schema.
    fn table_iden() -> (Alias, Alias) {
        (
            Alias::new(Self::schema_name()),
            Alias::new(Self::table_name()),
        )
    }
}
