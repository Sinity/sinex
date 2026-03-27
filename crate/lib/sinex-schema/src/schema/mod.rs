//! The Canonical Database Schema for the Sinex System.
//!
//! This module and its submodules provide the definitive, single source of truth for
//! the entire Sinex database schema. It uses `sea-query` to programmatically define
//! all tables, columns, indexes, and constraints, ensuring type-safety and
//! maintainability.

use sea_query::Alias;

// Define the core schema modules. Each file is responsible for a logical
// domain of the database.
pub mod annotations;
pub mod blobs;
pub mod embeddings;
pub mod entities;
pub mod events;
pub mod operations;
pub mod sinex_schemas;
pub mod source_materials;
pub mod temporal_ledger;

// Re-export all schema definitions for easy access from apply orchestration and repositories.
pub use annotations::*;
pub use blobs::*;
pub use embeddings::*;
pub use entities::*;
pub use events::*;
pub use operations::*;
pub use sinex_schemas::*;
pub use source_materials::*;
pub use temporal_ledger::*;

// Create a records submodule that re-exports all Record structs
pub mod records {
    pub use super::annotations::{EventAnnotationRecord, TagRecord};
    pub use super::blobs::BlobRecord;
    pub use super::embeddings::EmbeddingModelRecord;
    pub use super::entities::EntityRecord;
    pub use super::events::{EventRecord, EventReplacementRecord};
    pub use super::sinex_schemas::{EventPayloadSchemaRecord, NodeManifestRecord, NodeRunRecord};
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
    #[must_use]
    fn table_iden() -> (Alias, Alias) {
        (
            Alias::new(Self::schema_name()),
            Alias::new(Self::table_name()),
        )
    }
}

/// Declarative metadata for schema-managed tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableMeta {
    pub schema: &'static str,
    pub name: &'static str,
    pub qualified_name: &'static str,
    pub is_hypertable: bool,
    pub has_triggers: bool,
    pub cleanup_protected: bool,
}

const ALL_TABLES: &[TableMeta] = &[
    TableMeta {
        schema: "raw",
        name: "temporal_ledger",
        qualified_name: "raw.temporal_ledger",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "audit",
        name: "archived_events",
        qualified_name: "audit.archived_events",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "event_tombstones",
        qualified_name: "core.event_tombstones",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "event_annotations",
        qualified_name: "core.event_annotations",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "event_cluster_members",
        qualified_name: "core.event_cluster_members",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "event_embeddings",
        qualified_name: "core.event_embeddings",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "embedding_cache",
        qualified_name: "core.embedding_cache",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "embedding_models",
        qualified_name: "core.embedding_models",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "entity_relations",
        qualified_name: "core.entity_relations",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "node_manifests",
        qualified_name: "core.node_manifests",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "sinex_schemas",
        name: "event_payload_schemas",
        qualified_name: "sinex_schemas.event_payload_schemas",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: true,
    },
    TableMeta {
        schema: "sinex_schemas",
        name: "validation_cache",
        qualified_name: "sinex_schemas.validation_cache",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "sinex_schemas",
        name: "gitops_schema_sources",
        qualified_name: "sinex_schemas.gitops_schema_sources",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "sinex_schemas",
        name: "dlq_events",
        qualified_name: "sinex_schemas.dlq_events",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "operations_log",
        qualified_name: "core.operations_log",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "tags",
        qualified_name: "core.tags",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "tagged_items",
        qualified_name: "core.tagged_items",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "blobs",
        qualified_name: "core.blobs",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "entities",
        qualified_name: "core.entities",
        is_hypertable: false,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "event_clusters",
        qualified_name: "core.event_clusters",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "raw",
        name: "source_material_registry",
        qualified_name: "raw.source_material_registry",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "node_runs",
        qualified_name: "core.node_runs",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "core",
        name: "events",
        qualified_name: "core.events",
        is_hypertable: true,
        has_triggers: true,
        cleanup_protected: false,
    },
    TableMeta {
        schema: "audit",
        name: "event_replacements",
        qualified_name: "audit.event_replacements",
        is_hypertable: false,
        has_triggers: false,
        cleanup_protected: false,
    },
];

#[must_use]
pub fn all_tables() -> &'static [TableMeta] {
    ALL_TABLES
}
