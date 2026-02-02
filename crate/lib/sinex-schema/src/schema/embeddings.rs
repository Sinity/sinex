//! The Canonical Database Schema for Embeddings, Clustering, and ML Infrastructure.
//!
//! This module defines the tables necessary to transform the Sinex event log into a
//! semantically searchable and analyzable knowledge base. It leverages the `pgvector`
//! extension to store and query high-dimensional vector embeddings directly within
//! `PostgreSQL`, enabling powerful AI-driven features.

use crate::schema::{Events, TableDef};
use crate::ulid::Ulid;
use sea_orm_migration::prelude::*;
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// A constant representing the dimensions of the embedding vectors.
// This should be chosen based on the primary embedding model used.
// e.g., OpenAI's text-embedding-ada-002 uses 1536.
// TODO: Hardcoded to 1536. Needs to be dynamic or configurable (BUG-018).
const EMBEDDING_DIMENSIONS: u32 = 1536;

// =============================================================================
// ML Model & Cache Management
// =============================================================================

/// **Table: `core.embedding_models`**
///
/// A registry for all embedding and ML models used by the system. This allows the
/// system to track which model generated which embedding, which is critical for
///
/// provenance, cost tracking, and future model-specific operations.
#[derive(Iden, Copy, Clone)]
pub enum EmbeddingModels {
    Table,
    Id,
    Provider,
    ModelName,
    Dimensions,
    IsActive,
    Metadata,
}

impl TableDef for EmbeddingModels {
    fn table_name() -> &'static str {
        "embedding_models"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EmbeddingModelRecord {
    pub id: Ulid,
    pub provider: String,
    pub model_name: String,
    pub dimensions: i32,
    pub is_active: bool,
    pub metadata: JsonValue,
}

impl EmbeddingModels {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EmbeddingModels::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(ColumnDef::new(EmbeddingModels::Provider).text().not_null())
            .col(ColumnDef::new(EmbeddingModels::ModelName).text().not_null())
            .col(
                ColumnDef::new(EmbeddingModels::Dimensions)
                    .integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmbeddingModels::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(EmbeddingModels::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![Index::create()
            .name("uk_embedding_models_provider_model")
            .table(Self::table_iden())
            .col(EmbeddingModels::Provider)
            .col(EmbeddingModels::ModelName)
            .unique()
            .to_owned()]
    }
}

/// **Table: `core.embedding_cache`**
///
/// A critical performance optimization. This table caches the embedding vector for a
/// given piece of text and a given model. Automata *must* check this cache before
/// making an expensive API call to an embedding service.
#[derive(Iden, Copy, Clone)]
pub enum EmbeddingCache {
    Table,
    Id,
    TextHash,
    EmbeddingModelId,
    Embedding,
    TextSample,
    UseCount,
    LastUsedAt,
}

impl TableDef for EmbeddingCache {
    fn table_name() -> &'static str {
        "embedding_cache"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EmbeddingCache {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EmbeddingCache::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(ColumnDef::new(EmbeddingCache::TextHash).text().not_null()) // SHA-256 of the text content.
            .col(
                ColumnDef::new(EmbeddingCache::EmbeddingModelId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EmbeddingCache::Embedding)
                    .custom(Alias::new(format!("vector({EMBEDDING_DIMENSIONS})")))
                    .not_null(),
            )
            .col(ColumnDef::new(EmbeddingCache::TextSample).text()) // First few chars of the text for debugging.
            .col(
                ColumnDef::new(EmbeddingCache::UseCount)
                    .big_integer()
                    .not_null()
                    .default(1),
            )
            .col(
                ColumnDef::new(EmbeddingCache::LastUsedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EmbeddingCache::EmbeddingModelId)
                    .to(EmbeddingModels::table_iden(), EmbeddingModels::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![Index::create()
            .name("uk_embedding_cache_hash_model")
            .table(Self::table_iden())
            .col(EmbeddingCache::TextHash)
            .col(EmbeddingCache::EmbeddingModelId)
            .unique()
            .to_owned()]
    }

    /// Creates indexes, including the crucial vector index for similarity search.
    #[must_use]
    pub fn create_indexes_sql() -> Vec<String> {
        vec![
            // Standard index for quick lookups by text hash and model.
            format!("CREATE UNIQUE INDEX IF NOT EXISTS ux_embedding_cache_hash_model ON core.embedding_cache (text_hash, embedding_model_id);"),
            // A vector index is ESSENTIAL for performant similarity search. HNSW is generally preferred for its speed and accuracy.
            format!("CREATE INDEX IF NOT EXISTS ix_embedding_cache_vector ON core.embedding_cache USING hnsw (embedding vector_cosine_ops);"),
        ]
    }
}

// =============================================================================
// EVENT-LEVEL EMBEDDINGS & CLUSTERING
// =============================================================================

/// **Table: `core.event_embeddings`**
///
/// Stores the vector embedding for the textual content of an event. This enables
/// semantic search and clustering of events based on their meaning, not just their
/// metadata.
#[derive(Iden, Copy, Clone)]
pub enum EventEmbeddings {
    Table,
    Id,
    EventId,
    EmbeddingModelId,
    EmbeddedText,
    Embedding,
}

impl TableDef for EventEmbeddings {
    fn table_name() -> &'static str {
        "event_embeddings"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventEmbeddings {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventEmbeddings::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(EventEmbeddings::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventEmbeddings::EmbeddingModelId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventEmbeddings::EmbeddedText)
                    .text()
                    .not_null(),
            ) // The actual text that was embedded.
            .col(
                ColumnDef::new(EventEmbeddings::Embedding)
                    .custom(Alias::new(format!("vector({EMBEDDING_DIMENSIONS})")))
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EventEmbeddings::EventId)
                    .to(Events::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EventEmbeddings::EmbeddingModelId)
                    .to(EmbeddingModels::table_iden(), EmbeddingModels::Id),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![Index::create()
            .name("uk_event_embeddings_event_model")
            .table(Self::table_iden())
            .col(EventEmbeddings::EventId)
            .col(EventEmbeddings::EmbeddingModelId)
            .unique()
            .to_owned()]
    }

    #[must_use]
    pub fn create_indexes_sql() -> Vec<String> {
        vec![
            format!("CREATE INDEX IF NOT EXISTS ix_event_embeddings_vector ON core.event_embeddings USING hnsw (embedding vector_cosine_ops);"),
        ]
    }
}

/// **Table: `core.event_clusters`**
///
/// Stores metadata about clusters of semantically similar events, identified
/// through vector clustering algorithms (e.g., K-Means, DBSCAN) run by an automaton.
#[derive(Iden, Copy, Clone)]
pub enum EventClusters {
    Table,
    Id,
    ClusterType,
    Summary,
    TimeStart,
    TimeEnd,
    Metadata,
}

impl TableDef for EventClusters {
    fn table_name() -> &'static str {
        "event_clusters"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventClusters {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventClusters::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(ColumnDef::new(EventClusters::ClusterType).text().not_null()) // e.g., 'semantic', 'temporal', 'source-based'
            .col(ColumnDef::new(EventClusters::Summary).text()) // AI-generated summary of the cluster's theme.
            .col(
                ColumnDef::new(EventClusters::TimeStart)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventClusters::TimeEnd)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventClusters::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .to_owned()
    }
}

/// **Table: `core.event_cluster_members`**
///
/// A junction table linking events to the clusters they belong to.
#[derive(Iden, Copy, Clone)]
pub enum EventClusterMembers {
    Table,
    ClusterId,
    EventId,
    Role,
}

impl TableDef for EventClusterMembers {
    fn table_name() -> &'static str {
        "event_cluster_members"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "(cluster_id, event_id)"
    }
}

impl EventClusterMembers {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventClusterMembers::ClusterId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventClusterMembers::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(EventClusterMembers::Role).text()) // e.g., 'centroid', 'outlier', 'member'
            .primary_key(
                Index::create()
                    .col(EventClusterMembers::ClusterId)
                    .col(EventClusterMembers::EventId),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EventClusterMembers::ClusterId)
                    .to(EventClusters::table_iden(), EventClusters::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EventClusterMembers::EventId)
                    .to(Events::table_iden(), Alias::new("id"))
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }
}
