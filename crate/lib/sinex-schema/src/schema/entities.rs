//! The Canonical Database Schema for the Knowledge Graph.
//!
//! This module defines the tables that form the structured, queryable representation
//! of the system's understanding: `core.entities` and `core.entity_relations`.
//! These tables are considered **materialized projections** of the `core.events` log.
//! They are populated by automata and can, in theory, be completely rebuilt by
//! replaying those automata over the event history. This is the physical
//_ implementation of the "Structure is Emergent" principle.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, ColumnType, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden,
    Index, IndexCreateStatement, IntoIden, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.entities` Table
// =============================================================================

/// **Table: `core.entities`**
///
/// Represents the "nouns" of the user's world: people, projects, files, concepts, etc.
/// Entities are synthesized by automata from the event stream and provide a stable
/// identity for concepts that may be referred to in different ways across different events.
///
/// **Design Rationale:**
/// - A `UUID` surrogate key (`id`) is used for stability and performance. An entity's
///   human-readable `name` can change, but its `id` is immutable.
/// - The `merged_into_id` field allows for robust entity resolution, creating a
///   redirect from a duplicate entity to its canonical version without losing history.
#[derive(Iden, Copy, Clone)]
pub enum Entities {
    Table,
    Id,
    EntityType,
    Name,
    CanonicalName,
    Aliases,
    Properties,
    SourceEventIds,
    ConfidenceScore,
    IsMerged,
    MergedIntoId,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for Entities {
    fn table_name() -> &'static str {
        "entities"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `core.entities`.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EntityRecord {
    pub id: Uuid,
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub properties: JsonValue,
    pub source_event_ids: Vec<Uuid>,
    pub confidence_score: f64,
    pub is_merged: bool,
    pub merged_into_id: Option<Uuid>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Entities {
    /// Generates the `CREATE TABLE` statement for `core.entities`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Entities::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(ColumnDef::new(Entities::EntityType).text().not_null())
            .col(ColumnDef::new(Entities::Name).text().not_null())
            .col(
                ColumnDef::new(Entities::CanonicalName)
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(Entities::Aliases)
                    .array(ColumnType::Text)
                    .not_null()
                    .default(Expr::cust("'{}'::text[]")),
            )
            .col(
                ColumnDef::new(Entities::Properties)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Entities::SourceEventIds)
                    .array(ColumnType::Custom(Alias::new("UUID").into_iden()))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Entities::ConfidenceScore)
                    .double()
                    .not_null()
                    .default(1.0)
                    .check(Expr::cust(
                        "confidence_score >= 0 AND confidence_score <= 1.0",
                    )),
            )
            .col(
                ColumnDef::new(Entities::IsMerged)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(Entities::MergedIntoId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Entities::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Entities::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    /// Raw SQL fixup for the self-referencing foreign key on `merged_into_id`.
    ///
    /// sea-query has a bug where `on_delete(ForeignKeyAction::SetNull)` on a self-referencing
    /// FK emits `ON DELETE CASCADE` instead of `ON DELETE SET NULL`. We work around this by
    /// defining the FK via raw `ALTER TABLE` SQL after table creation, bypassing sea-query.
    #[must_use]
    pub fn create_fk_fixup_sql() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS entities_merged_into_id_fkey",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT entities_merged_into_id_fkey \
                 FOREIGN KEY (merged_into_id) REFERENCES {}.{}(id) ON DELETE SET NULL",
                Self::schema_name(),
                Self::table_name(),
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// Generates indexes for `core.entities`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint on entity type and name combination
            Index::create()
                .if_not_exists()
                .name("uk_entities_type_name")
                .table(Self::table_iden())
                .col(Entities::EntityType)
                .col(Entities::Name)
                .unique()
                .to_owned(),
            // Note: GIN indexes require raw SQL - see create_gin_indexes_sql()
            Index::create()
                .if_not_exists()
                .unique()
                .name("ix_entities_merged")
                .table(Self::table_iden())
                .col(Entities::MergedIntoId)
                .cond_where(Expr::col(Entities::IsMerged).eq(true))
                .to_owned(),
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature)
    #[must_use]
    pub fn create_gin_indexes_sql() -> Vec<String> {
        vec![
            format!(
                "CREATE INDEX IF NOT EXISTS ix_entities_aliases_gin ON {}.{} USING GIN (aliases)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_entities_properties_gin ON {}.{} USING GIN (properties jsonb_path_ops)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_entities_source_events_gin ON {}.{} USING GIN (source_event_ids)",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// Generates raw SQL for trigram indexes (`PostgreSQL` `pg_trgm` extension).
    #[must_use]
    pub fn create_trigram_indexes_sql() -> Vec<String> {
        vec![
            format!(
                "CREATE INDEX IF NOT EXISTS ix_entities_name_trgm ON {}.{} USING GIN (name gin_trgm_ops)",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_entities_canonical_name_trgm ON {}.{} USING GIN (canonical_name gin_trgm_ops)",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// Creates a trigger to update the `updated_at` column
    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r"
            DROP TRIGGER IF EXISTS trg_entities_updated_at ON {}.{};
            CREATE TRIGGER trg_entities_updated_at
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            ",
            Self::schema_name(),
            Self::table_name(),
            Self::schema_name(),
            Self::table_name()
        )
    }
}

// =============================================================================
// The `core.entity_relations` Table
// =============================================================================

/// **Table: `core.entity_relations`**
///
/// Represents the directed, typed connections (the "verbs") between entities in the
/// knowledge graph. Like entities, these are synthesized from the event stream.
#[derive(Iden, Copy, Clone)]
pub enum EntityRelations {
    Table,
    Id,
    FromEntityId,
    ToEntityId,
    RelationType,
    Properties,
    SourceEventIds,
    ConfidenceScore,
    IsActive,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for EntityRelations {
    fn table_name() -> &'static str {
        "entity_relations"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EntityRelations {
    /// Generates the `CREATE TABLE` statement for `core.entity_relations`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EntityRelations::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(EntityRelations::FromEntityId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::ToEntityId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::RelationType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::Properties)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(EntityRelations::SourceEventIds)
                    .array(ColumnType::Custom(Alias::new("UUID").into_iden()))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::ConfidenceScore)
                    .double()
                    .not_null()
                    .default(1.0)
                    .check(Expr::cust(
                        "confidence_score >= 0 AND confidence_score <= 1.0",
                    )),
            )
            .col(
                ColumnDef::new(EntityRelations::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(EntityRelations::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(EntityRelations::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EntityRelations::FromEntityId)
                    .to(Entities::table_iden(), Entities::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EntityRelations::ToEntityId)
                    .to(Entities::table_iden(), Entities::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .check(Expr::cust("from_entity_id <> to_entity_id"))
            .to_owned()
    }

    /// Generates indexes for `core.entity_relations`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Unique constraint on the relationship tuple
            Index::create()
                .name("uk_entity_relations_triple")
                .table(Self::table_iden())
                .col(EntityRelations::FromEntityId)
                .col(EntityRelations::ToEntityId)
                .col(EntityRelations::RelationType)
                .unique()
                .to_owned(),
        ]
    }

    /// Generates the trigger to automatically update the `updated_at` timestamp.
    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        r"
            DROP TRIGGER IF EXISTS trg_entity_relations_updated_at ON core.entity_relations;
            CREATE TRIGGER trg_entity_relations_updated_at
            BEFORE UPDATE ON core.entity_relations
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            "
        .to_string()
    }
}
