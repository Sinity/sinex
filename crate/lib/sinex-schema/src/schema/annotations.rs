//! The Canonical Database Schema for Annotations and Tagging.
//!
//! This module defines the tables that support the "Human-in-the-Loop" and
//! "Structure is Emergent" principles. It provides the mechanisms for users and
//! automata to enrich the raw event stream with notes, tags, and other forms
//! of metadata.

use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};

use crate::primitives::{Timestamp, Uuid};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.tags` and `core.tagged_items` Tables
// =============================================================================

/// **Table: `core.tags`**
///
/// The central definition table for all tags. Tags are a primary organizational
/// primitive for creating a flexible, non-hierarchical knowledge structure.
///
/// **Design Rationale:**
/// - A `UUID` surrogate key (`id`) is used as the primary key. This is crucial for
///   performance and maintainability. It allows a tag's human-readable `name` to be
///   renamed in a single, fast update to this table, without requiring a costly
///   cascading update across millions of rows in the `tagged_items` junction table.
/// - The `parent_tag_id` allows for the creation of tag hierarchies (e.g., 'programming' -> 'rust').
#[derive(Iden, Copy, Clone)]
pub enum Tags {
    Table,
    Id,
    Name,
    ParentTagId,
    Description,
    Color,
    Icon,
    UsageCount,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for Tags {
    fn table_name() -> &'static str {
        "tags"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct TagRecord {
    pub id: Uuid,
    pub name: String,
    pub parent_tag_id: Option<Uuid>,
    pub description: Option<String>,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub usage_count: i64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Tags {
    /// Generates the `CREATE TABLE` statement for `core.tags`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Tags::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(ColumnDef::new(Tags::Name).text().not_null().unique_key())
            .col(ColumnDef::new(Tags::ParentTagId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Tags::Description).text())
            .col(ColumnDef::new(Tags::Color).text())
            .col(ColumnDef::new(Tags::Icon).text())
            .col(
                ColumnDef::new(Tags::UsageCount)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Tags::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Tags::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    /// Raw SQL fixup for the self-referencing foreign key on `parent_tag_id`.
    ///
    /// sea-query has a bug where `on_delete(ForeignKeyAction::SetNull)` on a self-referencing
    /// FK emits `ON DELETE CASCADE` instead of `ON DELETE SET NULL`. We work around this by
    /// defining the FK via raw `ALTER TABLE` SQL after table creation, bypassing sea-query.
    #[must_use]
    pub fn create_fk_fixup_sql() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS tags_parent_tag_id_fkey",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT tags_parent_tag_id_fkey \
                 FOREIGN KEY (parent_tag_id) REFERENCES {}.{}(id) ON DELETE SET NULL",
                Self::schema_name(),
                Self::table_name(),
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }
}

/// **Table: `core.tagged_items`**
///
/// A many-to-many junction table for applying tags to various items in the system
/// (events, entities, blobs, etc.). This polymorphic design is central to the
/// "Tags, not Hierarchies" philosophy.
#[derive(Iden, Copy, Clone)]
pub enum TaggedItems {
    Table,
    TagId,
    ItemId,
    ItemType,
    TaggedAt,
}

impl TableDef for TaggedItems {
    fn table_name() -> &'static str {
        "tagged_items"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "(tag_id, item_id, item_type)"
    }
}

impl TaggedItems {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(TaggedItems::TagId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(TaggedItems::ItemId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(TaggedItems::ItemType).text().not_null())
            .col(
                ColumnDef::new(TaggedItems::TaggedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .primary_key(
                Index::create()
                    .col(TaggedItems::TagId)
                    .col(TaggedItems::ItemId)
                    .col(TaggedItems::ItemType),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), TaggedItems::TagId)
                    .to(Tags::table_iden(), Tags::Id)
                    .on_delete(ForeignKeyAction::Cascade), // If a tag is deleted, all its associations are removed.
            )
            .to_owned()
    }

    /// Generates indexes for `core.tagged_items`. The index on `(item_id, item_type)` is
    /// crucial for efficiently finding all tags for a specific item.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_tagged_items_item")
                .table(Self::table_iden())
                .col(TaggedItems::ItemId)
                .col(TaggedItems::ItemType)
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `core.event_annotations` Table
// =============================================================================

/// **Table: `core.event_annotations`**
///
/// Allows users or automata to attach rich, structured notes or metadata directly
/// to specific events. This is a primary mechanism for the "Human-in-the-Loop"
/// principle, enabling direct curation and sense-making on the event stream.
#[derive(Iden, Copy, Clone)]
pub enum EventAnnotations {
    Table,
    Id,
    EventId,
    AnnotationType,
    Content,
    Metadata,
    CreatedBy,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for EventAnnotations {
    fn table_name() -> &'static str {
        "event_annotations"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct EventAnnotationRecord {
    pub id: Uuid,
    pub event_id: Uuid,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl EventAnnotations {
    /// Generates the `CREATE TABLE` statement for `core.event_annotations`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventAnnotations::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(EventAnnotations::EventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::AnnotationType)
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "length(BTRIM(annotation_type, E' \\t\\n\\r\\v\\f')) > 0",
                    )),
            )
            .col(ColumnDef::new(EventAnnotations::Content).text().not_null())
            .col(
                ColumnDef::new(EventAnnotations::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(EventAnnotations::CreatedBy)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(EventAnnotations::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            // No declarative FK to core.events(id): TimescaleDB does not allow
            // hypertables as FK targets (timescale/timescaledb#865), so any
            // `FOREIGN KEY (event_id) REFERENCES core.events(id)` declaration
            // is silently absent after apply. Cascade-on-event-delete is
            // enforced by the `core.fn_archive_before_delete` trigger
            // (see crate/lib/sinex-schema/src/schema/events.rs), which
            // archives + deletes matching annotation rows in the same
            // transaction as the parent event delete.
            .to_owned()
    }

    /// Generates indexes for `core.event_annotations`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Index to quickly find all annotations for a given event.
            Index::create()
                .if_not_exists()
                .name("ix_event_annotations_event_id")
                .table(Self::table_iden())
                .col(EventAnnotations::EventId)
                .to_owned(),
            // Index to find annotations of a specific type.
            Index::create()
                .if_not_exists()
                .name("ix_event_annotations_type")
                .table(Self::table_iden())
                .col(EventAnnotations::AnnotationType)
                .to_owned(),
            // Note: GIN index for full-text search requires raw SQL - see create_gin_indexes_sql()
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature)
    #[must_use]
    pub fn create_gin_indexes_sql() -> Vec<String> {
        vec![
            // GIN index for full-text search on the annotation content
            format!(
                "CREATE INDEX IF NOT EXISTS ix_event_annotations_content_gin ON {}.{} USING GIN (to_tsvector('english', {}))",
                Self::schema_name(),
                Self::table_name(),
                "content"
            ),
        ]
    }

    /// Creates a trigger to update the `updated_at` column
    #[must_use]
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r"
            DROP TRIGGER IF EXISTS trg_event_annotations_updated_at ON {}.{};
            CREATE TRIGGER trg_event_annotations_updated_at
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
