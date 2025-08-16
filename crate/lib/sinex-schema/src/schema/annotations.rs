//! The Canonical Database Schema for Annotations and Tagging.
//!
//! This module defines the tables that support the "Human-in-the-Loop" and
//! "Structure is Emergent" principles. It provides the mechanisms for users and
//! automata to enrich the raw event stream with notes, tags, and other forms
//! of metadata.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

use crate::ulid::Ulid;
use chrono::{DateTime, Utc};
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
/// - A `ULID` surrogate key (`id`) is used as the primary key. This is crucial for
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

#[derive(Debug, FromRow)]
pub struct TagRecord {
    pub id: Ulid,
    pub name: String,
    pub parent_tag_id: Option<Ulid>,
    pub description: Option<String>,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub usage_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Tags {
    /// Generates the `CREATE TABLE` statement for `core.tags`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Tags::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(ColumnDef::new(Tags::Name).text().not_null().unique_key())
            .col(ColumnDef::new(Tags::ParentTagId).custom(Alias::new("ULID")))
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
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Tags::ParentTagId)
                    .to(Self::table_iden(), Tags::Id)
                    .on_delete(ForeignKeyAction::SetNull), // If a parent is deleted, children become top-level.
            )
            .to_owned()
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
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(TaggedItems::TagId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(TaggedItems::ItemId)
                    .custom(Alias::new("ULID"))
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
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![Index::create()
            .name("ix_tagged_items_item")
            .table(Self::table_iden())
            .col(TaggedItems::ItemId)
            .col(TaggedItems::ItemType)
            .to_owned()]
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

#[derive(Debug, FromRow)]
pub struct EventAnnotationRecord {
    pub id: Ulid,
    pub event_id: Ulid,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl EventAnnotations {
    /// Generates the `CREATE TABLE` statement for `core.event_annotations`.
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(EventAnnotations::Id)
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(EventAnnotations::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::AnnotationType)
                    .text()
                    .not_null(),
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
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), EventAnnotations::EventId)
                    .to(Events::table_iden(), Alias::new("id")) // `Events::Iden` is fine
                    .on_delete(ForeignKeyAction::Cascade), // If the event is deleted, its annotations are also deleted.
            )
            .to_owned()
    }

    /// Generates indexes for `core.event_annotations`.
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // Index to quickly find all annotations for a given event.
            Index::create()
                .name("ix_event_annotations_event_id")
                .table(Self::table_iden())
                .col(EventAnnotations::EventId)
                .to_owned(),
            // Index to find annotations of a specific type.
            Index::create()
                .name("ix_event_annotations_type")
                .table(Self::table_iden())
                .col(EventAnnotations::AnnotationType)
                .to_owned(),
            // Note: GIN index for full-text search requires raw SQL - see create_gin_indexes_sql()
        ]
    }

    /// Generates raw SQL for GIN indexes (PostgreSQL-specific feature)
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

    /// Creates a trigger to update the updated_at column
    pub fn create_updated_at_trigger_sql() -> String {
        format!(
            r#"
            DROP TRIGGER IF EXISTS trg_event_annotations_updated_at ON {}.{};
            CREATE TRIGGER trg_event_annotations_updated_at
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
            "#,
            Self::schema_name(),
            Self::table_name(),
            Self::schema_name(),
            Self::table_name()
        )
    }
}
