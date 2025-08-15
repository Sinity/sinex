//! Schema definitions for annotations tables

use crate::schema::core_events::Events;
use sea_orm_migration::prelude::*;

#[derive(Iden, Copy, Clone)]
pub enum EventAnnotations {
    #[iden = "event_annotations"]
    Table,
    Id,
    EventId,
    AnnotationType,
    Content,
    Metadata,
    AnnotationData,
    CreatedAt,
    UpdatedAt,
    CreatedBy,
}

impl EventAnnotations {
    pub const TABLE: &'static str = "event_annotations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const ANNOTATION_TYPE: &'static str = "annotation_type";
    pub const CONTENT: &'static str = "content";
    pub const METADATA: &'static str = "metadata";
    pub const ANNOTATION_DATA: &'static str = "annotation_data";
    pub const CREATED_BY: &'static str = "created_by";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), EventAnnotations::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventAnnotations::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
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
            // Note: AnnotationData might be a newer field, keeping both for compatibility
            .col(
                ColumnDef::new(EventAnnotations::AnnotationData)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
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
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on event_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_event_annotations_event ON {}.{} (event_id)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on annotation_type
            format!(
                "CREATE INDEX IF NOT EXISTS idx_event_annotations_type ON {}.{} (annotation_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // Composite index on (event_id, annotation_type)
            format!(
                "CREATE INDEX IF NOT EXISTS idx_event_annotations_event_type ON {}.{} (event_id, annotation_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on metadata
            format!(
                "CREATE INDEX IF NOT EXISTS idx_event_annotations_metadata ON {}.{} USING GIN (metadata)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on annotation_data if it exists
            format!(
                "CREATE INDEX IF NOT EXISTS idx_event_annotations_data ON {}.{} USING GIN (annotation_data)",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }

    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_annotations_event FOREIGN KEY (event_id) REFERENCES {}.{} (id) ON DELETE CASCADE",
            Self::SCHEMA, Self::TABLE, Events::SCHEMA, Events::TABLE
        )]
    }
}

/// Tags table for categorizing events and entities
#[derive(Iden, Copy, Clone)]
pub enum Tags {
    Table,
    Id,
    TagName,
    TagCategory,
    Description,
    Color,
    Icon,
    ParentTagId,
    IsActive,
    CreatedAt,
    UpdatedAt,
    CreatedBy,
    UsageCount,
}

impl Tags {
    pub const TABLE: &'static str = "tags";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const TAG_NAME: &'static str = "tag_name";
    pub const TAG_CATEGORY: &'static str = "tag_category";
    pub const DESCRIPTION: &'static str = "description";
    pub const COLOR: &'static str = "color";
    pub const ICON: &'static str = "icon";
    pub const PARENT_TAG_ID: &'static str = "parent_tag_id";
    pub const IS_ACTIVE: &'static str = "is_active";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CREATED_BY: &'static str = "created_by";
    pub const USAGE_COUNT: &'static str = "usage_count";

    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Tags::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(Tags::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(ColumnDef::new(Tags::TagName).text().not_null().unique_key())
            .col(ColumnDef::new(Tags::TagCategory).text())
            .col(ColumnDef::new(Tags::Description).text())
            .col(ColumnDef::new(Tags::Color).text())
            .col(ColumnDef::new(Tags::Icon).text())
            .col(ColumnDef::new(Tags::ParentTagId).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(Tags::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
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
            .col(ColumnDef::new(Tags::CreatedBy).text())
            .col(
                ColumnDef::new(Tags::UsageCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on tag_category
            format!(
                "CREATE INDEX IF NOT EXISTS idx_tags_category ON {}.{} (tag_category)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on parent_tag_id for hierarchical queries
            format!(
                "CREATE INDEX IF NOT EXISTS idx_tags_parent ON {}.{} (parent_tag_id) WHERE parent_tag_id IS NOT NULL",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on is_active
            format!(
                "CREATE INDEX IF NOT EXISTS idx_tags_active ON {}.{} (is_active)",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }

    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_tags_parent FOREIGN KEY (parent_tag_id) REFERENCES {}.{} (id)",
            Self::SCHEMA, Self::TABLE, Self::SCHEMA, Self::TABLE
        )]
    }
}
