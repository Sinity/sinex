use crate::schema::{Events, TableDef};
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Event annotations table schema definition
#[derive(Copy, Clone)]
pub struct EventAnnotations;

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

impl EventAnnotations {
    pub const TABLE: &'static str = "event_annotations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const ANNOTATION_TYPE: &'static str = "annotation_type";
    pub const ANNOTATION_DATA: &'static str = "annotation_data";
    pub const ANNOTATOR: &'static str = "annotator";
    pub const CONFIDENCE_SCORE: &'static str = "confidence_score";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const IS_HUMAN_VERIFIED: &'static str = "is_human_verified";
    pub const VERIFICATION_TIMESTAMP: &'static str = "verification_timestamp";
    pub const VERIFIED_BY: &'static str = "verified_by";

    /// Create the event annotations table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ANNOTATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ANNOTATION_DATA))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ANNOTATOR))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CONFIDENCE_SCORE))
                    .double()
                    .default(1.0),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_HUMAN_VERIFIED))
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(Alias::new(Self::VERIFICATION_TIMESTAMP)).timestamp_with_time_zone(),
            )
            .col(ColumnDef::new(Alias::new(Self::VERIFIED_BY)).text())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event annotations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on annotation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_type")
                .col(Alias::new(Self::ANNOTATION_TYPE))
                .build(PostgresQueryBuilder),
            // Composite index on (event_id, annotation_type)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_event_type")
                .col(Alias::new(Self::EVENT_ID))
                .col(Alias::new(Self::ANNOTATION_TYPE))
                .build(PostgresQueryBuilder),
            // GIN index on annotation_data
            format!(
                "CREATE INDEX idx_event_annotations_data ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::ANNOTATION_DATA
            ),
            // Partial index on human-verified annotations
            format!(
                "CREATE INDEX idx_event_annotations_verified ON {}.{} ({}) WHERE {} = true",
                Self::SCHEMA,
                Self::TABLE,
                Self::EVENT_ID,
                Self::IS_HUMAN_VERIFIED
            ),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_annotations_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
            Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
            Events::SCHEMA, Events::TABLE, Events::ID
        )]
    }
}

/// Tags table schema definition
#[derive(Copy, Clone)]
pub struct Tags;

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

    /// Create the tags table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TAG_NAME))
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(ColumnDef::new(Alias::new(Self::TAG_CATEGORY)).text())
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
            .col(ColumnDef::new(Alias::new(Self::COLOR)).text())
            .col(ColumnDef::new(Alias::new(Self::ICON)).text())
            .col(ColumnDef::new(Alias::new(Self::PARENT_TAG_ID)).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::CREATED_BY)).text())
            .col(
                ColumnDef::new(Alias::new(Self::USAGE_COUNT))
                    .integer()
                    .not_null()
                    .default(0),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the tags table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on tag_category
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_tags_category")
                .col(Alias::new(Self::TAG_CATEGORY))
                .build(PostgresQueryBuilder),
            // Index on parent_tag_id for hierarchical queries
            format!(
                "CREATE INDEX idx_tags_parent ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::PARENT_TAG_ID,
                Self::PARENT_TAG_ID
            ),
            // Index on is_active
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_tags_active")
                .col(Alias::new(Self::IS_ACTIVE))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create self-referential foreign key constraint
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_tags_parent FOREIGN KEY ({}) REFERENCES {}.{}({})",
            Self::SCHEMA,
            Self::TABLE,
            Self::PARENT_TAG_ID,
            Self::SCHEMA,
            Self::TABLE,
            Self::ID
        )]
    }
}
