use crate::schema::{Events, TableDef};
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Event relations table schema definition
#[derive(Copy, Clone)]
pub struct EventRelations;

impl TableDef for EventRelations {
    fn table_name() -> &'static str {
        "event_relations"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventRelations {
    pub const TABLE: &'static str = "event_relations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const FROM_EVENT_ID: &'static str = "from_event_id";
    pub const TO_EVENT_ID: &'static str = "to_event_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const METADATA: &'static str = "metadata";
    pub const CONFIDENCE_SCORE: &'static str = "confidence_score";
    pub const CREATED_AT: &'static str = "created_at";
    pub const CREATED_BY: &'static str = "created_by";

    /// Create the event relations table
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
                ColumnDef::new(Alias::new(Self::FROM_EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
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
                ColumnDef::new(Alias::new(Self::CREATED_BY))
                    .text()
                    .not_null()
                    .default("system"),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on from_event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_from")
                .col(Alias::new(Self::FROM_EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on to_event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_to")
                .col(Alias::new(Self::TO_EVENT_ID))
                .build(PostgresQueryBuilder),
            // Composite index on (from_event_id, relation_type)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_from_type")
                .col(Alias::new(Self::FROM_EVENT_ID))
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
            // Index on relation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::FROM_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TO_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
        ]
    }
}

/// Event clusters table schema definition
#[derive(Copy, Clone)]
pub struct EventClusters;

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
    pub const TABLE: &'static str = "event_clusters";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const CLUSTER_NAME: &'static str = "cluster_name";
    pub const CLUSTER_TYPE: &'static str = "cluster_type";
    pub const DESCRIPTION: &'static str = "description";
    pub const CENTROID: &'static str = "centroid";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const ALGORITHM: &'static str = "algorithm";
    pub const PARAMETERS: &'static str = "parameters";

    /// Create the event clusters table
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
                ColumnDef::new(Alias::new(Self::CLUSTER_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CLUSTER_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
            .col(ColumnDef::new(Alias::new(Self::CENTROID)).custom(Alias::new("vector(1536)")))
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
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
            .col(ColumnDef::new(Alias::new(Self::ALGORITHM)).text())
            .col(ColumnDef::new(Alias::new(Self::PARAMETERS)).json_binary())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event clusters table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on cluster_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_clusters_type")
                .col(Alias::new(Self::CLUSTER_TYPE))
                .build(PostgresQueryBuilder),
            // Index on cluster_name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_clusters_name")
                .col(Alias::new(Self::CLUSTER_NAME))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Event cluster members table schema definition
#[derive(Copy, Clone)]
pub struct EventClusterMembers;

impl TableDef for EventClusterMembers {
    fn table_name() -> &'static str {
        "event_cluster_members"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventClusterMembers {
    pub const TABLE: &'static str = "event_cluster_members";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const CLUSTER_ID: &'static str = "cluster_id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const DISTANCE_TO_CENTROID: &'static str = "distance_to_centroid";
    pub const MEMBERSHIP_SCORE: &'static str = "membership_score";
    pub const JOINED_AT: &'static str = "joined_at";

    /// Create the event cluster members table
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
                ColumnDef::new(Alias::new(Self::CLUSTER_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::DISTANCE_TO_CENTROID)).double())
            .col(ColumnDef::new(Alias::new(Self::MEMBERSHIP_SCORE)).double())
            .col(
                ColumnDef::new(Alias::new(Self::JOINED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event cluster members table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (cluster_id, event_id)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_cluster_members_unique")
                .col(Alias::new(Self::CLUSTER_ID))
                .col(Alias::new(Self::EVENT_ID))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on event_id for reverse lookups
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_cluster_members_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_cluster_members_cluster FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::CLUSTER_ID,
                EventClusters::SCHEMA, EventClusters::TABLE, EventClusters::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_cluster_members_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
        ]
    }
}
