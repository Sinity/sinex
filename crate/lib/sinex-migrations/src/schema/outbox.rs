use crate::schema::{Events, TableDef};
use sea_query::{Alias, ColumnDef, Expr, Index, IndexOrder, IntoIden, PostgresQueryBuilder, Table};

/// Outbox table schema definition
#[derive(Copy, Clone)]
pub struct Outbox;

impl TableDef for Outbox {
    fn table_name() -> &'static str {
        "outbox"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl Outbox {
    pub const TABLE: &'static str = "outbox";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const TOPIC: &'static str = "topic";
    pub const PARTITION_KEY: &'static str = "partition_key";
    pub const PAYLOAD: &'static str = "payload";
    pub const HEADERS: &'static str = "headers";
    pub const STATUS: &'static str = "status";
    pub const CREATED_AT: &'static str = "created_at";
    pub const PROCESSED_AT: &'static str = "processed_at";
    pub const RETRY_COUNT: &'static str = "retry_count";
    pub const ERROR_MESSAGE: &'static str = "error_message";

    /// Create the outbox table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .big_integer()
                    .not_null()
                    .primary_key()
                    .extra("GENERATED ALWAYS AS IDENTITY"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::TOPIC)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::PARTITION_KEY)).text())
            .col(
                ColumnDef::new(Alias::new(Self::PAYLOAD))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::HEADERS))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::STATUS))
                    .text()
                    .not_null()
                    .default("pending"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::PROCESSED_AT)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::RETRY_COUNT))
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Alias::new(Self::ERROR_MESSAGE)).text())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the outbox table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on status for pending messages
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_outbox_status")
                .col(Alias::new(Self::STATUS))
                .build(PostgresQueryBuilder),
            // Composite index for processing (status, created_at)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_outbox_processing")
                .col(Alias::new(Self::STATUS))
                .col(Alias::new(Self::CREATED_AT))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_outbox_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
            Self::SCHEMA,
            Self::TABLE,
            Self::EVENT_ID,
            Events::SCHEMA,
            Events::TABLE,
            Events::ID
        )]
    }
}

/// Operations log table schema definition
#[derive(Copy, Clone)]
pub struct OperationsLog;

impl TableDef for OperationsLog {
    fn table_name() -> &'static str {
        "operations_log"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl OperationsLog {
    pub const TABLE: &'static str = "operations_log";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ACTOR: &'static str = "actor";
    pub const SCOPE: &'static str = "scope";
    pub const STATE: &'static str = "state";
    pub const PREVIEW_SUMMARY: &'static str = "preview_summary";
    pub const CHECKPOINT: &'static str = "checkpoint";
    pub const APPROVED_BY: &'static str = "approved_by";
    pub const APPROVED_AT: &'static str = "approved_at";
    pub const EXECUTOR_NODE: &'static str = "executor_node";
    pub const STARTED_AT: &'static str = "started_at";
    pub const FINISHED_AT: &'static str = "finished_at";
    pub const OUTCOME: &'static str = "outcome";
    pub const ERROR_DETAILS: &'static str = "error_details";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the operations log table
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
            .col(ColumnDef::new(Alias::new(Self::ACTOR)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::SCOPE))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::STATE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::PREVIEW_SUMMARY)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::CHECKPOINT)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::APPROVED_BY)).text())
            .col(ColumnDef::new(Alias::new(Self::APPROVED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::EXECUTOR_NODE)).text())
            .col(ColumnDef::new(Alias::new(Self::STARTED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::FINISHED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::OUTCOME)).text())
            .col(ColumnDef::new(Alias::new(Self::ERROR_DETAILS)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the operations log table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on state
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_state")
                .col(Alias::new(Self::STATE))
                .build(PostgresQueryBuilder),
            // Index on actor
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_actor")
                .col(Alias::new(Self::ACTOR))
                .build(PostgresQueryBuilder),
            // Index on started_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_started")
                .col((Alias::new(Self::STARTED_AT), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on created_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_created")
                .col((Alias::new(Self::CREATED_AT), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // GIN index on scope for JSON queries
            format!(
                "CREATE INDEX idx_operations_log_scope ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::SCOPE
            ),
        ]
    }
}
