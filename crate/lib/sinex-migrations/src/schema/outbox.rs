use super::{Events, TableDef};
use sea_query::{Alias, ColumnDef, Expr, Index, IndexOrder, PostgresQueryBuilder, Table};

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
        "audit"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl OperationsLog {
    pub const TABLE: &'static str = "operations_log";
    pub const SCHEMA: &'static str = "audit";

    pub const ID: &'static str = "id";
    pub const OPERATION_ID: &'static str = "operation_id";
    pub const OPERATION_TYPE: &'static str = "operation_type";
    pub const OPERATOR: &'static str = "operator";
    pub const TARGET_TABLE: &'static str = "target_table";
    pub const TARGET_SCHEMA: &'static str = "target_schema";
    pub const TARGET_IDS: &'static str = "target_ids";
    pub const OPERATION_PARAMS: &'static str = "operation_params";
    pub const STATUS: &'static str = "status";
    pub const STARTED_AT: &'static str = "started_at";
    pub const COMPLETED_AT: &'static str = "completed_at";
    pub const ERROR_MESSAGE: &'static str = "error_message";
    pub const AFFECTED_ROWS: &'static str = "affected_rows";
    pub const METADATA: &'static str = "metadata";
    pub const JUSTIFICATION: &'static str = "justification";
    pub const APPROVAL_ID: &'static str = "approval_id";

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
            .col(
                ColumnDef::new(Alias::new(Self::OPERATION_ID))
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .unique(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::OPERATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::OPERATOR)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::TARGET_TABLE)).text())
            .col(ColumnDef::new(Alias::new(Self::TARGET_SCHEMA)).text())
            .col(
                ColumnDef::new(Alias::new(Self::TARGET_IDS)).array(sea_query::ColumnType::Custom(
                    Alias::new("ULID").into_iden(),
                )),
            )
            .col(
                ColumnDef::new(Alias::new(Self::OPERATION_PARAMS))
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
                ColumnDef::new(Alias::new(Self::STARTED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::COMPLETED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::ERROR_MESSAGE)).text())
            .col(ColumnDef::new(Alias::new(Self::AFFECTED_ROWS)).big_integer())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(Alias::new(Self::JUSTIFICATION)).text())
            .col(ColumnDef::new(Alias::new(Self::APPROVAL_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the operations log table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on operation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_type")
                .col(Alias::new(Self::OPERATION_TYPE))
                .build(PostgresQueryBuilder),
            // Index on status
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_status")
                .col(Alias::new(Self::STATUS))
                .build(PostgresQueryBuilder),
            // Index on started_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_started")
                .col((Alias::new(Self::STARTED_AT), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on operator
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_operator")
                .col(Alias::new(Self::OPERATOR))
                .build(PostgresQueryBuilder),
            // GIN index on target_ids
            format!(
                "CREATE INDEX idx_operations_log_target_ids ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::TARGET_IDS
            ),
        ]
    }
}
