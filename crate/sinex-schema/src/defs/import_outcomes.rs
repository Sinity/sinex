//! Durable admission outcomes for operation-scoped import audit reports.

use crate::TableDef;
use crate::primitives::{Timestamp, Uuid};
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};
use sqlx::FromRow;

/// **Table: `audit.import_outcomes`**
///
/// Records candidate events that did not become live rows. Admitted and
/// superseded counts remain derived from the event and replacement ledgers;
/// this table preserves the otherwise lost suppression evidence needed to
/// compare repeated imports.
#[derive(Iden, Copy, Clone)]
pub enum ImportOutcomes {
    Table,
    Id,
    OperationId,
    CandidateEventId,
    SourceMaterialId,
    Source,
    EventType,
    Outcome,
    Reason,
    ExistingEventId,
    RecordedAt,
}

impl TableDef for ImportOutcomes {
    fn table_name() -> &'static str {
        "import_outcomes"
    }

    fn schema_name() -> &'static str {
        "audit"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct ImportOutcomeRecord {
    pub id: Uuid,
    pub operation_id: Uuid,
    pub candidate_event_id: Uuid,
    pub source_material_id: Option<Uuid>,
    pub source: String,
    pub event_type: String,
    pub outcome: String,
    pub reason: String,
    pub existing_event_id: Option<Uuid>,
    pub recorded_at: Timestamp,
}

impl ImportOutcomes {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Self::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(
                ColumnDef::new(Self::OperationId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::CandidateEventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::SourceMaterialId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::Source).text().not_null())
            .col(ColumnDef::new(Self::EventType).text().not_null())
            .col(
                ColumnDef::new(Self::Outcome)
                    .text()
                    .not_null()
                    .check(Expr::cust("outcome IN ('suppressed', 'failed', 'dlq')")),
            )
            .col(ColumnDef::new(Self::Reason).text().not_null())
            .col(ColumnDef::new(Self::ExistingEventId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::RecordedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::OperationId)
                    .to(
                        (Alias::new("core"), Alias::new("operations_log")),
                        Alias::new("id"),
                    )
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_import_outcomes_operation")
                .table(Self::table_iden())
                .col(Self::OperationId)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ux_import_outcomes_candidate")
                .table(Self::table_iden())
                .col(Self::OperationId)
                .col(Self::CandidateEventId)
                .col(Self::Outcome)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_import_outcomes_operation_breakdown")
                .table(Self::table_iden())
                .col(Self::OperationId)
                .col(Self::Source)
                .col(Self::EventType)
                .col(Self::Outcome)
                .to_owned(),
        ]
    }
}
