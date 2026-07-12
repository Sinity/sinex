//! Semantic epoch and shadow-lane registry tables.
//!
//! These tables store experiment artifacts outside canonical entity/relation
//! projections. Promotion into canonical state must happen through explicit
//! operator authority, not by reading lane outputs as ordinary projections.
//!
//! Keep this schema narrow. New derived outputs should first consider the
//! Proposal/Judgment/Operation path, DerivationSpec, Artifact/Projection rows,
//! and output-kind discipline. Semantic lanes remain for shadow comparison and
//! epoch/lane diffs; they must not become a parallel authority path for model
//! effects or admitted facts.

use crate::primitives::{Timestamp, Uuid};
use crate::{OperationsLog, SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

#[derive(Iden, Copy, Clone)]
pub enum SemanticEpochs {
    Table,
    Id,
    Name,
    Scope,
    CodeRef,
    ConfigHash,
    Components,
    PromptSetHash,
    ModelConfigHash,
    CreatedBy,
    OperationId,
    CreatedAt,
    SupersedesEpochId,
}

impl TableDef for SemanticEpochs {
    fn table_name() -> &'static str {
        "epochs"
    }

    fn schema_name() -> &'static str {
        "semantic"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SemanticEpochRecord {
    pub id: Uuid,
    pub name: String,
    pub scope: JsonValue,
    pub code_ref: Option<String>,
    pub config_hash: String,
    pub components: JsonValue,
    pub prompt_set_hash: Option<String>,
    pub model_config_hash: Option<String>,
    pub created_by: String,
    pub operation_id: Option<Uuid>,
    pub created_at: Timestamp,
    pub supersedes_epoch_id: Option<Uuid>,
}

impl SemanticEpochs {
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
            .col(ColumnDef::new(Self::Name).text().not_null())
            .col(ColumnDef::new(Self::Scope).json_binary().not_null())
            .col(ColumnDef::new(Self::CodeRef).text())
            .col(ColumnDef::new(Self::ConfigHash).text().not_null())
            .col(ColumnDef::new(Self::Components).json_binary().not_null())
            .col(ColumnDef::new(Self::PromptSetHash).text())
            .col(ColumnDef::new(Self::ModelConfigHash).text())
            .col(ColumnDef::new(Self::CreatedBy).text().not_null())
            .col(ColumnDef::new(Self::OperationId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Self::SupersedesEpochId).custom(Alias::new("UUID")))
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::OperationId)
                    .to(OperationsLog::table_iden(), OperationsLog::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::SupersedesEpochId)
                    .to(Self::table_iden(), Self::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_semantic_epochs_scope_config")
                .table(Self::table_iden())
                .col(Self::Scope)
                .col(Self::ConfigHash)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_semantic_epochs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}

#[derive(Iden, Copy, Clone)]
pub enum SemanticLanes {
    Table,
    Id,
    Name,
    Kind,
    BaseEpochId,
    CandidateEpochId,
    Scope,
    Status,
    Purpose,
    OperationId,
    CreatedAt,
    CompletedAt,
    ExpiresAt,
}

impl TableDef for SemanticLanes {
    fn table_name() -> &'static str {
        "lanes"
    }

    fn schema_name() -> &'static str {
        "semantic"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SemanticLaneRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub base_epoch_id: Option<Uuid>,
    pub candidate_epoch_id: Uuid,
    pub scope: JsonValue,
    pub status: String,
    pub purpose: Option<String>,
    pub operation_id: Option<Uuid>,
    pub created_at: Timestamp,
    pub completed_at: Option<Timestamp>,
    pub expires_at: Option<Timestamp>,
}

impl SemanticLanes {
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
            .col(ColumnDef::new(Self::Name).text().not_null())
            .col(ColumnDef::new(Self::Kind).text().not_null())
            .col(ColumnDef::new(Self::BaseEpochId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::CandidateEpochId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::Scope).json_binary().not_null())
            .col(ColumnDef::new(Self::Status).text().not_null())
            .col(ColumnDef::new(Self::Purpose).text())
            .col(ColumnDef::new(Self::OperationId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Self::CompletedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Self::ExpiresAt).timestamp_with_time_zone())
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::BaseEpochId)
                    .to(SemanticEpochs::table_iden(), SemanticEpochs::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::CandidateEpochId)
                    .to(SemanticEpochs::table_iden(), SemanticEpochs::Id)
                    .on_delete(ForeignKeyAction::Restrict),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::OperationId)
                    .to(OperationsLog::table_iden(), OperationsLog::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_semantic_lanes_status")
                .table(Self::table_iden())
                .col(Self::Status)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_semantic_lanes_candidate_epoch")
                .table(Self::table_iden())
                .col(Self::CandidateEpochId)
                .to_owned(),
        ]
    }
}

#[derive(Iden, Copy, Clone)]
pub enum SemanticLaneOutputs {
    Table,
    LaneId,
    OutputKind,
    OutputKey,
    SourceEventId,
    SourceMaterialId,
    SourceAnchor,
    OutputHash,
    Payload,
    Metadata,
    CreatedAt,
}

impl TableDef for SemanticLaneOutputs {
    fn table_name() -> &'static str {
        "lane_outputs"
    }

    fn schema_name() -> &'static str {
        "semantic"
    }

    fn primary_key() -> &'static str {
        "(lane_id, output_kind, output_key)"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SemanticLaneOutputRecord {
    pub lane_id: Uuid,
    pub output_kind: String,
    pub output_key: String,
    pub source_event_id: Option<Uuid>,
    pub source_material_id: Option<Uuid>,
    pub source_anchor: Option<JsonValue>,
    pub output_hash: String,
    pub payload: JsonValue,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
}

impl SemanticLaneOutputs {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Self::LaneId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::OutputKind).text().not_null())
            .col(ColumnDef::new(Self::OutputKey).text().not_null())
            .col(ColumnDef::new(Self::SourceEventId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::SourceMaterialId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::SourceAnchor).json_binary())
            .col(ColumnDef::new(Self::OutputHash).text().not_null())
            .col(ColumnDef::new(Self::Payload).json_binary().not_null())
            .col(
                ColumnDef::new(Self::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .primary_key(
                Index::create()
                    .col(Self::LaneId)
                    .col(Self::OutputKind)
                    .col(Self::OutputKey),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::LaneId)
                    .to(SemanticLanes::table_iden(), SemanticLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            // No declarative FK on source_event_id -> core.events(id): an inbound FK
            // to the events hypertable blocks columnstore compression of its chunks
            // ("found a FK into a chunk while truncating"), silently defeating the
            // policy on the whole hypertable. source_event_id is a soft provenance
            // pointer (nullable, and event ids are per-interpretation identities that
            // churn on replay), not a referential-integrity invariant, so it carries
            // no enforced cascade. Ref sinex-h8no.
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::SourceMaterialId)
                    .to(
                        SourceMaterialRegistry::table_iden(),
                        SourceMaterialRegistry::Id,
                    )
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_semantic_lane_outputs_kind_hash")
                .table(Self::table_iden())
                .col(Self::OutputKind)
                .col(Self::OutputHash)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_semantic_lane_outputs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}

#[derive(Iden, Copy, Clone)]
pub enum SemanticLaneDiffs {
    Table,
    Id,
    BaselineLaneId,
    CandidateLaneId,
    DiffKind,
    Counts,
    Examples,
    ReportHash,
    CreatedAt,
}

impl TableDef for SemanticLaneDiffs {
    fn table_name() -> &'static str {
        "lane_diffs"
    }

    fn schema_name() -> &'static str {
        "semantic"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct SemanticLaneDiffRecord {
    pub id: Uuid,
    pub baseline_lane_id: Uuid,
    pub candidate_lane_id: Uuid,
    pub diff_kind: String,
    pub counts: JsonValue,
    pub examples: JsonValue,
    pub report_hash: String,
    pub created_at: Timestamp,
}

impl SemanticLaneDiffs {
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
                ColumnDef::new(Self::BaselineLaneId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::CandidateLaneId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::DiffKind).text().not_null())
            .col(ColumnDef::new(Self::Counts).json_binary().not_null())
            .col(
                ColumnDef::new(Self::Examples)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'[]'::jsonb")),
            )
            .col(ColumnDef::new(Self::ReportHash).text().not_null())
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::BaselineLaneId)
                    .to(SemanticLanes::table_iden(), SemanticLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::CandidateLaneId)
                    .to(SemanticLanes::table_iden(), SemanticLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_semantic_lane_diffs_report")
                .table(Self::table_iden())
                .col(Self::BaselineLaneId)
                .col(Self::CandidateLaneId)
                .col(Self::DiffKind)
                .col(Self::ReportHash)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_semantic_lane_diffs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}
