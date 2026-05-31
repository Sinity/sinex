//! Model-effect record schema (#1063).
//!
//! Records LLM call outputs keyed by composite input hash so non-deterministic
//! derived events can be replayed against a recorded effect rather than
//! re-issued. This is an event-spine provenance artifact, not a parallel lookup
//! cache. Replay policy governs whether to reuse a record, fail if missing, or
//! always re-evaluate. NOTE: not yet wired — `pool.model_effects()` has no
//! production callers (#1063).

use crate::TableDef;
use crate::primitives::Uuid;
use sea_query::{
    Alias, ColumnDef, Expr, Iden, Index, IndexCreateStatement, Table, TableCreateStatement,
};
use sqlx::FromRow;

/// **Table: `core.model_effects`**
///
/// Immutable record of a completed LLM call. Keyed by composite hash of
/// (provider, model, `prompt_hash`, `schema_hash`, `input_hash`) so identical
/// requests can replay without re-invoking the model.
#[derive(Iden, Copy, Clone)]
pub enum ModelEffects {
    Table,
    Id,
    Provider,
    Model,
    PromptHash,
    SchemaHash,
    InputHash,
    CompositeKey,
    Output,
    OutputHash,
    ReplayPolicy,
    RecordedAt,
    RecordedBy,
    SourceNodeId,
    SourceEventId,
}

impl TableDef for ModelEffects {
    fn table_name() -> &'static str {
        "model_effects"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ModelEffects {
    #[must_use] 
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Id).uuid().not_null().primary_key())
            .col(ColumnDef::new(Self::Provider).string().not_null())
            .col(ColumnDef::new(Self::Model).string().not_null())
            .col(ColumnDef::new(Self::PromptHash).string().not_null())
            .col(ColumnDef::new(Self::SchemaHash).string())
            .col(ColumnDef::new(Self::InputHash).string().not_null())
            .col(ColumnDef::new(Self::CompositeKey).string().not_null())
            .col(ColumnDef::new(Self::Output).text().not_null())
            .col(ColumnDef::new(Self::OutputHash).string().not_null())
            .col(ColumnDef::new(Self::ReplayPolicy).string().not_null())
            .col(
                ColumnDef::new(Self::RecordedAt)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(Self::RecordedBy).string().not_null())
            .col(ColumnDef::new(Self::SourceNodeId).string())
            .col(ColumnDef::new(Self::SourceEventId).uuid())
            .col(
                ColumnDef::new(Alias::new("ts_coided"))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::cust("now()")),
            )
            .to_owned()
    }

    #[must_use] 
    pub fn composite_key_index() -> IndexCreateStatement {
        Index::create()
            .name("idx_model_effects_composite_key")
            .table(Self::Table)
            .col(Self::CompositeKey)
            .to_owned()
    }
}

/// Row returned by repository queries.
#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct ModelEffectRow {
    pub id: Uuid,
    pub provider: String,
    pub model: String,
    pub prompt_hash: String,
    pub schema_hash: Option<String>,
    pub input_hash: String,
    pub composite_key: String,
    pub output: String,
    pub output_hash: String,
    pub replay_policy: String,
    pub recorded_at: time::OffsetDateTime,
    pub recorded_by: String,
    pub source_node_id: Option<String>,
    pub source_event_id: Option<Uuid>,
}
