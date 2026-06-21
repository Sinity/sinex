//! Model-effect record schema.
//!
//! Dormant schema surface for recorded model effects. The table records LLM
//! call outputs keyed by composite input hash so non-deterministic derived
//! outputs can be replayed against a recorded effect rather than re-issued.
//!
//! Current ownership is deliberately explicit: the schema and repository are
//! registered, but no production caller records or replays model effects yet.
//! Future live use must enter through the Proposal/Judgment/Operation or
//! derivation path that consumes this table; `core.model_effects` must not
//! become a parallel event log, hidden authority channel, or ad hoc model cache.

use crate::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, Iden, Index, IndexCreateStatement, Table, TableCreateStatement,
};

/// **Table: `core.model_effects`**
///
/// Immutable record of a completed LLM call.
///
/// The row is keyed by a composite hash of `(provider, model, prompt_hash,
/// schema_hash, input_hash)` so identical requests can replay without
/// re-invoking the model once a production effect path is wired.
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
    SourceModuleName,
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
            .table(Self::table_iden())
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
            .col(ColumnDef::new(Self::SourceModuleName).string())
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
            .table(Self::table_iden())
            .col(Self::CompositeKey)
            .to_owned()
    }
}

// The row type returned by `pool.model_effects()` queries lives with the
// repository in sinex-db (`crate::models::model_effect::ModelEffectRow`); the
// schema crate only owns the table definition.
