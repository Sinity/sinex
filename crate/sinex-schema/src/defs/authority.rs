//! Authority finalizer registry (sinex-0vx.4 / W1).
//!
//! `authority.finalizer_registry` is the write-side registry for the
//! `authority_finalizer` write surface (`DerivationWriteSurface::AuthorityFinalizer`
//! in `sinex_primitives::derivation`) — the only mechanism permitted to emit
//! `operator_judgment` product-class events. A finalizer converts a curation
//! proposal (`proposal_kind`) into an accepted/rejected/superseded judgment
//! event; whether that requires human judgment vs. a deterministic
//! auto-accept policy is declared per row here, never inferred at write time.
//!
//! Keep this schema narrow: it is the finalizer catalog, not a parallel
//! authority path. The actual proposal/judgment event flow lives in
//! `core.events`/`reflection.events` (`product_class = 'operator_judgment'`),
//! gated by `derivation.enforce_event_product_declaration()`
//! (`defs/events.rs`).

use crate::primitives::Timestamp;
use crate::{DerivationProductDeclarations, TableDef};
use sea_query::{
    ColumnDef, Expr, ForeignKey, Iden, Index, IndexCreateStatement, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `authority.finalizer_registry` Table
// =============================================================================

/// **Table: `authority.finalizer_registry`**
#[derive(Iden, Copy, Clone)]
pub enum AuthorityFinalizerRegistry {
    Table,
    FinalizerId,
    ProposalKind,
    OutputSource,
    OutputEventType,
    OutputProductClass,
    DerivationDeclarationId,
    RequiresHumanJudgment,
    AutoAcceptPolicy,
    Active,
    RegisteredBy,
    CreatedAt,
}

impl TableDef for AuthorityFinalizerRegistry {
    fn table_name() -> &'static str {
        "finalizer_registry"
    }

    fn schema_name() -> &'static str {
        "authority"
    }

    fn primary_key() -> &'static str {
        "finalizer_id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct AuthorityFinalizerRegistryRecord {
    pub finalizer_id: String,
    pub proposal_kind: String,
    pub output_source: String,
    pub output_event_type: String,
    pub output_product_class: String,
    pub derivation_declaration_id: String,
    pub requires_human_judgment: bool,
    pub auto_accept_policy: Option<JsonValue>,
    pub active: bool,
    pub registered_by: String,
    pub created_at: Timestamp,
}

impl AuthorityFinalizerRegistry {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Self::FinalizerId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Self::ProposalKind).text().not_null())
            .col(ColumnDef::new(Self::OutputSource).text().not_null())
            .col(ColumnDef::new(Self::OutputEventType).text().not_null())
            .col(
                ColumnDef::new(Self::OutputProductClass).text().not_null().check(
                    Expr::cust(
                        "output_product_class IN ('canonical_derived_event', 'analysis_claim', 'semantic_candidate', 'report_artifact')",
                    ),
                ),
            )
            .col(
                ColumnDef::new(Self::DerivationDeclarationId)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::RequiresHumanJudgment)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(ColumnDef::new(Self::AutoAcceptPolicy).json_binary())
            .col(
                ColumnDef::new(Self::Active)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(ColumnDef::new(Self::RegisteredBy).text().not_null())
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::DerivationDeclarationId)
                    .to(
                        DerivationProductDeclarations::table_iden(),
                        DerivationProductDeclarations::DeclarationId,
                    ),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_authority_finalizer_proposal_output")
                .table(Self::table_iden())
                .col(Self::ProposalKind)
                .col(Self::OutputSource)
                .col(Self::OutputEventType)
                .col(Self::Active)
                .unique()
                .to_owned(),
        ]
    }
}

#[cfg(test)]
#[path = "authority_test.rs"]
mod tests;
