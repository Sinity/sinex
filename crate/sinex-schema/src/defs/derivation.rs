//! Derivation control-plane schema (sinex-0vx.4 / W1).
//!
//! DB mirror of the Rust vocabulary in `sinex_primitives::derivation`
//! (`DerivedProductClass`, `ClaimSupport`, `DerivationWriteSurface`,
//! `InputEligibility`, `DerivationScope`). The `product_declarations` table is
//! the write-side registry every derived output must be declared against;
//! `epochs`/`lanes`/`lane_outputs`/`lane_diffs` generalize the existing
//! `semantic.*` shadow-lane machinery to every `DerivedProductClass`, not just
//! entity/relation candidates; `projection_registry` and
//! `projection_dependencies` track rebuildable read-model freshness.
//!
//! Keep this schema narrow, same discipline as `semantic.rs`: these tables
//! are the write-side and freshness-side registries for the control plane,
//! not a parallel authority path for admitted facts. Promotion into
//! canonical state happens through `authority.finalizer_registry`
//! (`defs::authority`) and explicit operator judgment.

use crate::primitives::{Timestamp, Uuid};
use crate::{OperationsLog, SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction, Iden, Index,
    IndexCreateStatement, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `derivation.product_declarations` Table
// =============================================================================

/// **Table: `derivation.product_declarations`**
///
/// Mirrors `sinex_primitives::derivation::DerivationOutputDeclaration`. Every
/// automaton/writer that emits a `DerivedProductClass` output registers a row
/// here first; `derivation.enforce_event_product_declaration()` (see
/// `Events::create_no_update_trigger_sql`-style raw-SQL functions below)
/// rejects any `core.events`/`reflection.events` row whose declared
/// `product_class` has no matching declaration.
#[derive(Iden, Copy, Clone)]
pub enum DerivationProductDeclarations {
    Table,
    DeclarationId,
    Owner,
    ProductClass,
    WriteSurface,
    OutputSource,
    OutputEventType,
    ProjectionKind,
    ArtifactKind,
    ProposalKind,
    SemanticsVersion,
    InputEligibility,
    DefaultClaimSupport,
    VerificationCommand,
    CreatedAt,
}

impl TableDef for DerivationProductDeclarations {
    fn table_name() -> &'static str {
        "product_declarations"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "declaration_id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationProductDeclarationRecord {
    pub declaration_id: String,
    pub owner: String,
    pub product_class: String,
    pub write_surface: String,
    pub output_source: Option<String>,
    pub output_event_type: Option<String>,
    pub projection_kind: Option<String>,
    pub artifact_kind: Option<String>,
    pub proposal_kind: Option<String>,
    pub semantics_version: String,
    pub input_eligibility: String,
    pub default_claim_support: JsonValue,
    pub verification_command: String,
    pub created_at: Timestamp,
}

impl DerivationProductDeclarations {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Self::DeclarationId)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Self::Owner).text().not_null())
            .col(
                ColumnDef::new(Self::ProductClass).text().not_null().check(
                    Expr::cust(
                        "product_class IN ('canonical_derived_event', 'projection_row', 'analysis_claim', 'report_artifact', 'semantic_candidate', 'operator_judgment')",
                    ),
                ),
            )
            .col(
                ColumnDef::new(Self::WriteSurface).text().not_null().check(
                    Expr::cust(
                        "write_surface IN ('derived_output', 'projection_writer', 'artifact_writer', 'curation_writer', 'authority_finalizer')",
                    ),
                ),
            )
            .col(ColumnDef::new(Self::OutputSource).text())
            .col(ColumnDef::new(Self::OutputEventType).text())
            .col(ColumnDef::new(Self::ProjectionKind).text())
            .col(ColumnDef::new(Self::ArtifactKind).text())
            .col(ColumnDef::new(Self::ProposalKind).text())
            .col(ColumnDef::new(Self::SemanticsVersion).text().not_null())
            .col(
                ColumnDef::new(Self::InputEligibility).text().not_null().check(
                    Expr::cust(
                        "input_eligibility IN ('default_canonical_input', 'explicit_only', 'never_input')",
                    ),
                ),
            )
            .col(
                ColumnDef::new(Self::DefaultClaimSupport)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::VerificationCommand)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            // A derived_output writer must carry a full (output_source, output_event_type)
            // identity, and only a derived_output writer may carry one.
            .check(Expr::cust(
                "(write_surface = 'derived_output') = (output_source IS NOT NULL AND output_event_type IS NOT NULL)",
            ))
            // Corrected from the originating blueprint draft, whose form
            // `(product_class = 'projection_row') = (projection_kind IS NOT NULL) OR product_class <> 'projection_row'`
            // is a tautology (true whenever the left side is false, regardless of
            // projection_kind) and never rejects a malformed row. This form actually
            // enforces "projection_row implies projection_kind set".
            .check(Expr::cust(
                "product_class <> 'projection_row' OR projection_kind IS NOT NULL",
            ))
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_derivation_product_event_output")
                .table(Self::table_iden())
                .col(Self::OutputSource)
                .col(Self::OutputEventType)
                .col(Self::ProductClass)
                .col(Self::SemanticsVersion)
                .unique()
                .cond_where(
                    Expr::col(Self::OutputSource)
                        .is_not_null()
                        .and(Expr::col(Self::OutputEventType).is_not_null()),
                )
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `derivation.epochs` Table
// =============================================================================

/// **Table: `derivation.epochs`**
///
/// Generalizes `semantic.epochs` to every `DerivedProductClass`, keyed
/// against a `derivation.product_declarations` row instead of being implicitly
/// entity/relation-scoped. `scope_model` names which `DerivationScope`
/// variant `scope` encodes.
#[derive(Iden, Copy, Clone)]
pub enum DerivationEpochs {
    Table,
    Id,
    DeclarationId,
    Name,
    ProductClass,
    ScopeModel,
    Scope,
    SemanticsVersion,
    CodeRef,
    ConfigHash,
    Components,
    PromptSetHash,
    ModelConfigHash,
    CreatedBy,
    OperationId,
    SupersedesEpochId,
    CreatedAt,
}

impl TableDef for DerivationEpochs {
    fn table_name() -> &'static str {
        "epochs"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationEpochRecord {
    pub id: Uuid,
    pub declaration_id: String,
    pub name: String,
    pub product_class: String,
    pub scope_model: String,
    pub scope: JsonValue,
    pub semantics_version: String,
    pub code_ref: Option<String>,
    pub config_hash: String,
    pub components: JsonValue,
    pub prompt_set_hash: Option<String>,
    pub model_config_hash: Option<String>,
    pub created_by: String,
    pub operation_id: Option<Uuid>,
    pub supersedes_epoch_id: Option<Uuid>,
    pub created_at: Timestamp,
}

impl DerivationEpochs {
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
            .col(ColumnDef::new(Self::DeclarationId).text().not_null())
            .col(ColumnDef::new(Self::Name).text().not_null())
            .col(ColumnDef::new(Self::ProductClass).text().not_null())
            .col(
                ColumnDef::new(Self::ScopeModel).text().not_null().check(
                    Expr::cust(
                        "scope_model IN ('event_set', 'source_material_set', 'document_chunk_set', 'stream_checkpoint', 'time_window', 'scope_reconciler_key', 'projection_scope')",
                    ),
                ),
            )
            .col(ColumnDef::new(Self::Scope).json_binary().not_null())
            .col(ColumnDef::new(Self::SemanticsVersion).text().not_null())
            .col(ColumnDef::new(Self::CodeRef).text())
            .col(ColumnDef::new(Self::ConfigHash).text().not_null())
            .col(ColumnDef::new(Self::Components).json_binary().not_null())
            .col(ColumnDef::new(Self::PromptSetHash).text())
            .col(ColumnDef::new(Self::ModelConfigHash).text())
            .col(ColumnDef::new(Self::CreatedBy).text().not_null())
            .col(ColumnDef::new(Self::OperationId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::SupersedesEpochId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::DeclarationId)
                    .to(
                        DerivationProductDeclarations::table_iden(),
                        DerivationProductDeclarations::DeclarationId,
                    ),
            )
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
                .name("uk_derivation_epochs_declaration_scope_semver_confighash")
                .table(Self::table_iden())
                .col(Self::DeclarationId)
                .col(Self::Scope)
                .col(Self::SemanticsVersion)
                .col(Self::ConfigHash)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_derivation_epochs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `derivation.lanes` Table
// =============================================================================

/// **Table: `derivation.lanes`**
///
/// Generalizes `semantic.lanes` to every `DerivedProductClass`.
#[derive(Iden, Copy, Clone)]
pub enum DerivationLanes {
    Table,
    Id,
    DeclarationId,
    Name,
    Kind,
    ProductClass,
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

impl TableDef for DerivationLanes {
    fn table_name() -> &'static str {
        "lanes"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationLaneRecord {
    pub id: Uuid,
    pub declaration_id: String,
    pub name: String,
    pub kind: String,
    pub product_class: String,
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

impl DerivationLanes {
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
            .col(ColumnDef::new(Self::DeclarationId).text().not_null())
            .col(ColumnDef::new(Self::Name).text().not_null())
            .col(
                ColumnDef::new(Self::Kind)
                    .text()
                    .not_null()
                    .check(Expr::cust("kind IN ('canonical', 'shadow', 'experiment')")),
            )
            .col(ColumnDef::new(Self::ProductClass).text().not_null())
            .col(ColumnDef::new(Self::BaseEpochId).custom(Alias::new("UUID")))
            .col(
                ColumnDef::new(Self::CandidateEpochId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::Scope).json_binary().not_null())
            .col(
                ColumnDef::new(Self::Status).text().not_null().check(
                    Expr::cust(
                        "status IN ('planned', 'running', 'completed', 'promoted', 'discarded', 'expired', 'failed')",
                    ),
                ),
            )
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
                    .from(Self::table_iden(), Self::DeclarationId)
                    .to(
                        DerivationProductDeclarations::table_iden(),
                        DerivationProductDeclarations::DeclarationId,
                    ),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::BaseEpochId)
                    .to(DerivationEpochs::table_iden(), DerivationEpochs::Id)
                    .on_delete(ForeignKeyAction::SetNull),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::CandidateEpochId)
                    .to(DerivationEpochs::table_iden(), DerivationEpochs::Id)
                    .on_delete(ForeignKeyAction::Cascade),
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
                .name("uk_derivation_lanes_declaration_kind_candidate_scope")
                .table(Self::table_iden())
                .col(Self::DeclarationId)
                .col(Self::Kind)
                .col(Self::CandidateEpochId)
                .col(Self::Scope)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_derivation_lanes_status")
                .table(Self::table_iden())
                .col(Self::Status)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_derivation_lanes_candidate_epoch")
                .table(Self::table_iden())
                .col(Self::CandidateEpochId)
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `derivation.lane_outputs` Table
// =============================================================================

/// **Table: `derivation.lane_outputs`**
///
/// Generalizes `semantic.lane_outputs`. Carries the `ClaimSupport` vector
/// alongside every output; shape validation (`ClaimSupport::is_shape_valid`)
/// happens in Rust before the JSONB is ever written — there is no SQL-side
/// `claim_support_is_valid()` function, deliberately: mirroring the
/// adjudication-status/evidence-id shape invariant in PL/pgSQL would
/// duplicate logic already enforced at construction time in
/// `sinex_primitives::derivation::ClaimSupport`, and is out of scope for this
/// bead (sinex-0vx.4).
#[derive(Iden, Copy, Clone)]
pub enum DerivationLaneOutputs {
    Table,
    LaneId,
    ProductClass,
    OutputKind,
    OutputKey,
    OutputHash,
    Payload,
    ClaimSupport,
    SourceEventId,
    SourceMaterialId,
    SourceAnchor,
    Metadata,
    CreatedAt,
}

impl TableDef for DerivationLaneOutputs {
    fn table_name() -> &'static str {
        "lane_outputs"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "(lane_id, product_class, output_kind, output_key)"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationLaneOutputRecord {
    pub lane_id: Uuid,
    pub product_class: String,
    pub output_kind: String,
    pub output_key: String,
    pub output_hash: String,
    pub payload: JsonValue,
    pub claim_support: JsonValue,
    pub source_event_id: Option<Uuid>,
    pub source_material_id: Option<Uuid>,
    pub source_anchor: Option<JsonValue>,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
}

impl DerivationLaneOutputs {
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
            .col(ColumnDef::new(Self::ProductClass).text().not_null())
            .col(ColumnDef::new(Self::OutputKind).text().not_null())
            .col(ColumnDef::new(Self::OutputKey).text().not_null())
            .col(ColumnDef::new(Self::OutputHash).text().not_null())
            .col(ColumnDef::new(Self::Payload).json_binary().not_null())
            .col(ColumnDef::new(Self::ClaimSupport).json_binary().not_null())
            .col(ColumnDef::new(Self::SourceEventId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::SourceMaterialId).custom(Alias::new("UUID")))
            .col(ColumnDef::new(Self::SourceAnchor).json_binary())
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
                    .col(Self::ProductClass)
                    .col(Self::OutputKind)
                    .col(Self::OutputKey),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::LaneId)
                    .to(DerivationLanes::table_iden(), DerivationLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            // No declarative FK on source_event_id -> core.events(id): an inbound FK
            // to the events hypertable blocks columnstore compression of its chunks
            // ("found a FK into a chunk while truncating"), same rationale as
            // semantic.lane_outputs.source_event_id (Ref sinex-h8no) and as
            // events.adjudication_event_id below (defs/events.rs). Soft provenance
            // pointer only, no enforced cascade.
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
                .name("ix_derivation_lane_outputs_kind_hash")
                .table(Self::table_iden())
                .col(Self::ProductClass)
                .col(Self::OutputKind)
                .col(Self::OutputHash)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_derivation_lane_outputs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `derivation.lane_diffs` Table
// =============================================================================

/// **Table: `derivation.lane_diffs`**
///
/// Generalizes `semantic.lane_diffs`.
#[derive(Iden, Copy, Clone)]
pub enum DerivationLaneDiffs {
    Table,
    Id,
    BaselineLaneId,
    CandidateLaneId,
    ProductClass,
    DiffKind,
    Counts,
    Examples,
    ReportHash,
    CreatedAt,
}

impl TableDef for DerivationLaneDiffs {
    fn table_name() -> &'static str {
        "lane_diffs"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationLaneDiffRecord {
    pub id: Uuid,
    pub baseline_lane_id: Uuid,
    pub candidate_lane_id: Uuid,
    pub product_class: String,
    pub diff_kind: String,
    pub counts: JsonValue,
    pub examples: JsonValue,
    pub report_hash: String,
    pub created_at: Timestamp,
}

impl DerivationLaneDiffs {
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
            .col(ColumnDef::new(Self::ProductClass).text().not_null())
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
                    .to(DerivationLanes::table_iden(), DerivationLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::CandidateLaneId)
                    .to(DerivationLanes::table_iden(), DerivationLanes::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("uk_derivation_lane_diffs_report")
                .table(Self::table_iden())
                .col(Self::BaselineLaneId)
                .col(Self::CandidateLaneId)
                .col(Self::ProductClass)
                .col(Self::DiffKind)
                .col(Self::ReportHash)
                .unique()
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_derivation_lane_diffs_created_at")
                .table(Self::table_iden())
                .col(Self::CreatedAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// The `derivation.projection_registry` Table
// =============================================================================

/// **Table: `derivation.projection_registry`**
///
/// Freshness/status tracker for rebuildable read-model (`ProjectionRow`)
/// state. `coverage_window` is a `tstzrange` (same custom-type idiom as
/// `OperationsLog::ScopeWindow`); the GiST index on it
/// (`create_gist_indexes_sql`) needs no extra extension — a single-column
/// range-typed index uses PostgreSQL's built-in `range_ops` GiST support
/// (`btree_gist` is only required when mixing scalar equality columns with a
/// range/geometric type in the same index, or for `EXCLUDE` constraints; this
/// is a plain single-column index and neither applies).
#[derive(Iden, Copy, Clone)]
pub enum DerivationProjectionRegistry {
    Table,
    Id,
    ProjectionKind,
    ScopeKey,
    SemanticsVersion,
    InputFingerprint,
    CoverageWindow,
    Status,
    FreshnessClass,
    AcceptableStaleness,
    BuiltAt,
    SourceCounts,
    StaleReason,
    LastError,
    VerificationCommand,
    UpdatedAt,
}

impl TableDef for DerivationProjectionRegistry {
    fn table_name() -> &'static str {
        "projection_registry"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationProjectionRegistryRecord {
    pub id: Uuid,
    pub projection_kind: String,
    pub scope_key: String,
    pub semantics_version: String,
    pub input_fingerprint: String,
    pub status: String,
    pub freshness_class: String,
    pub built_at: Option<Timestamp>,
    pub source_counts: JsonValue,
    pub stale_reason: Option<String>,
    pub last_error: Option<String>,
    pub verification_command: String,
    pub updated_at: Timestamp,
}

impl DerivationProjectionRegistry {
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
            .col(ColumnDef::new(Self::ProjectionKind).text().not_null())
            .col(ColumnDef::new(Self::ScopeKey).text().not_null())
            .col(ColumnDef::new(Self::SemanticsVersion).text().not_null())
            .col(ColumnDef::new(Self::InputFingerprint).text().not_null())
            .col(
                ColumnDef::new(Self::CoverageWindow)
                    .custom(Alias::new("tstzrange"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::Status).text().not_null().check(
                    Expr::cust(
                        "status IN ('absent', 'building', 'ready', 'stale', 'failed', 'partial')",
                    ),
                ),
            )
            .col(
                ColumnDef::new(Self::FreshnessClass).text().not_null().check(
                    Expr::cust(
                        "freshness_class IN ('seconds', 'minutes', 'hours', 'days', 'manual')",
                    ),
                ),
            )
            .col(
                ColumnDef::new(Self::AcceptableStaleness)
                    .custom(Alias::new("interval"))
                    .not_null(),
            )
            .col(ColumnDef::new(Self::BuiltAt).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Self::SourceCounts)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(Self::StaleReason).text())
            .col(ColumnDef::new(Self::LastError).text())
            .col(
                ColumnDef::new(Self::VerificationCommand)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            // Corrected from the originating blueprint draft's tautological
            // `(status = 'ready') = (built_at IS NOT NULL) OR status <> 'ready'`
            // form (true whenever the left side is false, never rejects a
            // malformed row). This form actually enforces "ready implies built_at
            // set".
            .check(Expr::cust(
                "status <> 'ready' OR built_at IS NOT NULL",
            ))
            .check(Expr::cust(
                "status NOT IN ('stale', 'failed', 'partial') OR stale_reason IS NOT NULL",
            ))
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_projection_registry_lookup")
                .table(Self::table_iden())
                .col(Self::ProjectionKind)
                .col(Self::ScopeKey)
                .col(Self::Status)
                .col(Self::SemanticsVersion)
                .to_owned(),
        ]
    }

    /// GiST index on `coverage_window` (tstzrange). Raw SQL: sea-query has no
    /// GiST `IndexType` in this workspace's version, same reason
    /// `Events::create_gin_indexes_sql` and the embedding HNSW indexes are
    /// raw SQL too.
    #[must_use]
    pub fn create_gist_indexes_sql() -> Vec<String> {
        vec![format!(
            "CREATE INDEX IF NOT EXISTS ix_projection_registry_coverage ON {}.{} USING GIST (coverage_window)",
            Self::schema_name(),
            Self::table_name()
        )]
    }
}

// =============================================================================
// The `derivation.projection_dependencies` Table
// =============================================================================

/// **Table: `derivation.projection_dependencies`**
///
/// What inputs a `derivation.projection_registry` row's freshness depends on
/// — the invalidation edge set. A projection is stale once any dependency's
/// live state diverges from what was captured here at build time.
#[derive(Iden, Copy, Clone)]
pub enum DerivationProjectionDependencies {
    Table,
    ProjectionId,
    DependencyKind,
    DependencyKey,
    EventSource,
    EventType,
    ScopeKey,
    CoverageWindow,
    InputFingerprint,
    SourceCount,
    CreatedAt,
}

impl TableDef for DerivationProjectionDependencies {
    fn table_name() -> &'static str {
        "projection_dependencies"
    }

    fn schema_name() -> &'static str {
        "derivation"
    }

    fn primary_key() -> &'static str {
        "(projection_id, dependency_kind, dependency_key)"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DerivationProjectionDependencyRecord {
    pub projection_id: Uuid,
    pub dependency_kind: String,
    pub dependency_key: String,
    pub event_source: Option<String>,
    pub event_type: Option<String>,
    pub scope_key: Option<String>,
    pub input_fingerprint: Option<String>,
    pub source_count: i64,
    pub created_at: Timestamp,
}

impl DerivationProjectionDependencies {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Self::ProjectionId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Self::DependencyKind).text().not_null().check(
                    Expr::cust(
                        "dependency_kind IN ('event_source_type', 'event_id', 'source_material', 'automaton', 'derivation_declaration', 'semantics_version', 'redaction_policy', 'schema')",
                    ),
                ),
            )
            .col(ColumnDef::new(Self::DependencyKey).text().not_null())
            .col(ColumnDef::new(Self::EventSource).text())
            .col(ColumnDef::new(Self::EventType).text())
            .col(ColumnDef::new(Self::ScopeKey).text())
            .col(ColumnDef::new(Self::CoverageWindow).custom(Alias::new("tstzrange")))
            .col(ColumnDef::new(Self::InputFingerprint).text())
            .col(
                ColumnDef::new(Self::SourceCount)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Self::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .primary_key(
                Index::create()
                    .col(Self::ProjectionId)
                    .col(Self::DependencyKind)
                    .col(Self::DependencyKey),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(Self::table_iden(), Self::ProjectionId)
                    .to(
                        DerivationProjectionRegistry::table_iden(),
                        DerivationProjectionRegistry::Id,
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
                .name("ix_projection_dependencies_event_scope")
                .table(Self::table_iden())
                .col(Self::EventSource)
                .col(Self::EventType)
                .col(Self::ScopeKey)
                .cond_where(
                    Expr::col(Self::EventSource)
                        .is_not_null()
                        .and(Expr::col(Self::EventType).is_not_null()),
                )
                .to_owned(),
        ]
    }
}

#[cfg(test)]
#[path = "derivation_test.rs"]
mod tests;
