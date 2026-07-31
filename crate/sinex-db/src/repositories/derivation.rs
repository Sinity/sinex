//! Repository for `derivation.product_declarations` (sinex-0vx.4): the
//! write-side registry `derivation.enforce_event_product_declaration()`
//! checks every derived-event write against. Row shape mirrors
//! `sinex_primitives::derivation::DerivationOutputDeclaration`.
//!
//! This repository is intentionally narrow (find-by-id + insert-if-absent):
//! reconciliation policy — deciding whether an existing row that disagrees
//! with a static declaration should be treated as a fail-closed startup
//! error — is a `sinexd` supervisor-startup concern
//! (`crate::automata::product_declarations` there), not a DB-layer one. See
//! sinex-x79t.

use super::common::{DbResult, Repository, db_error};
use sinex_primitives::derivation::DerivationOutputDeclaration;
use sinex_primitives::error::SinexError;
use sqlx::PgPool;

/// A `derivation.product_declarations` row as currently stored, for
/// comparison against a static [`DerivationOutputDeclaration`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExistingProductDeclaration {
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
    pub default_claim_support: serde_json::Value,
    pub verification_command: String,
}

pub struct ProductDeclarationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ProductDeclarationRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl ProductDeclarationRepository<'_> {
    /// Fetch the current row for `declaration_id`, if one exists.
    pub async fn find_by_declaration_id(
        &self,
        declaration_id: &str,
    ) -> DbResult<Option<ExistingProductDeclaration>> {
        sqlx::query_as!(
            ExistingProductDeclaration,
            r#"
            SELECT
                owner,
                product_class,
                write_surface,
                output_source,
                output_event_type,
                projection_kind,
                artifact_kind,
                proposal_kind,
                semantics_version,
                input_eligibility,
                default_claim_support,
                verification_command
            FROM derivation.product_declarations
            WHERE declaration_id = $1
            "#,
            declaration_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find product declaration by id"))
    }

    /// Insert a new row for `declaration`. A no-op (`ON CONFLICT DO
    /// NOTHING`) if a row for this `declaration_id` already exists — callers
    /// that need fail-closed mismatch detection must call
    /// `find_by_declaration_id` first and compare before inserting.
    pub async fn insert(&self, declaration: &DerivationOutputDeclaration) -> DbResult<()> {
        let default_claim_support =
            serde_json::to_value(declaration.default_support).map_err(|error| {
                SinexError::database(format!(
                    "declaration '{}' default_support could not be serialized: {error}",
                    declaration.declaration_id
                ))
            })?;

        sqlx::query!(
            r#"
            INSERT INTO derivation.product_declarations (
                declaration_id, owner, product_class, write_surface,
                output_source, output_event_type, projection_kind, artifact_kind,
                proposal_kind, semantics_version, input_eligibility,
                default_claim_support, verification_command
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (declaration_id) DO NOTHING
            "#,
            declaration.declaration_id,
            declaration.owner,
            declaration.product_class.as_str(),
            declaration.write_surface.as_str(),
            declaration.output_source,
            declaration.output_event_type,
            declaration.projection_kind,
            declaration.artifact_kind,
            declaration.proposal_kind,
            declaration.semantics_version,
            declaration.input_eligibility.as_str(),
            default_claim_support,
            declaration.verification_command,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "insert product declaration"))?;

        Ok(())
    }
}
