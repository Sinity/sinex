//! Repository for `authority.finalizer_registry` (sinex-0vx.5): the
//! persistent, DB-backed registry that a curation finalizer must find a
//! matching row in before it may emit a finalized/adjudicated output.
//!
//! Replaces the v0 declaration-only `FinalizerRegistration`
//! (`sinex_primitives::authority::FinalizerRegistration`) — that type stays
//! as a fixture/doc DTO, but production enforcement now goes through this
//! table. `derivation.enforce_event_product_declaration()` (sinex-0vx.4)
//! already gates *which* `(source, event_type, product_class)` triples may be
//! written; this repository gates whether a *specific curation proposal
//! kind* is even allowed to reach a finalized output at all, and under what
//! actor-kind policy — the curation-bypass rejection this bead is named for.
//!
//! Like `ProductDeclarationRepository`, this repository is intentionally
//! narrow (find-by-id, find-active-for-lookup, insert-if-absent):
//! reconciliation policy (fail-closed startup diff) is a `sinexd`
//! supervisor-startup concern (`crate::authority` there), not a DB-layer one.

use super::common::{DbResult, Repository, db_error};
use serde_json::Value as JsonValue;
use sqlx::PgPool;

/// A `authority.finalizer_registry` row as currently stored, for comparison
/// against a static finalizer declaration during startup reconciliation.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExistingFinalizerRegistration {
    pub proposal_kind: String,
    pub output_source: String,
    pub output_event_type: String,
    pub output_product_class: String,
    pub derivation_declaration_id: String,
    pub requires_human_judgment: bool,
    pub auto_accept_policy: Option<JsonValue>,
    pub active: bool,
    pub registered_by: String,
}

/// The row a curation finalizer looks up at finalize time: does a registered,
/// active finalizer exist for this `(proposal_kind, output_source,
/// output_event_type)` triple, and what actor-kind policy does it carry.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActiveFinalizerPolicy {
    pub finalizer_id: String,
    pub requires_human_judgment: bool,
    pub auto_accept_policy: Option<JsonValue>,
}

pub struct AuthorityRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for AuthorityRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl AuthorityRepository<'_> {
    /// Fetch the current row for `finalizer_id`, if one exists — used by the
    /// startup reconciler to detect static-vs-DB drift.
    pub async fn find_by_finalizer_id(
        &self,
        finalizer_id: &str,
    ) -> DbResult<Option<ExistingFinalizerRegistration>> {
        sqlx::query_as!(
            ExistingFinalizerRegistration,
            r#"
            SELECT
                proposal_kind,
                output_source,
                output_event_type,
                output_product_class,
                derivation_declaration_id,
                requires_human_judgment,
                auto_accept_policy,
                active,
                registered_by
            FROM authority.finalizer_registry
            WHERE finalizer_id = $1
            "#,
            finalizer_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find finalizer registration by id"))
    }

    /// The curation-bypass rejection gate: is there an active, registered
    /// finalizer for this exact `(proposal_kind, output_source,
    /// output_event_type)` triple? `handle_curation_finalize` must find
    /// `Some` here before it is permitted to emit a finalized output —
    /// `None` means "no finalizer registered", the bypass case this bead
    /// rejects.
    pub async fn find_active_finalizer(
        &self,
        proposal_kind: &str,
        output_source: &str,
        output_event_type: &str,
    ) -> DbResult<Option<ActiveFinalizerPolicy>> {
        sqlx::query_as!(
            ActiveFinalizerPolicy,
            r#"
            SELECT finalizer_id, requires_human_judgment, auto_accept_policy
            FROM authority.finalizer_registry
            WHERE proposal_kind = $1
              AND output_source = $2
              AND output_event_type = $3
              AND active = true
            "#,
            proposal_kind,
            output_source,
            output_event_type,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find active finalizer registration"))
    }

    /// Insert a new row. A no-op (`ON CONFLICT DO NOTHING`) if a row for
    /// this `finalizer_id` already exists — callers that need fail-closed
    /// mismatch detection must call `find_by_finalizer_id` first and compare
    /// before inserting (mirrors `ProductDeclarationRepository::insert`).
    #[allow(clippy::too_many_arguments)]
    pub async fn insert(
        &self,
        finalizer_id: &str,
        proposal_kind: &str,
        output_source: &str,
        output_event_type: &str,
        output_product_class: &str,
        derivation_declaration_id: &str,
        requires_human_judgment: bool,
        auto_accept_policy: Option<&JsonValue>,
        registered_by: &str,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            INSERT INTO authority.finalizer_registry (
                finalizer_id, proposal_kind, output_source, output_event_type,
                output_product_class, derivation_declaration_id,
                requires_human_judgment, auto_accept_policy, active, registered_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, true, $9
            )
            ON CONFLICT (finalizer_id) DO NOTHING
            "#,
            finalizer_id,
            proposal_kind,
            output_source,
            output_event_type,
            output_product_class,
            derivation_declaration_id,
            requires_human_judgment,
            auto_accept_policy,
            registered_by,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "insert finalizer registration"))?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "authority_test.rs"]
mod tests;
