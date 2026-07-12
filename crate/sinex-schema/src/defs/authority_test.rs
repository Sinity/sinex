//! Live-DB tests for `authority.finalizer_registry` (sinex-0vx.4 / W1).

use super::*;
use xtask::sandbox::prelude::*;

const UNREVIEWED_CLAIM_SUPPORT: &str = r#"{
    "support_level": "unsupported",
    "source_coverage": "unknown",
    "temporal_quality": "unknown",
    "adjudication": "unreviewed",
    "evidence_event_count": 0,
    "evidence_material_count": 0,
    "support_family_count": 0,
    "counterevidence_count": 0
}"#;

/// `authority_finalizer` (like `projection_writer`/`artifact_writer`/
/// `curation_writer`) is NOT a `derived_output` write surface, so per the
/// `(write_surface = 'derived_output') = (output_source IS NOT NULL AND
/// output_event_type IS NOT NULL)` CHECK — the DB mirror of
/// `DerivationOutputDeclaration::validate()` in sinex-primitives — this
/// declaration must leave `output_source`/`output_event_type` NULL. The
/// concrete (source, event_type) pair a given finalizer writes to is pinned
/// on `authority.finalizer_registry` itself (NOT NULL there), not on the
/// shared declaration; several finalizers can share one declaration_id.
async fn insert_declaration(pool: &sqlx::PgPool, declaration_id: &str) -> TestResult<()> {
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            semantics_version, input_eligibility, default_claim_support, verification_command
        )
        VALUES ($1, 'test-owner', 'operator_judgment', 'authority_finalizer',
                'v1', 'never_input', $2::jsonb, 'xtask test -p sinex-schema')
        "#,
    )
    .bind(declaration_id)
    .bind(UNREVIEWED_CLAIM_SUPPORT)
    .execute(pool)
    .await?;
    Ok(())
}

#[sinex_test]
async fn authority_finalizer_registry_registered_declaration_succeeds(
    ctx: TestContext,
) -> TestResult<()> {
    insert_declaration(
        ctx.pool(),
        "test.authority_finalizer_registry.registered_declaration_succeeds",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO authority.finalizer_registry (
            finalizer_id, proposal_kind, output_source, output_event_type,
            output_product_class, derivation_declaration_id, registered_by
        )
        VALUES (
            'test.finalizer.registered_declaration_succeeds', 'test.proposal',
            'test.authority', 'test.judgment', 'canonical_derived_event',
            'test.authority_finalizer_registry.registered_declaration_succeeds', 'test-owner'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect("a finalizer referencing a registered product_declarations row must succeed");

    let (requires_human_judgment, active): (bool, bool) = sqlx::query_as(
        "SELECT requires_human_judgment, active FROM authority.finalizer_registry WHERE finalizer_id = $1",
    )
    .bind("test.finalizer.registered_declaration_succeeds")
    .fetch_one(ctx.pool())
    .await?;
    assert!(requires_human_judgment, "requires_human_judgment defaults to true");
    assert!(active, "active defaults to true");
    Ok(())
}

#[sinex_test]
async fn authority_finalizer_registry_rejects_unregistered_declaration(
    ctx: TestContext,
) -> TestResult<()> {
    let error = sqlx::query(
        r#"
        INSERT INTO authority.finalizer_registry (
            finalizer_id, proposal_kind, output_source, output_event_type,
            output_product_class, derivation_declaration_id, registered_by
        )
        VALUES (
            'test.finalizer.rejects_unregistered_declaration', 'test.proposal',
            'test.authority', 'test.judgment', 'canonical_derived_event',
            'test.authority_finalizer_registry.no_such_declaration', 'test-owner'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect_err("a finalizer referencing a missing product_declarations row must violate the FK");
    assert!(
        error.to_string().to_lowercase().contains("foreign key"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn authority_finalizer_registry_rejects_duplicate_active_route(
    ctx: TestContext,
) -> TestResult<()> {
    insert_declaration(
        ctx.pool(),
        "test.authority_finalizer_registry.duplicate_active_route",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO authority.finalizer_registry (
            finalizer_id, proposal_kind, output_source, output_event_type,
            output_product_class, derivation_declaration_id, registered_by
        )
        VALUES (
            'test.finalizer.duplicate_active_route.first', 'test.proposal.dup',
            'test.authority.dup', 'test.judgment.dup', 'canonical_derived_event',
            'test.authority_finalizer_registry.duplicate_active_route', 'test-owner'
        )
        "#,
    )
    .execute(ctx.pool())
    .await?;

    let error = sqlx::query(
        r#"
        INSERT INTO authority.finalizer_registry (
            finalizer_id, proposal_kind, output_source, output_event_type,
            output_product_class, derivation_declaration_id, registered_by
        )
        VALUES (
            'test.finalizer.duplicate_active_route.second', 'test.proposal.dup',
            'test.authority.dup', 'test.judgment.dup', 'canonical_derived_event',
            'test.authority_finalizer_registry.duplicate_active_route', 'test-owner'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect_err(
        "a second active finalizer for the same (proposal_kind, output_source, \
         output_event_type) must violate uk_authority_finalizer_proposal_output",
    );
    assert!(
        error.to_string().to_lowercase().contains("duplicate key"),
        "unexpected error: {error}"
    );
    Ok(())
}
