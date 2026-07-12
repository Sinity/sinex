//! Live-DB tests for the derivation control-plane schema (sinex-0vx.4 / W1):
//! table creation, the corrected CHECK constraints (the blueprint's originals
//! were tautological — see the doc comments in `derivation.rs`), and the
//! `derivation.enforce_event_product_declaration()` trigger on `core.events`.

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

/// An `adjudication: accepted` claim-support vector deliberately missing
/// `adjudication_event_id` — the malformed shape the trigger must reject for
/// any product_class other than `operator_judgment`.
const ACCEPTED_CLAIM_SUPPORT_WITHOUT_JUDGMENT: &str = r#"{
    "support_level": "direct",
    "source_coverage": "covered",
    "temporal_quality": "realtime_capture",
    "adjudication": "accepted",
    "evidence_event_count": 1,
    "evidence_material_count": 1,
    "support_family_count": 1,
    "counterevidence_count": 0
}"#;

async fn insert_material(pool: &sqlx::PgPool, label: &str) -> TestResult<Uuid> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO raw.source_material_registry (
            material_kind, source_identifier, status, timing_info_type, metadata, total_bytes
        )
        VALUES ('local_cas', $1, 'completed', 'staged_at', '{}'::jsonb, 1000)
        RETURNING id
        "#,
    )
    .bind(format!("test.derivation-schema.{label}"))
    .fetch_one(pool)
    .await?;
    Ok(id)
}

async fn insert_declaration(
    pool: &sqlx::PgPool,
    declaration_id: &str,
    product_class: &str,
    output_source: &str,
    output_event_type: &str,
) -> TestResult<()> {
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        )
        VALUES ($1, 'test-owner', $2, 'derived_output', $3, $4, 'v1',
                'default_canonical_input', $5::jsonb, 'xtask test -p sinex-schema')
        "#,
    )
    .bind(declaration_id)
    .bind(product_class)
    .bind(output_source)
    .bind(output_event_type)
    .bind(UNREVIEWED_CLAIM_SUPPORT)
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts a material-provenance `core.events` row carrying the derivation
/// control-plane columns, for exercising
/// `derivation.enforce_event_product_declaration()` without needing a parent
/// derived event.
async fn insert_material_event_with_product_class(
    pool: &sqlx::PgPool,
    source_material_id: Uuid,
    source: &str,
    event_type: &str,
    product_class: Option<&str>,
    claim_support: Option<&str>,
    derivation_declaration_id: Option<&str>,
    adjudication_event_id: Option<Uuid>,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload, ts_orig,
            source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
            product_class, claim_support, derivation_declaration_id, adjudication_event_id
        )
        VALUES (
            uuidv7(), $1, $2, 'test-host', '{}'::jsonb, now(),
            $3, 0, 0, 0, 'byte',
            $4, $5::jsonb, $6, $7
        )
        RETURNING id
        "#,
    )
    .bind(source)
    .bind(event_type)
    .bind(source_material_id)
    .bind(product_class)
    .bind(claim_support)
    .bind(derivation_declaration_id)
    .bind(adjudication_event_id)
    .fetch_one(pool)
    .await
}

#[sinex_test]
async fn derivation_schema_creates_all_tables_cleanly(ctx: TestContext) -> TestResult<()> {
    for qualified_name in [
        "derivation.product_declarations",
        "derivation.epochs",
        "derivation.lanes",
        "derivation.lane_outputs",
        "derivation.lane_diffs",
        "derivation.projection_registry",
        "derivation.projection_dependencies",
        "authority.finalizer_registry",
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(qualified_name)
            .fetch_one(ctx.pool())
            .await?;
        assert!(exists, "{qualified_name} must exist after apply()");
    }
    Ok(())
}

#[sinex_test]
async fn derivation_schema_declared_write_succeeds(ctx: TestContext) -> TestResult<()> {
    let material_id = insert_material(ctx.pool(), "declared-write-succeeds").await?;
    insert_declaration(
        ctx.pool(),
        "test.derivation_schema.declared_write_succeeds",
        "canonical_derived_event",
        "test.schema_derivation",
        "test.declared_event",
    )
    .await?;

    let event_id = insert_material_event_with_product_class(
        ctx.pool(),
        material_id,
        "test.schema_derivation",
        "test.declared_event",
        Some("canonical_derived_event"),
        Some(UNREVIEWED_CLAIM_SUPPORT),
        Some("test.derivation_schema.declared_write_succeeds"),
        None,
    )
    .await
    .expect("declared product write with a registered declaration must succeed");

    let stored_product_class: String =
        sqlx::query_scalar("SELECT product_class FROM core.events WHERE id = $1")
            .bind(event_id)
            .fetch_one(ctx.pool())
            .await?;
    assert_eq!(stored_product_class, "canonical_derived_event");
    Ok(())
}

#[sinex_test]
async fn derivation_schema_undeclared_product_write_is_rejected(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = insert_material(ctx.pool(), "undeclared-write-rejected").await?;

    let error = insert_material_event_with_product_class(
        ctx.pool(),
        material_id,
        "test.schema_derivation",
        "test.undeclared_event",
        Some("canonical_derived_event"),
        Some(UNREVIEWED_CLAIM_SUPPORT),
        Some("test.derivation_schema.no_such_declaration"),
        None,
    )
    .await
    .expect_err("a product_class write with no matching product_declarations row must be rejected");

    assert!(
        error.to_string().contains("undeclared product write"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn derivation_schema_adjudicated_claim_requires_adjudication_event_id(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = insert_material(ctx.pool(), "adjudicated-without-judgment").await?;
    insert_declaration(
        ctx.pool(),
        "test.derivation_schema.adjudicated_without_judgment",
        "analysis_claim",
        "test.schema_derivation",
        "test.adjudicated_event",
    )
    .await?;

    let error = insert_material_event_with_product_class(
        ctx.pool(),
        material_id,
        "test.schema_derivation",
        "test.adjudicated_event",
        Some("analysis_claim"),
        Some(ACCEPTED_CLAIM_SUPPORT_WITHOUT_JUDGMENT),
        Some("test.derivation_schema.adjudicated_without_judgment"),
        None,
    )
    .await
    .expect_err(
        "an accepted claim_support with no adjudication_event_id must be rejected for a \
         non-operator_judgment product_class",
    );

    assert!(
        error
            .to_string()
            .contains("adjudicated claim requires adjudication_event_id"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn derivation_schema_adjudicated_claim_with_judgment_event_succeeds(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = insert_material(ctx.pool(), "adjudicated-with-judgment").await?;
    insert_declaration(
        ctx.pool(),
        "test.derivation_schema.adjudicated_with_judgment.judgment",
        "operator_judgment",
        "test.schema_derivation",
        "test.judgment_event",
    )
    .await?;
    insert_declaration(
        ctx.pool(),
        "test.derivation_schema.adjudicated_with_judgment.claim",
        "analysis_claim",
        "test.schema_derivation",
        "test.adjudicated_event_ok",
    )
    .await?;

    // The judgment event itself: product_class = operator_judgment, so the
    // trigger's adjudication-event-id requirement does not apply to it.
    let judgment_event_id = insert_material_event_with_product_class(
        ctx.pool(),
        material_id,
        "test.schema_derivation",
        "test.judgment_event",
        Some("operator_judgment"),
        Some(UNREVIEWED_CLAIM_SUPPORT),
        Some("test.derivation_schema.adjudicated_with_judgment.judgment"),
        None,
    )
    .await?;

    let claim_event_id = insert_material_event_with_product_class(
        ctx.pool(),
        material_id,
        "test.schema_derivation",
        "test.adjudicated_event_ok",
        Some("analysis_claim"),
        Some(ACCEPTED_CLAIM_SUPPORT_WITHOUT_JUDGMENT),
        Some("test.derivation_schema.adjudicated_with_judgment.claim"),
        Some(judgment_event_id),
    )
    .await
    .expect("an accepted claim with a real adjudication_event_id must succeed");

    let stored_adjudication_event_id: Uuid = sqlx::query_scalar(
        "SELECT adjudication_event_id FROM core.events WHERE id = $1",
    )
    .bind(claim_event_id)
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(stored_adjudication_event_id, judgment_event_id);
    Ok(())
}

#[sinex_test]
async fn derivation_schema_product_declarations_projection_kind_check(
    ctx: TestContext,
) -> TestResult<()> {
    // Corrected CHECK (the originating blueprint's form was tautological —
    // see the doc comment on DerivationProductDeclarations::
    // create_table_statement): projection_row requires projection_kind.
    let error = sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            semantics_version, input_eligibility, default_claim_support, verification_command
        )
        VALUES (
            'test.derivation_schema.projection_missing_kind', 'test-owner', 'projection_row',
            'projection_writer', 'v1', 'never_input', $1::jsonb, 'xtask test -p sinex-schema'
        )
        "#,
    )
    .bind(UNREVIEWED_CLAIM_SUPPORT)
    .execute(ctx.pool())
    .await
    .expect_err("projection_row without projection_kind must violate the corrected CHECK");
    assert!(error.to_string().to_lowercase().contains("check"));

    // The same row with projection_kind set must succeed — proves the CHECK
    // is not simply always-failing.
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            projection_kind, semantics_version, input_eligibility,
            default_claim_support, verification_command
        )
        VALUES (
            'test.derivation_schema.projection_with_kind', 'test-owner', 'projection_row',
            'projection_writer', 'test_projection', 'v1', 'never_input', $1::jsonb,
            'xtask test -p sinex-schema'
        )
        "#,
    )
    .bind(UNREVIEWED_CLAIM_SUPPORT)
    .execute(ctx.pool())
    .await
    .expect("projection_row with projection_kind set must satisfy the CHECK");
    Ok(())
}

#[sinex_test]
async fn projection_registry_ready_requires_built_at(ctx: TestContext) -> TestResult<()> {
    // Corrected CHECK (see the doc comment on DerivationProjectionRegistry::
    // create_table_statement): status = 'ready' requires built_at IS NOT NULL.
    let error = sqlx::query(
        r#"
        INSERT INTO derivation.projection_registry (
            id, projection_kind, scope_key, semantics_version, input_fingerprint,
            coverage_window, status, freshness_class, acceptable_staleness,
            verification_command
        )
        VALUES (
            uuidv7(), 'test_projection', 'scope-a', 'v1', 'fp-1',
            tstzrange(now() - interval '1 day', now()), 'ready', 'hours',
            interval '1 hour', 'xtask test -p sinex-schema'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect_err("status = 'ready' without built_at must violate the corrected CHECK");
    assert!(error.to_string().to_lowercase().contains("check"));

    sqlx::query(
        r#"
        INSERT INTO derivation.projection_registry (
            id, projection_kind, scope_key, semantics_version, input_fingerprint,
            coverage_window, status, freshness_class, acceptable_staleness,
            built_at, verification_command
        )
        VALUES (
            uuidv7(), 'test_projection', 'scope-b', 'v1', 'fp-2',
            tstzrange(now() - interval '1 day', now()), 'ready', 'hours',
            interval '1 hour', now(), 'xtask test -p sinex-schema'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect("status = 'ready' with built_at set must satisfy the CHECK");
    Ok(())
}

#[sinex_test]
async fn projection_registry_stale_requires_reason(ctx: TestContext) -> TestResult<()> {
    let error = sqlx::query(
        r#"
        INSERT INTO derivation.projection_registry (
            id, projection_kind, scope_key, semantics_version, input_fingerprint,
            coverage_window, status, freshness_class, acceptable_staleness,
            verification_command
        )
        VALUES (
            uuidv7(), 'test_projection', 'scope-c', 'v1', 'fp-3',
            tstzrange(now() - interval '1 day', now()), 'stale', 'hours',
            interval '1 hour', 'xtask test -p sinex-schema'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect_err("status = 'stale' without stale_reason must violate the CHECK");
    assert!(error.to_string().to_lowercase().contains("check"));

    sqlx::query(
        r#"
        INSERT INTO derivation.projection_registry (
            id, projection_kind, scope_key, semantics_version, input_fingerprint,
            coverage_window, status, freshness_class, acceptable_staleness,
            stale_reason, verification_command
        )
        VALUES (
            uuidv7(), 'test_projection', 'scope-d', 'v1', 'fp-4',
            tstzrange(now() - interval '1 day', now()), 'stale', 'hours',
            interval '1 hour', 'upstream source drifted', 'xtask test -p sinex-schema'
        )
        "#,
    )
    .execute(ctx.pool())
    .await
    .expect("status = 'stale' with stale_reason set must satisfy the CHECK");
    Ok(())
}
