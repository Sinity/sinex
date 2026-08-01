use super::*;
use sinex_primitives::authority::{AutoAcceptPolicy, judgment_actor_sufficient_for_acceptance};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::events::CurationJudgmentActorKind;
use xtask::sandbox::prelude::*;

/// A test-local `derivation.product_declarations` row satisfying the
/// `finalizer_registry.derivation_declaration_id` FK (`authority.rs`,
/// sinex-schema `defs/authority.rs`).
const TEST_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "sinex-db-test.authority.finalized",
    owner: "sinex-db-test",
    product_class: DerivedProductClass::ReportArtifact,
    write_surface: DerivationWriteSurface::ArtifactWriter,
    output_source: None,
    output_event_type: None,
    projection_kind: None,
    artifact_kind: None,
    proposal_kind: None,
    semantics_version: "1.0.0",
    input_eligibility: InputEligibility::NeverInput,
    default_support: ClaimSupportTemplate::new(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
    ),
    verification_command: "true",
};

/// The curation-bypass rejection AC, proved at the repository/DB layer: with
/// no `authority.finalizer_registry` row registered for a given
/// `(proposal_kind, output_source, output_event_type)` triple,
/// `find_active_finalizer` — the exact lookup `handle_curation_finalize`
/// uses to decide whether it may emit a finalized output at all — returns
/// `None`. Once a matching row is registered, the same lookup finds it.
/// Deleting the `active = true` filter or the triple match in the
/// repository's `WHERE` clause would turn this vacuous (it would find
/// unrelated/inactive rows); this test proves both the negative (bypass
/// blocked) and positive (registered finalizer found) cases against a real
/// Postgres round trip.
#[sinex_test]
async fn finalizer_registry_rejects_bypass(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    pool.product_declarations().insert(&TEST_DECLARATION).await?;

    let proposal_kind = "sinex-db-test.bypass_check";
    let output_source = "sinex-db-test";
    let output_event_type = "sinex_db_test.finalized";

    // No finalizer registered yet: the bypass lookup must return None.
    let missing = pool
        .authority()
        .find_active_finalizer(proposal_kind, output_source, output_event_type)
        .await?;
    assert!(
        missing.is_none(),
        "an unregistered (proposal_kind, output_source, output_event_type) triple must not \
         resolve to an active finalizer -- this is the bypass this bead rejects"
    );

    pool.authority()
        .insert(
            "sinex-db-test.bypass_check.finalizer",
            proposal_kind,
            output_source,
            output_event_type,
            DerivedProductClass::ReportArtifact.as_str(),
            TEST_DECLARATION.declaration_id,
            true,
            None,
            "sinex-db-test",
        )
        .await?;

    let found = pool
        .authority()
        .find_active_finalizer(proposal_kind, output_source, output_event_type)
        .await?
        .expect("a registered, active finalizer must now be found");
    assert_eq!(found.finalizer_id, "sinex-db-test.bypass_check.finalizer");
    assert!(found.requires_human_judgment);
    assert!(found.auto_accept_policy.is_none());

    // A different output triple sharing the same proposal_kind must still
    // miss -- the match is on the full triple, not just proposal_kind.
    let different_output = pool
        .authority()
        .find_active_finalizer(proposal_kind, output_source, "sinex_db_test.other_event")
        .await?;
    assert!(
        different_output.is_none(),
        "a finalizer registered for one output_event_type must not match a different one"
    );

    Ok(())
}

/// End-to-end (through real Postgres storage, not just in-memory structs)
/// proof that an `Agent` actor's judgment is not auto-accepted by default:
/// insert a finalizer registry row through the DEFAULT `requires_human_judgment`
/// (the schema DEFAULT is `true`, sinex-schema `defs/authority.rs`) and with
/// no `auto_accept_policy`, round-trip it back out through
/// `find_active_finalizer`, and confirm
/// `judgment_actor_sufficient_for_acceptance` refuses `Agent` for the
/// round-tripped row while still accepting the pre-existing trusted actor
/// kinds. Also proves the positive case: a row that explicitly sets
/// `requires_human_judgment = false` and grants `Agent` in
/// `auto_accept_policy` round-trips into a permitted decision.
#[sinex_test]
async fn agent_judgment_not_auto_accepted(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    pool.product_declarations().insert(&TEST_DECLARATION).await?;

    // Row inserted relying on the DB DEFAULT for requires_human_judgment
    // (raw SQL bypassing the repository's explicit-argument insert, to
    // prove the *schema's own default* is the safe posture, not merely
    // that the Rust call site happened to pass `true`).
    sqlx::query!(
        r#"
        INSERT INTO authority.finalizer_registry (
            finalizer_id, proposal_kind, output_source, output_event_type,
            output_product_class, derivation_declaration_id, registered_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
        "sinex-db-test.agent_default.finalizer",
        "sinex-db-test.agent_default",
        "sinex-db-test",
        "sinex_db_test.agent_default_finalized",
        DerivedProductClass::ReportArtifact.as_str(),
        TEST_DECLARATION.declaration_id,
        "sinex-db-test",
    )
    .execute(pool)
    .await
    .map_err(|e| eyre!("insert default-posture finalizer row: {e}"))?;

    let default_row = pool
        .authority()
        .find_active_finalizer(
            "sinex-db-test.agent_default",
            "sinex-db-test",
            "sinex_db_test.agent_default_finalized",
        )
        .await?
        .expect("row inserted via raw SQL must still be found by the repository lookup");
    assert!(
        default_row.requires_human_judgment,
        "schema DEFAULT for requires_human_judgment must be true (the safe posture)"
    );
    assert!(default_row.auto_accept_policy.is_none());

    let default_policy: Option<AutoAcceptPolicy> = default_row
        .auto_accept_policy
        .as_ref()
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|e| eyre!("deserialize auto_accept_policy: {e}"))?;
    assert!(!judgment_actor_sufficient_for_acceptance(
        CurationJudgmentActorKind::Agent,
        default_row.requires_human_judgment,
        default_policy.as_ref(),
    ));
    // Trusted actor kinds are unaffected by the same round-tripped row.
    assert!(judgment_actor_sufficient_for_acceptance(
        CurationJudgmentActorKind::Operator,
        default_row.requires_human_judgment,
        default_policy.as_ref(),
    ));

    // Positive case: explicit grant round-trips into a permitted decision.
    let granting_policy = serde_json::to_value(AutoAcceptPolicy {
        granted_actor_kinds: vec![CurationJudgmentActorKind::Agent],
    })
    .map_err(|e| eyre!("serialize granting policy: {e}"))?;
    pool.authority()
        .insert(
            "sinex-db-test.agent_granted.finalizer",
            "sinex-db-test.agent_granted",
            "sinex-db-test",
            "sinex_db_test.agent_granted_finalized",
            DerivedProductClass::ReportArtifact.as_str(),
            TEST_DECLARATION.declaration_id,
            false,
            Some(&granting_policy),
            "sinex-db-test",
        )
        .await?;

    let granted_row = pool
        .authority()
        .find_active_finalizer(
            "sinex-db-test.agent_granted",
            "sinex-db-test",
            "sinex_db_test.agent_granted_finalized",
        )
        .await?
        .expect("granted-policy row must be found");
    assert!(!granted_row.requires_human_judgment);
    let granted_policy: AutoAcceptPolicy = serde_json::from_value(
        granted_row
            .auto_accept_policy
            .expect("granted row must carry a policy"),
    )
    .map_err(|e| eyre!("deserialize granted policy: {e}"))?;
    assert!(judgment_actor_sufficient_for_acceptance(
        CurationJudgmentActorKind::Agent,
        granted_row.requires_human_judgment,
        Some(&granted_policy),
    ));

    Ok(())
}
