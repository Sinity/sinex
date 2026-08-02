use super::*;
use crate::api::handlers::curation::CURATION_FINALIZER_DECLARATIONS;
use crate::automata::product_declarations::reconcile_declarations;
use sinex_primitives::derivation::DerivedProductClass;
use xtask::sandbox::prelude::*;

/// Seed the `derivation.product_declarations` FK target the finalizer
/// registrations point at, through the real production reconciler (mirrors
/// `tests/api/common::seed_rpc_handler_product_declarations`, duplicated
/// here rather than pulled in because that helper lives in an integration
/// test crate this unit-test module cannot depend on).
async fn seed_curation_output_declarations(pool: &DbPool) -> TestResult<()> {
    reconcile_declarations(
        pool,
        "curation-rpc",
        crate::api::handlers::curation::CURATION_OUTPUT_DECLARATIONS,
    )
    .await?;
    Ok(())
}

/// End-to-end proof of the gap this half of sinex-0vx.5 closes: against a
/// freshly schema-applied database with no pre-existing
/// `authority.finalizer_registry` rows, `authorize_finalization` refuses
/// EVERY actor kind (including the trusted ones) until the reconciler has
/// run — only after `reconcile_finalizer_registrations` populates the table
/// does the registered proposal kind become finalizable. Deleting the
/// reconciler call (or its `INSERT`) turns the second half of this test red
/// with the "no registered finalizer" rejection.
#[sinex_test]
async fn reconcile_finalizer_registrations_allows_real_finalize(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_curation_output_declarations(pool).await?;

    let declaration = CURATION_FINALIZER_DECLARATIONS
        .iter()
        .find(|d| d.proposal_kind == "curation.duplicate_resolution")
        .expect("curation.duplicate_resolution finalizer declaration must exist");

    // Before reconciliation: even a TestFixture (trusted) actor is refused,
    // because no finalizer is registered at all yet.
    let before = authorize_finalization(
        pool,
        declaration.proposal_kind,
        declaration.output_source,
        declaration.output_event_type,
        CurationJudgmentActorKind::TestFixture,
    )
    .await;
    assert!(
        before.is_err(),
        "authorize_finalization must refuse everyone before the reconciler has run"
    );
    assert!(
        before.unwrap_err().to_string().contains("no registered finalizer"),
        "pre-reconcile rejection should be the bypass rejection, not something else"
    );

    let inserted = reconcile_finalizer_registrations(pool, CURATION_FINALIZER_DECLARATIONS).await?;
    assert!(inserted > 0, "expected at least one row freshly inserted");

    // Re-running is a pure no-op.
    let inserted_again =
        reconcile_finalizer_registrations(pool, CURATION_FINALIZER_DECLARATIONS).await?;
    assert_eq!(inserted_again, 0);

    // After reconciliation: a trusted actor kind is now authorized.
    authorize_finalization(
        pool,
        declaration.proposal_kind,
        declaration.output_source,
        declaration.output_event_type,
        CurationJudgmentActorKind::TestFixture,
    )
    .await?;

    Ok(())
}

/// Fail-closed AC, mirrored from the product-declaration reconciler: an
/// existing `authority.finalizer_registry` row that disagrees with the
/// static declaration for the same `finalizer_id` must make the reconciler
/// refuse to start, never silently overwrite or accept drift.
#[sinex_test]
async fn reconcile_finalizer_registrations_rejects_conflicting_existing_row(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    seed_curation_output_declarations(pool).await?;

    let declaration = CURATION_FINALIZER_DECLARATIONS[0].clone();
    let conflicting = FinalizerDeclaration {
        requires_human_judgment: false,
        ..declaration.clone()
    };

    // Seed the conflicting row directly, bypassing the reconciler (a DB
    // that drifted from the code).
    pool.authority()
        .insert(
            conflicting.finalizer_id,
            conflicting.proposal_kind,
            conflicting.output_source,
            conflicting.output_event_type,
            conflicting.output_product_class.as_str(),
            conflicting.derivation_declaration_id,
            conflicting.requires_human_judgment,
            None,
            conflicting.registered_by,
        )
        .await?;

    let error = reconcile_finalizer_registrations(pool, CURATION_FINALIZER_DECLARATIONS)
        .await
        .expect_err("reconciler must refuse to start on a conflicting existing row");
    let message = error.to_string();
    assert!(
        message.contains(declaration.finalizer_id),
        "error should name the conflicting finalizer_id, got: {message}"
    );
    assert!(
        message.contains("requires_human_judgment"),
        "error should name the specific field that disagreed, got: {message}"
    );

    Ok(())
}

/// Core sinex-0vx.5 decision (a) end-to-end proof, through the real
/// production `authorize_finalization` path (not just the pure function
/// unit-tested in `sinex-primitives`): an `Agent` judgment on a registered
/// proposal kind that has NOT been granted auto-accept authority is refused,
/// while the same proposal kind is finalizable by a trusted actor kind.
#[sinex_test]
async fn authorize_finalization_rejects_agent_without_grant(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_curation_output_declarations(pool).await?;
    reconcile_finalizer_registrations(pool, CURATION_FINALIZER_DECLARATIONS).await?;

    let declaration = CURATION_FINALIZER_DECLARATIONS
        .iter()
        .find(|d| d.proposal_kind == "knowledge.tag_applied")
        .expect("knowledge.tag_applied finalizer declaration must exist");
    assert!(
        declaration.requires_human_judgment,
        "the currently-registered declaration must keep the default safe posture"
    );
    assert!(declaration.auto_accept_policy.is_none());

    let agent_error = authorize_finalization(
        pool,
        declaration.proposal_kind,
        declaration.output_source,
        declaration.output_event_type,
        CurationJudgmentActorKind::Agent,
    )
    .await
    .expect_err("an Agent judgment must not be sufficient without an explicit grant");
    assert!(
        agent_error.to_string().contains("not sufficient authority"),
        "expected the actor-kind-insufficient rejection, got: {agent_error}"
    );

    // The same proposal kind is authorized for a trusted actor kind.
    authorize_finalization(
        pool,
        declaration.proposal_kind,
        declaration.output_source,
        declaration.output_event_type,
        CurationJudgmentActorKind::Operator,
    )
    .await?;

    Ok(())
}

/// The positive half: a finalizer explicitly granting `Agent` auto-accept
/// authority (`requires_human_judgment = false` + `auto_accept_policy`
/// listing `Agent`) permits an `Agent` judgment to authorize finalization —
/// proving the gate is a real policy switch, not a hardcoded Agent ban.
#[sinex_test]
async fn authorize_finalization_permits_agent_with_explicit_grant(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    seed_curation_output_declarations(pool).await?;

    let granted = FinalizerDeclaration {
        finalizer_id: "sinexd-test.authority.agent_granted",
        proposal_kind: "sinexd-test.agent_granted_kind",
        output_source: "curation",
        output_event_type: "curation.finalized",
        output_product_class: DerivedProductClass::ReportArtifact,
        derivation_declaration_id: "curation-rpc.curation.finalized",
        requires_human_judgment: false,
        auto_accept_policy: Some(sinex_primitives::authority::AutoAcceptPolicy {
            granted_actor_kinds: vec![CurationJudgmentActorKind::Agent],
        }),
        registered_by: "sinexd-test",
    };
    reconcile_finalizer_registrations(pool, &[granted.clone()]).await?;

    authorize_finalization(
        pool,
        granted.proposal_kind,
        granted.output_source,
        granted.output_event_type,
        CurationJudgmentActorKind::Agent,
    )
    .await?;

    Ok(())
}

/// sinex-audit-actorkind: `clamp_judgment_actor_kind` is the pure decision
/// function the record-judgment handlers apply to the client-supplied
/// `actor_kind` before persisting a judgment. Below `Role::Admin`, whatever
/// the caller requests must come back `Agent` -- there is no role tier
/// between `Write` and `Admin` that legitimately asserts a trusted actor
/// kind. Only `Role::Admin` passes the requested kind through unchanged.
#[test]
fn clamp_judgment_actor_kind_forces_agent_below_admin() {
    use crate::api::auth::Role;

    let requested_kinds = [
        CurationJudgmentActorKind::User,
        CurationJudgmentActorKind::Operator,
        CurationJudgmentActorKind::DeterministicPolicy,
        CurationJudgmentActorKind::Agent,
    ];

    for role in [Role::ReadOnly, Role::Write] {
        for requested in requested_kinds {
            assert_eq!(
                clamp_judgment_actor_kind(role, requested),
                CurationJudgmentActorKind::Agent,
                "role {role:?} must never be able to assert {requested:?} directly"
            );
        }
    }

    for requested in requested_kinds {
        assert_eq!(
            clamp_judgment_actor_kind(Role::Admin, requested),
            requested,
            "Admin must be able to assert its requested actor_kind unchanged"
        );
    }
}
