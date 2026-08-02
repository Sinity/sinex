//! Startup reconciler for `authority.finalizer_registry` and the actor-kind
//! acceptance gate curation finalization enforces against it (sinex-0vx.5).
//!
//! Mirrors `crate::automata::product_declarations` for the same reason:
//! `derivation.enforce_event_product_declaration()` (sinex-0vx.4) already
//! gates *which* `(source, event_type, product_class)` triples any writer may
//! emit; this module gates a narrower, curation-specific question —
//! whether a *specific proposal kind* is even permitted to reach a finalized
//! output at all (`authority.finalizer_registry` must have a matching, active
//! row — the curation-bypass rejection this bead is named for), and under
//! what actor-kind policy (`requires_human_judgment` /
//! `auto_accept_policy`, decision (a) of the 0vx design pass).
//!
//! Like the product-declaration reconciler, this runs once at `sinexd`
//! startup (`Supervisor::run`, alongside the product-declaration reconcile
//! call and after it — `finalizer_registry.derivation_declaration_id`
//! foreign-keys into `derivation.product_declarations`, so the declaration
//! must already exist) and fails closed on any static-vs-DB mismatch: a
//! finalizer registration is security-relevant enforcement configuration,
//! never something that should silently drift between the code that declares
//! it and the database that enforces it.

use sinex_db::DbPool;
use sinex_db::repositories::{DbPoolExt, ExistingFinalizerRegistration};
use sinex_primitives::authority::{
    AutoAcceptPolicy, judgment_actor_sufficient_for_acceptance,
};
use sinex_primitives::derivation::DerivedProductClass;
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::events::CurationJudgmentActorKind;
use tracing::info;

use crate::api::auth::Role;

/// Server-side clamp for the `actor_kind` a curation judgment is persisted
/// with (sinex-audit-actorkind).
///
/// `CurationRecordJudgmentRequest.actor_kind` is a plain client-supplied
/// JSON field. Before this fix it was copied verbatim into the persisted
/// `curation.judgment` event, and
/// [`judgment_actor_sufficient_for_acceptance`] treats every actor kind
/// except `Agent` as automatically sufficient authority to finalize — so a
/// holder of an ordinary `Role::Write` token (the same tier as event
/// ingestion) could self-assert `actor_kind: "operator"` and then finalize
/// its own judgment, bypassing the sinex-0vx.5 authority gate entirely.
///
/// This is the derivation: `actor_kind` is trusted from the request body
/// only up to what the AUTHENTICATED caller's role can legitimately assert.
/// `Role::Admin` is the operator's own elevated token (or
/// `RpcAuthContext::system()` for trusted internal/automaton callers) and
/// may assert whatever kind it requests. Any lower role — `Role::Write` or
/// `Role::ReadOnly` (the latter should never reach a judgment-recording
/// handler at all, since those are `Role::Write`-gated RPCs, but is clamped
/// too as defense in depth) — can only ever record a judgment as
/// `Agent`-kind, which `judgment_actor_sufficient_for_acceptance` never
/// treats as sufficient by itself.
#[must_use]
pub fn clamp_judgment_actor_kind(
    role: Role,
    requested: CurationJudgmentActorKind,
) -> CurationJudgmentActorKind {
    match role {
        Role::Admin => requested,
        Role::Write | Role::ReadOnly => CurationJudgmentActorKind::Agent,
    }
}

/// A static finalizer registration a curation-domain writer declares, mapping
/// one `proposal_kind` to the exact `(output_source, output_event_type)`
/// finalized output it is permitted to emit, and the actor-kind policy
/// governing whether a judgment alone is sufficient to reach it.
///
/// This is the finalizer-registry analog of
/// `sinex_primitives::derivation::DerivationOutputDeclaration` — the set of
/// legitimate finalizable proposal kinds is an explicit, declared registry,
/// not an ambient capability any proposal can claim by naming a
/// `proposal_kind` string.
#[derive(Debug, Clone)]
pub struct FinalizerDeclaration {
    /// Stable identifier for this registration (the DB primary key).
    pub finalizer_id: &'static str,
    /// The `CurationProposalPayload::proposal_kind` this finalizer governs.
    pub proposal_kind: &'static str,
    /// The `candidate_source` a proposal of this kind must declare.
    pub output_source: &'static str,
    /// The `candidate_event_type` a proposal of this kind must declare.
    pub output_event_type: &'static str,
    /// Product class of the resulting `curation.finalized` output.
    pub output_product_class: DerivedProductClass,
    /// The `derivation.product_declarations` row this finalizer's own output
    /// is stamped with (FK target).
    pub derivation_declaration_id: &'static str,
    /// Default posture is `true`: a judgment alone is not sufficient without
    /// human (or explicitly policy-granted) confirmation.
    pub requires_human_judgment: bool,
    /// Actor kinds explicitly granted auto-accept authority when
    /// `requires_human_judgment` is `false`. `None`/empty grants no one.
    pub auto_accept_policy: Option<AutoAcceptPolicy>,
    pub registered_by: &'static str,
}

/// Reconcile `authority.finalizer_registry` against every declaration in
/// `declarations`. Returns the number of rows newly inserted. Fails closed
/// on the first `finalizer_id` whose static shape disagrees with an existing
/// DB row.
pub async fn reconcile_finalizer_registrations(
    pool: &DbPool,
    declarations: &[FinalizerDeclaration],
) -> Result<usize> {
    let mut inserted = 0usize;

    for declaration in declarations {
        if reconcile_one(pool, declaration).await? {
            inserted += 1;
        }
    }

    info!(
        declarations = declarations.len(),
        inserted, "reconciled authority.finalizer_registry from static declarations"
    );

    Ok(inserted)
}

async fn reconcile_one(pool: &DbPool, declaration: &FinalizerDeclaration) -> Result<bool> {
    let repo = pool.authority();

    let Some(existing) = repo.find_by_finalizer_id(declaration.finalizer_id).await? else {
        let auto_accept_policy_json = declaration
            .auto_accept_policy
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| {
                SinexError::configuration(format!(
                    "finalizer '{}' auto_accept_policy could not be serialized: {error}",
                    declaration.finalizer_id
                ))
            })?;
        repo.insert(
            declaration.finalizer_id,
            declaration.proposal_kind,
            declaration.output_source,
            declaration.output_event_type,
            declaration.output_product_class.as_str(),
            declaration.derivation_declaration_id,
            declaration.requires_human_judgment,
            auto_accept_policy_json.as_ref(),
            declaration.registered_by,
        )
        .await?;
        return Ok(true);
    };

    match declaration_matches(declaration, &existing) {
        Ok(()) => Ok(false),
        Err(mismatch) => Err(SinexError::configuration(format!(
            "static finalizer declaration '{}' conflicts with the existing \
             authority.finalizer_registry row: {mismatch}. Refusing to start rather than \
             silently overwrite or silently accept drift on a security-relevant enforcement \
             table — reconcile the code declaration or the DB row by hand.",
            declaration.finalizer_id
        ))),
    }
}

fn declaration_matches(
    declaration: &FinalizerDeclaration,
    existing: &ExistingFinalizerRegistration,
) -> std::result::Result<(), String> {
    macro_rules! check {
        ($field:expr, $expected:expr, $actual:expr) => {
            if $expected != $actual {
                return Err(format!(
                    "{} differs (code declares {:?}, DB has {:?})",
                    $field, $expected, $actual
                ));
            }
        };
    }

    check!("proposal_kind", declaration.proposal_kind, existing.proposal_kind);
    check!("output_source", declaration.output_source, existing.output_source);
    check!(
        "output_event_type",
        declaration.output_event_type,
        existing.output_event_type
    );
    check!(
        "output_product_class",
        declaration.output_product_class.as_str(),
        existing.output_product_class
    );
    check!(
        "derivation_declaration_id",
        declaration.derivation_declaration_id,
        existing.derivation_declaration_id
    );
    check!(
        "requires_human_judgment",
        declaration.requires_human_judgment,
        existing.requires_human_judgment
    );

    let expected_policy = declaration
        .auto_accept_policy
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| format!("auto_accept_policy could not be serialized: {error}"))?;
    check!(
        "auto_accept_policy",
        expected_policy,
        existing.auto_accept_policy.clone()
    );
    check!("active", true, existing.active);

    Ok(())
}

/// The curation-finalize enforcement site for sinex-0vx.5 decisions (both
/// halves of the bead): looks up the registered, active finalizer for
/// `(proposal_kind, output_source, output_event_type)` and evaluates whether
/// `actor_kind`'s judgment is, by itself, sufficient authority to promote
/// this proposal to an Accepted adjudication.
///
/// Returns `Err` in two distinct cases the caller should surface distinctly
/// to callers/operators:
/// - No active finalizer is registered at all (the curation-bypass
///   rejection: AC "authority.finalizer_registry is required before
///   finalizer output can be emitted").
/// - A finalizer IS registered, but `actor_kind`'s judgment is not
///   sufficient under its policy (AC "an Agent-actor Accept does not by
///   itself produce an Accepted adjudication unless policy-granted").
pub async fn authorize_finalization(
    pool: &DbPool,
    proposal_kind: &str,
    output_source: &str,
    output_event_type: &str,
    actor_kind: CurationJudgmentActorKind,
) -> Result<()> {
    let Some(finalizer) = pool
        .authority()
        .find_active_finalizer(proposal_kind, output_source, output_event_type)
        .await?
    else {
        return Err(SinexError::validation(
            "curation.finalize: no registered finalizer authorizes this proposal kind",
        )
        .with_context("proposal_kind", proposal_kind)
        .with_context("output_source", output_source)
        .with_context("output_event_type", output_event_type));
    };

    let auto_accept_policy: Option<AutoAcceptPolicy> = finalizer
        .auto_accept_policy
        .as_ref()
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|error| {
            SinexError::database(format!(
                "finalizer '{}' auto_accept_policy is not valid: {error}",
                finalizer.finalizer_id
            ))
        })?;

    if !judgment_actor_sufficient_for_acceptance(
        actor_kind,
        finalizer.requires_human_judgment,
        auto_accept_policy.as_ref(),
    ) {
        return Err(SinexError::validation(
            "curation.finalize: this actor kind's judgment is not sufficient authority to \
             finalize this proposal kind without an explicit human/policy-granted confirmation",
        )
        .with_context("proposal_kind", proposal_kind)
        .with_context("actor_kind", format!("{actor_kind:?}"))
        .with_context("finalizer_id", finalizer.finalizer_id));
    }

    Ok(())
}

#[cfg(test)]
#[path = "authority_test.rs"]
mod tests;
