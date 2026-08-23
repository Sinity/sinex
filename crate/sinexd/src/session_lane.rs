//! Session-boundary shadow-lane declarations (sinex-0vx.7).
//!
//! Second real `LaneOutputKind` producer/promoter after the entity/relation
//! semantic-lane RPC handlers (`crate::api::handlers::semantic`,
//! sinex-0vx.6) and the curation duplicate-resolution finalizer
//! (sinex-0vx.5) — proves the derivation control plane generalizes:
//!
//! - [`SESSION_LANE_OUTPUT_DECLARATIONS`] registers the
//!   `derivation.product_declarations` row that admits `session_boundary`
//!   lane-output writes (`sinex_db::repositories::DerivationRepository`'s
//!   `write_session_lane_outputs`/`seed_session_lane_outputs_from_*`,
//!   sinex-0vx.7 in `sinex-db`), reconciled from `Supervisor::run` alongside
//!   every other non-automaton writer's declarations.
//! - [`SESSION_LANE_FINALIZER_DECLARATIONS`] registers the
//!   `authority.finalizer_registry` row that permits a
//!   `session.lane_promotion` curation proposal to reach
//!   `handle_curation_finalize` at all (the curation-bypass rejection,
//!   sinex-0vx.5). Promotion itself flows through the generic
//!   `candidate_payload.lane_id` bridge in
//!   `crate::api::handlers::curation::handle_curation_finalize` — this file
//!   declares WHO may finalize a `session.lane_promotion` proposal and under
//!   what actor-kind policy, not the promotion mechanism itself (which has
//!   no session-specific knowledge).
//!
//! There is no dedicated RPC surface for creating/seeding/diffing a session
//! lane yet — the generic `DerivationRepository` methods plus the generic
//! curation proposal/judgment/finalize handlers are the whole surface,
//! exercised end to end by
//! `crate/sinexd/tests/automata/session_lane_shadow_diff_test.rs`. A CLI/RPC
//! convenience wrapper is a natural follow-up once an operator workflow
//! needs one; the control-plane proof this bead exists for does not.

use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};

/// `derivation.product_declarations` registration for
/// `output_kind = "session_boundary"` lane-output writes. `SemanticCandidate`
/// / `curation_writer` — mirrors `SEMANTIC_ENTITY_RELATION_DECLARATION`
/// exactly: a shadow-lane write is a candidate awaiting explicit
/// authority/deterministic-policy finalization, never itself canonical.
pub const SESSION_LANE_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[SESSION_LANE_DECLARATION];

const SESSION_LANE_DECLARATION: DerivationOutputDeclaration = DerivationOutputDeclaration {
    declaration_id: "session-lane.session_boundary.semantic_candidate",
    owner: "session-lane",
    product_class: DerivedProductClass::SemanticCandidate,
    write_surface: DerivationWriteSurface::CurationWriter,
    output_source: None,
    output_event_type: None,
    projection_kind: None,
    artifact_kind: None,
    proposal_kind: None,
    semantics_version: "1.0.0",
    input_eligibility: InputEligibility::ExplicitOnly,
    default_support: ClaimSupportTemplate::new(
        SupportLevel::Convergent,
        SourceCoverage::Covered,
        ClaimTemporalQuality::WindowBoundary,
    ),
    verification_command: "xtask test -p sinexd -E 'test(session_lane_shadow_diff_promotion)'",
};

/// `authority.finalizer_registry` registration for
/// `proposal_kind = "session.lane_promotion"`. Default safe posture
/// (`requires_human_judgment: true`, no `auto_accept_policy`) — identical
/// stance to every other curation-rpc finalizer declaration
/// (`crate::api::handlers::curation::CURATION_FINALIZER_DECLARATIONS`):
/// an `Agent` judgment is never sufficient by itself.
pub const SESSION_LANE_FINALIZER_DECLARATIONS: &[crate::authority::FinalizerDeclaration] =
    &[crate::authority::FinalizerDeclaration {
        finalizer_id: "session-lane.session.lane_promotion",
        proposal_kind: "session.lane_promotion",
        output_source: "session-lane",
        output_event_type: "session.lane_promotion",
        output_product_class: DerivedProductClass::ReportArtifact,
        derivation_declaration_id: crate::api::handlers::curation::CURATION_FINALIZED_DECLARATION
            .declaration_id,
        requires_human_judgment: true,
        auto_accept_policy: None,
        registered_by: "session-lane",
    }];
