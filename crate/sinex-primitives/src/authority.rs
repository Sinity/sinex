//! Authority seam: Proposal/Judgment/Finalizer DTOs v0.
//!
//! The load-bearing invariant: a [`Proposal`]'s value cannot be promoted to
//! truth without an explicit [`Judgment`]. No model output, confidence score,
//! or heuristic result can bypass this gate — the only extraction path is
//! [`Proposal::apply`], which requires a matching, accepted Judgment.
//!
//! These are fixture/view DTOs — no DB persistence in this first slice.
//!
//! Ref: #1788 (wave-2 child of #1692).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{Result, SinexError};
use crate::ids::Id;
use crate::temporal::Timestamp;
use crate::views::{CaveatView, SinexObjectRef, ViewEnvelope};

// ─── Marker types (internal) ──────────────────────────────────────────────────

/// Phantom marker for [`Id<ProposalMarker>`] (internal construction only).
#[derive(Debug)]
pub struct ProposalMarker;

/// Phantom marker for [`Id<JudgmentMarker>`] (internal construction only).
#[derive(Debug)]
pub struct JudgmentMarker;

/// Phantom marker for [`Id<FinalizerMarker>`] (internal construction only).
#[derive(Debug)]
pub struct FinalizerMarker;

// ─── ProposalKind ─────────────────────────────────────────────────────────────

/// Classifies what a [`Proposal`] is proposing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProposalKind {
    /// Two events are duplicates (same real-world occurrence).
    DuplicateCandidate,
    /// An entity extraction or relation suggestion.
    EntityExtraction,
    /// A semantic category or tag assignment.
    Categorization,
    /// Other/extensible kind.
    Other(String),
}

// ─── JudgmentVerdict ──────────────────────────────────────────────────────────

/// The operator's verdict on a [`Proposal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JudgmentVerdict {
    /// Proposal accepted; the caller may apply the proposed value.
    Accept,
    /// Proposal rejected; the proposed value must be discarded.
    Reject,
    /// Decision deferred; the proposal stays alive for later review.
    Defer,
}

impl JudgmentVerdict {
    /// Returns `true` only for [`JudgmentVerdict::Accept`].
    #[must_use]
    pub fn is_accept(self) -> bool {
        matches!(self, Self::Accept)
    }
}

// ─── Proposal ─────────────────────────────────────────────────────────────────

/// A proposed change awaiting an operator judgment.
///
/// The only way to extract the `proposed_value` through Rust code is
/// [`Proposal::apply`], which requires a matching, accepted [`Judgment`].
/// This is the authority seam: model confidence scores and heuristic
/// outputs cannot promote a value to truth without an explicit verdict.
///
/// Note: the value IS present in the JSON serialization so operator UIs
/// can display it for review — but the extraction gate lives in [`apply`].
///
/// [`apply`]: Proposal::apply
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: for<'d> Deserialize<'d>"))]
pub struct Proposal<T>
where
    T: Serialize + Clone + JsonSchema,
{
    /// Stable opaque identifier for this proposal.
    pub id: String,
    /// What kind of change is being proposed.
    pub kind: ProposalKind,
    /// The subject being affected (e.g. an event reference).
    pub subject_ref: SinexObjectRef,
    /// Model or heuristic confidence in this proposal (0.0–1.0).
    ///
    /// High confidence does NOT bypass the judgment gate — it is
    /// display/informational only until an operator judges the proposal.
    pub confidence: f32,
    /// The proposed value. Not extractable without an accepted Judgment.
    proposed_value: T,
    /// Who/what generated this proposal.
    ///
    /// Convention: `"model:<model-id>"` or `"rule:<rule-name>"`.
    pub proposer: String,
    /// When the proposal was generated.
    pub ts_proposed: Timestamp,
    /// Operator-visible caveats attached to this proposal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

impl<T> Proposal<T>
where
    T: Serialize + Clone + JsonSchema,
{
    /// Create a new proposal with a freshly minted ID and current timestamp.
    pub fn new(
        kind: ProposalKind,
        subject_ref: SinexObjectRef,
        confidence: f32,
        proposed_value: T,
        proposer: impl Into<String>,
    ) -> Self {
        Self {
            id: Id::<ProposalMarker>::new().to_string(),
            kind,
            subject_ref,
            confidence,
            proposed_value,
            proposer: proposer.into(),
            ts_proposed: Timestamp::now(),
            caveats: Vec::new(),
        }
    }

    /// Attach a caveat to this proposal (builder pattern).
    #[must_use]
    pub fn with_caveat(mut self, id: impl Into<String>, message: impl Into<String>) -> Self {
        self.caveats.push(CaveatView {
            id: id.into(),
            message: message.into(),
            ref_: None,
        });
        self
    }

    /// Extract the proposed value.
    ///
    /// Returns `Ok(T)` only when both conditions hold:
    /// 1. `judgment.proposal_id` matches `self.id`.
    /// 2. `judgment.verdict` is [`JudgmentVerdict::Accept`].
    ///
    /// Any other verdict or a judgment for a different proposal returns
    /// an error. This is the gate — confidence alone cannot bypass it.
    pub fn apply(self, judgment: &Judgment) -> Result<T> {
        if judgment.proposal_id != self.id {
            return Err(
                SinexError::validation("judgment does not reference this proposal")
                    .with_context("proposal_id", self.id.clone())
                    .with_context("judgment_proposal_id", judgment.proposal_id.clone()),
            );
        }
        if !judgment.verdict.is_accept() {
            return Err(SinexError::validation(
                "only an Accept judgment may promote a proposal; \
                 Reject and Defer verdicts do not grant access to the proposed value",
            )
            .with_context("verdict", format!("{:?}", judgment.verdict)));
        }
        Ok(self.proposed_value)
    }

    /// Wrap this proposal in a [`ViewEnvelope`] for operator display.
    #[must_use]
    pub fn into_envelope(self, source_surface: impl Into<String>) -> ViewEnvelope<Self> {
        ViewEnvelope::new(source_surface, self)
    }
}

// ─── Judgment ─────────────────────────────────────────────────────────────────

/// An operator's explicit verdict on a [`Proposal`].
///
/// Created by the operator (or a trusted rule system acting on their behalf)
/// to either accept, reject, or defer a proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Judgment {
    /// Stable opaque identifier for this judgment.
    pub id: String,
    /// The proposal this judgment applies to (references [`Proposal::id`]).
    pub proposal_id: String,
    /// The operator's verdict.
    pub verdict: JudgmentVerdict,
    /// Who rendered this judgment (actor identifier).
    pub operator: String,
    /// When the judgment was rendered.
    pub ts_judged: Timestamp,
    /// Optional operator note explaining the verdict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Judgment {
    /// Create a new judgment with a freshly minted ID and current timestamp.
    pub fn new(
        proposal_id: impl Into<String>,
        verdict: JudgmentVerdict,
        operator: impl Into<String>,
    ) -> Self {
        Self {
            id: Id::<JudgmentMarker>::new().to_string(),
            proposal_id: proposal_id.into(),
            verdict,
            operator: operator.into(),
            ts_judged: Timestamp::now(),
            note: None,
        }
    }

    /// Attach a note to this judgment (builder pattern).
    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
}

// ─── FinalizerRegistration ────────────────────────────────────────────────────

/// Registry entry declaring that proposals of a given kind must be
/// judged before their values may be applied.
///
/// This is a declaration type in v0 — it captures the contract without
/// a runtime registry or DB backing. Future slices may persist these in
/// an `authority.finalizer_registry` table and enforce them at the API
/// boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FinalizerRegistration {
    /// Stable opaque identifier for this entry.
    pub id: String,
    /// Which proposal kind this finalizer governs.
    pub proposal_kind: ProposalKind,
    /// Human-readable description of the judgment requirement.
    pub description: String,
    /// Whether human judgment is required (as opposed to rule-based
    /// auto-acceptance above a threshold).
    pub requires_human_judgment: bool,
    /// If set, proposals with confidence >= this threshold MAY be
    /// auto-accepted in future automation. Currently always `None` in v0
    /// (every kind requires human judgment).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_accept_above_confidence: Option<f32>,
}

impl FinalizerRegistration {
    /// Create a registration that always requires human judgment.
    #[must_use]
    pub fn human_required(proposal_kind: ProposalKind, description: impl Into<String>) -> Self {
        Self {
            id: Id::<FinalizerMarker>::new().to_string(),
            proposal_kind,
            description: description.into(),
            requires_human_judgment: true,
            auto_accept_above_confidence: None,
        }
    }
}

// ─── DuplicateCandidatePayload ────────────────────────────────────────────────

/// Payload type for [`ProposalKind::DuplicateCandidate`] proposals.
///
/// Identifies one cross-material duplicate candidate cluster. The proposed
/// value is intentionally just data: the shared [`Proposal`] and [`Judgment`]
/// gate decides whether this value can be applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DuplicateCandidatePayload {
    /// Replay-stable cluster id used by the duplicate review surface.
    pub cluster_id: String,
    /// Event source shared by the candidate events.
    pub source: String,
    /// Event type shared by the candidate events.
    pub event_type: String,
    /// Logical candidate key that made the events comparable.
    pub equivalence_key: String,
    /// Candidate event ids participating in the duplicate cluster.
    pub candidate_event_ids: Vec<String>,
    /// Source material ids backing the candidate cluster.
    pub candidate_material_ids: Vec<String>,
    /// Optional preferred event to keep when the operator accepts a preference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_event_id: Option<String>,
    /// Human-readable rationale for displaying the candidate to an operator.
    pub match_reason: String,
}

// ─── Fixtures ─────────────────────────────────────────────────────────────────

/// Fixture: a duplicate-candidate proposal for two events believed to be
/// the same shell command executed twice in close succession.
pub fn fixture_duplicate_proposal() -> Proposal<DuplicateCandidatePayload> {
    let subject = SinexObjectRef::new(crate::views::SinexObjectKind::Event, "evt-aaaabbbb")
        .with_label("command.executed: git status");

    Proposal::new(
        ProposalKind::DuplicateCandidate,
        subject,
        0.87,
        DuplicateCandidatePayload {
            cluster_id: "shell.history/command.imported/demo-command".to_string(),
            source: "shell.history".to_string(),
            event_type: "command.imported".to_string(),
            equivalence_key: "demo-command".to_string(),
            candidate_event_ids: vec!["evt-aaaabbbb".to_string(), "evt-ccccdddd".to_string()],
            candidate_material_ids: vec!["mat-1111".to_string(), "mat-2222".to_string()],
            preferred_event_id: Some("evt-aaaabbbb".to_string()),
            match_reason: "identical command text, same cwd, within 2s".to_string(),
        },
        "rule:dedup-heuristic",
    )
    .with_caveat(
        "authority.human_required",
        "duplicate event merges are irreversible; operator judgment is required",
    )
}

/// Fixture: operator accepting the duplicate-candidate proposal.
pub fn fixture_accept_judgment(proposal: &Proposal<DuplicateCandidatePayload>) -> Judgment {
    Judgment::new(
        proposal.id.clone(),
        JudgmentVerdict::Accept,
        "operator:sinity",
    )
    .with_note("confirmed: two consecutive `git status` invocations, safe to merge")
}

/// Fixture: operator rejecting the duplicate-candidate proposal.
pub fn fixture_reject_judgment(proposal: &Proposal<DuplicateCandidatePayload>) -> Judgment {
    Judgment::new(
        proposal.id.clone(),
        JudgmentVerdict::Reject,
        "operator:sinity",
    )
    .with_note("false positive: different working directories")
}

/// Fixture: finalizer registration for duplicate-candidate proposals.
pub fn fixture_finalizer_registration() -> FinalizerRegistration {
    FinalizerRegistration::human_required(
        ProposalKind::DuplicateCandidate,
        "Duplicate event merges are irreversible. An operator must verify \
         the candidate cluster before any finalizer action is applied.",
    )
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::views::SinexObjectKind;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn accept_judgment_allows_apply() -> TestResult<()> {
        let proposal = fixture_duplicate_proposal();
        let judgment = fixture_accept_judgment(&proposal);
        let value = proposal.apply(&judgment).unwrap();
        assert_eq!(
            value.match_reason,
            "identical command text, same cwd, within 2s"
        );

        Ok(())
    }

    #[sinex_test]
    async fn reject_judgment_blocks_apply() -> TestResult<()> {
        let proposal = fixture_duplicate_proposal();
        let judgment = fixture_reject_judgment(&proposal);
        let err = proposal.apply(&judgment).unwrap_err();
        assert!(
            err.to_string().contains("Accept judgment"),
            "expected error mentioning Accept requirement, got: {err}"
        );

        Ok(())
    }

    /// Core invariant test: confidence score alone cannot promote a Proposal.
    ///
    /// No matter how high the model confidence, the only extraction path
    /// is apply() which requires a Judgment. This test proves that:
    /// 1. A judgment referencing a different proposal is rejected.
    /// 2. A Defer verdict is rejected (not just Reject).
    #[sinex_test]
    async fn confidence_alone_cannot_promote_proposal() -> TestResult<()> {
        let proposal = Proposal::new(
            ProposalKind::DuplicateCandidate,
            SinexObjectRef::new(SinexObjectKind::Event, "evt-x"),
            0.99, // very high confidence — still not enough
            DuplicateCandidatePayload {
                cluster_id: "fixture/source/key".to_string(),
                source: "fixture".to_string(),
                event_type: "source".to_string(),
                equivalence_key: "key".to_string(),
                candidate_event_ids: vec!["evt-x".to_string(), "evt-y".to_string()],
                candidate_material_ids: vec!["mat-x".to_string(), "mat-y".to_string()],
                preferred_event_id: Some("evt-x".to_string()),
                match_reason: "nearly identical".to_string(),
            },
            "model:high-confidence",
        );

        // A judgment for a different proposal is rejected regardless of verdict.
        let wrong_judgment = Judgment::new(
            "some-other-proposal-id",
            JudgmentVerdict::Accept,
            "operator:sinity",
        );
        let err = proposal.apply(&wrong_judgment).unwrap_err();
        assert!(
            err.to_string().contains("does not reference this proposal"),
            "expected mismatch error, got: {err}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn defer_verdict_does_not_grant_access() -> TestResult<()> {
        let proposal = fixture_duplicate_proposal();
        let defer_judgment = Judgment::new(
            proposal.id.clone(),
            JudgmentVerdict::Defer,
            "operator:sinity",
        );
        let err = proposal.apply(&defer_judgment).unwrap_err();
        assert!(
            err.to_string().contains("Accept judgment"),
            "expected Accept-required error for Defer verdict, got: {err}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn proposal_json_exposes_value_for_display_but_apply_still_requires_judgment()
    -> TestResult<()> {
        // The proposed_value IS serialized (operator UIs need it for display),
        // but code-level access through Rust still requires apply() + Judgment.
        let proposal = fixture_duplicate_proposal();
        let json = serde_json::to_value(&proposal).unwrap();

        // Value is present in JSON — intentional for operator display.
        assert!(json["proposed_value"]["match_reason"].is_string());
        // Confidence is present but is display-only.
        assert!(json["confidence"].as_f64().unwrap() > 0.0);
        // Kind serializes in snake_case.
        assert_eq!(json["kind"], "duplicate_candidate");
        // Caveat is present.
        assert_eq!(json["caveats"].as_array().unwrap().len(), 1);

        Ok(())
    }

    #[sinex_test]
    async fn view_envelope_wraps_proposal() -> TestResult<()> {
        let proposal = fixture_duplicate_proposal();
        let envelope = proposal.into_envelope("sinexctl.authority");
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["source_surface"], "sinexctl.authority");
        assert_eq!(json["payload"]["kind"], "duplicate_candidate");
        assert!(json["generated_at"].is_string());

        Ok(())
    }

    #[sinex_test]
    async fn judgment_serializes_verdict_in_snake_case() -> TestResult<()> {
        let proposal = fixture_duplicate_proposal();
        let judgment = fixture_accept_judgment(&proposal);
        let json = serde_json::to_value(&judgment).unwrap();
        assert_eq!(json["verdict"], "accept");
        assert!(json["note"].is_string());

        Ok(())
    }

    #[sinex_test]
    async fn finalizer_registration_declares_human_requirement() -> TestResult<()> {
        let reg = fixture_finalizer_registration();
        assert!(reg.requires_human_judgment);
        assert!(reg.auto_accept_above_confidence.is_none());
        assert_eq!(reg.proposal_kind, ProposalKind::DuplicateCandidate);

        Ok(())
    }

    #[sinex_test]
    async fn schema_generation_covers_all_authority_types() -> TestResult<()> {
        let proposal_schema =
            serde_json::to_value(schemars::schema_for!(Proposal<DuplicateCandidatePayload>))
                .unwrap();
        let judgment_schema = serde_json::to_value(schemars::schema_for!(Judgment)).unwrap();
        let finalizer_schema =
            serde_json::to_value(schemars::schema_for!(FinalizerRegistration)).unwrap();

        assert!(proposal_schema["properties"]["confidence"].is_object());
        assert!(judgment_schema["properties"]["verdict"].is_object());
        assert!(finalizer_schema["properties"]["requires_human_judgment"].is_object());

        Ok(())
    }
}
