//! Derivation control-plane primitives: product-class declarations, the
//! claim-support vector, and derivation scope kinds.
//!
//! This is the W0 root of the interpretation-plane campaign (sinex-0vx /
//! sinex-8cr): every later child (automaton output declarations, DB schema,
//! finalizer registry, lane machinery) builds on the vocabulary defined here.
//! See `.agent/scratch/027-0vx-derivation-control-plane-design.md` for the
//! full design and
//! `.agent/scratch/new-gpt-pro/04-interpretation-plane-blueprint.report.md`
//! for the originating blueprint.
//!
//! [`DerivedProductClass`] and [`ClaimSupport`] are orthogonal axes: a
//! canonical derived event can have weak temporal quality; a semantic
//! candidate can have direct evidence. Only `canonical_derived_event` (plus
//! accepted claims) are default-eligible inputs to further canonical
//! derivation — never a bare `semantic_candidate` or `analysis_claim`.
//!
//! [`ClaimSupport`] replaces the old scalar-`EvidenceTier` idea: storage
//! keeps the full vector, never a collapsed badge (a UI may still render a
//! badge, but it is always derived from the vector). Confidence cannot
//! become authority — doctrine already enforced by the `curation`/
//! `authority` proposal/judgment seam — and this module extends that
//! doctrine with a compile-time-enforced construction path: an adjudicated
//! (`Accepted`/`Rejected`/`Superseded`) [`ClaimSupport`] cannot be built
//! without a typed [`Id<Event>`] pointing at the judgment event that
//! authorizes it.
//!
//! [`DerivationScope`] is the input-scope vocabulary for a derivation epoch.
//! The batch variants (`EventSet`/`SourceMaterialSet`/`DocumentChunkSet`)
//! enumerate a finite input id set and hash it — this is what
//! [`crate::semantic::SemanticScope`] already does and does NOT lift to
//! continuous automata. [`DerivationScope::StreamCheckpoint`] is the
//! generalization: a durable `JetStream` consumer checkpoint range, comparable
//! only via an explicit FREEZE (`end_seq = Some(..)`), never a wall clock.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{Result, SinexError};
use crate::events::Event;
use crate::ids::Id;
use crate::temporal::Timestamp;

// ─── DerivedProductClass ───────────────────────────────────────────────────

/// The output-layer epistemic-class axis every derived output must declare.
///
/// Orthogonal to [`ClaimSupport`] and to
/// [`crate::semantic::SemanticLaneKind`] (lane lifecycle class — a canonical
/// lane can still emit `heuristic`-support outputs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DerivedProductClass {
    /// A deterministic interpretation of admitted parents, default-eligible
    /// as input to further canonical derivation (canonicalizer, rollups,
    /// intervals, document parsing).
    CanonicalDerivedEvent,
    /// Rebuildable read-model state outside the event spine.
    ProjectionRow,
    /// A derived assertion or evaluation that is not default-eligible as
    /// canonical input unless a consumer explicitly opts in or it is
    /// accepted through authority (health reports, expectation status).
    AnalysisClaim,
    /// A persisted generated report, receipt, export, or artifact pointer.
    ReportArtifact,
    /// An entity/relation/tag/category suggestion awaiting explicit
    /// authority or deterministic-policy finalization.
    SemanticCandidate,
    /// An authority decision event. Only the curation/authority finalizer
    /// writer may emit this class.
    OperatorJudgment,
}

impl DerivedProductClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalDerivedEvent => "canonical_derived_event",
            Self::ProjectionRow => "projection_row",
            Self::AnalysisClaim => "analysis_claim",
            Self::ReportArtifact => "report_artifact",
            Self::SemanticCandidate => "semantic_candidate",
            Self::OperatorJudgment => "operator_judgment",
        }
    }

    /// Only `canonical_derived_event` is default-eligible as input to
    /// further canonical derivation (interpretation-plane doctrine:
    /// candidate claims require explicit opt-in or authority acceptance).
    #[must_use]
    pub const fn default_canonical_input_eligible(self) -> bool {
        matches!(self, Self::CanonicalDerivedEvent)
    }
}

impl std::fmt::Display for DerivedProductClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── DerivationWriteSurface / InputEligibility ─────────────────────────────

/// Which writer mechanism produces a declared output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DerivationWriteSurface {
    /// `DerivedOutput<T>` through the automaton adapter.
    DerivedOutput,
    /// A projection writer backed by the projection registry.
    ProjectionWriter,
    /// An artifact writer (reports, receipts, exports).
    ArtifactWriter,
    /// A curation proposal producer.
    CurationWriter,
    /// A registered authority finalizer.
    AuthorityFinalizer,
}

/// Whether a declared output is a default-eligible input to further
/// canonical derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputEligibility {
    /// Eligible without an explicit consumer opt-in.
    DefaultCanonicalInput,
    /// Only usable when a consumer explicitly opts into this class.
    ExplicitOnly,
    /// Never eligible as input to further derivation (terminal output).
    NeverInput,
}

// ─── ClaimSupport vector ────────────────────────────────────────────────────

/// Direct/indirect evidentiary support level for a claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Direct,
    Convergent,
    Heuristic,
    SelfReport,
    ModelInferred,
    Unsupported,
}

/// How much of the referenced evidence is actually resolvable/visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverage {
    Covered,
    Partial,
    Unknown,
    Unavailable,
    Redacted,
}

/// Temporal-quality rung for a claim, unifying material `ts_quality` rungs
/// and derived [`crate::domain::temporal::SyntheticTemporalPolicy`] values
/// into one view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClaimTemporalQuality {
    RealtimeCapture,
    IntrinsicContent,
    InferredMtime,
    InferredCtime,
    InferredUser,
    StagedAt,
    InheritParent,
    LatestInput,
    WindowBoundary,
    DeclaredEffective,
    Unknown,
}

/// Adjudication lifecycle for a claim's support vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdjudicationStatus {
    Unreviewed,
    Accepted,
    Rejected,
    Superseded,
}

/// The claim-support vector attached to every declared derived output.
///
/// Fields are private: the only public construction paths are
/// [`ClaimSupport::unreviewed`] (and its convenience [`ClaimSupport::unknown`]
/// baseline) and [`ClaimSupport::adjudicated`], which requires a typed
/// [`Id<Event>`] proving a judgment event exists. This is the compile-time
/// rung of "confidence cannot become authority": no caller outside this
/// module can write an `Accepted`/`Rejected`/`Superseded` vector via a struct
/// literal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClaimSupport {
    support_level: SupportLevel,
    source_coverage: SourceCoverage,
    temporal_quality: ClaimTemporalQuality,
    adjudication: AdjudicationStatus,
    evidence_event_count: u32,
    evidence_material_count: u32,
    support_family_count: u32,
    counterevidence_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    adjudication_event_id: Option<Id<Event>>,
}

impl ClaimSupport {
    /// Construct an unreviewed claim-support vector. This is the path every
    /// automaton/declaration uses — adjudication starts `Unreviewed` and
    /// carries no judgment event id.
    #[must_use]
    pub fn unreviewed(
        support_level: SupportLevel,
        source_coverage: SourceCoverage,
        temporal_quality: ClaimTemporalQuality,
        evidence_event_count: u32,
        evidence_material_count: u32,
        support_family_count: u32,
        counterevidence_count: u32,
    ) -> Self {
        Self {
            support_level,
            source_coverage,
            temporal_quality,
            adjudication: AdjudicationStatus::Unreviewed,
            evidence_event_count,
            evidence_material_count,
            support_family_count,
            counterevidence_count,
            adjudication_event_id: None,
        }
    }

    /// The unknown/low baseline: `Unsupported` x `Unknown` x `Unknown`,
    /// zero evidence, unreviewed. Declarations or automata that have not
    /// classified their evidentiary shape yet MUST start here — doctrine
    /// forbids a fabricated `Direct`/`Covered` default with empty evidence
    /// refs (the numeric-confidence analog: never default to 1.0 with no
    /// evidence).
    #[must_use]
    pub fn unknown() -> Self {
        Self::unreviewed(
            SupportLevel::Unsupported,
            SourceCoverage::Unknown,
            ClaimTemporalQuality::Unknown,
            0,
            0,
            0,
            0,
        )
    }

    /// Construct an adjudicated (`Accepted`/`Rejected`/`Superseded`) support
    /// vector. Requires the [`Id<Event>`] of the actual `curation.judgment`
    /// (or `operator_judgment`) event that authorizes this status — a typed
    /// judgment reference, never a bare enum value or a confidence score.
    ///
    /// Returns an error for `AdjudicationStatus::Unreviewed` (use
    /// [`ClaimSupport::unreviewed`] instead — an unreviewed vector never
    /// carries a judgment event id).
    pub fn adjudicated(
        support_level: SupportLevel,
        source_coverage: SourceCoverage,
        temporal_quality: ClaimTemporalQuality,
        status: AdjudicationStatus,
        judgment_event_id: Id<Event>,
        evidence_event_count: u32,
        evidence_material_count: u32,
        support_family_count: u32,
        counterevidence_count: u32,
    ) -> Result<Self> {
        if status == AdjudicationStatus::Unreviewed {
            return Err(SinexError::validation(
                "ClaimSupport::adjudicated requires Accepted/Rejected/Superseded; \
                 use ClaimSupport::unreviewed for Unreviewed",
            ));
        }
        Ok(Self {
            support_level,
            source_coverage,
            temporal_quality,
            adjudication: status,
            evidence_event_count,
            evidence_material_count,
            support_family_count,
            counterevidence_count,
            adjudication_event_id: Some(judgment_event_id),
        })
    }

    /// Mirrors the DB trigger invariant (`adjudicated claim requires
    /// adjudication_event_id`) so the same rule is testable without a
    /// database — catches wire-deserialized vectors that bypass the
    /// constructors above.
    #[must_use]
    pub fn is_shape_valid(&self) -> bool {
        match self.adjudication {
            AdjudicationStatus::Unreviewed => self.adjudication_event_id.is_none(),
            AdjudicationStatus::Accepted
            | AdjudicationStatus::Rejected
            | AdjudicationStatus::Superseded => self.adjudication_event_id.is_some(),
        }
    }

    #[must_use]
    pub fn support_level(&self) -> SupportLevel {
        self.support_level
    }

    #[must_use]
    pub fn source_coverage(&self) -> SourceCoverage {
        self.source_coverage
    }

    #[must_use]
    pub fn temporal_quality(&self) -> ClaimTemporalQuality {
        self.temporal_quality
    }

    #[must_use]
    pub fn adjudication(&self) -> AdjudicationStatus {
        self.adjudication
    }

    #[must_use]
    pub fn evidence_event_count(&self) -> u32 {
        self.evidence_event_count
    }

    #[must_use]
    pub fn evidence_material_count(&self) -> u32 {
        self.evidence_material_count
    }

    #[must_use]
    pub fn support_family_count(&self) -> u32 {
        self.support_family_count
    }

    #[must_use]
    pub fn counterevidence_count(&self) -> u32 {
        self.counterevidence_count
    }

    #[must_use]
    pub fn adjudication_event_id(&self) -> Option<Id<Event>> {
        self.adjudication_event_id
    }
}

impl Default for ClaimSupport {
    fn default() -> Self {
        Self::unknown()
    }
}

/// A `'static`-friendly template for an automaton's default `ClaimSupport`.
///
/// Deliberately excludes `adjudication`/`adjudication_event_id`/evidence
/// counts — a static declaration cannot know per-output evidence counts, and
/// a static declaration can never pre-declare an adjudicated status (that
/// would let a declaration author fabricate authority). Use
/// [`ClaimSupportTemplate::instantiate`] to turn the template plus observed
/// evidence counts into a concrete unreviewed [`ClaimSupport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClaimSupportTemplate {
    pub support_level: SupportLevel,
    pub source_coverage: SourceCoverage,
    pub temporal_quality: ClaimTemporalQuality,
}

impl ClaimSupportTemplate {
    /// The unknown/low baseline template. Declarations that have not
    /// classified their evidentiary shape yet MUST use this rather than
    /// guessing `Direct`/`Covered`.
    pub const UNKNOWN: Self = Self {
        support_level: SupportLevel::Unsupported,
        source_coverage: SourceCoverage::Unknown,
        temporal_quality: ClaimTemporalQuality::Unknown,
    };

    #[must_use]
    pub const fn new(
        support_level: SupportLevel,
        source_coverage: SourceCoverage,
        temporal_quality: ClaimTemporalQuality,
    ) -> Self {
        Self {
            support_level,
            source_coverage,
            temporal_quality,
        }
    }

    /// Materialize a concrete, unreviewed [`ClaimSupport`] from this
    /// template plus the runtime evidence counts an automaton observed for
    /// one output.
    #[must_use]
    pub fn instantiate(
        &self,
        evidence_event_count: u32,
        evidence_material_count: u32,
        support_family_count: u32,
        counterevidence_count: u32,
    ) -> ClaimSupport {
        ClaimSupport::unreviewed(
            self.support_level,
            self.source_coverage,
            self.temporal_quality,
            evidence_event_count,
            evidence_material_count,
            support_family_count,
            counterevidence_count,
        )
    }
}

// ─── DerivationOutputDeclaration ────────────────────────────────────────────

/// Stable identifier for a [`DerivationOutputDeclaration`].
pub type DerivationDeclarationId = &'static str;

/// What an automaton (or other writer) declares about one class of output it
/// produces. `AutomatonSpec.outputs: &'static [DerivationOutputDeclaration]`
/// (landing in sinex-0vx.1) is the runtime registry startup checks against;
/// this type is the declaration shape itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DerivationOutputDeclaration {
    pub declaration_id: DerivationDeclarationId,
    pub owner: &'static str,
    pub product_class: DerivedProductClass,
    pub write_surface: DerivationWriteSurface,
    pub output_source: Option<&'static str>,
    pub output_event_type: Option<&'static str>,
    pub projection_kind: Option<&'static str>,
    pub artifact_kind: Option<&'static str>,
    pub proposal_kind: Option<&'static str>,
    pub semantics_version: &'static str,
    pub input_eligibility: InputEligibility,
    pub default_support: ClaimSupportTemplate,
    pub verification_command: &'static str,
}

impl DerivationOutputDeclaration {
    #[must_use]
    pub const fn is_derived_output_surface(&self) -> bool {
        matches!(self.write_surface, DerivationWriteSurface::DerivedOutput)
    }

    /// Validate the shape invariants a `derivation.product_declarations` row
    /// enforces via CHECK constraints (kept here so a malformed `&'static`
    /// const declaration fails a cheap unit test instead of only failing at
    /// DB apply time — see `crate-sinex-schema` DDL sketch in the blueprint).
    pub fn validate(&self) -> Result<()> {
        let has_output_identity = self.output_source.is_some() && self.output_event_type.is_some();
        if self.is_derived_output_surface() != has_output_identity {
            return Err(SinexError::validation(
                "derived_output write surface requires output_source and \
                 output_event_type, and vice versa",
            )
            .with_context("declaration_id", self.declaration_id));
        }
        if matches!(self.product_class, DerivedProductClass::ProjectionRow)
            && self.projection_kind.is_none()
        {
            return Err(
                SinexError::validation("projection_row product class requires projection_kind")
                    .with_context("declaration_id", self.declaration_id),
            );
        }
        Ok(())
    }
}

// ─── TstzRange ───────────────────────────────────────────────────────────

/// A closed timestamp interval `[start, end]`.
///
/// Named to mirror the eventual Postgres `tstzrange` column
/// (`derivation.epochs.scope->>'coverage_window'`). Distinct from
/// [`crate::query::TimeRange`], which allows open bounds for query
/// filtering — a coverage window describes an actual observed/approximate
/// span with defined edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TstzRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TstzRange {
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self> {
        if start > end {
            return Err(SinexError::validation("TstzRange start must not be after end")
                .with_context("start", start.to_string())
                .with_context("end", end.to_string()));
        }
        Ok(Self { start, end })
    }
}

// ─── DerivationScope ────────────────────────────────────────────────────────

/// Input scope for a derivation epoch.
///
/// The batch variants (`EventSet`/`SourceMaterialSet`/`DocumentChunkSet`)
/// enumerate a finite input id set — this is what
/// [`crate::semantic::SemanticScope`] already does. `StreamCheckpoint` is the
/// scope kind continuous automata need, because "all input ids" cannot be
/// enumerated over an unbounded confirmed-event stream. `input_set_hash` is
/// the UNIVERSAL comparability primitive: it is the one field every variant
/// carries, and lane diffs key on it regardless of scope kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DerivationScope {
    EventSet {
        input_ids: Vec<String>,
        input_set_hash: String,
    },
    SourceMaterialSet {
        input_ids: Vec<String>,
        input_set_hash: String,
    },
    DocumentChunkSet {
        input_ids: Vec<String>,
        input_set_hash: String,
    },
    /// A durable `JetStream` consumer checkpoint range for a continuous
    /// automaton. `end_seq = None` means the live, open-ended canonical
    /// lane (not diff-comparable); `end_seq = Some(seq)` is an explicit
    /// FREEZE at lane-creation instant, making a shadow/experiment lane a
    /// bounded, comparable snapshot. The freeze is an operation, not a wall
    /// clock — same doctrine as closure-by-next-arrival for intervals.
    StreamCheckpoint {
        stream: String,
        filter_subjects: Vec<String>,
        /// Consumer stream-seq lower bound (exclusive); 0 = from start.
        start_seq: u64,
        /// Frozen high-watermark; `None` = open-ended live canonical lane.
        end_seq: Option<u64>,
        /// Honest/approximate `ts_orig` span the seq range covers, for
        /// query alignment. Not authoritative — `start_seq`/`end_seq` are.
        coverage_window: Option<TstzRange>,
        input_set_hash: String,
    },
    /// Civil-time summarizer scope (hourly/daily rollups).
    TimeWindow {
        bucket: String,
        start: Timestamp,
        end: Timestamp,
        input_set_hash: String,
    },
    /// Per-scope reconciler key (e.g. per-entity enrichment).
    ScopeReconcilerKey {
        scope_key: String,
        input_set_hash: String,
    },
    ProjectionScope {
        projection_kind: String,
        scope_key: String,
        input_set_hash: String,
    },
}

impl DerivationScope {
    /// The universal comparability primitive — present on every variant.
    #[must_use]
    pub fn input_set_hash(&self) -> &str {
        match self {
            Self::EventSet { input_set_hash, .. }
            | Self::SourceMaterialSet { input_set_hash, .. }
            | Self::DocumentChunkSet { input_set_hash, .. }
            | Self::StreamCheckpoint { input_set_hash, .. }
            | Self::TimeWindow { input_set_hash, .. }
            | Self::ScopeReconcilerKey { input_set_hash, .. }
            | Self::ProjectionScope { input_set_hash, .. } => input_set_hash,
        }
    }

    /// The `derivation.epochs.scope_model` CHECK-vocabulary string for this
    /// variant.
    #[must_use]
    pub const fn scope_model(&self) -> &'static str {
        match self {
            Self::EventSet { .. } => "event_set",
            Self::SourceMaterialSet { .. } => "source_material_set",
            Self::DocumentChunkSet { .. } => "document_chunk_set",
            Self::StreamCheckpoint { .. } => "stream_checkpoint",
            Self::TimeWindow { .. } => "time_window",
            Self::ScopeReconcilerKey { .. } => "scope_reconciler_key",
            Self::ProjectionScope { .. } => "projection_scope",
        }
    }

    /// True only for a `StreamCheckpoint` scope with `end_seq = None` — the
    /// live, open-ended canonical lane. Any other scope kind, or a
    /// `StreamCheckpoint` with a frozen `end_seq`, returns `false`.
    #[must_use]
    pub const fn is_open_ended_stream(&self) -> bool {
        matches!(self, Self::StreamCheckpoint { end_seq: None, .. })
    }
}

#[cfg(test)]
#[path = "derivation_test.rs"]
mod tests;
