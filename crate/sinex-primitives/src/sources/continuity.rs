//! Source continuity diagnostics — operator-facing scorecards (#1085).
//!
//! These types are the structured shape behind `sources.continuity.list`,
//! `sources.continuity.get`, and `sources.continuity.explain_gap`. They
//! deliberately use richer, typed enums (rather than free-form strings) so
//! agents and dashboards can reason about seam classification, gap
//! attribution, and replayability dimensions without re-parsing prose.
//!
//! See [`crate::rpc::sources::SourcesContinuityResponse`] for the older,
//! per-identifier diagnostic surface that this module supplements rather
//! than replaces. The list/get/explain-gap surface aggregates across the
//! `source_family` axis; the existing `sources.continuity` method aggregates
//! by `source_identifier`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::temporal::Timestamp;

use super::SourceFamily;

// ──────────────────────────────────────────────────────────────────────────
// Coverage contract
// ──────────────────────────────────────────────────────────────────────────

/// Operator's expectation about how a source covers time.
///
/// This is *declared intent*, not measured coverage. A source may declare
/// `Continuous` even when actual coverage has gaps; the gap surface
/// (`CoverageGap`) records the divergence between intent and reality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoverageContract {
    /// Source emits whenever the underlying world produces an event;
    /// silent windows are presumed to be gaps.
    Continuous,
    /// Source emits in scheduled or operator-triggered batches (cron,
    /// nightly export, takeout). Quiet periods between dumps are expected.
    PeriodicDump,
    /// Source is imported when the operator notices something to import
    /// (rarely, on demand). Quiet periods are expected and uninteresting.
    OpportunisticImport,
    /// Source is one-shot: a finite archive that does not grow.
    FiniteOneShot,
    /// Source is a live stream that is not retained — gaps are unrecoverable.
    EphemeralStream,
}

// ──────────────────────────────────────────────────────────────────────────
// Declared coverage contract (stored shape, #1174)
// ──────────────────────────────────────────────────────────────────────────

/// Kind discriminator for the stored, operator-declared coverage contract.
///
/// Mirrors [`CoverageContract`] for the five live shapes plus an explicit
/// `Unknown` value used as the default for legacy rows. `Unknown` allows
/// `sinexctl sources continuity` to flag "configuration gap" rather than
/// "data gap" — the two are different operator concerns.
///
/// Persisted as a `TEXT`-shaped string inside the `coverage_contract` JSONB
/// column on `raw.source_material_registry`; the named CHECK constraint
/// `source_material_registry_coverage_contract_kind_check` keeps the column
/// in sync with this enum (see `crate/sinex-schema/src/converge.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum DeclaredCoverageContractKind {
    Continuous,
    PeriodicDump,
    OpportunisticImport,
    FiniteOneShot,
    EphemeralStream,
    /// Legacy default — operator has not declared an intent yet.
    Unknown,
}

impl DeclaredCoverageContractKind {
    /// Return the canonical `PascalCase` wire form persisted in JSONB.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continuous => "Continuous",
            Self::PeriodicDump => "PeriodicDump",
            Self::OpportunisticImport => "OpportunisticImport",
            Self::FiniteOneShot => "FiniteOneShot",
            Self::EphemeralStream => "EphemeralStream",
            Self::Unknown => "Unknown",
        }
    }

    /// All canonical kind strings, in declaration order. Used by the schema
    /// CHECK constraint generator and by validation paths that need to
    /// confirm a value is permitted before persisting.
    pub const ALL: &'static [&'static str] = &[
        "Continuous",
        "PeriodicDump",
        "OpportunisticImport",
        "FiniteOneShot",
        "EphemeralStream",
        "Unknown",
    ];
}

/// Operator-declared coverage contract for a source material.
///
/// Distinct from [`CoverageContract`] which is the inferred-from-observation
/// shape. `DeclaredCoverageContract` is the stored shape: what the operator
/// said the source *should* look like, plus a structured set of expected
/// horizons / cadences and a declaration timestamp.
///
/// The default ([`DeclaredCoverageContract::unknown`]) is what legacy rows
/// receive — its `kind` is [`DeclaredCoverageContractKind::Unknown`] and
/// `declared_at` is `None`. Continuity reports treat `Unknown` rows as
/// "configuration gap, no operator intent recorded".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeclaredCoverageContract {
    pub kind: DeclaredCoverageContractKind,
    /// Event types the source is expected to emit. Empty for `Unknown`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_event_types: Vec<String>,
    /// Expected coverage horizon in seconds (e.g. "covers the last 30 days").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_horizon_seconds: Option<i64>,
    /// Expected cadence in seconds between successive dumps / fetches.
    /// Meaningful primarily for `PeriodicDump` / `OpportunisticImport`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cadence_seconds: Option<i64>,
    /// When the operator declared this contract. `None` for legacy rows
    /// that received the `Unknown` default at column-add time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_at: Option<Timestamp>,
    /// Free-form attribution string identifying the operator or process
    /// that declared the contract. Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_by: Option<String>,
}

impl DeclaredCoverageContract {
    /// The canonical "no operator intent declared" contract used as the
    /// column default for legacy rows.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            kind: DeclaredCoverageContractKind::Unknown,
            expected_event_types: Vec::new(),
            expected_horizon_seconds: None,
            expected_cadence_seconds: None,
            declared_at: None,
            declared_by: None,
        }
    }

    /// Returns true when the operator has not yet declared an intent for
    /// this source. Continuity reports treat this as a "configuration gap".
    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        matches!(self.kind, DeclaredCoverageContractKind::Unknown)
    }
}

impl Default for DeclaredCoverageContract {
    fn default() -> Self {
        Self::unknown()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Privacy class (declared per-material, #1174)
// ──────────────────────────────────────────────────────────────────────────

/// Operator-declared privacy classification for a source material.
///
/// Persisted as a `TEXT` column on `raw.source_material_registry` with the
/// named CHECK constraint `source_material_registry_privacy_class_check`
/// keeping the live values in sync with this enum.
///
/// `Unknown` is the column default for legacy rows that pre-date the
/// classification surface; private-mode classification at the seam level
/// (`SeamKind::PrivateModeGap`) only fires when the classification is one
/// of `Personal`, `Secret`, or `Redacted` — never on `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum PrivacyClass {
    /// Material may be shown / shared without redaction.
    Public,
    /// Personal data — visible to the operator, redacted in shared surfaces.
    Personal,
    /// Secret material (credentials, tokens, identity documents).
    Secret,
    /// Material that has already been redacted at capture time.
    Redacted,
    /// Operator has not classified this material yet (legacy default).
    #[default]
    Unknown,
}

impl PrivacyClass {
    /// Canonical wire-form string used in the database column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Personal => "personal",
            Self::Secret => "secret",
            Self::Redacted => "redacted",
            Self::Unknown => "unknown",
        }
    }

    /// All canonical class strings, in declaration order. Used by the schema
    /// CHECK constraint generator and by validation paths that need to
    /// confirm a value is permitted before persisting.
    pub const ALL: &'static [&'static str] =
        &["public", "personal", "secret", "redacted", "unknown"];

    /// Returns true when the operator has not yet classified this material.
    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Returns true when this class is one of the privacy-sensitive shapes
    /// (`Personal`, `Secret`, or `Redacted`). `Public` and `Unknown` return
    /// `false`. Used by seam classification to decide whether a gap is
    /// attributable to private mode.
    #[must_use]
    pub const fn is_private(self) -> bool {
        matches!(self, Self::Personal | Self::Secret | Self::Redacted)
    }
}

impl std::str::FromStr for PrivacyClass {
    type Err = crate::SinexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "public" => Ok(Self::Public),
            "personal" => Ok(Self::Personal),
            "secret" => Ok(Self::Secret),
            "redacted" => Ok(Self::Redacted),
            "unknown" => Ok(Self::Unknown),
            other => Err(crate::SinexError::validation(format!(
                "invalid privacy_class '{other}'; must be one of {:?}",
                Self::ALL
            ))),
        }
    }
}

impl std::fmt::Display for PrivacyClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Temporal seams (boundaries between adjacent material chunks)
// ──────────────────────────────────────────────────────────────────────────

/// Classification of an adjacency between two material chunks.
///
/// A seam is the point at which two material boundaries meet on the timeline.
/// The `kind` records what kind of meeting it is — clean continuation, an
/// expected pause, an unexplained gap, or genuine corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SeamKind {
    /// Two chunks meet cleanly with no gap between them, as expected.
    ExpectedContinuation,
    /// Two chunks overlap in time (later chunk starts before earlier ends).
    Overlap,
    /// A measurable gap appears that the coverage contract does not justify.
    Discontinuity,
    /// A material recovered partially after a crash; the seam bridges the
    /// recovered region and the next normal chunk.
    RecoveredPartial,
    /// Gap aligns with private mode being active — the absence is intentional.
    PrivateModeGap,
    /// The seam exists but its cause is not classified.
    Unknown,
}

/// A boundary between two adjacent source-material chunks for the same source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TemporalSeam {
    pub kind: SeamKind,
    /// End of the earlier chunk.
    pub before_ts: Option<Timestamp>,
    /// Start of the later chunk.
    pub after_ts: Option<Timestamp>,
    /// Free-form supporting evidence — material kinds, statuses, byte
    /// offsets — that explains why this seam was classified the way it was.
    /// Path strings should be redacted upstream; do not embed home paths.
    #[serde(default)]
    pub evidence: serde_json::Value,
}

// ──────────────────────────────────────────────────────────────────────────
// Coverage gaps (measured absences in the timeline)
// ──────────────────────────────────────────────────────────────────────────

/// Why coverage is missing in a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GapKind {
    /// Operator-toggled private mode covered the window.
    PrivateMode,
    /// The capturing service crashed or was restarted.
    ServiceCrash,
    /// The source was disabled in configuration / not deployed.
    DisabledSource,
    /// The parser ran but produced no events / errored out.
    ParserFailure,
    /// The gap is part of normal expected downtime (e.g. periodic dump).
    ExpectedDownTime,
    /// No attribution found.
    Unknown,
}

/// A measured absence in coverage for a source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CoverageGap {
    pub from_ts: Timestamp,
    pub to_ts: Timestamp,
    pub kind: GapKind,
    /// Human-readable attribution string (e.g. "private mode active 14:00–14:35").
    /// Optional — agents should rely on `kind` for routing decisions and use
    /// `attribution` for display only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────
// Replayability scorecard
// ──────────────────────────────────────────────────────────────────────────

/// Operator scorecard describing how safely a source can be replayed.
///
/// Each dimension is a coarse boolean rather than a probability — the
/// operator question is "is this dimension in good standing?", not
/// "what is the failure rate?". `weak_points` carries human-readable
/// caveats (e.g. "`anchor_byte` unstable across re-exports").
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Replayability {
    /// Source bytes are still on disk (blob staged) and re-readable.
    pub raw_bytes_preserved: bool,
    /// Captured timing is precise (`exact` precision, monotonic / wall clock
    /// rather than inferred mtime / ctime).
    pub timing_quality: bool,
    /// `(material_id, anchor_byte)` is durable across re-imports — the
    /// natural key won't move under the events that already reference it.
    pub anchor_stability: bool,
    /// The parser is deterministic — replay produces the same events from
    /// the same bytes. Operator-asserted, not measured.
    pub parser_determinism: bool,
    /// Privacy redaction is applied at replay time so historical replays
    /// reflect current policy rather than the policy at original capture.
    pub privacy_safe_replay: bool,
    /// Free-form caveats. Empty when no weaknesses were detected.
    #[serde(default)]
    pub weak_points: Vec<String>,
}

impl Replayability {
    /// Convenience: number of green dimensions out of five.
    #[must_use]
    pub fn green_count(&self) -> u8 {
        u8::from(self.raw_bytes_preserved)
            + u8::from(self.timing_quality)
            + u8::from(self.anchor_stability)
            + u8::from(self.parser_determinism)
            + u8::from(self.privacy_safe_replay)
    }

    /// Build a scorecard for a single source material from its persisted
    /// facts.
    ///
    /// This mirrors the family-level heuristic in
    /// `sinex_db::repositories::ContinuityRepository::build_replayability`
    /// but operates on one material's properties — the shape needed by
    /// `sinexctl replay preview` when surfacing per-material scorecards
    /// across a replay scope.
    ///
    /// Inputs are the registry-row facts `sources.show` returns:
    /// - `status`: registry status string (`completed`, `sensing`,
    ///   `failed`, `recovered_partial`, `cancelled`).
    /// - `has_blob`: `true` when `optional_blob_id IS NOT NULL` — source
    ///   bytes are still on disk and re-readable.
    /// - `timing_info_type`: registry timing classification (`realtime`,
    ///   `intrinsic`, `inferred`, etc.).
    /// - `total_bytes`: registry size in bytes; `None` means the material
    ///   is still in-flight (`status='sensing'`) and anchor stability is
    ///   not yet guaranteed.
    ///
    /// `parser_determinism` and `privacy_safe_replay` are operator-asserted
    /// rather than measured at this layer; they default to `true` and
    /// always carry a `weak_points` caveat so the report stays honest
    /// about what is asserted vs measured.
    #[must_use]
    pub fn from_material_facts(
        status: crate::domain::MaterialStatus,
        has_blob: bool,
        timing_info_type: crate::domain::SourceMaterialTimingInfoType,
        total_bytes: Option<i64>,
    ) -> Self {
        use crate::domain::SourceMaterialTimingInfoType as TimingType;
        let timing_quality = matches!(
            timing_info_type,
            TimingType::Realtime | TimingType::Intrinsic
        );
        let anchor_stability = total_bytes.is_some();
        let any_failed = status == crate::domain::MaterialStatus::Failed;
        let any_recovered = status == crate::domain::MaterialStatus::RecoveredPartial;

        let mut weak_points: Vec<String> = Vec::new();
        if !has_blob {
            weak_points.push("no source bytes preserved (no blob backing)".into());
        }
        if !timing_quality {
            weak_points.push(
                "timing inferred from filesystem mtime/ctime — replay times may drift".into(),
            );
        }
        if !anchor_stability {
            weak_points.push(
                "material still in `sensing` (total_bytes unset); anchor stability not guaranteed"
                    .into(),
            );
        }
        if any_failed {
            weak_points.push("material status=failed".into());
        }
        if any_recovered {
            weak_points.push(
                "material status=recovered_partial; replay covers the recovered subset only".into(),
            );
        }
        weak_points.push(
            "parser_determinism is asserted, not measured; bug fixes may produce different events on replay"
                .into(),
        );

        Self {
            raw_bytes_preserved: has_blob,
            timing_quality,
            anchor_stability,
            // Operator-asserted dimensions — see weak_points caveats above.
            parser_determinism: true,
            privacy_safe_replay: true,
            weak_points,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Per-material replayability scorecard (#1174 Phase 5.5)
// ──────────────────────────────────────────────────────────────────────────

/// Per-material replayability scorecard rendered by
/// `sinexctl replay preview` when a replay scope crosses material
/// boundaries.
///
/// The aggregate scorecard on [`SourceContinuityReport`] hides per-material
/// weakness — a single failed material in a 50-material scope can drag the
/// aggregate down without telling the operator which material is at fault.
/// This scorecard surfaces the per-material score so the operator can see
/// the weakness dimension and the responsible material identifier in the
/// same view.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MaterialReplayabilityScorecard {
    /// UUID of the source material this scorecard describes.
    pub material_id: String,
    /// Operator-visible source identifier (file path / URI / source name).
    pub source_identifier: String,
    /// Material kind (e.g. `annex`, `inline`, `archive`).
    pub material_kind: String,
    /// Registry status.
    pub status: crate::domain::MaterialStatus,
    /// Replayability scorecard derived from this material's facts.
    pub replayability: Replayability,
}

// ──────────────────────────────────────────────────────────────────────────
// Aggregate report
// ──────────────────────────────────────────────────────────────────────────

/// Operator-facing continuity report for a `SourceFamily`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityReport {
    pub source_family: SourceFamily,
    pub coverage_contract: CoverageContract,
    /// True when `coverage_contract` was read from an operator-declared
    /// `DeclaredCoverageContract` (kind != Unknown) on the source-material
    /// registry. False when the value is inferred from family-name and
    /// timing heuristics because no declared contract was found (or the
    /// declared kind was `Unknown`).
    ///
    /// Continuity reports treat declared and inferred contracts identically
    /// for routing purposes; this flag exists so operator surfaces can
    /// disambiguate "configuration gap" (no declared intent) from "data
    /// gap" (declared continuous, observed gaps).
    #[serde(default)]
    pub is_declared: bool,
    pub replayability: Replayability,
    /// Adjacencies between material chunks for this source family.
    #[serde(default)]
    pub seams: Vec<TemporalSeam>,
    /// Measured absences in coverage.
    #[serde(default)]
    pub gaps: Vec<CoverageGap>,
    /// Earliest known coverage point (start of earliest material).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earliest_ts: Option<Timestamp>,
    /// Latest known coverage point (end of latest material).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_ts: Option<Timestamp>,
    /// Number of staged source materials backing this family.
    pub material_count: i64,
    /// Number of events derived from those materials.
    pub event_count: i64,
}

// ──────────────────────────────────────────────────────────────────────────
// RPC envelope types
// ──────────────────────────────────────────────────────────────────────────

/// Request: `sources.continuity.list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SourcesContinuityListRequest {
    /// Restrict to material whose `staged_at` is at or after this timestamp.
    /// `None` means no lower bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<Timestamp>,
}

/// Response: `sources.continuity.list`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesContinuityListResponse {
    pub reports: Vec<SourceContinuityReport>,
}

/// Request: `sources.continuity.get`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesContinuityGetRequest {
    pub source_family: SourceFamily,
}

/// Response: `sources.continuity.get`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesContinuityGetResponse {
    pub report: Option<SourceContinuityReport>,
}

/// Request: `sources.continuity.explain_gap`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesExplainGapRequest {
    pub source_family: SourceFamily,
    /// A point in time inside the suspected gap. The handler resolves the
    /// surrounding window from material/event observations.
    pub at: Timestamp,
}

/// Response: `sources.continuity.explain_gap`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourcesExplainGapResponse {
    pub source_family: SourceFamily,
    pub at: Timestamp,
    /// Resolved gap, if `at` falls inside one. `None` means coverage was
    /// present at `at` and there is nothing to explain.
    pub gap: Option<CoverageGap>,
    /// Long-form explanation suitable for CLI / UI display.
    pub explanation: String,
}

#[cfg(test)]
#[path = "continuity_test.rs"]
mod tests;
