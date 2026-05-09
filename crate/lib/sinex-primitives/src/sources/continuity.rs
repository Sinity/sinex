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
/// caveats (e.g. "anchor_byte unstable across re-exports").
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
}

// ──────────────────────────────────────────────────────────────────────────
// Aggregate report
// ──────────────────────────────────────────────────────────────────────────

/// Operator-facing continuity report for a `SourceFamily`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityReport {
    pub source_family: SourceFamily,
    pub coverage_contract: CoverageContract,
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
mod tests {
    use super::*;

    #[test]
    fn replayability_green_count() {
        let r = Replayability {
            raw_bytes_preserved: true,
            timing_quality: true,
            anchor_stability: false,
            parser_determinism: true,
            privacy_safe_replay: false,
            weak_points: vec!["anchor moves on re-export".into()],
        };
        assert_eq!(r.green_count(), 3);
    }

    #[test]
    fn report_serializes_with_seam_kind_snake_case() {
        let report = SourceContinuityReport {
            source_family: SourceFamily::from_static("terminal"),
            coverage_contract: CoverageContract::Continuous,
            replayability: Replayability {
                raw_bytes_preserved: true,
                timing_quality: true,
                anchor_stability: true,
                parser_determinism: true,
                privacy_safe_replay: true,
                weak_points: Vec::new(),
            },
            seams: vec![TemporalSeam {
                kind: SeamKind::ExpectedContinuation,
                before_ts: None,
                after_ts: None,
                evidence: serde_json::Value::Null,
            }],
            gaps: Vec::new(),
            earliest_ts: None,
            latest_ts: None,
            material_count: 0,
            event_count: 0,
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(
            json["seams"][0]["kind"],
            serde_json::Value::String("expected_continuation".into())
        );
        assert_eq!(
            json["coverage_contract"],
            serde_json::Value::String("continuous".into())
        );
    }
}
