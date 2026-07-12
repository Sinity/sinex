//! Event relation / evidence-window query layer (v0).
//!
//! This module answers questions such as "commands I ran while discussing X"
//! without inventing a moment ontology. It is a small, explicit relation layer
//! over events plus an evidence-window DTO that records *why* each piece of
//! evidence was included.
//!
//! Design source: #1729 / #1789 (`EventRelationExpr`, `ObservedRange`,
//! `EvidenceWindow`, `ExpansionTrace`). Deliberate non-goals for v0:
//!
//! - **No arbitrary graph query engine.** [`EventRelationExpr`] is a flat enum of
//!   a handful of relation forms, not a composable boolean tree. Unsupported
//!   forms are unrepresentable rather than silently mis-evaluated.
//! - **No semantic / model inference.** Contradictions are *explicitly supplied*
//!   by the caller ([`EvidenceWindow::with_contradiction`]); the evaluator never
//!   infers a contradiction. The trace makes no causality claim.
//! - **No persistence.** An evidence window is an ephemeral projection. It is
//!   persisted only when the operator saves a context pack/artifact or uses it as
//!   evidence for an explicit proposal/judgment — never as an event side effect of
//!   running a query.
//!
//! Seed *selection* (the `EventQuery` filter from the design doc) is the
//! gateway's responsibility: callers evaluate a query, then hand the resulting
//! seed and candidate events to [`EventRelationExpr::evaluate`]. That keeps this
//! layer a pure, fixture-testable function with no DB/FTS coupling.

use crate::JsonValue;
use crate::domain::TemporalSourceType;
use crate::events::{Event, Timestamp};
use crate::views::{CaveatView, SinexObjectKind, SinexObjectRef, ViewEnvelope};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use time::Duration;

/// The basis on which an [`ObservedRange`] was derived.
///
/// This is the coarse, overlap-oriented projection of the ingest-time temporal
/// ladder ([`TemporalSourceType`]); it answers "what kind of evidence produced
/// this time?" rather than the precise ingest rung.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    /// Time came from the data itself (realtime capture or intrinsic content).
    SourceIntrinsic,
    /// Time is anchored to source material but not otherwise resolved.
    MaterialAnchor,
    /// Time is the moment a slice was staged for ingestion.
    StagingTime,
    /// Time describes a derived interval (windowed/automaton output).
    DerivedInterval,
    /// Time was inferred from filesystem or user-supplied metadata.
    Inferred,
    /// No usable time; the evidence is anchored atemporally.
    AtemporalAnchor,
}

/// How trustworthy an [`ObservedRange`]'s endpoints are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimeQuality {
    /// Endpoints are exact to the source's resolution.
    Exact,
    /// Endpoints bound the true time but may be off within the bound.
    Bounded,
    /// Endpoints are coarse (e.g. day-granularity staging time).
    Coarse,
    /// Endpoints were inferred from weak metadata.
    Inferred,
    /// No usable time information.
    Unknown,
}

/// A queryable observed time, normalized to a (possibly open) interval.
///
/// Not all evidence is a point timestamp. `core.events.ts_orig` remains the
/// authoritative occurrence time, but relation/overlap semantics need an interval
/// plus a quality caveat, which is what this type carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ObservedRange {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<Timestamp>,
    pub basis: TimeBasis,
    pub quality: TimeQuality,
}

impl ObservedRange {
    /// A zero-width range at a single instant.
    #[must_use]
    pub fn point(at: Timestamp, basis: TimeBasis, quality: TimeQuality) -> Self {
        Self {
            start: Some(at),
            end: Some(at),
            basis,
            quality,
        }
    }

    /// A range with no usable time information.
    #[must_use]
    pub fn unknown(basis: TimeBasis) -> Self {
        Self {
            start: None,
            end: None,
            basis,
            quality: TimeQuality::Unknown,
        }
    }

    /// Derive the observed range of an event from its `ts_orig`, `ts_quality`,
    /// and provenance. Material events with no resolved time anchor to
    /// [`TimeBasis::MaterialAnchor`]; derived events to
    /// [`TimeBasis::DerivedInterval`].
    #[must_use]
    pub fn from_event<T>(event: &Event<T>) -> Self {
        let derived = event.is_synthesized_event();
        let Some(ts) = event.ts_orig else {
            return Self::unknown(if derived {
                TimeBasis::DerivedInterval
            } else {
                TimeBasis::MaterialAnchor
            });
        };
        let (basis, quality) = match event.ts_quality {
            Some(TemporalSourceType::RealtimeCapture | TemporalSourceType::IntrinsicContent) => {
                (TimeBasis::SourceIntrinsic, TimeQuality::Exact)
            }
            Some(TemporalSourceType::InferredMtime | TemporalSourceType::InferredCtime) => {
                (TimeBasis::Inferred, TimeQuality::Bounded)
            }
            Some(TemporalSourceType::InferredUser) => (TimeBasis::Inferred, TimeQuality::Coarse),
            Some(TemporalSourceType::StagedAt) => (TimeBasis::StagingTime, TimeQuality::Coarse),
            None if derived => (TimeBasis::DerivedInterval, TimeQuality::Inferred),
            None => (TimeBasis::Inferred, TimeQuality::Inferred),
        };
        Self::point(ts, basis, quality)
    }

    /// Whether this range has any usable time.
    #[must_use]
    pub fn is_timed(&self) -> bool {
        self.start.is_some() || self.end.is_some()
    }

    /// The effective lower bound, falling back to the upper bound for a point.
    #[must_use]
    fn lower(&self) -> Option<Timestamp> {
        self.start.or(self.end)
    }

    /// The effective upper bound, falling back to the lower bound for a point.
    #[must_use]
    fn upper(&self) -> Option<Timestamp> {
        self.end.or(self.start)
    }

    /// Whether two ranges overlap. Open-ended sides extend to infinity. Ranges
    /// with no time at all never overlap.
    #[must_use]
    pub fn overlaps(&self, other: &ObservedRange) -> bool {
        if !self.is_timed() || !other.is_timed() {
            return false;
        }
        let after_other_end = match (self.lower(), other.upper()) {
            (Some(a_start), Some(b_end)) => a_start > b_end,
            _ => false,
        };
        let before_other_start = match (self.upper(), other.lower()) {
            (Some(a_end), Some(b_start)) => a_end < b_start,
            _ => false,
        };
        !(after_other_end || before_other_start)
    }

    /// The smallest gap between two ranges (zero if they overlap). `None` when
    /// either range has no time.
    #[must_use]
    pub fn gap_to(&self, other: &ObservedRange) -> Option<Duration> {
        if self.overlaps(other) {
            return Some(Duration::ZERO);
        }
        match (self.lower(), self.upper(), other.lower(), other.upper()) {
            (Some(a_lo), Some(a_hi), Some(b_lo), Some(b_hi)) => {
                if a_hi < b_lo {
                    Some(b_lo - a_hi)
                } else if b_hi < a_lo {
                    Some(a_lo - b_hi)
                } else {
                    Some(Duration::ZERO)
                }
            }
            _ => None,
        }
    }

    /// The union of two ranges, taking the looser quality of the two.
    #[must_use]
    pub fn union(&self, other: &ObservedRange) -> ObservedRange {
        let start = min_opt(self.start, other.start);
        let end = max_opt(self.end, other.end);
        ObservedRange {
            start,
            end,
            basis: self.basis,
            quality: looser_quality(self.quality, other.quality),
        }
    }
}

fn min_opt(a: Option<Timestamp>, b: Option<Timestamp>) -> Option<Timestamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (some, None) | (None, some) => some,
    }
}

fn max_opt(a: Option<Timestamp>, b: Option<Timestamp>) -> Option<Timestamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (some, None) | (None, some) => some,
    }
}

fn looser_quality(a: TimeQuality, b: TimeQuality) -> TimeQuality {
    // Higher ordinal = looser. Unknown is loosest.
    fn rank(q: TimeQuality) -> u8 {
        match q {
            TimeQuality::Exact => 0,
            TimeQuality::Bounded => 1,
            TimeQuality::Coarse => 2,
            TimeQuality::Inferred => 3,
            TimeQuality::Unknown => 4,
        }
    }
    if rank(a) >= rank(b) { a } else { b }
}

/// A field whose value two events must share for the `Same` relation.
///
/// `Payload(key)` reads a top-level string field from the event payload, which
/// is where source-specific identity fields (repo, object key, path) live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SameField {
    Source,
    ScopeKey,
    EquivalenceKey,
    Payload(String),
}

impl SameField {
    fn extract<T: Serialize>(&self, event: &Event<T>) -> Option<String> {
        match self {
            SameField::Source => Some(event.source.to_string()),
            SameField::ScopeKey => event.scope_key.clone(),
            SameField::EquivalenceKey => event.equivalence_key.clone(),
            SameField::Payload(key) => match serde_json::to_value(&event.payload).ok()? {
                JsonValue::Object(map) => match map.get(key) {
                    Some(JsonValue::String(s)) => Some(s.clone()),
                    _ => None,
                },
                _ => None,
            },
        }
    }
}

/// A single v0 relation between a seed set and candidate events.
///
/// Flat by design: there is no `And`/`Or` composition in v0 (that would be the
/// start of an arbitrary query engine). Each variant has a clear, pure
/// evaluation contract — see [`EventRelationExpr::evaluate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "relation")]
pub enum EventRelationExpr {
    /// Candidate's observed range lies within `within_secs` of some seed's range.
    Within { within_secs: i64 },
    /// Candidate's observed range overlaps some seed's range.
    Overlaps,
    /// Candidate occurs strictly before some seed, with gap ≤ `max_gap_secs`.
    Before { max_gap_secs: i64 },
    /// Candidate occurs strictly after some seed, with gap ≤ `max_gap_secs`.
    After { max_gap_secs: i64 },
    /// Candidate shares `field`'s value with some seed.
    Same { field: SameField },
    /// The candidates themselves form a time-ordered chain spanning ≤ `within_secs`.
    /// (Seeds are ignored for this relation.)
    Sequence { within_secs: i64 },
}

/// The role a piece of evidence plays relative to the seed claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceRole {
    Support,
    Contradiction,
}

/// A referenced piece of evidence with its role and the rule that included it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceRef {
    pub object: SinexObjectRef,
    pub role: EvidenceRole,
    pub observed_range: ObservedRange,
    /// Why this evidence was included (the relation rule, or operator rationale).
    pub rationale: String,
}

/// The kind of step recorded in an [`ExpansionTrace`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpansionStepKind {
    /// A seed hit matched the query.
    SeedMatched,
    /// The window policy expanded the interval.
    WindowExpanded,
    /// A relation rule included an event/material.
    RelationIncluded,
    /// A source-coverage / timing gap added a caveat.
    CoverageGapCaveat,
}

/// One recorded step in why evidence was assembled. The trace explains
/// inclusion; it makes **no causality claim**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExpansionStep {
    pub kind: ExpansionStepKind,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_ref: Option<SinexObjectRef>,
}

/// The ordered record of how an [`EvidenceWindow`] was assembled.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExpansionTrace {
    pub steps: Vec<ExpansionStep>,
}

impl ExpansionTrace {
    fn push(&mut self, kind: ExpansionStepKind, detail: impl Into<String>) {
        self.steps.push(ExpansionStep {
            kind,
            detail: detail.into(),
            object_ref: None,
        });
    }

    fn push_ref(
        &mut self,
        kind: ExpansionStepKind,
        detail: impl Into<String>,
        object_ref: SinexObjectRef,
    ) {
        self.steps.push(ExpansionStep {
            kind,
            detail: detail.into(),
            object_ref: Some(object_ref),
        });
    }
}

/// The result of evaluating an [`EventRelationExpr`]: seeds, supporting and
/// contradicting evidence, caveats, the union observed range, and the trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceWindow {
    pub seed_refs: Vec<SinexObjectRef>,
    pub support_refs: Vec<EvidenceRef>,
    pub contradiction_refs: Vec<EvidenceRef>,
    pub caveats: Vec<CaveatView>,
    pub observed_range: ObservedRange,
    pub expansion_trace: ExpansionTrace,
    pub generated_at: Timestamp,
    pub query: EventRelationExpr,
}

impl EvidenceWindow {
    /// Attach an explicit, operator-supplied contradiction. v0 never infers
    /// contradictions; this is the only way one enters a window.
    #[must_use]
    pub fn with_contradiction(
        mut self,
        object: SinexObjectRef,
        observed_range: ObservedRange,
        rationale: impl Into<String>,
    ) -> Self {
        let rationale = rationale.into();
        self.expansion_trace.push_ref(
            ExpansionStepKind::RelationIncluded,
            format!("operator-supplied contradiction: {rationale}"),
            object.clone(),
        );
        self.contradiction_refs.push(EvidenceRef {
            object,
            role: EvidenceRole::Contradiction,
            observed_range,
            rationale,
        });
        self
    }

    /// Attach an explicit caveat to the window.
    #[must_use]
    pub fn with_caveat(mut self, id: impl Into<String>, message: impl Into<String>) -> Self {
        self.caveats.push(CaveatView {
            id: id.into(),
            message: message.into(),
            ref_: None,
        });
        self
    }

    /// Attach an explicit caveat that points at the limited/suppressed object.
    #[must_use]
    pub fn with_caveat_ref(
        mut self,
        id: impl Into<String>,
        message: impl Into<String>,
        object_ref: SinexObjectRef,
    ) -> Self {
        let id = id.into();
        let message = message.into();
        self.expansion_trace.push_ref(
            ExpansionStepKind::CoverageGapCaveat,
            format!("{id}: {message}"),
            object_ref.clone(),
        );
        self.caveats.push(CaveatView {
            id,
            message,
            ref_: Some(object_ref),
        });
        self
    }

    /// Wrap the window in a [`ViewEnvelope`] for the read-only CLI/API surface,
    /// lifting the window's caveats onto the envelope.
    #[must_use]
    pub fn into_view(self, source_surface: impl Into<String>) -> ViewEnvelope<EvidenceWindow> {
        let mut envelope = ViewEnvelope::new(source_surface, self);
        envelope.caveats = envelope.payload.caveats.clone();
        envelope
    }
}

fn event_object_ref<T>(event: &Event<T>) -> SinexObjectRef {
    let id = event
        .id
        .map_or_else(|| "unpersisted".to_string(), |id| id.to_string());
    SinexObjectRef::new(SinexObjectKind::Event, id)
        .with_label(format!("{} · {}", event.source, event.event_type))
}

impl EventRelationExpr {
    /// Evaluate this relation against a seed set and a candidate pool, producing
    /// an [`EvidenceWindow`]. Pure function: no I/O, deterministic given inputs
    /// (except `generated_at`, stamped from the clock).
    ///
    /// A candidate is included as **support** when it satisfies the relation
    /// against *any* seed. Candidates whose timing is unknown for a temporal
    /// relation are skipped and recorded as a coverage caveat rather than
    /// silently included.
    #[must_use]
    pub fn evaluate<T: Serialize>(
        &self,
        seeds: &[Event<T>],
        candidates: &[Event<T>],
    ) -> EvidenceWindow {
        let mut trace = ExpansionTrace::default();
        let mut caveats: Vec<CaveatView> = Vec::new();
        let mut support: Vec<EvidenceRef> = Vec::new();

        let seed_ranges: Vec<ObservedRange> = seeds.iter().map(ObservedRange::from_event).collect();
        let seed_refs: Vec<SinexObjectRef> = seeds.iter().map(event_object_ref).collect();
        for (seed, range) in seeds.iter().zip(&seed_ranges) {
            trace.push_ref(
                ExpansionStepKind::SeedMatched,
                format!("seed matched query ({})", describe_range(range)),
                event_object_ref(seed),
            );
        }

        let mut union_range = seed_ranges
            .iter()
            .copied()
            .reduce(|acc, r| acc.union(&r))
            .unwrap_or_else(|| ObservedRange::unknown(TimeBasis::AtemporalAnchor));

        if let EventRelationExpr::Sequence { within_secs } = self {
            return self.evaluate_sequence(*within_secs, seeds, seed_refs, trace);
        }

        for candidate in candidates {
            let cand_range = ObservedRange::from_event(candidate);
            let cand_ref = event_object_ref(candidate);

            let needs_time = !matches!(self, EventRelationExpr::Same { .. });
            if needs_time && !cand_range.is_timed() {
                caveats.push(CaveatView {
                    id: "evidence.timing_unknown".to_string(),
                    message: format!(
                        "candidate {} has no usable time; excluded from the temporal relation",
                        cand_ref.id
                    ),
                    ref_: Some(cand_ref.clone()),
                });
                trace.push_ref(
                    ExpansionStepKind::CoverageGapCaveat,
                    "candidate skipped: no usable observed time",
                    cand_ref.clone(),
                );
                continue;
            }

            if let Some(rationale) = self.matches(candidate, &cand_range, seeds, &seed_ranges) {
                trace.push_ref(
                    ExpansionStepKind::RelationIncluded,
                    rationale.clone(),
                    cand_ref.clone(),
                );
                if cand_range.is_timed() {
                    union_range = union_range.union(&cand_range);
                    trace.push(
                        ExpansionStepKind::WindowExpanded,
                        format!("window expanded to {}", describe_range(&union_range)),
                    );
                }
                support.push(EvidenceRef {
                    object: cand_ref,
                    role: EvidenceRole::Support,
                    observed_range: cand_range,
                    rationale,
                });
            }
        }

        EvidenceWindow {
            seed_refs,
            support_refs: support,
            contradiction_refs: Vec::new(),
            caveats,
            observed_range: union_range,
            expansion_trace: trace,
            generated_at: Timestamp::now(),
            query: self.clone(),
        }
    }

    /// Returns `Some(rationale)` if `candidate` satisfies this relation against
    /// any seed. Only called for the per-candidate relations (not `Sequence`).
    fn matches<T: Serialize>(
        &self,
        candidate: &Event<T>,
        cand_range: &ObservedRange,
        seeds: &[Event<T>],
        seed_ranges: &[ObservedRange],
    ) -> Option<String> {
        match self {
            EventRelationExpr::Within { within_secs } => {
                let bound = Duration::seconds(*within_secs);
                seed_ranges.iter().find_map(|seed| {
                    cand_range
                        .gap_to(seed)
                        .filter(|gap| *gap <= bound)
                        .map(|gap| {
                            format!(
                                "within {within_secs}s of a seed (gap {}s)",
                                gap.whole_seconds()
                            )
                        })
                })
            }
            EventRelationExpr::Overlaps => seed_ranges
                .iter()
                .any(|seed| cand_range.overlaps(seed))
                .then(|| "overlaps a seed range".to_string()),
            EventRelationExpr::Before { max_gap_secs } => {
                let bound = Duration::seconds(*max_gap_secs);
                seed_ranges.iter().find_map(|seed| {
                    let (cand_hi, seed_lo) = (cand_range.upper()?, seed.lower()?);
                    (cand_hi < seed_lo && (seed_lo - cand_hi) <= bound).then(|| {
                        format!(
                            "before a seed (gap {}s ≤ {max_gap_secs}s)",
                            (seed_lo - cand_hi).whole_seconds()
                        )
                    })
                })
            }
            EventRelationExpr::After { max_gap_secs } => {
                let bound = Duration::seconds(*max_gap_secs);
                seed_ranges.iter().find_map(|seed| {
                    let (cand_lo, seed_hi) = (cand_range.lower()?, seed.upper()?);
                    (cand_lo > seed_hi && (cand_lo - seed_hi) <= bound).then(|| {
                        format!(
                            "after a seed (gap {}s ≤ {max_gap_secs}s)",
                            (cand_lo - seed_hi).whole_seconds()
                        )
                    })
                })
            }
            EventRelationExpr::Same { field } => {
                let cand_value = field.extract(candidate)?;
                seeds.iter().find_map(|seed| {
                    let seed_value = field.extract(seed)?;
                    (seed_value == cand_value)
                        .then(|| format!("shares {field:?} = {cand_value:?} with a seed"))
                })
            }
            EventRelationExpr::Sequence { .. } => None,
        }
    }

    /// `Sequence` evaluation: the candidates (here, the seeds) must be in
    /// non-decreasing observed order spanning ≤ `within_secs`. Support is the
    /// ordered chain; a caveat is recorded if the span is exceeded or any
    /// member lacks time.
    fn evaluate_sequence<T: Serialize>(
        &self,
        within_secs: i64,
        events: &[Event<T>],
        seed_refs: Vec<SinexObjectRef>,
        mut trace: ExpansionTrace,
    ) -> EvidenceWindow {
        let mut caveats: Vec<CaveatView> = Vec::new();
        let mut support: Vec<EvidenceRef> = Vec::new();
        let mut union_range = ObservedRange::unknown(TimeBasis::AtemporalAnchor);

        let mut last: Option<Timestamp> = None;
        let mut ordered = true;
        let mut first_ts: Option<Timestamp> = None;
        let mut final_ts: Option<Timestamp> = None;

        for event in events {
            let range = ObservedRange::from_event(event);
            let cand_ref = event_object_ref(event);
            let Some(ts) = range.lower() else {
                caveats.push(CaveatView {
                    id: "sequence.timing_unknown".to_string(),
                    message: format!("sequence member {} has no usable time", cand_ref.id),
                    ref_: Some(cand_ref.clone()),
                });
                trace.push_ref(
                    ExpansionStepKind::CoverageGapCaveat,
                    "sequence member skipped: no usable observed time",
                    cand_ref,
                );
                continue;
            };
            if let Some(prev) = last
                && ts < prev
            {
                ordered = false;
            }
            last = Some(ts);
            first_ts.get_or_insert(ts);
            final_ts = Some(ts);
            union_range = union_range.union(&range);
            trace.push_ref(
                ExpansionStepKind::RelationIncluded,
                "sequence member in order",
                cand_ref.clone(),
            );
            support.push(EvidenceRef {
                object: cand_ref,
                role: EvidenceRole::Support,
                observed_range: range,
                rationale: "ordered sequence member".to_string(),
            });
        }

        if !ordered {
            caveats.push(CaveatView {
                id: "sequence.out_of_order".to_string(),
                message: "sequence members are not in non-decreasing observed order".to_string(),
                ref_: None,
            });
        }
        if let (Some(start), Some(end)) = (first_ts, final_ts) {
            let span = end - start;
            if span > Duration::seconds(within_secs) {
                caveats.push(CaveatView {
                    id: "sequence.span_exceeded".to_string(),
                    message: format!(
                        "sequence spans {}s, exceeding the {within_secs}s bound",
                        span.whole_seconds()
                    ),
                    ref_: None,
                });
            }
        }

        EvidenceWindow {
            seed_refs,
            support_refs: support,
            contradiction_refs: Vec::new(),
            caveats,
            observed_range: union_range,
            expansion_trace: trace,
            generated_at: Timestamp::now(),
            query: self.clone(),
        }
    }
}

fn describe_range(range: &ObservedRange) -> String {
    match (range.start, range.end) {
        (Some(s), Some(e)) if s == e => format!("@{}", s.format_rfc3339()),
        (Some(s), Some(e)) => format!("{}..{}", s.format_rfc3339(), e.format_rfc3339()),
        (Some(s), None) => format!("{}..", s.format_rfc3339()),
        (None, Some(e)) => format!("..{}", e.format_rfc3339()),
        (None, None) => "untimed".to_string(),
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fixtures {
    //! Deterministic relation fixtures for native Sinex replacements of
    //! external analysis products.

    use super::*;
    use crate::domain::{EventSource, EventType, HostName};
    use crate::events::SourceMaterial;
    use crate::events::builder::Provenance;
    use crate::ids::Id;
    use serde_json::json;

    /// Native Sinex fixture for machine-session causal-footprint behavior: an
    /// agent work interval, the work/build evidence near it, and a
    /// privacy-limited material ref that explains missing evidence.
    #[derive(Debug, Clone)]
    pub struct CausalFootprintFixture {
        pub source_behavior: &'static str,
        pub native_owner_surface: &'static str,
        pub query: EventRelationExpr,
        pub seed_events: Vec<Event<JsonValue>>,
        pub candidate_events: Vec<Event<JsonValue>>,
        pub suppressed_refs: Vec<SinexObjectRef>,
    }

    impl CausalFootprintFixture {
        /// Evaluate the fixture as a read-only native EvidenceWindow view.
        #[must_use]
        pub fn evidence_window(&self) -> EvidenceWindow {
            let mut window = self
                .query
                .evaluate(&self.seed_events, &self.candidate_events);
            for suppressed in &self.suppressed_refs {
                window = window.with_caveat_ref(
                    "privacy.evidence_suppressed",
                    "source coverage/redaction limits this causal-footprint window",
                    suppressed.clone(),
                );
            }
            window
        }

        /// Render through the native owner surface; the result is a view, not a
        /// canonical event.
        #[must_use]
        pub fn view(&self) -> ViewEnvelope<EvidenceWindow> {
            self.evidence_window().into_view(self.native_owner_surface)
        }
    }

    /// Causal-footprint fixture for an agent session seed, nearby xtask/build
    /// work as supporting evidence, unrelated work outside the window, and a
    /// suppressed source-material ref represented as a caveat.
    #[must_use]
    pub fn machine_session_causal_footprint() -> CausalFootprintFixture {
        let seed = material_event(
            "polylogue.agent-session",
            "agent.session.active",
            Some(at(0)),
            json!({
                "session_id": "session-42",
                "project": "sinex",
                "scope": "sinnix-agent-codex-42"
            }),
        );
        let xtask = material_event(
            "dev.xtask",
            "xtask.invoked",
            Some(at(45)),
            json!({
                "command": "xtask test -p sinex-primitives --lib",
                "scope": "sinnix-build-xtask-42"
            }),
        );
        let rust_build = material_event(
            "machine.scope",
            "build.scope.completed",
            Some(at(90)),
            json!({
                "comm": "rustc",
                "io_mb": 6144,
                "attributed_agent_scope": "sinnix-agent-codex-42"
            }),
        );
        let co_present_agent = material_event(
            "polylogue.agent-session",
            "agent.session.active",
            Some(at(120)),
            json!({
                "session_id": "session-99",
                "project": "sinex",
                "scope": "sinnix-agent-codex-99"
            }),
        );
        let unrelated = material_event(
            "machine.scope",
            "build.scope.completed",
            Some(at(7200)),
            json!({
                "comm": "nix",
                "io_mb": 128,
                "attributed_agent_scope": null
            }),
        );

        CausalFootprintFixture {
            source_behavior: "machine session causal footprint",
            native_owner_surface: "sinex.relations.evidence_window",
            query: EventRelationExpr::Within { within_secs: 300 },
            seed_events: vec![seed],
            candidate_events: vec![xtask, rust_build, co_present_agent, unrelated],
            suppressed_refs: vec![
                SinexObjectRef::new(
                    SinexObjectKind::SourceMaterial,
                    "journald.redacted/session-42",
                )
                .with_label("redacted journald slice"),
            ],
        }
    }

    fn material_event(
        source: &'static str,
        event_type: &'static str,
        ts: Option<Timestamp>,
        payload: JsonValue,
    ) -> Event<JsonValue> {
        Event {
            id: Some(Id::<Event<JsonValue>>::new()),
            source: EventSource::from_static(source),
            event_type: EventType::from_static(event_type),
            payload,
            ts_orig: ts,
            ts_quality: ts.map(|_| TemporalSourceType::RealtimeCapture),
            host: HostName::from_static("fixture-host"),
            module_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
            anchor_payload_hash: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
            product_class: None,
            claim_support: None,
            derivation_declaration_id: None,
            derivation_epoch_id: None,
            derivation_lane_id: None,
            adjudication_event_id: None,
        }
    }

    fn at(secs: i64) -> Timestamp {
        match Timestamp::from_unix_timestamp(1_700_000_000 + secs) {
            Some(timestamp) => timestamp,
            None => panic!("fixture timestamp must be in range"),
        }
    }
}

#[cfg(test)]
#[path = "relations_test.rs"]
mod tests;
