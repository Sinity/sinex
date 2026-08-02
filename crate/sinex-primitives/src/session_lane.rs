//! Session-boundary lane output and shadow-diff primitives (sinex-0vx.7).
//!
//! Second [`LaneOutputKind`] implementation after
//! [`crate::semantic::EntityRelationLaneOutputs`] (sinex-0vx.6) — the proof
//! that the generic `derivation.epochs`/`derivation.lanes`/
//! `derivation.lane_outputs`/`derivation.lane_diffs` control plane
//! generalizes beyond entity/relation. `SessionBoundaryDiffCounts`/
//! `SessionBoundaryDiffExample` are per-kind detail, same as
//! `EntityRelationDiffCounts`/`EntityRelationDiffExample` — `diff` below
//! stays entirely local to this module; only the cross-kind
//! [`LaneDiffSummary`] generalizes.
//!
//! [`compute_session_boundaries`] is a batch-mode replica of the SAME
//! gap-closure policy `crate::automata::session::SessionDetector` uses in
//! `sinexd` (accumulate windows until a `Gap`-closed window ends the
//! session), not a reimplementation of a different algorithm: the streaming
//! `Windowed` automaton is not itself diff-comparable (it never stops), so a
//! shadow lane over a finite `event_set` scope needs a pure function over an
//! already-materialized, ordered window set instead. The trailing group (no
//! `Gap` close observed before the scope ends) is always flushed as a
//! session too — the batch analog of the automaton's clock-driven
//! `flush_due` backstop, appropriate here because a shadow lane's scope is
//! bounded by definition.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::activity::{ActivitySourceKind, primary_activity_source};
use crate::derivation::{LaneDiffSummary, LaneOutputKind};
use crate::events::payloads::{ActivityWindowCloseReason, ActivityWindowSummaryPayload};
use crate::temporal::Timestamp;

/// Candidate/baseline session-boundary output inside a lane. Mirrors the
/// canonical `activity.session.boundary` shape (`ActivitySessionBoundaryPayload`)
/// minus the fields that don't participate in shadow-diff comparison
/// (`source_count`/`sources`/`activity_sources` are summarizable from
/// `event_count`/`window_count`/`primary_source` for diff purposes; the full
/// canonical payload is still what gets promoted, not this lane-local view).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SessionBoundaryOutput {
    /// Lane-local stable session key. Uses the SAME
    /// `activity-session:{first_window_id}` occurrence-key format the real
    /// `SessionDetector` automaton mints (sinex-ecy), so a shadow-lane
    /// candidate session and its canonical counterpart compare as the same
    /// key when they cover the same occurrence.
    pub session_key: String,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration_secs: u64,
    pub event_count: u64,
    pub window_count: u64,
    pub primary_source: ActivitySourceKind,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub metadata: JsonValue,
}

/// Inputs to a session-boundary lane comparison.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SessionLaneOutputs {
    #[serde(default)]
    pub boundaries: Vec<SessionBoundaryOutput>,
}

/// Aggregate churn counts for a session-boundary lane diff.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SessionBoundaryDiffCounts {
    pub session_new: usize,
    pub session_missing: usize,
    pub duration_changed: usize,
    pub event_count_changed: usize,
    pub window_count_changed: usize,
}

/// One representative session-boundary diff example.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionBoundaryDiffExample {
    SessionNew {
        session_key: String,
    },
    SessionMissing {
        session_key: String,
    },
    DurationChanged {
        session_key: String,
        baseline_secs: u64,
        candidate_secs: u64,
    },
    EventCountChanged {
        session_key: String,
        baseline: u64,
        candidate: u64,
    },
    WindowCountChanged {
        session_key: String,
        baseline: u64,
        candidate: u64,
    },
}

/// Session boundary is the second [`LaneOutputKind`] implementation
/// (sinex-0vx.7), proving `LaneDiffReport`/`LaneOutputKind` generalize
/// beyond `EntityRelationLaneOutputs` (sinex-0vx.6).
impl LaneOutputKind for SessionLaneOutputs {
    type Counts = SessionBoundaryDiffCounts;
    type Example = SessionBoundaryDiffExample;

    fn output_kind() -> &'static str {
        "session_boundary"
    }

    fn diff(
        baseline: &Self,
        candidate: &Self,
        max_examples: usize,
    ) -> (LaneDiffSummary, Self::Counts, Vec<Self::Example>) {
        let baseline_map = boundary_map(&baseline.boundaries);
        let candidate_map = boundary_map(&candidate.boundaries);

        let mut counts = SessionBoundaryDiffCounts::default();
        let mut examples = Vec::new();
        let mut unchanged = 0usize;

        for key in candidate_map
            .keys()
            .filter(|key| !baseline_map.contains_key(*key))
        {
            counts.session_new += 1;
            push_example(
                &mut examples,
                max_examples,
                SessionBoundaryDiffExample::SessionNew {
                    session_key: (*key).clone(),
                },
            );
        }

        for key in baseline_map
            .keys()
            .filter(|key| !candidate_map.contains_key(*key))
        {
            counts.session_missing += 1;
            push_example(
                &mut examples,
                max_examples,
                SessionBoundaryDiffExample::SessionMissing {
                    session_key: (*key).clone(),
                },
            );
        }

        for (key, baseline_boundary) in &baseline_map {
            let Some(candidate_boundary) = candidate_map.get(key) else {
                continue;
            };
            let mut changed = false;
            if baseline_boundary.duration_secs != candidate_boundary.duration_secs {
                changed = true;
                counts.duration_changed += 1;
                push_example(
                    &mut examples,
                    max_examples,
                    SessionBoundaryDiffExample::DurationChanged {
                        session_key: key.clone(),
                        baseline_secs: baseline_boundary.duration_secs,
                        candidate_secs: candidate_boundary.duration_secs,
                    },
                );
            }
            if baseline_boundary.event_count != candidate_boundary.event_count {
                changed = true;
                counts.event_count_changed += 1;
                push_example(
                    &mut examples,
                    max_examples,
                    SessionBoundaryDiffExample::EventCountChanged {
                        session_key: key.clone(),
                        baseline: baseline_boundary.event_count,
                        candidate: candidate_boundary.event_count,
                    },
                );
            }
            if baseline_boundary.window_count != candidate_boundary.window_count {
                changed = true;
                counts.window_count_changed += 1;
                push_example(
                    &mut examples,
                    max_examples,
                    SessionBoundaryDiffExample::WindowCountChanged {
                        session_key: key.clone(),
                        baseline: baseline_boundary.window_count,
                        candidate: candidate_boundary.window_count,
                    },
                );
            }
            if !changed {
                unchanged += 1;
            }
        }

        let summary = LaneDiffSummary {
            added: counts.session_new,
            removed: counts.session_missing,
            changed: counts.duration_changed + counts.event_count_changed + counts.window_count_changed,
            unchanged,
        };

        (summary, counts, examples)
    }
}

fn boundary_map(boundaries: &[SessionBoundaryOutput]) -> BTreeMap<String, &SessionBoundaryOutput> {
    boundaries
        .iter()
        .map(|boundary| (boundary.session_key.clone(), boundary))
        .collect()
}

fn push_example(
    examples: &mut Vec<SessionBoundaryDiffExample>,
    max_examples: usize,
    example: SessionBoundaryDiffExample,
) {
    if examples.len() < max_examples {
        examples.push(example);
    }
}

/// Occurrence-stable session key, matching
/// `crate::automata::session::session_occurrence_key` in `sinexd` exactly
/// (duplicated as a one-line format string rather than shared across the
/// primitives/daemon boundary — see module doc).
fn session_occurrence_key(first_window_id: &str) -> String {
    format!("activity-session:{first_window_id}")
}

/// Batch-mode replica of `SessionDetector::accumulate`/`emit`'s gap-closure
/// grouping policy (see module doc): groups an ordered, finite window set
/// into sessions, starting a new session after each `Gap`-closed window and
/// always flushing a trailing incomplete group at the end of the input.
#[must_use]
pub fn compute_session_boundaries(
    windows: impl IntoIterator<Item = ActivityWindowSummaryPayload>,
) -> Vec<SessionBoundaryOutput> {
    let mut windows: Vec<_> = windows.into_iter().collect();
    windows.sort_by_key(|window| window.window_start);

    let mut boundaries = Vec::new();
    let mut accumulator: Option<Accumulator> = None;

    for window in windows {
        let gap_closed = matches!(window.close_reason, ActivityWindowCloseReason::Gap);
        let entry = accumulator.get_or_insert_with(|| {
            Accumulator::new(window.window_start, window.window_id.clone())
        });
        entry.last_end = window.window_end;
        entry.event_count += window.event_count;
        entry.window_count += 1;
        entry.sources.extend(window.sources);
        for (source, count) in window.activity_source_counts {
            *entry.activity_source_counts.entry(source).or_insert(0) += count;
        }
        if gap_closed && let Some(finished) = accumulator.take() {
            boundaries.push(finished.finish());
        }
    }
    if let Some(remaining) = accumulator {
        boundaries.push(remaining.finish());
    }

    boundaries
}

struct Accumulator {
    start: Timestamp,
    last_end: Timestamp,
    first_window_id: String,
    event_count: u64,
    window_count: u64,
    sources: BTreeSet<String>,
    activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
}

impl Accumulator {
    fn new(start: Timestamp, first_window_id: String) -> Self {
        Self {
            start,
            last_end: start,
            first_window_id,
            event_count: 0,
            window_count: 0,
            sources: BTreeSet::new(),
            activity_source_counts: BTreeMap::new(),
        }
    }

    fn finish(self) -> SessionBoundaryOutput {
        let duration_secs = (self.last_end - self.start).whole_seconds().max(0) as u64;
        let primary_source = primary_activity_source(&self.activity_source_counts);
        SessionBoundaryOutput {
            session_key: session_occurrence_key(&self.first_window_id),
            start_time: self.start,
            end_time: self.last_end,
            duration_secs,
            event_count: self.event_count,
            window_count: self.window_count,
            primary_source,
            metadata: serde_json::json!({
                "sources": self.sources.into_iter().collect::<Vec<_>>(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "session_lane_test.rs"]
mod tests;
