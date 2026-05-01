#![doc = include_str!("../docs/README.md")]

//! Session detector -- [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** -- accumulates bounded
//! `activity.window.summary` events into completed activity sessions.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedAggregationMeta, DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, WindowedNode};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::events::{
    EventPayload,
    payloads::{
        ActivitySessionBoundaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
    },
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;
use std::collections::{BTreeMap, BTreeSet};

/// Persistent window state tracking the current activity session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionState {
    /// Start time of the current session.
    pub session_start: Option<Timestamp>,

    /// End time of the most recent window included in the session.
    pub last_window_end: Option<Timestamp>,

    /// Number of raw activity events represented by the session.
    pub event_count: u64,

    /// Number of bounded activity windows represented by the session.
    pub window_count: u64,

    /// Unique raw event sources observed in the current session.
    pub sources: BTreeSet<String>,

    /// Logical activity sources observed in the current session.
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,

    /// `UUIDv7` IDs of contributing `activity.window.summary` events.
    pub window_event_ids: Vec<Uuid>,

    /// Session counter for generating deterministic session IDs.
    pub session_counter: u64,

    /// Whether the current session has received a gap-closed final window.
    #[serde(default)]
    pub session_complete: bool,
}

impl SessionState {
    /// Reset state for a new session, preserving the counter.
    fn reset_session(&mut self) {
        self.session_start = None;
        self.last_window_end = None;
        self.event_count = 0;
        self.window_count = 0;
        self.sources.clear();
        self.activity_source_counts.clear();
        self.window_event_ids.clear();
        self.session_complete = false;
    }
}

#[derive(Default)]
pub struct SessionDetector;

impl WindowedNode for SessionDetector {
    type State = SessionState;
    type Input = ActivityWindowSummaryPayload;
    type Output = ActivitySessionBoundaryPayload;

    fn name(&self) -> &'static str {
        "session-detector"
    }

    fn input_event_type(&self) -> &'static str {
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        ActivitySessionBoundaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        ActivitySessionBoundaryPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<(), NodeLogicError> {
        if state.session_start.is_none() {
            state.session_start = Some(input.window_start);
        }

        state.last_window_end = Some(input.window_end);
        state.event_count += input.event_count;
        state.window_count += 1;
        state.sources.extend(input.sources);
        for (source, count) in input.activity_source_counts {
            *state.activity_source_counts.entry(source).or_insert(0) += count;
        }
        state.window_event_ids.push(context.trigger_uuid());
        state.session_complete = matches!(input.close_reason, ActivityWindowCloseReason::Gap);

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.session_complete && state.window_count > 0
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let Some(start_time) = state.session_start else {
            return Ok(None);
        };

        let end_time = state.last_window_end.unwrap_or(start_time);
        let duration_secs = (end_time - start_time).whole_seconds().max(0) as u64;

        state.session_counter += 1;
        let session_id = format!("session-{}", state.session_counter);

        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let activity_sources: Vec<ActivitySourceKind> =
            state.activity_source_counts.keys().copied().collect();
        let primary_source = primary_activity_source(&state.activity_source_counts);
        let source_event_ids = std::mem::take(&mut state.window_event_ids);
        let event_count = state.event_count;
        let window_count = state.window_count;

        let payload = ActivitySessionBoundaryPayload {
            session_id: session_id.clone(),
            start_time,
            end_time,
            duration_secs,
            event_count,
            window_count,
            source_count: state.sources.len() as u64,
            sources,
            activity_sources,
            activity_source_counts: state.activity_source_counts.clone(),
            primary_source,
        };

        let output = DerivedOutput::windowed(payload, end_time, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("2.0.0")
            .with_equivalence_key(session_id)
            .with_aggregation(DerivedAggregationMeta::new(
                "activity.session",
                1,
                event_count,
            ));

        state.reset_session();
        Ok(Some(output))
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type SessionDetectorNode = WindowedNodeAdapter<SessionDetector>;

// --- Source-unit descriptor (issue #690 / #734) ---

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape,
    SourceUnitDescriptor,
};

// Session detector consumes activity-window summaries and emits session
// boundary events when the inactivity gap closes the current window.
register_source_unit! {
    SourceUnitDescriptor {
        id: "session-detector",
        namespace: "derived",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("derived.session-detector", "activity.session.boundary"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, parent_event_ids)",
        ),
    }
}
