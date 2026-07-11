//! Hourly summarizer -- [`Windowed`] implementation.
//!
//! Model classification: **Windowed** -- accumulates
//! `activity.window.summary` inputs into completed UTC-hour summaries.

use crate::runtime::automaton::{
    AutomatonContext, DerivedAggregationMeta, DerivedOutput, WindowedAdapter,
};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::events::{
    EventPayload,
    payloads::{ActivityHourlySummaryPayload, ActivityWindowSummaryPayload},
};
use sinex_primitives::temporal::Timestamp;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

fn floor_to_hour(timestamp: Timestamp) -> Timestamp {
    // sinex-2ged: bucket on the operator-local civil hour (DST-aware), not UTC.
    super::civil::floor_to_civil_hour(timestamp)
}

fn hour_end(hour_start: Timestamp) -> Timestamp {
    super::civil::civil_hour_end(hour_start)
}

fn sorted_top_sources(counts: &BTreeMap<String, u64>) -> Vec<String> {
    let mut ranked: Vec<(&String, &u64)> = counts.iter().collect();
    ranked.sort_by(|(left_name, left_count), (right_name, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_name.cmp(right_name))
    });
    ranked.into_iter().map(|(name, _)| name.clone()).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWindowSummary {
    bucket_start: Timestamp,
    payload: ActivityWindowSummaryPayload,
    event_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HourlySummaryState {
    pub hour_start: Option<Timestamp>,
    pub duration_secs: u64,
    pub window_count: u64,
    pub event_count: u64,
    pub sources: BTreeSet<String>,
    pub source_window_counts: BTreeMap<String, u64>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub focus_time_secs_by_source: BTreeMap<ActivitySourceKind, u64>,
    pub source_event_ids: Vec<Uuid>,
    pub summary_counter: u32,
    pending_window: Option<PendingWindowSummary>,
    last_window_end: Option<Timestamp>,
}

impl HourlySummaryState {
    fn reset_hour(&mut self) {
        self.hour_start = None;
        self.duration_secs = 0;
        self.window_count = 0;
        self.event_count = 0;
        self.sources.clear();
        self.source_window_counts.clear();
        self.activity_source_counts.clear();
        self.focus_time_secs_by_source.clear();
        self.source_event_ids.clear();
        self.pending_window = None;
        self.last_window_end = None;
    }

    fn accumulate_window(
        &mut self,
        bucket_start: Timestamp,
        payload: ActivityWindowSummaryPayload,
        event_id: Uuid,
    ) {
        let window_end = payload.window_end;
        if self.hour_start.is_none() {
            self.hour_start = Some(bucket_start);
        }

        self.duration_secs += payload.duration_secs;
        self.window_count += 1;
        self.event_count += payload.event_count;
        self.sources.extend(payload.sources.iter().cloned());
        for source in payload.sources {
            *self.source_window_counts.entry(source).or_insert(0) += 1;
        }
        for (source, count) in payload.activity_source_counts {
            *self.activity_source_counts.entry(source).or_insert(0) += count;
        }
        *self
            .focus_time_secs_by_source
            .entry(payload.primary_source)
            .or_insert(0) += payload.duration_secs;
        self.last_window_end = Some(window_end);
        self.source_event_ids.push(event_id);
        self.pending_window = None;
    }
}

#[derive(Default)]
pub struct HourlySummarizer;

impl Windowed for HourlySummarizer {
    type State = HourlySummaryState;
    type Input = ActivityWindowSummaryPayload;
    type Output = ActivityHourlySummaryPayload;

    fn name(&self) -> &'static str {
        "hourly-summarizer"
    }

    fn input_event_type(&self) -> &'static str {
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        ActivityHourlySummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        ActivityHourlySummaryPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }
    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
        let bucket_start = floor_to_hour(input.window_end);
        let event_id = context.trigger_uuid();

        if let Some(current_bucket) = state.hour_start
            && state.window_count > 0
            && current_bucket != bucket_start
        {
            state.pending_window = Some(PendingWindowSummary {
                bucket_start,
                payload: input,
                event_id,
            });
            return Ok(());
        }

        state.accumulate_window(bucket_start, input, event_id);
        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.pending_window.is_some() && state.window_count > 0
    }

    /// Clock-driven trailing-bucket flush.
    ///
    /// Returns `true` when there is an accumulated bucket AND the current wall
    /// time is past that bucket's end boundary (the hour has elapsed). This
    /// allows the periodic timer to emit the latest completed hour without
    /// waiting for the first event of the next hour to arrive.
    fn flush_due(&self, state: &Self::State, now: Timestamp) -> bool {
        if state.window_count == 0 {
            return false;
        }
        let Some(hour_start) = state.hour_start else {
            return false;
        };
        // Do not flush if there is already a pending window from a next-bucket
        // event — the normal window_complete path will handle that.
        if state.pending_window.is_some() {
            return false;
        }
        let end = hour_end(hour_start);
        let due = now >= end;
        if due {
            debug!(
                module = "hourly-summarizer",
                hour_start = %hour_start,
                hour_end = %end,
                now = %now,
                "Flush due: emitting trailing hour bucket via timer"
            );
        }
        due
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let Some(hour_start) = state.hour_start else {
            return Ok(None);
        };

        let hour_end = hour_end(hour_start);
        state.summary_counter += 1;
        let hour_id = format!("activity-hour-{}", hour_start.inner().unix_timestamp());
        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let top_sources = sorted_top_sources(&state.source_window_counts);
        let activity_sources: Vec<ActivitySourceKind> =
            state.activity_source_counts.keys().copied().collect();
        let primary_source = primary_activity_source(&state.focus_time_secs_by_source);
        let source_event_ids = std::mem::take(&mut state.source_event_ids);
        let event_count = state.event_count;

        let payload = ActivityHourlySummaryPayload {
            hour_id: hour_id.clone(),
            hour_start,
            hour_end,
            duration_secs: state.duration_secs,
            window_count: state.window_count,
            event_count,
            source_count: state.sources.len() as u64,
            sources,
            top_sources,
            source_window_counts: state.source_window_counts.clone(),
            activity_sources,
            activity_source_counts: state.activity_source_counts.clone(),
            focus_time_secs_by_source: state.focus_time_secs_by_source.clone(),
            primary_source,
        };

        let event_timestamp = state.last_window_end.unwrap_or(hour_start);
        let output = DerivedOutput::windowed(payload, event_timestamp, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("2.0.0")
            .with_equivalence_key(hour_id)
            .with_aggregation(DerivedAggregationMeta::new(
                "activity.summary.hourly",
                state.summary_counter - 1,
                event_count,
            ));

        let pending_window = state.pending_window.take();
        state.reset_hour();
        if let Some(pending_window) = pending_window {
            state.accumulate_window(
                pending_window.bucket_start,
                pending_window.payload,
                pending_window.event_id,
            );
        }

        Ok(Some(output))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type HourlySummarizerRuntime = WindowedAdapter<HourlySummarizer>;

// --- Source descriptor (issue #690 / #734) ---

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "hourly-summarizer",
        namespace: "derived",
        event_types: &[
            ("derived.hourly-summarizer", "activity.summary.hourly"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, hour_bucket, parent_event_ids)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:hourly-summarizer"),
        "hourly-summarizer",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("activity.summary.hourly")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("hourly-summarizer")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
