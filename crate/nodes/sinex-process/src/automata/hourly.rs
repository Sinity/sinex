//! Hourly summarizer -- [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** -- accumulates
//! `activity.window.summary` inputs into completed UTC-hour summaries.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedAggregationMeta, DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, WindowedNode};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::events::{
    EventPayload,
    payloads::{ActivityHourlySummaryPayload, ActivityWindowSummaryPayload},
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::{BTreeMap, BTreeSet};

fn floor_to_hour(timestamp: Timestamp) -> Timestamp {
    let rounded = timestamp
        .inner()
        .replace_minute(0)
        .expect("0 minute is always valid")
        .replace_second(0)
        .expect("0 second is always valid")
        .replace_nanosecond(0)
        .expect("0 nanosecond is always valid");
    Timestamp::from(rounded)
}

fn hour_end(hour_start: Timestamp) -> Timestamp {
    Timestamp::from(hour_start.inner() + Duration::hours(1))
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
    }

    fn accumulate_window(
        &mut self,
        bucket_start: Timestamp,
        payload: ActivityWindowSummaryPayload,
        event_id: Uuid,
    ) {
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
        self.source_event_ids.push(event_id);
        self.pending_window = None;
    }
}

#[derive(Default)]
pub struct HourlySummarizer;

impl WindowedNode for HourlySummarizer {
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

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<(), NodeLogicError> {
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

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
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

        let output = DerivedOutput::windowed(payload, hour_end, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("1.0.0")
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

/// Node type alias for use with `node_entrypoint!`.
pub type HourlySummarizerNode = WindowedNodeAdapter<HourlySummarizer>;

// --- Source-unit descriptor (issue #690 / #734) ---

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

register_source_unit! {
    SourceUnitDescriptor {
        id: "hourly-summarizer",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("derived.hourly-summarizer", "activity.summary.hourly"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, hour_bucket, parent_event_ids)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
