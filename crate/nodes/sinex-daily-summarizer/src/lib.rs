#![doc = include_str!("../docs/README.md")]

//! Daily summarizer -- [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** -- accumulates
//! `activity.summary.hourly` inputs into completed UTC-day summaries.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedAggregationMeta, DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, WindowedNode};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::events::{
    EventPayload,
    payloads::{ActivityDailySummaryPayload, ActivityHourlySummaryPayload},
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::{BTreeMap, BTreeSet};

fn floor_to_day(timestamp: Timestamp) -> Timestamp {
    let rounded = timestamp
        .inner()
        .replace_hour(0)
        .expect("0 hour is always valid")
        .replace_minute(0)
        .expect("0 minute is always valid")
        .replace_second(0)
        .expect("0 second is always valid")
        .replace_nanosecond(0)
        .expect("0 nanosecond is always valid");
    Timestamp::from(rounded)
}

fn day_end(day_start: Timestamp) -> Timestamp {
    Timestamp::from(day_start.inner() + Duration::days(1))
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
struct PendingHourlySummary {
    bucket_start: Timestamp,
    payload: ActivityHourlySummaryPayload,
    event_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DailySummaryState {
    pub day_start: Option<Timestamp>,
    pub duration_secs: u64,
    pub hour_count: u64,
    pub window_count: u64,
    pub event_count: u64,
    pub sources: BTreeSet<String>,
    pub source_window_counts: BTreeMap<String, u64>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub focus_time_secs_by_source: BTreeMap<ActivitySourceKind, u64>,
    pub source_event_ids: Vec<Uuid>,
    pub summary_counter: u32,
    pending_hour: Option<PendingHourlySummary>,
}

impl DailySummaryState {
    fn reset_day(&mut self) {
        self.day_start = None;
        self.duration_secs = 0;
        self.hour_count = 0;
        self.window_count = 0;
        self.event_count = 0;
        self.sources.clear();
        self.source_window_counts.clear();
        self.activity_source_counts.clear();
        self.focus_time_secs_by_source.clear();
        self.source_event_ids.clear();
        self.pending_hour = None;
    }

    fn accumulate_hour(
        &mut self,
        bucket_start: Timestamp,
        payload: ActivityHourlySummaryPayload,
        event_id: Uuid,
    ) {
        if self.day_start.is_none() {
            self.day_start = Some(bucket_start);
        }

        self.duration_secs += payload.duration_secs;
        self.hour_count += 1;
        self.window_count += payload.window_count;
        self.event_count += payload.event_count;
        self.sources.extend(payload.sources.iter().cloned());
        for (source, count) in payload.source_window_counts {
            *self.source_window_counts.entry(source).or_insert(0) += count;
        }
        for (source, count) in payload.activity_source_counts {
            *self.activity_source_counts.entry(source).or_insert(0) += count;
        }
        for (source, secs) in payload.focus_time_secs_by_source {
            *self.focus_time_secs_by_source.entry(source).or_insert(0) += secs;
        }
        self.source_event_ids.push(event_id);
        self.pending_hour = None;
    }
}

#[derive(Default)]
pub struct DailySummarizer;

impl WindowedNode for DailySummarizer {
    type State = DailySummaryState;
    type Input = ActivityHourlySummaryPayload;
    type Output = ActivityDailySummaryPayload;

    fn name(&self) -> &'static str {
        "daily-summarizer"
    }

    fn input_event_type(&self) -> &'static str {
        ActivityHourlySummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        ActivityDailySummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        ActivityDailySummaryPayload::SOURCE.as_static_str()
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
        let bucket_start = floor_to_day(input.hour_start);
        let event_id = context.trigger_uuid();

        if let Some(current_bucket) = state.day_start
            && state.hour_count > 0
            && current_bucket != bucket_start
        {
            state.pending_hour = Some(PendingHourlySummary {
                bucket_start,
                payload: input,
                event_id,
            });
            return Ok(());
        }

        state.accumulate_hour(bucket_start, input, event_id);
        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.pending_hour.is_some() && state.hour_count > 0
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let Some(day_start) = state.day_start else {
            return Ok(None);
        };

        let day_end = day_end(day_start);
        state.summary_counter += 1;
        let day_id = format!("activity-day-{}", day_start.inner().unix_timestamp());
        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let top_sources = sorted_top_sources(&state.source_window_counts);
        let activity_sources: Vec<ActivitySourceKind> =
            state.activity_source_counts.keys().copied().collect();
        let primary_source = primary_activity_source(&state.focus_time_secs_by_source);
        let source_event_ids = std::mem::take(&mut state.source_event_ids);
        let event_count = state.event_count;

        let payload = ActivityDailySummaryPayload {
            day_id: day_id.clone(),
            day_start,
            day_end,
            duration_secs: state.duration_secs,
            hour_count: state.hour_count,
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

        let output = DerivedOutput::windowed(payload, day_end, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("1.0.0")
            .with_equivalence_key(day_id)
            .with_aggregation(DerivedAggregationMeta::new(
                "activity.summary.daily",
                state.summary_counter - 1,
                event_count,
            ));

        let pending_hour = state.pending_hour.take();
        state.reset_day();
        if let Some(pending_hour) = pending_hour {
            state.accumulate_hour(
                pending_hour.bucket_start,
                pending_hour.payload,
                pending_hour.event_id,
            );
        }

        Ok(Some(output))
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type DailySummarizerNode = WindowedNodeAdapter<DailySummarizer>;
