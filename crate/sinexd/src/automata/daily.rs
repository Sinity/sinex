//! Daily summarizer -- [`Windowed`] implementation.
//!
//! Model classification: **Windowed** -- accumulates
//! `activity.summary.hourly` inputs into completed UTC-day summaries.

use crate::node_sdk::derived_node::{
    AutomatonContext, DerivedAggregationMeta, DerivedOutput, WindowedNodeAdapter,
};
use crate::node_sdk::{InputProvenanceFilter, NodeLogicError, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::events::{
    EventPayload,
    payloads::{ActivityDailySummaryPayload, ActivityHourlySummaryPayload},
};
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

fn floor_to_day(timestamp: Timestamp) -> Timestamp {
    let Ok(rounded) = timestamp
        .inner()
        .replace_hour(0)
        .and_then(|value| value.replace_minute(0))
        .and_then(|value| value.replace_second(0))
        .and_then(|value| value.replace_nanosecond(0))
    else {
        unreachable!("zeroed hour, minute, second, and nanosecond are valid time components");
    };
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

impl Windowed for DailySummarizer {
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
    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
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

    /// Clock-driven trailing-bucket flush.
    ///
    /// Returns `true` when there is an accumulated bucket AND the current wall
    /// time is past that bucket's end boundary (the day has elapsed). This
    /// allows the periodic timer to emit the latest completed day without
    /// waiting for the first event of the next day to arrive.
    fn flush_due(&self, state: &Self::State, now: Timestamp) -> bool {
        if state.hour_count == 0 {
            return false;
        }
        let Some(day_start) = state.day_start else {
            return false;
        };
        // Do not flush if there is already a pending hour from a next-bucket
        // event — the normal window_complete path will handle that.
        if state.pending_hour.is_some() {
            return false;
        }
        let end = day_end(day_start);
        let due = now >= end;
        if due {
            debug!(
                node = "daily-summarizer",
                day_start = %day_start,
                day_end = %end,
                now = %now,
                "Flush due: emitting trailing day bucket via timer"
            );
        }
        due
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &AutomatonContext,
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

/// Node type alias registered via `AutomatonSpec` in `automata::registry`.
pub type DailySummarizerNode = WindowedNodeAdapter<DailySummarizer>;

// --- Source-unit descriptor (issue #690 / #734) ---

use sinex_primitives::proof::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitBinding,
    SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "daily-summarizer",
        namespace: "derived",
        event_types: &[
            ("derived.daily-summarizer", "activity.summary.daily"),
        ],
        privacy_tier: SuPrivacyTier::Sensitive,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, day_bucket, parent_event_ids)",
        ),
        access_policy: "event_stream_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:daily-summarizer"),
        "daily-summarizer",
        "derived",
    )
    .implementation("sinex-process")
    .adapter("AutomatonRuntime")
    .output_event_type("activity.summary.daily")
    .privacy_context("inherits_from_parents")
    .material_policy("derived_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_unit_id("daily-summarizer")
    .runner_pack("process")
    .checkpoint_family(SuCheckpointFamily::AppendStream)
    .runtime_shape(SuRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:process")
    .build_impact(sinex_primitives::proof::SourceUnitBuildImpact::ZERO)
    .build()
}
