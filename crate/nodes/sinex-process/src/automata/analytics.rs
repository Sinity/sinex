//! Analytics automaton — [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** — accumulates trusted activity signals
//! into bounded windows and emits `activity.window.summary` rollups when a gap,
//! duration bound, or parent-count budget closes the current window.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedAggregationMeta, DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter,
};
use sinex_node_sdk::{InputProvenanceFilter, NodeLogicError, WindowedNode};
use sinex_primitives::activity::{
    ActivitySourceKind, classify_trusted_activity_signal, primary_activity_source,
};
use sinex_primitives::events::{
    EventPayload,
    payloads::{ActivityWindowCloseReason, ActivityWindowSummaryPayload},
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{JsonValue, Uuid, env as shared_env};
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

const DEFAULT_WINDOW_GAP_THRESHOLD_SECS: i64 = 300;
const DEFAULT_WINDOW_MAX_DURATION_SECS: i64 = 900;
const DEFAULT_WINDOW_MAX_EVENTS: usize = 250;

fn parse_positive_i64_env(var: &str, description: &str, default: i64) -> i64 {
    shared_env::parse_optional::<i64>(var, description)
        .filter(|&value| {
            if value > 0 {
                true
            } else {
                warn!(
                    env = var,
                    parsed = value,
                    "Activity window override must be positive; using default"
                );
                false
            }
        })
        .unwrap_or(default)
}

fn parse_positive_usize_env(var: &str, description: &str, default: usize) -> usize {
    shared_env::parse_optional::<usize>(var, description)
        .filter(|&value| {
            if value > 0 {
                true
            } else {
                warn!(
                    env = var,
                    parsed = value,
                    "Activity window override must be positive; using default"
                );
                false
            }
        })
        .unwrap_or(default)
}

fn window_gap_threshold() -> Duration {
    let secs = shared_env::parse_optional::<i64>(
        "SINEX_ACTIVITY_WINDOW_GAP_SECS",
        "activity window gap threshold",
    )
    .or_else(|| {
        shared_env::parse_optional::<i64>("SINEX_SESSION_GAP_SECS", "legacy session gap threshold")
    })
    .filter(|&secs| {
        if secs > 0 {
            true
        } else {
            warn!(
                env = "SINEX_ACTIVITY_WINDOW_GAP_SECS",
                parsed = secs,
                "Activity window gap override must be positive; using default"
            );
            false
        }
    })
    .unwrap_or(DEFAULT_WINDOW_GAP_THRESHOLD_SECS);
    Duration::seconds(secs)
}

fn window_max_duration() -> Duration {
    Duration::seconds(parse_positive_i64_env(
        "SINEX_ACTIVITY_WINDOW_MAX_DURATION_SECS",
        "activity window max duration",
        DEFAULT_WINDOW_MAX_DURATION_SECS,
    ))
}

fn window_max_events() -> usize {
    parse_positive_usize_env(
        "SINEX_ACTIVITY_WINDOW_MAX_EVENTS",
        "activity window max event count",
        DEFAULT_WINDOW_MAX_EVENTS,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalyticsState {
    pub window_start: Option<Timestamp>,
    pub last_event_time: Option<Timestamp>,
    pub event_count: u64,
    pub sources: BTreeSet<String>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub event_ids: Vec<Uuid>,
    pub window_counter: u64,
    pub close_reason: Option<ActivityWindowCloseReason>,
    pub pending_window_seed: Option<PendingWindowSeed>,
}

impl AnalyticsState {
    fn reset_window(&mut self) {
        self.window_start = None;
        self.last_event_time = None;
        self.event_count = 0;
        self.sources.clear();
        self.activity_source_counts.clear();
        self.event_ids.clear();
        self.close_reason = None;
        self.pending_window_seed = None;
    }

    fn seed_window(&mut self, seed: PendingWindowSeed) {
        self.window_start = Some(seed.event_time);
        self.last_event_time = Some(seed.event_time);
        self.event_count = 1;
        self.sources.clear();
        self.sources.insert(seed.raw_source);
        self.activity_source_counts.clear();
        self.activity_source_counts.insert(seed.activity_source, 1);
        self.event_ids.clear();
        self.event_ids.push(seed.event_id);
        self.close_reason = None;
        self.pending_window_seed = None;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWindowSeed {
    pub event_time: Timestamp,
    pub raw_source: String,
    pub activity_source: ActivitySourceKind,
    pub event_id: Uuid,
}

pub struct AnalyticsAutomaton {
    gap_threshold: Duration,
    max_duration: Duration,
    max_events: usize,
}

impl Default for AnalyticsAutomaton {
    fn default() -> Self {
        Self {
            gap_threshold: window_gap_threshold(),
            max_duration: window_max_duration(),
            max_events: window_max_events(),
        }
    }
}

impl WindowedNode for AnalyticsAutomaton {
    type State = AnalyticsState;
    type Input = JsonValue;
    type Output = ActivityWindowSummaryPayload;

    fn name(&self) -> &'static str {
        "analytics-automaton"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn output_event_type(&self) -> &'static str {
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        ActivityWindowSummaryPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<(), NodeLogicError> {
        let Some(activity_source) =
            classify_trusted_activity_signal(context.source.as_str(), context.event_type.as_str())
        else {
            return Ok(());
        };

        let event_time = context.require_ts_orig()?;
        let event_id = context.trigger_uuid();
        let raw_source = context.source.as_str().to_string();

        let seed = PendingWindowSeed {
            event_time,
            raw_source,
            activity_source,
            event_id,
        };

        if let Some(last_time) = state.last_event_time
            && state.event_count > 0
            && (event_time - last_time) >= self.gap_threshold
        {
            state.close_reason = Some(ActivityWindowCloseReason::Gap);
            state.pending_window_seed = Some(seed);
            return Ok(());
        }

        if let Some(start_time) = state.window_start
            && state.event_count > 0
            && (event_time - start_time) >= self.max_duration
        {
            state.close_reason = Some(ActivityWindowCloseReason::MaxDuration);
            state.pending_window_seed = Some(seed);
            return Ok(());
        }

        if state.event_count as usize >= self.max_events {
            state.close_reason = Some(ActivityWindowCloseReason::MaxEventCount);
            state.pending_window_seed = Some(seed);
            return Ok(());
        }

        if state.window_start.is_none() {
            state.window_start = Some(event_time);
        }
        state.last_event_time = Some(event_time);
        state.event_count += 1;
        state.sources.insert(seed.raw_source);
        *state
            .activity_source_counts
            .entry(seed.activity_source)
            .or_insert(0) += 1;
        state.event_ids.push(seed.event_id);

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.close_reason.is_some() && state.event_count > 0
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let Some(start_time) = state.window_start else {
            return Ok(None);
        };
        let Some(close_reason) = state.close_reason else {
            return Ok(None);
        };

        let end_time = state.last_event_time.unwrap_or(start_time);
        let duration_secs = (end_time - start_time).whole_seconds().max(0) as u64;

        state.window_counter += 1;
        let window_id = format!("activity-window-{}", state.window_counter);
        let event_count = state.event_count;
        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let activity_sources: Vec<ActivitySourceKind> =
            state.activity_source_counts.keys().copied().collect();
        let primary_source = primary_activity_source(&state.activity_source_counts);
        let source_event_ids = std::mem::take(&mut state.event_ids);

        let payload = ActivityWindowSummaryPayload {
            window_id: window_id.clone(),
            window_start: start_time,
            window_end: end_time,
            duration_secs,
            event_count,
            source_count: state.sources.len() as u64,
            sources,
            activity_sources,
            activity_source_counts: state.activity_source_counts.clone(),
            primary_source,
            close_reason,
        };

        let output = DerivedOutput::windowed(payload, end_time, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("2.0.0")
            .with_equivalence_key(window_id)
            .with_aggregation(DerivedAggregationMeta::new(
                "activity.window",
                0,
                event_count,
            ));

        let pending_seed = state.pending_window_seed.take();
        state.reset_window();
        if let Some(seed) = pending_seed {
            state.seed_window(seed);
        }

        Ok(Some(output))
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type AnalyticsAutomatonNode = WindowedNodeAdapter<AnalyticsAutomaton>;

// --- Source-unit descriptor (issue #690 / #734) ---

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

// Analytics is a derived source: it consumes trusted-activity inputs and
// emits `activity.window.summary` rollups. Its checkpoint is the consumer
// position on the upstream event stream (append-stream).
register_source_unit! {
    SourceUnitDescriptor {
        id: "analytics",
        namespace: "derived",
        runner_pack: "process",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("derived.activity-window", "activity.window.summary"),
        ],
        // Inherits the privacy tier of its inputs (window titles, commands).
        privacy_tier: SuPrivacyTier::Sensitive,
        runtime_shape: SuRuntimeShape::Continuous,
        horizons: &[SuHorizon::Continuous],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Uuid5From(
            "(source_unit, parent_event_ids)",
        ),
        access_policy: "event_stream_read",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:process",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}
