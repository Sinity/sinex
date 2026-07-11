//! Analytics automaton — [`Windowed`] implementation.
//!
//! Model classification: **Windowed** — accumulates trusted activity signals
//! into bounded windows and emits `activity.window.summary` rollups when a gap,
//! duration bound, or parent-count budget closes the current window.

use crate::runtime::automaton::{
    AutomatonContext, DerivedAggregationMeta, DerivedOutput, WindowedAdapter,
};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::activity::{
    ActivitySourceKind, classify_trusted_activity_signal, primary_activity_source,
};
use sinex_primitives::events::{
    EventPayload,
    payloads::{
        ActivityWatchBrowserTabActivePayload, ActivityWatchWindowActivePayload,
        ActivityWindowCloseReason, ActivityWindowSummaryPayload, HyprlandWindowFocusedPayload,
        KittyCommandExecutedPayload, PageVisitedPayload,
    },
};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{JsonValue, Uuid, env as shared_env};
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

const DEFAULT_WINDOW_GAP_THRESHOLD_SECS: i64 = 300;
const DEFAULT_WINDOW_MAX_DURATION_SECS: i64 = 900;
const DEFAULT_WINDOW_MAX_EVENTS: usize = 100;

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
    /// Processing-order counter — RETAINED as a display ordinal only, never as
    /// occurrence identity (sinex-ecy: counter keys collide across
    /// replay/checkpoint-reset and silently suppress fresh derived rows).
    pub window_counter: u64,
    /// Material occurrence of the first contributing event of the current
    /// window — the occurrence anchor for the window's stable equivalence key.
    #[serde(default)]
    pub start_material_id: Option<Uuid>,
    #[serde(default)]
    pub start_anchor_byte: Option<i64>,
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
        self.start_material_id = None;
        self.start_anchor_byte = None;
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
        self.start_material_id = seed.material_id;
        self.start_anchor_byte = seed.anchor_byte;
        self.close_reason = None;
        self.pending_window_seed = None;
    }
}

/// Occurrence-stable identity for an `activity.window.summary`, derived from the
/// material occurrence of the window's first contributing event (sinex-ecy).
/// Never a processing-order counter: counters restart on replay/checkpoint-reset
/// and collide with unrelated live rows, so admission's fail-open dedup silently
/// drops fresh derived windows. Analytics is `MaterialOnly`, so the coordinates
/// are normally present; the timestamp fallback keeps identity occurrence-derived
/// (never a counter) in the degenerate case. The `:`-delimited format never
/// collides with the old `activity-window-{counter}` keys, so the format change
/// causes no false suppression across the migration.
fn activity_window_occurrence_key(
    material_id: Option<Uuid>,
    anchor_byte: Option<i64>,
    window_start: Timestamp,
) -> String {
    match (material_id, anchor_byte) {
        (Some(id), Some(anchor)) => format!("activity-window:{id}:{anchor}"),
        _ => format!("activity-window:ts:{window_start}"),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWindowSeed {
    pub event_time: Timestamp,
    pub raw_source: String,
    pub activity_source: ActivitySourceKind,
    pub event_id: Uuid,
    /// Material occurrence of this event (analytics is `MaterialOnly`, so a
    /// trigger always carries these) — the occurrence anchor for the window's
    /// stable equivalence key (sinex-ecy).
    #[serde(default)]
    pub material_id: Option<Uuid>,
    #[serde(default)]
    pub anchor_byte: Option<i64>,
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

impl Windowed for AnalyticsAutomaton {
    type State = AnalyticsState;
    type Input = JsonValue;
    type Output = ActivityWindowSummaryPayload;

    fn name(&self) -> &'static str {
        "analytics-automaton"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn input_event_types(&self) -> Vec<&'static str> {
        vec![
            HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str(),
            ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str(),
            ActivityWatchBrowserTabActivePayload::EVENT_TYPE.as_static_str(),
            PageVisitedPayload::EVENT_TYPE.as_static_str(),
            KittyCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        ]
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
    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
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
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
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
            state.start_material_id = seed.material_id;
            state.start_anchor_byte = seed.anchor_byte;
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
        _context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let Some(start_time) = state.window_start else {
            return Ok(None);
        };
        let Some(close_reason) = state.close_reason else {
            return Ok(None);
        };

        let end_time = state.last_event_time.unwrap_or(start_time);
        let duration_secs = (end_time - start_time).whole_seconds().max(0) as u64;

        state.window_counter += 1;
        let window_id = activity_window_occurrence_key(
            state.start_material_id,
            state.start_anchor_byte,
            start_time,
        );
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

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type AnalyticsAutomatonRuntime = WindowedAdapter<AnalyticsAutomaton>;

// --- Source descriptor (issue #690 / #734) ---

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// Analytics is a derived source: it consumes trusted-activity inputs and
// emits `activity.window.summary` rollups. Its checkpoint is the consumer
// position on the upstream event stream (append-stream).
register_source_contract! {
    SourceContract {
        id: "analytics",
        namespace: "derived",
        event_types: &[
            ("derived.activity-window", "activity.window.summary"),
        ],
        // Inherits the privacy tier of its inputs (window titles, commands).
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(first_contributing_event_material_id, first_contributing_event_anchor_byte)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:analytics"),
        "analytics",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("activity.window.summary")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("analytics")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}

#[cfg(test)]
#[path = "analytics_test.rs"]
mod tests;
