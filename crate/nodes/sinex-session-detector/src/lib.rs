#![doc = include_str!("../docs/README.md")]

//! Session detector -- [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** -- accumulates events, detects session
//! boundaries when the gap between consecutive event timestamps exceeds a
//! configurable threshold (default 5 minutes).
//! Emits `activity.session.boundary` events with session metadata.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter};
use sinex_node_sdk::{NodeLogicError, WindowedNode};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp, now};
use sinex_primitives::{JsonValue, Uuid};
use std::collections::BTreeSet;

/// Default session gap threshold in seconds (5 minutes).
const DEFAULT_GAP_THRESHOLD_SECS: i64 = 300;

/// Session gap threshold, configurable via `SINEX_SESSION_GAP_SECS`.
fn gap_threshold() -> Duration {
    let secs = std::env::var("SINEX_SESSION_GAP_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(DEFAULT_GAP_THRESHOLD_SECS);
    Duration::seconds(secs)
}

/// Persistent window state tracking the current activity session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    /// Start time of the current session.
    pub session_start: Option<Timestamp>,

    /// Timestamp of the most recent event in the current session.
    pub last_event_time: Option<Timestamp>,

    /// Number of events accumulated in the current session.
    pub event_count: u64,

    /// Unique event sources observed in the current session.
    pub sources: BTreeSet<String>,

    /// UUIDv7 IDs of events in the current session (for provenance).
    pub event_ids: Vec<Uuid>,

    /// Session counter for generating deterministic session IDs.
    pub session_counter: u64,

    /// Whether a gap was detected between the last event and the current one.
    /// Set in `accumulate()` using event timestamps (not wall clock), making
    /// session boundary detection deterministic and replay-correct.
    #[serde(default)]
    pub gap_detected: bool,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            session_start: None,
            last_event_time: None,
            event_count: 0,
            sources: BTreeSet::new(),
            event_ids: Vec::new(),
            session_counter: 0,
            gap_detected: false,
        }
    }
}

impl SessionState {
    /// Reset state for a new session, preserving the counter.
    fn reset_session(&mut self) {
        self.session_start = None;
        self.last_event_time = None;
        self.event_count = 0;
        self.sources.clear();
        self.event_ids.clear();
        self.gap_detected = false;
    }
}

#[derive(Default)]
pub struct SessionDetector;

impl WindowedNode for SessionDetector {
    type State = SessionState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "session-detector"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn output_event_type(&self) -> &'static str {
        "activity.session.boundary"
    }

    fn output_event_source(&self) -> &'static str {
        "derived.session-detector"
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
        let event_time = context.ts_orig.unwrap_or_else(now);
        let source = context.source.as_str().to_string();

        // Detect session gap using event timestamps (replay-correct).
        // A gap is detected when the incoming event's ts_orig is more than
        // gap_threshold after the last event's ts_orig. This uses event
        // time, not wall clock, so replay produces the same boundaries.
        if let Some(last_time) = state.last_event_time {
            if state.event_count > 0 && (event_time - last_time) >= gap_threshold() {
                state.gap_detected = true;
            }
        }

        // Initialize session start if this is the first event
        if state.session_start.is_none() {
            state.session_start = Some(event_time);
        }

        state.last_event_time = Some(event_time);
        state.event_count += 1;
        state.sources.insert(source);
        state.event_ids.push(context.trigger_uuid());

        // Cap provenance list to prevent unbounded growth in very long sessions
        if state.event_ids.len() > 10_000 {
            // Keep first and last 5000 for provenance bookends
            let last_5k: Vec<Uuid> = state.event_ids[state.event_ids.len() - 5000..].to_vec();
            state.event_ids.truncate(5000);
            state.event_ids.extend(last_5k);
        }

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        // Gap detection is done in accumulate() using event timestamps,
        // making it deterministic and replay-correct.
        state.gap_detected && state.event_count > 0
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let Some(start_time) = state.session_start else {
            return Ok(None);
        };

        let end_time = state.last_event_time.unwrap_or(start_time);
        let duration = end_time - start_time;
        let duration_secs = duration.whole_seconds();

        state.session_counter += 1;
        let session_id = format!("session-{}", state.session_counter);

        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let source_event_ids = std::mem::take(&mut state.event_ids);
        let event_count = state.event_count;

        let payload = serde_json::json!({
            "session_id": session_id,
            "start_time": start_time.format_rfc3339(),
            "end_time": end_time.format_rfc3339(),
            "event_count": event_count,
            "sources": sources,
            "duration_secs": duration_secs,
        });

        // Use WindowBoundary policy: the emission represents the boundary
        // between sessions, not any single input event's time.
        let output = DerivedOutput::windowed(payload, end_time, source_event_ids)
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary);

        // Reset state for the next session
        state.reset_session();

        Ok(Some(output))
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type SessionDetectorNode = WindowedNodeAdapter<SessionDetector>;
