#![doc = include_str!("../docs/README.md")]

//! Analytics automaton — [`WindowedNode`] implementation.
//!
//! Model classification: **Windowed** — accumulates events in a sliding window
//! (last 1000), emits a summary every 100 events. `ts_orig` is derived from the
//! latest event in the window, ensuring temporal determinism across replays.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext, WindowedNodeAdapter};
use sinex_node_sdk::{NodeLogicError, WindowedNode};
use sinex_primitives::JsonValue;
use sinex_primitives::Uuid;
use sinex_primitives::temporal::{Timestamp, now};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalyticsState {
    pub recent_events: VecDeque<EventSummary>,
    pub event_counts: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSummary {
    pub event_type: String,
    pub timestamp: Timestamp,
    pub event_id: Uuid,
}

#[derive(Default)]
pub struct AnalyticsAutomaton;

impl WindowedNode for AnalyticsAutomaton {
    type State = AnalyticsState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "analytics-automaton"
    }
    fn input_event_type(&self) -> &'static str {
        "*"
    }
    fn output_event_type(&self) -> &'static str {
        "analytics.insight"
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<(), NodeLogicError> {
        let event_type_str = context.event_type.as_str().to_string();
        *state
            .event_counts
            .entry(event_type_str.clone())
            .or_insert(0) += 1;

        state.recent_events.push_back(EventSummary {
            event_type: event_type_str,
            timestamp: context.ts_orig.unwrap_or_else(now),
            event_id: context.trigger_uuid(),
        });

        // Prune window (keep last 1000)
        if state.recent_events.len() > 1000 {
            state.recent_events.pop_front();
        }

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.recent_events.len() % 100 == 0 && !state.recent_events.is_empty()
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let source_event_ids: Vec<Uuid> = state.recent_events.iter().map(|e| e.event_id).collect();

        // Derive ts_orig from the latest event in the window — deterministic across replays.
        let ts_orig = state
            .recent_events
            .back()
            .map_or(context.ts_coided, |e| e.timestamp);

        let payload = serde_json::json!({
            "top_events": state.event_counts,
            "window_size": state.recent_events.len(),
        });

        Ok(Some(DerivedOutput::windowed(
            payload,
            ts_orig,
            source_event_ids,
        )))
    }
}

/// Node type alias for use with `node_entrypoint!`.
pub type AnalyticsAutomatonNode = WindowedNodeAdapter<AnalyticsAutomaton>;
