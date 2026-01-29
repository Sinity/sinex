#![doc = include_str!("../docs/README.md")]

//! Modernized `SimpleNode` implementation for the Analytics Automaton.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::JsonValue;
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};
use std::collections::{HashMap, VecDeque};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalyticsState {
    pub recent_events: VecDeque<EventSummary>,
    pub event_counts: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSummary {
    pub event_type: String,
    pub timestamp: OffsetDateTime,
}

#[derive(Default)]
pub struct AnalyticsAutomaton;

#[async_trait]
impl SimpleNode for AnalyticsAutomaton {
    type State = AnalyticsState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "analytics-automaton"
    }
    fn input_event_type(&self) -> &'static str {
        "*"
    } // Match all events for global analytics
    fn output_event_type(&self) -> &'static str {
        "analytics.insight"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        _input: Self::Input,
        context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        // Track frequency
        *state
            .event_counts
            .entry(context.event_type.clone())
            .or_insert(0) += 1;

        // Add to window
        state.recent_events.push_back(EventSummary {
            event_type: context.event_type.clone(),
            timestamp: context.ts_orig.unwrap_or_else(OffsetDateTime::now_utc),
        });

        // Prune window (keep last 1000)
        if state.recent_events.len() > 1000 {
            state.recent_events.pop_front();
        }

        // Emit report every 100 events
        if state.recent_events.len() % 100 == 0 {
            Ok(Some(serde_json::json!({
                "top_events": state.event_counts,
                "window_size": state.recent_events.len(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub type AnalyticsAutomatonNode = SimpleNodeWrapper<AnalyticsAutomaton>;
