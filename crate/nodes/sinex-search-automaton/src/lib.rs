#![doc = include_str!("../docs/README.md")]

//! Modernized `SimpleNode` implementation for the Search Automaton.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_core::JsonValue;
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchState {
    pub index_entries: VecDeque<JsonValue>,
}

#[derive(Default)]
pub struct SearchAutomaton;

#[async_trait]
impl SimpleNode for SearchAutomaton {
    type State = SearchState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "search-automaton"
    }
    fn input_event_type(&self) -> &'static str {
        "*"
    }
    fn output_event_type(&self) -> &'static str {
        "search.index_update"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        let content = input
            .get("content")
            .or(input.get("text"))
            .and_then(|v| v.as_str());
        if let Some(c) = content {
            state.index_entries.push_back(serde_json::json!({
                "content": c,
                "timestamp": chrono::Utc::now(),
            }));
            if state.index_entries.len() > 1000 {
                state.index_entries.pop_front();
            }

            if state.index_entries.len() % 50 == 0 {
                return Ok(Some(
                    serde_json::json!({ "index_size": state.index_entries.len() }),
                ));
            }
        }
        Ok(None)
    }
}

pub type SearchAutomatonNode = SimpleNodeWrapper<SearchAutomaton>;
