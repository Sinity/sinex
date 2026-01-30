#![doc = include_str!("../docs/README.md")]

//! Modernized `SimpleNode` implementation for the Content Automaton.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::JsonValue;
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContentState {
    pub processed_blobs: u64,
}

#[derive(Default)]
pub struct ContentAutomaton;

#[async_trait]
impl SimpleNode for ContentAutomaton {
    type State = ContentState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "content-automaton"
    }
    fn input_event_type(&self) -> &'static str {
        "blob.stored"
    }
    fn output_event_type(&self) -> &'static str {
        "content.analyzed"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        state.processed_blobs += 1;
        Ok(Some(serde_json::json!({
            "blob_id": input.get("id"),
            "status": "indexed",
            "total_count": state.processed_blobs,
        })))
    }
}

pub type ContentAutomatonNode = SimpleNodeWrapper<ContentAutomaton>;
