#![doc = include_str!("../docs/README.md")]

//! Modernized `SimpleNode` implementation for the PKM Automaton.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};
use sinex_primitives::JsonValue;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PKMState {
    pub keywords: HashMap<String, u64>,
    pub sessions_detected: u64,
}

#[derive(Default)]
pub struct PKMAutomaton;

#[async_trait]
impl SimpleNode for PKMAutomaton {
    type State = PKMState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "pkm-automaton"
    }
    fn input_event_type(&self) -> &'static str {
        "*"
    }
    fn output_event_type(&self) -> &'static str {
        "pkm.knowledge_extraction"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        Ok(None)
    }
}

pub type PKMAutomatonNode = SimpleNodeWrapper<PKMAutomaton>;
