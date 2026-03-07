#![cfg(feature = "messaging")]

use serde::{Deserialize, Serialize};
use sinex_node_sdk::{
    AutomatonNode, AutomatonNodeAdapter, NodeAdapterConfig, NodeEventContext, NodeLogicError,
};
use xtask::sandbox::prelude::*;

#[derive(Serialize, Deserialize, Default)]
struct TestState {
    count: u64,
}

#[derive(Deserialize)]
struct TestInput {
    value: String,
}

#[derive(Serialize)]
struct TestOutput {
    processed_value: String,
}

struct TestNodeLogic;

impl AutomatonNode for TestNodeLogic {
    type State = TestState;
    type Input = TestInput;
    type Output = TestOutput;

    fn name(&self) -> &'static str {
        "test-node"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &NodeEventContext,
    ) -> Result<Option<Self::Output>, NodeLogicError> {
        state.count += 1;
        Ok(Some(TestOutput {
            processed_value: input.value.to_uppercase(),
        }))
    }
}

#[sinex_test]
async fn test_automaton_node_creation() -> TestResult<()> {
    let node_logic = TestNodeLogic;
    let node = AutomatonNodeAdapter::new(node_logic);
    assert_eq!(node.events_processed(), 0);
    Ok(())
}

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = NodeAdapterConfig::default();
    assert_eq!(config.checkpoint_interval, 1000);
    assert_eq!(config.checkpoint_timeout_secs, 10);
    assert_eq!(config.batch_size, 100);
    Ok(())
}
