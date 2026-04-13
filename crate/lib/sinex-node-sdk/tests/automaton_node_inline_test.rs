#![cfg(feature = "messaging")]

//! Tests for the derived node model family (`TransducerNode`, `DerivedNodeAdapter`, `DerivedNodeConfig`).
//!
//! These exercise the derived-node processing surface directly.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext};
use sinex_node_sdk::{DerivedNodeConfig, NodeLogicError, TransducerNode};
use sinex_primitives::domain::{ProcessingMode, SyntheticTemporalPolicy, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
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

impl TransducerNode for TestNodeLogic {
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

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        state.count += 1;
        Ok(Some(DerivedOutput::transduced(
            TestOutput {
                processed_value: input.value.to_uppercase(),
            },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            context.trigger_uuid(),
        )))
    }
}

fn make_context() -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: "test".into(),
        event_type: "test.input".into(),
        ts_orig: Some(Timestamp::now()),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

#[sinex_test]
async fn test_transducer_node_process() -> TestResult<()> {
    let mut node = TestNodeLogic;
    let mut state = TestState::default();
    let ctx = make_context();

    let input = TestInput {
        value: "hello".to_string(),
    };
    let result = node.process(&mut state, input, &ctx).await?.unwrap();

    assert_eq!(result.payload.processed_value, "HELLO");
    assert_eq!(state.count, 1);
    assert_eq!(
        result.temporal_policy,
        SyntheticTemporalPolicy::InheritParent
    );
    assert_eq!(result.source_event_ids.len(), 1);
    assert_eq!(result.source_event_ids[0], ctx.trigger_uuid());
    Ok(())
}

#[sinex_test]
async fn test_transducer_state_accumulates() -> TestResult<()> {
    let mut node = TestNodeLogic;
    let mut state = TestState::default();

    for _ in 0..5 {
        let ctx = make_context();
        let input = TestInput {
            value: "x".to_string(),
        };
        node.process(&mut state, input, &ctx).await?;
    }

    assert_eq!(state.count, 5);
    Ok(())
}

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = DerivedNodeConfig::default();
    assert_eq!(config.checkpoint_interval, 1000);
    assert_eq!(config.checkpoint_timeout_secs, 10);
    assert_eq!(config.batch_size, 100);
    Ok(())
}
