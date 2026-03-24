#![cfg(feature = "messaging")]
#![allow(deprecated)]

use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_node_sdk::derived_node::{
    DerivedOutput, DerivedTriggerContext, DerivedNodeAdapter, TransducerWrapper,
};
use sinex_node_sdk::automaton_node::{
    AutomatonNode, AutomatonNodeAdapter, NodeEventContext, OutputEvent,
};
use sinex_node_sdk::{ErrorAction, NodeLogicError, TransducerNode};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::prelude::*;
use xtask::sandbox::prelude::*;

#[derive(Default, Serialize, Deserialize)]
struct TestState;

#[derive(Deserialize)]
struct TestInput {
    value: String,
}

struct PassthroughDerivedNode;

impl TransducerNode for PassthroughDerivedNode {
    type State = TestState;
    type Input = TestInput;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "adapter-regression-derived"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        Ok(Some(DerivedOutput::transduced(
            json!({ "value": input.value }),
            context.ts_orig.unwrap_or_else(Timestamp::now),
            context.trigger_uuid(),
        )))
    }
}

struct DlqDerivedNode;

impl TransducerNode for DlqDerivedNode {
    type State = TestState;
    type Input = TestInput;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "adapter-regression-derived-dlq"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &DerivedTriggerContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        Err(NodeLogicError::Processing("derived failure".to_string()))
    }

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }
}

struct PassthroughAutomaton;

impl AutomatonNode for PassthroughAutomaton {
    type State = TestState;
    type Input = TestInput;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "adapter-regression-automaton"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &NodeEventContext,
    ) -> std::result::Result<Option<OutputEvent<Self::Output>>, NodeLogicError> {
        Ok(Some(OutputEvent {
            payload: json!({ "value": input.value }),
            ts_orig: context.ts_orig.unwrap_or_else(Timestamp::now),
            source_event_ids: vec![context.event_id],
        }))
    }
}

struct DlqAutomaton;

impl AutomatonNode for DlqAutomaton {
    type State = TestState;
    type Input = TestInput;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "adapter-regression-automaton-dlq"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &NodeEventContext,
    ) -> std::result::Result<Option<OutputEvent<Self::Output>>, NodeLogicError> {
        Err(NodeLogicError::Processing("automaton failure".to_string()))
    }

    fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
        ErrorAction::SendToDLQ
    }
}

fn make_event() -> std::result::Result<Event<JsonValue>, SinexError> {
    DynamicPayload::new("test.source", "test.input", json!({ "value": "hello" }))
        .from_parents([Id::<Event<JsonValue>>::new()])?
        .build()
}

#[sinex_test]
async fn derived_adapter_rejects_missing_trigger_id() -> TestResult<()> {
    let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(PassthroughDerivedNode));
    let mut event = make_event()?;
    event.id = None;

    let err = adapter.process_one(event).await.expect_err("missing id must fail");
    assert!(format!("{err}").contains("missing an id"));
    Ok(())
}

#[sinex_test]
async fn derived_adapter_errors_when_dlq_transport_is_missing() -> TestResult<()> {
    let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(DlqDerivedNode));

    let err = adapter
        .process_one(make_event()?)
        .await
        .expect_err("missing transport must fail");
    assert!(!format!("{err}").is_empty());
    Ok(())
}

#[sinex_test]
async fn automaton_adapter_rejects_missing_trigger_id() -> TestResult<()> {
    let mut adapter = AutomatonNodeAdapter::new(PassthroughAutomaton);
    let mut event = make_event()?;
    event.id = None;

    let err = adapter.process_one(event).await.expect_err("missing id must fail");
    assert!(format!("{err}").contains("missing an id"));
    Ok(())
}

#[sinex_test]
async fn automaton_adapter_errors_when_dlq_transport_is_missing() -> TestResult<()> {
    let mut adapter = AutomatonNodeAdapter::new(DlqAutomaton);

    let err = adapter
        .process_one(make_event()?)
        .await
        .expect_err("missing transport must fail");
    assert!(!format!("{err}").is_empty());
    Ok(())
}
