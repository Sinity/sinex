#![cfg(feature = "messaging")]

use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_node_sdk::derived_node::{
    DerivedNodeAdapter, DerivedOutput, DerivedTriggerContext, TransducerWrapper,
};
use sinex_node_sdk::{ErrorAction, NodeLogicError, TransducerNode};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::ProcessingContext;
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

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Command
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

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
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
        ErrorAction::SendToProcessingFailureQueue
    }
}

fn make_event_with_value(value: &str) -> std::result::Result<Event<JsonValue>, SinexError> {
    let mut event = DynamicPayload::new("test.source", "test.input", json!({ "value": value }))
        .from_parents([Id::<Event<JsonValue>>::new()])?
        .build()?;
    event.id = Some(event.id.unwrap_or_else(Id::new));
    Ok(event)
}

fn make_event() -> std::result::Result<Event<JsonValue>, SinexError> {
    make_event_with_value("hello")
}

#[sinex_test]
async fn derived_adapter_rejects_missing_trigger_id() -> TestResult<()> {
    let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(PassthroughDerivedNode));
    let mut event = make_event()?;
    event.id = None;

    let err = adapter
        .process_one(event)
        .await
        .expect_err("missing id must fail");
    assert!(format!("{err}").contains("missing an id"));
    Ok(())
}

#[sinex_test]
async fn derived_adapter_uses_runtime_continuous_loop_bridge() -> TestResult<()> {
    let adapter = DerivedNodeAdapter::new(TransducerWrapper(PassthroughDerivedNode));
    let capabilities = sinex_node_sdk::runtime::stream::Node::capabilities(&adapter);

    assert!(capabilities.supports_continuous);
    assert!(!capabilities.manages_own_continuous_loop);

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
async fn derived_adapter_redacts_output_payloads() -> TestResult<()> {
    let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(PassthroughDerivedNode));
    let output = adapter
        .process_one(make_event_with_value(
            "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij",
        )?)
        .await?;
    let output = output
        .into_iter()
        .next()
        .expect("derived node should emit output");

    let value = output.payload["value"]
        .as_str()
        .expect("redacted payload should stay string");
    assert_eq!(value, "<GITHUB_TOKEN>");
    Ok(())
}
