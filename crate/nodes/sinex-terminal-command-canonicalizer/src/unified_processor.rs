#![doc = include_str!("../docs/unified_processor.md")]

//! Modernized `SimpleNode` implementation for the terminal command canonicalizer.

use async_trait::async_trait;
use sinex_core::events::CanonicalCommandPayload;
use sinex_core::JsonValue;
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};
use tracing::info;

#[derive(Default)]
pub struct TerminalCommandCanonicalizer;

impl TerminalCommandCanonicalizer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SimpleNode for TerminalCommandCanonicalizer {
    type State = ();
    type Input = JsonValue;
    type Output = CanonicalCommandPayload;

    fn name(&self) -> &'static str {
        "terminal-command-canonicalizer"
    }

    fn input_event_type(&self) -> &'static str {
        "command.executed"
    }

    fn output_event_type(&self) -> &'static str {
        "command.canonical"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        match context.source.as_str() {
            "shell.kitty" | "shell.atuin" | "shell.history.bash" | "shell.history.zsh"
            | "shell.history.fish" => {}
            _ => return Ok(None),
        }

        let command = input
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if command.trim().is_empty() {
            return Ok(None);
        }

        info!("Canonicalizing command: {}", command);

        Ok(Some(CanonicalCommandPayload {
            command: command.to_string(),
            working_directory: input
                .get("working_directory")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            exit_code: input.get("exit_code").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            duration_ms: input
                .get("duration_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            start_time: context.ts_orig.unwrap_or_else(chrono::Utc::now),
            end_time: input
                .get("end_time")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(context.ts_orig.unwrap_or_else(chrono::Utc::now)),
            user: input
                .get("user")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            session_id: input
                .get("session_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            environment_hash: input
                .get("environment_hash")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            source_events: vec![context.event_id.to_string()],
            enrichment_history: Vec::new(),
        }))
    }
}

pub type TerminalCommandCanonicalizerNode = SimpleNodeWrapper<TerminalCommandCanonicalizer>;
