#![doc = include_str!("../docs/unified_node.md")]

//! Terminal command canonicalizer — [`TransducerNode`] implementation.
//!
//! Model classification: **Transducer** — stateless 1:1 transform that inherits
//! `ts_orig` from the input event. Each input `command.executed` produces exactly
//! zero or one `command.canonical` output.
//!
//! The spec's "expected mapping" suggested `ScopeReconcilerNode`, but the actual
//! processing logic is a pure per-event transform with no accumulated scope state.
//! If future replay invalidation requires scope-based targeting, this node can be
//! upgraded to `ScopeReconcilerNode` with `scope_keys()` derived from `session_id`.

use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext, TransducerNodeAdapter};
use sinex_node_sdk::{NodeLogicError, TransducerNode};
use sinex_primitives::JsonValue;
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    AtuinCommandExecutedPayload, BashCommandExecutedPayload, CanonicalCommandPayload,
    FishCommandExecutedPayload, KittyCommandExecutedPayload, ZshCommandExecutedPayload,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::now;
use tracing::info;

#[derive(Default)]
pub struct TerminalCommandCanonicalizer;

impl TerminalCommandCanonicalizer {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TransducerNode for TerminalCommandCanonicalizer {
    type State = ();
    type Input = JsonValue;
    type Output = CanonicalCommandPayload;

    fn name(&self) -> &'static str {
        "terminal-command-canonicalizer"
    }

    fn input_event_type(&self) -> &'static str {
        KittyCommandExecutedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        CanonicalCommandPayload::EVENT_TYPE.as_static_str()
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Command
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        if !is_accepted_source(context.source.as_str()) {
            return Ok(None);
        }

        let command = input
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if command.trim().is_empty() {
            return Ok(None);
        }

        info!("Canonicalizing command: {}", command);

        // 1:1 transform: ts_orig from input, single parent
        let ts_orig = context.ts_orig.unwrap_or_else(now);

        let payload = CanonicalCommandPayload {
            command: command.to_string(),
            working_directory: input
                .get("working_directory")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            exit_code: sinex_primitives::units::ExitCode::from_raw(
                input
                    .get("exit_code")
                    .and_then(sinex_primitives::JsonValue::as_i64)
                    .unwrap_or(0) as i32,
            ),
            duration_ms: input
                .get("duration_ms")
                .and_then(sinex_primitives::JsonValue::as_u64)
                .unwrap_or(0),
            start_time: ts_orig,
            end_time: input
                .get("end_time")
                .and_then(|v| v.as_str())
                .and_then(|s| {
                    sinex_primitives::temporal::parse_rfc3339(s)
                        .inspect_err(|e| {
                            tracing::warn!(original = %s, error = %e, "Failed to parse end_time, using event timestamp");
                        })
                        .ok()
                })
                .unwrap_or(ts_orig),
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
            source_events: vec![context.trigger_uuid().to_string()],
            enrichment_history: Vec::new(),
        };

        Ok(Some(
            DerivedOutput::transduced(payload, ts_orig, context.trigger_uuid())
                .with_temporal_policy(SyntheticTemporalPolicy::InheritParent),
        ))
    }
}

/// Returns `true` for sources whose `command.executed` events this node canonicalizes.
fn is_accepted_source(source: &str) -> bool {
    source == KittyCommandExecutedPayload::SOURCE.as_static_str()
        || source == AtuinCommandExecutedPayload::SOURCE.as_static_str()
        || source == BashCommandExecutedPayload::SOURCE.as_static_str()
        || source == ZshCommandExecutedPayload::SOURCE.as_static_str()
        || source == FishCommandExecutedPayload::SOURCE.as_static_str()
}

/// Node type alias for use with `node_entrypoint!`.
pub type TerminalCommandCanonicalizerNode = TransducerNodeAdapter<TerminalCommandCanonicalizer>;
