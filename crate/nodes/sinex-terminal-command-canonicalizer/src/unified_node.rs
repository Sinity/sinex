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

        // 1:1 transform: ts_orig from input, single parent
        let ts_orig = context.require_ts_orig()?;
        let Some(mut payload) = canonicalize_payload(context.source.as_str(), input, ts_orig)?
        else {
            return Ok(None);
        };
        info!("Canonicalizing command: {}", payload.command);
        payload.source_events = vec![context.trigger_uuid().to_string()];

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

fn canonicalize_payload(
    source: &str,
    input: JsonValue,
    ts_orig: sinex_primitives::Timestamp,
) -> Result<Option<CanonicalCommandPayload>, NodeLogicError> {
    match source {
        source if source == KittyCommandExecutedPayload::SOURCE.as_static_str() => {
            let payload = parse_payload::<KittyCommandExecutedPayload>(input, source)?;
            canonicalize_kitty(payload, ts_orig)
        }
        source if source == AtuinCommandExecutedPayload::SOURCE.as_static_str() => {
            let payload = parse_payload::<AtuinCommandExecutedPayload>(input, source)?;
            canonicalize_atuin(payload)
        }
        source if source == BashCommandExecutedPayload::SOURCE.as_static_str() => {
            let payload = parse_payload::<BashCommandExecutedPayload>(input, source)?;
            canonicalize_history(
                payload.command.to_string(),
                payload.working_directory,
                payload.exit_code,
                payload.duration_ms,
                payload.user,
                payload.session_id,
                payload.environment_hash,
                ts_orig,
            )
        }
        source if source == ZshCommandExecutedPayload::SOURCE.as_static_str() => {
            let payload = parse_payload::<ZshCommandExecutedPayload>(input, source)?;
            canonicalize_history(
                payload.command.to_string(),
                payload.working_directory,
                payload.exit_code,
                payload.duration_ms,
                payload.user,
                payload.session_id,
                payload.environment_hash,
                ts_orig,
            )
        }
        source if source == FishCommandExecutedPayload::SOURCE.as_static_str() => {
            let payload = parse_payload::<FishCommandExecutedPayload>(input, source)?;
            canonicalize_history(
                payload.command.to_string(),
                payload.working_directory,
                payload.exit_code,
                payload.duration_ms,
                payload.user,
                payload.session_id,
                payload.environment_hash,
                ts_orig,
            )
        }
        _ => Ok(None),
    }
}

fn parse_payload<T>(input: JsonValue, source: &str) -> Result<T, NodeLogicError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(input).map_err(|error| {
        NodeLogicError::InputParsing(format!(
            "failed to parse {source} command.executed payload: {error}"
        ))
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Signature symmetry with the fallible canonicalize_atuin for match-arm uniformity"
)]
fn canonicalize_kitty(
    payload: KittyCommandExecutedPayload,
    ts_orig: sinex_primitives::Timestamp,
) -> Result<Option<CanonicalCommandPayload>, NodeLogicError> {
    let command = payload.command.to_string();
    if command.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(CanonicalCommandPayload {
        command,
        working_directory: payload.working_directory.map(|path| path.to_string()),
        exit_code: payload.exit_status,
        duration_ms: payload.execution_time_ms,
        start_time: ts_orig,
        end_time: ts_orig,
        user: None,
        session_id: None,
        environment_hash: None,
        source_events: Vec::new(),
        enrichment_history: Vec::new(),
    }))
}

fn canonicalize_atuin(
    payload: AtuinCommandExecutedPayload,
) -> Result<Option<CanonicalCommandPayload>, NodeLogicError> {
    let command = payload.command_string.to_string();
    if command.trim().is_empty() {
        return Ok(None);
    }

    let duration_nanos = payload.duration_ns.as_nanos();
    let duration_ms = u64::try_from(duration_nanos / 1_000_000).map_err(|error| {
        NodeLogicError::InputParsing(format!(
            "shell.atuin command duration is too large to represent in milliseconds: {error}"
        ))
    })?;

    Ok(Some(CanonicalCommandPayload {
        command,
        working_directory: Some(payload.cwd.to_string()),
        exit_code: Some(payload.exit_code),
        duration_ms: Some(duration_ms),
        start_time: payload.ts_start_orig,
        end_time: payload.ts_end_orig,
        user: None,
        session_id: normalize_optional_string(Some(payload.atuin_session_id)),
        environment_hash: None,
        source_events: Vec::new(),
        enrichment_history: Vec::new(),
    }))
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(value)
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Signature symmetry with the fallible canonicalize_atuin for match-arm uniformity"
)]
fn canonicalize_history(
    command: String,
    working_directory: Option<sinex_primitives::domain::RecordedPath>,
    exit_code: Option<sinex_primitives::units::ExitCode>,
    duration_ms: Option<u64>,
    user: Option<String>,
    session_id: Option<String>,
    environment_hash: Option<String>,
    ts_orig: sinex_primitives::Timestamp,
) -> Result<Option<CanonicalCommandPayload>, NodeLogicError> {
    if command.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(CanonicalCommandPayload {
        command,
        working_directory: working_directory.map(|path| path.to_string()),
        exit_code,
        duration_ms,
        start_time: ts_orig,
        end_time: ts_orig,
        user: normalize_optional_string(user),
        session_id: normalize_optional_string(session_id),
        environment_hash: normalize_optional_string(environment_hash),
        source_events: Vec::new(),
        enrichment_history: Vec::new(),
    }))
}

/// Node type alias for use with `node_entrypoint!`.
pub type TerminalCommandCanonicalizerNode = TransducerNodeAdapter<TerminalCommandCanonicalizer>;
