//! Terminal command canonicalizer — [`Transducer`] implementation.
//!
//! Model classification: **Transducer** — stateless 1:1 transform that inherits
//! `ts_orig` from the input event. Each input `command.executed` produces exactly
//! zero or one `command.canonical` output.
//!
//! The spec's "expected mapping" suggested `ScopeReconciler`, but the actual
//! processing logic is a pure per-event transform with no accumulated scope state.
//! If future work needs late-arrival correction or richer cross-source context,
//! that should be a downstream scope reconciler keyed by session/activity scope
//! rather than widening `command.canonical` itself into a reconciled object.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Transducer};
use sinex_primitives::JsonValue;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    AtuinCommandExecutedPayload, BashCommandExecutedPayload, CanonicalCommandPayload,
    FishCommandExecutedPayload, KittyCommandExecutedPayload, ZshCommandExecutedPayload,
};
use tracing::info;

/// Derivation control-plane declaration for `canonicalizer` (sinex-0vx.1/0vx.3).
///
/// `canonical_derived_event`: a deterministic 1:1 transform of an admitted
/// shell-history event, default-eligible as input to further canonical
/// derivation (e.g. `document-parser` reads `command.canonical`).
pub const CANONICALIZER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "canonicalizer.command.canonical",
        owner: "canonicalizer",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("canonical.terminal"),
        output_event_type: Some("command.canonical"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Covered,
            ClaimTemporalQuality::InheritParent,
        ),
        verification_command: "xtask test -p sinexd -E 'test(canonicalizer)'",
    }];

#[derive(Default)]
pub struct TerminalCommandCanonicalizer;

impl TerminalCommandCanonicalizer {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Transducer for TerminalCommandCanonicalizer {
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

    fn output_event_source(&self) -> &'static str {
        CanonicalCommandPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        CANONICALIZER_OUTPUT_DECLARATIONS;

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
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

        let declaration = &CANONICALIZER_OUTPUT_DECLARATIONS[0];
        Ok(Some(
            DerivedOutput::transduced(payload, ts_orig, context.trigger_uuid())
                .with_temporal_policy(SyntheticTemporalPolicy::InheritParent)
                .with_declaration_id(declaration.declaration_id)
                .with_product_class(declaration.product_class)
                .with_claim_support(declaration.default_support.instantiate(1, 0, 1, 0)),
        ))
    }
}

/// Returns `true` for sources whose `command.executed` events this automaton canonicalizes.
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
) -> Result<Option<CanonicalCommandPayload>, AutomatonLogicError> {
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

fn parse_payload<T>(input: JsonValue, source: &str) -> Result<T, AutomatonLogicError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(input).map_err(|error| {
        AutomatonLogicError::InputParsing(format!(
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
) -> Result<Option<CanonicalCommandPayload>, AutomatonLogicError> {
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
) -> Result<Option<CanonicalCommandPayload>, AutomatonLogicError> {
    let command = payload.command_string.to_string();
    if command.trim().is_empty() {
        return Ok(None);
    }

    let duration_nanos = payload.duration_ns.as_nanos();
    let duration_ms = u64::try_from(duration_nanos / 1_000_000).map_err(|error| {
        AutomatonLogicError::InputParsing(format!(
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
) -> Result<Option<CanonicalCommandPayload>, AutomatonLogicError> {
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

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type TerminalCommandCanonicalizerRuntime = TransducerAdapter<TerminalCommandCanonicalizer>;

// --- Source descriptor (issue #690 / #734) ---

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// The terminal canonicalizer transduces shell-history events into normalized
// `command.canonical` outputs.
register_source_contract! {
    SourceContract {
        id: "terminal-canonicalizer",
        namespace: "derived",
        event_types: &[
            ("canonical.terminal", "command.canonical"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, parent_event_id)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal-canonicalizer"),
        "terminal-canonicalizer",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("command.canonical")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("terminal-canonicalizer")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
