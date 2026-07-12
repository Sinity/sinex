//! Session detector -- [`Windowed`] implementation.
//!
//! Model classification: **Windowed** -- accumulates bounded
//! `activity.window.summary` events into completed activity sessions.

use crate::runtime::automaton::{
    AutomatonContext, DerivedAggregationMeta, DerivedOutput, WindowedAdapter,
};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Windowed};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use sinex_primitives::activity::{ActivitySourceKind, primary_activity_source};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::events::{
    EventPayload,
    payloads::{
        ActivitySessionBoundaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
    },
};
use sinex_primitives::temporal::Timestamp;
use std::collections::{BTreeMap, BTreeSet};
use tracing::warn;

/// Occurrence-stable identity for an `activity.session.boundary`, derived from
/// the first contributing window's occurrence-stable id (sinex-ecy). Inherits
/// the window's stability; `session_counter` is a display ordinal only. The old
/// `session-{counter}` keys collided across replay/checkpoint-reset; the
/// `activity-session:` format never collides with them, so the migration causes
/// no false suppression.
fn session_occurrence_key(first_window_id: Option<&str>) -> String {
    match first_window_id {
        Some(id) => format!("activity-session:{id}"),
        None => "activity-session:unknown".to_string(),
    }
}

/// Persistent window state tracking the current activity session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionState {
    /// Start time of the current session.
    pub session_start: Option<Timestamp>,

    /// End time of the most recent window included in the session.
    pub last_window_end: Option<Timestamp>,

    /// Number of raw activity events represented by the session.
    pub event_count: u64,

    /// Number of bounded activity windows represented by the session.
    pub window_count: u64,

    /// Unique raw event sources observed in the current session.
    pub sources: BTreeSet<String>,

    /// Logical activity sources observed in the current session.
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,

    /// `UUIDv7` IDs of contributing `activity.window.summary` events.
    pub window_event_ids: Vec<Uuid>,

    /// Session counter — a display ordinal only, never occurrence identity
    /// (sinex-ecy: counter keys collide across replay/checkpoint-reset).
    pub session_counter: u64,

    /// Occurrence-stable id of the FIRST contributing window — the session's
    /// occurrence anchor (sinex-ecy). Inherits the window's occurrence stability.
    #[serde(default)]
    pub first_window_id: Option<String>,

    /// Whether the current session has received a gap-closed final window.
    #[serde(default)]
    pub session_complete: bool,
}

impl SessionState {
    /// Reset state for a new session, preserving the counter.
    fn reset_session(&mut self) {
        self.session_start = None;
        self.last_window_end = None;
        self.event_count = 0;
        self.window_count = 0;
        self.sources.clear();
        self.activity_source_counts.clear();
        self.window_event_ids.clear();
        self.first_window_id = None;
        self.session_complete = false;
    }
}

/// Maximum number of windows accumulated in a single session before a
/// force-emit. Prevents unbounded memory growth during sustained
/// `MaxDuration`/`MaxEventCount` activity (no 5-minute gap).
///
/// At typical analytics rates (~1 window per minute) this allows ~7 days of
/// continuous activity before force-closing. The force-close emits a partial
/// session with a `warn!` log so the truncation is visible.
pub const MAX_SESSION_WINDOW_COUNT: u64 = 10_000;

/// Derivation control-plane declaration for `session` (sinex-0vx.1/0vx.3).
pub const SESSION_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "session.activity.session.boundary",
        owner: "session",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("derived.session-detector"),
        output_event_type: Some("activity.session.boundary"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "2.0.0",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Convergent,
            SourceCoverage::Covered,
            ClaimTemporalQuality::WindowBoundary,
        ),
        verification_command: "xtask test -p sinexd -E 'test(session)'",
    }];

#[derive(Default)]
pub struct SessionDetector;

impl Windowed for SessionDetector {
    type State = SessionState;
    type Input = ActivityWindowSummaryPayload;
    type Output = ActivitySessionBoundaryPayload;

    fn name(&self) -> &'static str {
        "session-detector"
    }

    fn input_event_type(&self) -> &'static str {
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        ActivitySessionBoundaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        ActivitySessionBoundaryPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        SESSION_OUTPUT_DECLARATIONS;

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<(), AutomatonLogicError> {
        if state.session_start.is_none() {
            state.session_start = Some(input.window_start);
            state.first_window_id = Some(input.window_id.clone());
        }

        state.last_window_end = Some(input.window_end);
        state.event_count += input.event_count;
        state.window_count += 1;
        state.sources.extend(input.sources);
        for (source, count) in input.activity_source_counts {
            *state.activity_source_counts.entry(source).or_insert(0) += count;
        }
        state.window_event_ids.push(context.trigger_uuid());

        // A `Gap`-closed window signals the end of the activity session.
        let gap_closed = matches!(input.close_reason, ActivityWindowCloseReason::Gap);

        // Force-close when the accumulator exceeds the safety cap, regardless
        // of close_reason. This bounds memory under sustained activity that
        // never produces a 5-minute gap (MaxDuration/MaxEventCount windows
        // without a Gap — the same leak class as the historical relation-extractor
        // 4.5 GB bug). Silent truncation is forbidden; warn on force-close.
        if !gap_closed && state.window_count >= MAX_SESSION_WINDOW_COUNT {
            warn!(
                module = "session-detector",
                window_count = state.window_count,
                max = MAX_SESSION_WINDOW_COUNT,
                session_start = ?state.session_start,
                "Session window cap exceeded; force-closing session to bound accumulator memory"
            );
            state.session_complete = true;
        } else {
            state.session_complete = gap_closed;
        }

        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        state.session_complete && state.window_count > 0
    }

    /// Clock-driven closure (sinex-5s6): a backstop for a trailing session whose
    /// final contributing window did not carry a `Gap` close (e.g. a
    /// `MaxDuration`/`MaxEventCount` window) and is then followed by silence.
    /// Uses the same window gap threshold — a gap in activity long enough to
    /// close a window also ends the session. `watermark` is the adapter's
    /// two-mode input-time watermark (wall clock when live, max input `ts_orig`
    /// during replay/backfill). `emit` guards only on `session_start`, so no
    /// state mutation is needed here.
    fn flush_due(&self, state: &Self::State, watermark: Timestamp) -> bool {
        if state.window_count == 0 || state.session_complete {
            return false;
        }
        state.last_window_end.is_some_and(|last| {
            (watermark - last) >= crate::automata::analytics::window_gap_threshold()
        })
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let Some(start_time) = state.session_start else {
            return Ok(None);
        };

        let end_time = state.last_window_end.unwrap_or(start_time);
        let duration_secs = (end_time - start_time).whole_seconds().max(0) as u64;

        state.session_counter += 1;
        let session_id = session_occurrence_key(state.first_window_id.as_deref());

        let sources: Vec<String> = state.sources.iter().cloned().collect();
        let activity_sources: Vec<ActivitySourceKind> =
            state.activity_source_counts.keys().copied().collect();
        let primary_source = primary_activity_source(&state.activity_source_counts);
        let source_event_ids = std::mem::take(&mut state.window_event_ids);
        let event_count = state.event_count;
        let window_count = state.window_count;

        let payload = ActivitySessionBoundaryPayload {
            session_id: session_id.clone(),
            start_time,
            end_time,
            duration_secs,
            event_count,
            window_count,
            source_count: state.sources.len() as u64,
            sources,
            activity_sources,
            activity_source_counts: state.activity_source_counts.clone(),
            primary_source,
        };

        let declaration = &SESSION_OUTPUT_DECLARATIONS[0];
        let output = DerivedOutput::windowed(payload, end_time, source_event_ids)
            .with_temporal_policy(sinex_primitives::domain::SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version("2.0.0")
            .with_equivalence_key(session_id)
            .with_aggregation(DerivedAggregationMeta::new(
                "activity.session",
                1,
                event_count,
            ))
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            .with_claim_support(declaration.default_support.instantiate(
                event_count as u32,
                0,
                window_count as u32,
                0,
            ));

        state.reset_session();
        Ok(Some(output))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type SessionDetectorRuntime = WindowedAdapter<SessionDetector>;

// --- Source descriptor (issue #690 / #734) ---

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// Session detector consumes activity-window summaries and emits session
// boundary events when the inactivity gap closes the current window.
register_source_contract! {
    SourceContract {
        id: "session-detector",
        namespace: "derived",
        event_types: &[
            ("derived.session-detector", "activity.session.boundary"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(first_contributing_window_occurrence_key)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:session-detector"),
        "session-detector",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("activity.session.boundary")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("session-detector")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
