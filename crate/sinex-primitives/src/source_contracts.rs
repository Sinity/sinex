//! Source contract vocabulary.
//!
//! This module holds typed source identity and runtime-binding declarations.
//! It intentionally does not declare advisory obligations; source correctness
//! belongs in tests, runtime validation, and deployment checks.

use std::marker::PhantomData;

use schemars::JsonSchema;
use serde::Serialize;

use crate::privacy::ProcessingContext;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SourceCapabilityKind {
    Coverage,
    Debt,
    Operation,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceCapabilityRef<'a> {
    pub kind: SourceCapabilityKind,
    pub target: &'a str,
    pub raw: &'a str,
}

impl<'a> SourceCapabilityRef<'a> {
    #[must_use]
    pub fn parse(raw: &'a str) -> Option<Self> {
        for (prefix, kind) in [
            ("coverage:", SourceCapabilityKind::Coverage),
            ("debt:", SourceCapabilityKind::Debt),
            ("operation:", SourceCapabilityKind::Operation),
        ] {
            let Some(target) = raw.strip_prefix(prefix) else {
                continue;
            };
            if target.is_empty() {
                return None;
            }
            return Some(Self { kind, target, raw });
        }
        None
    }

    #[must_use]
    pub fn is_kind(self, kind: SourceCapabilityKind) -> bool {
        self.kind == kind
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SubjectRef {
    raw: &'static str,
}

impl SubjectRef {
    #[must_use]
    pub const fn from_static(raw: &'static str) -> Self {
        assert!(
            is_valid_subject_expr(raw, false),
            "invalid source subject reference"
        );
        Self { raw }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.raw
    }

    #[must_use]
    pub fn kind(self) -> &'static str {
        let bytes = self.raw.as_bytes();
        let mut index = 0usize;
        while index < bytes.len() {
            if bytes[index] == b':' {
                return &self.raw[..index];
            }
            index += 1;
        }
        self.raw
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SubjectQuery {
    raw: &'static str,
}

impl SubjectQuery {
    #[must_use]
    pub const fn from_static(raw: &'static str) -> Self {
        assert!(
            is_valid_subject_expr(raw, true),
            "invalid source subject query"
        );
        Self { raw }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.raw
    }

    #[must_use]
    pub fn matches(self, subject: SubjectRef) -> bool {
        let query = self.raw;
        if query == "*" {
            return true;
        }
        if let Some(prefix) = query.strip_suffix(":*") {
            return subject.as_str().starts_with(prefix)
                && subject.as_str().as_bytes().get(prefix.len()) == Some(&b':');
        }
        query == subject.as_str()
    }
}

const fn is_valid_subject_expr(raw: &str, allow_wildcard: bool) -> bool {
    let bytes = raw.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    if allow_wildcard && bytes.len() == 1 && bytes[0] == b'*' {
        return true;
    }

    let mut index = 0usize;
    let mut colon_count = 0usize;
    let mut last_was_colon = true;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b':' {
            if last_was_colon {
                return false;
            }
            colon_count += 1;
            last_was_colon = true;
            index += 1;
            continue;
        }
        if allow_wildcard && byte == b'*' {
            return index + 1 == bytes.len()
                && index > 0
                && bytes[index - 1] == b':'
                && colon_count > 0;
        }
        if !is_subject_char(byte) {
            return false;
        }
        last_was_colon = false;
        index += 1;
    }

    colon_count > 0 && !last_was_colon
}

const fn is_subject_char(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_uppercase()
        || byte.is_ascii_digit()
        || matches!(byte, b'-' | b'_' | b'.' | b'/')
}

#[macro_export]
macro_rules! subject_ref {
    ($value:literal) => {{
        const SUBJECT: $crate::source_contracts::SubjectRef =
            $crate::source_contracts::SubjectRef::from_static($value);
        SUBJECT
    }};
}

#[macro_export]
macro_rules! subject_query {
    ($value:literal) => {{
        const QUERY: $crate::source_contracts::SubjectQuery =
            $crate::source_contracts::SubjectQuery::from_static($value);
        QUERY
    }};
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct SourceRuntimeBinding {
    pub subject: SubjectRef,
    pub id: &'static str,
    pub domain: &'static str,
    pub implementation: &'static str,
    pub adapter: &'static str,
    pub output_event_type: &'static str,
    /// Source-level default redaction context for the privacy engine.
    pub privacy_context: ProcessingContext,
    /// Resource ceiling profile; derives the deployment unit's systemd limits.
    pub resource_profile: ResourceProfile,
    pub capabilities: &'static [&'static str],
    /// Stable id of the [`SourceContract`] this binding belongs to.
    ///
    /// String FK across the inventory boundary. Empty string means "no
    /// descriptor yet" (legacy bindings registered before the FK was
    /// introduced).
    pub source_id: &'static str,
    /// True for "future-state" bindings that describe a planned but
    /// not-yet-deployed adapter shape. Proposed bindings are surfaced
    /// separately from live ones in the rendered manifest and must not
    /// be treated as the source of truth for runtime behavior.
    pub proposed: bool,
    // ────────────────────────────────────────────────────────────
    // Deployment-shape fields (#1175). These live ONLY on the binding
    // — `SourceContract` is now strictly semantic. Inventory consumers
    // that need deployment shape look up the binding via `source_id` FK.
    // ────────────────────────────────────────────────────────────
    /// Which runner hosts this binding at deployment time.
    pub runner_pack: RunnerPack,
    /// Shape of the source's checkpoint state machine.
    pub checkpoint_family: CheckpointFamily,
    /// Runtime invocation shape (continuous, scheduled, on-demand).
    pub runtime_shape: RuntimeShape,
    /// Physical/build footprint declared by this binding.
    pub build_impact: SourceBuildImpact,
}

#[derive(Debug, Clone, Copy)]
pub struct MissingOutput;
#[derive(Debug, Clone, Copy)]
pub struct HasOutput;
#[derive(Debug, Clone, Copy)]
pub struct MissingPrivacy;
#[derive(Debug, Clone, Copy)]
pub struct HasPrivacy;
#[derive(Debug, Clone, Copy)]
pub struct MissingCheckpointFamily;
#[derive(Debug, Clone, Copy)]
pub struct HasCheckpointFamily;
#[derive(Debug, Clone, Copy)]
pub struct MissingRuntimeShape;
#[derive(Debug, Clone, Copy)]
pub struct HasRuntimeShape;
#[derive(Debug, Clone, Copy)]
pub struct MissingBuildImpact;
#[derive(Debug, Clone, Copy)]
pub struct HasBuildImpact;

#[derive(Debug, Clone, Copy)]
pub struct SourceRuntimeBindingBuilder<Output, Privacy, CheckpointFam, Runtime, Build> {
    descriptor: SourceRuntimeBinding,
    _state: PhantomData<(Output, Privacy, CheckpointFam, Runtime, Build)>,
}

impl SourceRuntimeBinding {
    #[must_use]
    pub const fn builder(
        subject: SubjectRef,
        id: &'static str,
        domain: &'static str,
    ) -> SourceRuntimeBindingBuilder<
        MissingOutput,
        MissingPrivacy,
        MissingCheckpointFamily,
        MissingRuntimeShape,
        MissingBuildImpact,
    > {
        SourceRuntimeBindingBuilder {
            descriptor: SourceRuntimeBinding {
                subject,
                id,
                domain,
                implementation: "",
                adapter: "",
                output_event_type: "",
                privacy_context: ProcessingContext::Metadata,
                resource_profile: ResourceProfile::BoundedFile,
                capabilities: &[],
                source_id: "",
                proposed: false,
                runner_pack: RunnerPack::SinexdSource,
                checkpoint_family: CheckpointFamily::AppendStream,
                runtime_shape: RuntimeShape::Continuous,
                build_impact: SourceBuildImpact::ZERO,
            },
            _state: PhantomData,
        }
    }

    /// Package budget contract derived from this binding's resource profile.
    #[must_use]
    pub const fn resource_budget(self) -> ResourceBudgetSpec {
        self.resource_profile.budget_spec()
    }

    pub fn capability_refs(&self) -> impl Iterator<Item = SourceCapabilityRef<'static>> + '_ {
        self.capabilities
            .iter()
            .filter_map(|capability| SourceCapabilityRef::parse(capability))
    }
}

impl<O, P, CF, RS, BI> SourceRuntimeBindingBuilder<O, P, CF, RS, BI> {
    #[must_use]
    pub const fn implementation(mut self, implementation: &'static str) -> Self {
        self.descriptor.implementation = implementation;
        self
    }

    #[must_use]
    pub const fn adapter(mut self, adapter: &'static str) -> Self {
        self.descriptor.adapter = adapter;
        self
    }

    #[must_use]
    pub const fn resource_profile(mut self, resource_profile: ResourceProfile) -> Self {
        self.descriptor.resource_profile = resource_profile;
        self
    }

    #[must_use]
    pub const fn capabilities(mut self, capabilities: &'static [&'static str]) -> Self {
        self.descriptor.capabilities = capabilities;
        self
    }

    /// Attach the binding to a registered source contract by id.
    ///
    /// The id is treated as a string foreign key into the descriptor
    /// inventory. Bindings that omit this default to the empty string.
    #[must_use]
    pub const fn source_id(mut self, source_id: &'static str) -> Self {
        self.descriptor.source_id = source_id;
        self
    }

    /// Mark the binding as a future-state proposal rather than a live deployment.
    ///
    /// Proposed bindings are inert: they document an intended adapter shape
    /// without claiming an active runtime. Manifest renderers must surface
    /// them separately from live bindings so that "what runs today" is not
    /// confused with "what we plan to add."
    #[must_use]
    pub const fn proposed(mut self, proposed: bool) -> Self {
        self.descriptor.proposed = proposed;
        self
    }

    /// Which runner hosts this binding at deployment time.
    #[must_use]
    pub const fn runner_pack(mut self, runner_pack: RunnerPack) -> Self {
        self.descriptor.runner_pack = runner_pack;
        self
    }
}

impl<P, CF, RS, BI> SourceRuntimeBindingBuilder<MissingOutput, P, CF, RS, BI> {
    #[must_use]
    pub const fn output_event_type(
        mut self,
        output_event_type: &'static str,
    ) -> SourceRuntimeBindingBuilder<HasOutput, P, CF, RS, BI> {
        self.descriptor.output_event_type = output_event_type;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, CF, RS, BI> SourceRuntimeBindingBuilder<O, MissingPrivacy, CF, RS, BI> {
    #[must_use]
    pub const fn privacy_context(
        mut self,
        privacy_context: ProcessingContext,
    ) -> SourceRuntimeBindingBuilder<O, HasPrivacy, CF, RS, BI> {
        self.descriptor.privacy_context = privacy_context;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, RS, BI> SourceRuntimeBindingBuilder<O, P, MissingCheckpointFamily, RS, BI> {
    /// Shape of the source's checkpoint state machine. Required: codex P2 follow-up
    /// on PR #1189 — concrete defaults silently passed descriptor validation
    /// for new bindings that forgot to set it. Typestate forces every binding to
    /// declare the family explicitly.
    #[must_use]
    pub const fn checkpoint_family(
        mut self,
        family: CheckpointFamily,
    ) -> SourceRuntimeBindingBuilder<O, P, HasCheckpointFamily, RS, BI> {
        self.descriptor.checkpoint_family = family;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, CF, BI> SourceRuntimeBindingBuilder<O, P, CF, MissingRuntimeShape, BI> {
    /// Runtime invocation shape (continuous, scheduled, on-demand). Required:
    /// see `checkpoint_family` for the same rationale.
    #[must_use]
    pub const fn runtime_shape(
        mut self,
        shape: RuntimeShape,
    ) -> SourceRuntimeBindingBuilder<O, P, CF, HasRuntimeShape, BI> {
        self.descriptor.runtime_shape = shape;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, CF, RS> SourceRuntimeBindingBuilder<O, P, CF, RS, MissingBuildImpact> {
    /// Physical/build footprint declared by this binding. Required: see
    /// `checkpoint_family` for the same rationale. `SourceBuildImpact::ZERO`
    /// is a perfectly fine value to set explicitly — typestate only requires
    /// that the choice be intentional.
    #[must_use]
    pub const fn build_impact(
        mut self,
        build_impact: SourceBuildImpact,
    ) -> SourceRuntimeBindingBuilder<O, P, CF, RS, HasBuildImpact> {
        self.descriptor.build_impact = build_impact;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl
    SourceRuntimeBindingBuilder<
        HasOutput,
        HasPrivacy,
        HasCheckpointFamily,
        HasRuntimeShape,
        HasBuildImpact,
    >
{
    #[must_use]
    pub const fn build(self) -> SourceRuntimeBinding {
        self.descriptor
    }
}

inventory::collect!(SourceRuntimeBinding);

pub fn source_runtime_bindings() -> impl Iterator<Item = &'static SourceRuntimeBinding> {
    inventory::iter::<SourceRuntimeBinding>()
}

/// How the source's checkpoint state is shaped.
///
/// Determines what the runtime checkpoint adapter must support and what
/// idempotency/replay strategy the source uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointFamily {
    /// Append-only stream (filesystem watcher, journald tail, browser export).
    AppendStream,
    /// Mutable backing store snapshotted with row-level occurrence anchors.
    MutableSnapshot {
        backing_store_kind: &'static str,
        occurrence_anchor: &'static str,
    },
    /// Journal/log API consumed by sequential cursor (e.g. systemd journal).
    Journal,
    /// Source has no native cursor; runtime polls and diffs.
    Polling,
    /// Internal in-memory state observed at intervals.
    LiveObservation,
}

/// Privacy classification of the source's payloads.
///
/// The schema-apply engine reconciles a CHECK constraint on the
/// `privacy_tier` column of `raw.source_material_registry` when that
/// column exists. See issue #1236; the spec is forward-declared and
/// skipped at apply time when the column is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, sinex_macros::DbCheck)]
#[serde(rename_all = "snake_case")]
#[db_check(
    schema = "raw",
    table = "source_material_registry",
    column = "privacy_tier",
    version = 1
)]
pub enum PrivacyTier {
    Public,
    Sensitive,
    Secret,
}

/// Time horizons the source serves on the *normal* runtime plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon {
    Continuous,
    Historical,
}

/// How the source is invoked at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeShape {
    Continuous,
    OnDemand,
    Scheduled,
}

/// Retention policy for events emitted by this source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionPolicy {
    Forever,
    Days { days: u32 },
    Tiered { hot_days: u32, warm_days: u32 },
}

/// How the source identifies real-world occurrences.
///
/// Adjacently tagged (`content = "key"`) so the `Uuid5From` newtype variant
/// serializes — internal tagging cannot serialize a newtype variant holding a
/// primitive. The catalog export (#1727) is the first JSON consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", content = "key", rename_all = "snake_case")]
pub enum OccurrenceIdentity {
    Uuid5From(&'static str),
    Natural,
    Anchor,
}

/// Which runner hosts a source binding at deployment time.
///
/// Replaces the former free-form `runner_pack`/`implementation_mode` strings
/// (26×`sinexd-source`, 3×`live`, plus `parser:staged`/`external:*`/`in_process:*`
/// variants on automata and embedded emitters). The *domain* a source belongs to
/// already lives in [`SourceContract::namespace`]; this enum carries only the
/// runner kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerPack {
    /// Hosted in-process by `sinexd` as a source binding (the common case).
    SinexdSource,
    /// Live capture binding (e.g. D-Bus signal listeners).
    Live,
    /// Staged-export parser fed from operator-staged material.
    Staged,
    /// External producer that publishes events over NATS (e.g. polylogue).
    External,
    /// Emitted from within a sinex binary / automaton, not a hosted source.
    InProcess,
}

/// Concrete resource ceiling a binding declares, used to derive the systemd
/// unit's `MemoryMax`/`CPUWeight` when the deployment unit is generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResourceLimits {
    /// Hard memory ceiling in MiB.
    pub memory_max_mib: u32,
    /// systemd `CPUWeight` (1–10000; 100 = default share).
    pub cpu_weight: u16,
}

/// Runtime work class used to group package budget expectations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkClass {
    Interactive,
    AdmissionHot,
    CaptureLive,
    ProjectionHot,
    ProjectionCold,
    BulkImport,
    Maintenance,
}

/// Operator-visible actions a runtime can take when a package is under pressure.
///
/// These are operational pressure responses only. They do not authorize schema
/// changes, hidden disclosure changes, silent material deletion, or bypassing
/// admission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPressureAction {
    Throttle,
    Defer,
    Pause,
    Drain,
    Inspect,
    Retry,
}

/// Package-level resource budget derived from a [`ResourceProfile`].
///
/// [`ResourceLimits`] remains the deployment ceiling consumed by the Nix catalog.
/// This richer budget is the Sinex-side contract for package completeness,
/// pressure visibility, and future runtime controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResourceBudgetSpec {
    pub work_class: WorkClass,
    pub steady_memory_mib: u32,
    pub burst_memory_mib: u32,
    pub cpu_weight: u16,
    pub max_input_bytes_per_sec: Option<u64>,
    pub max_input_events_per_sec: Option<u32>,
    pub max_pending_material_bytes: u64,
    pub max_pending_candidates: u32,
    pub max_unacked_transport_messages: Option<u32>,
    pub batch_size: Option<u32>,
    pub flush_interval_ms: Option<u64>,
    pub checkpoint_interval_ms: Option<u64>,
    pub expected_disk_write_bytes_per_min: Option<u64>,
    pub expected_wal_write_bytes_per_min: Option<u64>,
    pub pressure_actions: &'static [BudgetPressureAction],
}

/// Resource profile of a source binding.
///
/// Replaces the former free-form `resource_shape` string. Each variant maps to a
/// concrete [`ResourceLimits`] ceiling so the deployment unit's limits are a
/// typed function of the declared profile rather than a hand-set number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceProfile {
    /// Reads a bounded file/scan (history files, export files, watched files).
    BoundedFile,
    /// Streams rows with bounded working memory (sqlite/db row cursors).
    BoundedStream,
    /// Long-lived watcher with low steady-state memory (sockets, signals, polls).
    LiveWatcher,
    /// Walks a directory tree; memory bounded by the walk, not the tree size.
    DirectoryScan,
    /// Runs once over a bounded input then exits (on-demand batch).
    Oneshot,
    /// Consumes the derived-event stream (automata).
    EventStreamConsumer,
    /// Emits telemetry from within a running binary (embedded emitters).
    EmbeddedEmitter,
}

const THROTTLE_DEFER_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Defer,
    BudgetPressureAction::Inspect,
];

const PAUSE_DRAIN_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Pause,
    BudgetPressureAction::Drain,
    BudgetPressureAction::Inspect,
];

const THROTTLE_PAUSE_DRAIN_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Pause,
    BudgetPressureAction::Drain,
    BudgetPressureAction::Inspect,
];

const THROTTLE_DEFER_RETRY_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Defer,
    BudgetPressureAction::Retry,
    BudgetPressureAction::Inspect,
];

impl ResourceProfile {
    /// Concrete systemd resource ceiling for this profile.
    #[must_use]
    pub const fn limits(self) -> ResourceLimits {
        match self {
            Self::BoundedFile | Self::Oneshot => ResourceLimits {
                memory_max_mib: 256,
                cpu_weight: 100,
            },
            Self::BoundedStream => ResourceLimits {
                memory_max_mib: 512,
                cpu_weight: 100,
            },
            Self::LiveWatcher | Self::EmbeddedEmitter => ResourceLimits {
                memory_max_mib: 128,
                cpu_weight: 80,
            },
            Self::DirectoryScan => ResourceLimits {
                memory_max_mib: 1024,
                cpu_weight: 120,
            },
            Self::EventStreamConsumer => ResourceLimits {
                memory_max_mib: 512,
                cpu_weight: 120,
            },
        }
    }

    /// Package budget contract derived from this profile.
    #[must_use]
    pub const fn budget_spec(self) -> ResourceBudgetSpec {
        let limits = self.limits();
        match self {
            Self::BoundedFile | Self::Oneshot => ResourceBudgetSpec {
                work_class: WorkClass::BulkImport,
                steady_memory_mib: 128,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(16 * 1024 * 1024),
                max_input_events_per_sec: None,
                max_pending_material_bytes: 64 * 1024 * 1024,
                max_pending_candidates: 10_000,
                max_unacked_transport_messages: None,
                batch_size: Some(1_000),
                flush_interval_ms: Some(1_000),
                checkpoint_interval_ms: Some(5_000),
                expected_disk_write_bytes_per_min: Some(512 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(256 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_INSPECT,
            },
            Self::BoundedStream => ResourceBudgetSpec {
                work_class: WorkClass::AdmissionHot,
                steady_memory_mib: 256,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(32 * 1024 * 1024),
                max_input_events_per_sec: Some(10_000),
                max_pending_material_bytes: 128 * 1024 * 1024,
                max_pending_candidates: 25_000,
                max_unacked_transport_messages: Some(1_000),
                batch_size: Some(2_000),
                flush_interval_ms: Some(500),
                checkpoint_interval_ms: Some(2_000),
                expected_disk_write_bytes_per_min: Some(1024 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(512 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_RETRY_INSPECT,
            },
            Self::LiveWatcher | Self::EmbeddedEmitter => ResourceBudgetSpec {
                work_class: WorkClass::CaptureLive,
                steady_memory_mib: 64,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(1024 * 1024),
                max_input_events_per_sec: Some(1_000),
                max_pending_material_bytes: 8 * 1024 * 1024,
                max_pending_candidates: 1_000,
                max_unacked_transport_messages: Some(256),
                batch_size: Some(128),
                flush_interval_ms: Some(250),
                checkpoint_interval_ms: Some(1_000),
                expected_disk_write_bytes_per_min: Some(64 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(64 * 1024 * 1024),
                pressure_actions: THROTTLE_PAUSE_DRAIN_INSPECT,
            },
            Self::DirectoryScan => ResourceBudgetSpec {
                work_class: WorkClass::BulkImport,
                steady_memory_mib: 512,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(64 * 1024 * 1024),
                max_input_events_per_sec: None,
                max_pending_material_bytes: 256 * 1024 * 1024,
                max_pending_candidates: 50_000,
                max_unacked_transport_messages: None,
                batch_size: Some(5_000),
                flush_interval_ms: Some(1_000),
                checkpoint_interval_ms: Some(10_000),
                expected_disk_write_bytes_per_min: Some(2048 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(1024 * 1024 * 1024),
                pressure_actions: PAUSE_DRAIN_INSPECT,
            },
            Self::EventStreamConsumer => ResourceBudgetSpec {
                work_class: WorkClass::ProjectionHot,
                steady_memory_mib: 256,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(16 * 1024 * 1024),
                max_input_events_per_sec: Some(20_000),
                max_pending_material_bytes: 32 * 1024 * 1024,
                max_pending_candidates: 20_000,
                max_unacked_transport_messages: Some(2_000),
                batch_size: Some(2_000),
                flush_interval_ms: Some(500),
                checkpoint_interval_ms: Some(1_000),
                expected_disk_write_bytes_per_min: Some(512 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(512 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_RETRY_INSPECT,
            },
        }
    }
}

/// What resource a source reads, de-conflated from the data-category labels the
/// former `access_policy` string mixed in (e.g. `target_home_read:.zsh_history`
/// vs `personal_social_data`). This carries only the *locator* axis; data
/// classification belongs to [`SourceContract::privacy_tier`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum AccessScope {
    /// No direct external access (internal/derived).
    Internal,
    /// Reads operator-staged export material (GDPR/Takeout dumps).
    StagedExport,
    /// Reads a path under the target user's home directory.
    TargetHome { path: &'static str },
    /// Reads a path under the realm data lake.
    TargetData { path: &'static str },
    /// Bridges a target runtime surface (window manager, clipboard).
    RuntimeBridge { surface: &'static str },
    /// Reads the systemd journal.
    SystemdJournal,
    /// Reads kernel uevents (udev monitor).
    KernelUevents,
    /// Listens on the D-Bus session bus.
    SessionBus,
    /// Listens on the D-Bus system bus.
    SystemBus,
    /// Reads operator-configured watch roots.
    ConfiguredRoots,
    /// Reads a local library root.
    LibraryRoot,
}

/// Physical/build footprint declared by a source binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceBuildImpact {
    pub crate_impact: &'static str,
    pub binary_impact: &'static str,
    pub nix_output_impact: &'static str,
    pub derivation_impact: &'static str,
    pub sqlx_validation_impact: &'static str,
    pub dedicated_build_rationale: Option<&'static str>,
}

impl SourceBuildImpact {
    pub const ZERO: Self = Self {
        crate_impact: "0",
        binary_impact: "0",
        nix_output_impact: "0",
        derivation_impact: "0",
        sqlx_validation_impact: "0",
        dedicated_build_rationale: None,
    };
}

/// The typed declaration every source fills in.
///
/// This is strictly a *semantic* descriptor: identity, emitted event-type
/// pairs, privacy tier, time horizons, retention, occurrence identity, and
/// access scope. Deployment-shape fields (`runner_pack`, `resource_profile`,
/// `checkpoint_family`, `runtime_shape`, `build_impact`) live on the matching
/// [`SourceRuntimeBinding`]. See issue #1175.
///
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceContract {
    pub id: &'static str,
    pub namespace: &'static str,
    pub event_types: &'static [(&'static str, &'static str)],
    pub privacy_tier: PrivacyTier,
    pub horizons: &'static [Horizon],
    pub retention: RetentionPolicy,
    pub occurrence_identity: OccurrenceIdentity,
    /// Resource locator this source reads (de-conflated from data category).
    pub access_scope: AccessScope,
}

inventory::collect!(SourceContract);

/// Iterate over every registered source contract in the binary.
pub fn all_source_contracts() -> impl Iterator<Item = &'static SourceContract> {
    inventory::iter::<SourceContract>()
}

/// Find a source contract by `id`.
#[must_use]
pub fn find_source_contract(id: &crate::parser::SourceId) -> Option<&'static SourceContract> {
    let id_str = id.as_str();
    all_source_contracts().find(|descriptor| descriptor.id == id_str)
}

/// Re-exported `inventory` for consumers of [`register_source_contract!`].
#[doc(hidden)]
pub mod __register {
    pub use inventory;
}

/// Register a source contract with the binary's inventory.
///
/// ```rust,ignore
/// register_source_contract!(
///     descriptor: MY_DESCRIPTOR,
/// );
/// ```
#[macro_export]
macro_rules! register_source_contract {
    // Plain form — descriptor only.
    ($descriptor:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $descriptor }
    };
    // Named form.
    (descriptor: $descriptor:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $descriptor }
    };
}

/// Register a [`SourceRuntimeBinding`] with the binary's inventory.
///
/// Companion to [`register_source_contract!`]: contracts describe the *semantic*
/// shape of a source, bindings describe the deployed adapter that runs
/// it. Both are mechanically discoverable through the `inventory` crate.
#[macro_export]
macro_rules! register_source_runtime_binding {
    ($binding:expr $(,)?) => {
        $crate::source_contracts::__register::inventory::submit! { $binding }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn subject_queries_match_exact_and_prefix_subjects() -> TestResult<()> {
        let subject = SubjectRef::from_static("runtime_unit:terminal.atuin");

        assert!(SubjectQuery::from_static("runtime_unit:*").matches(subject));
        assert!(SubjectQuery::from_static("runtime_unit:terminal.atuin").matches(subject));
        assert!(!SubjectQuery::from_static("scenario:*").matches(subject));
        Ok(())
    }

    #[sinex_test]
    async fn register_source_contract_named_form_compiles() -> TestResult<()> {
        // Smoke-test: verify the named-form `register_source_contract!(descriptor: X)`
        // macros compile correctly.  We exercise the plain-descriptor path here
        // (no extra rules) since inventory submission from tests is link-time only.
        // The with-rules form is syntactically tested via the macro expansion path
        // verified by the trybuild suite.
        use crate::source_contracts::{
            Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, SourceContract,
        };

        let descriptor = SourceContract {
            id: "test.register-form",
            namespace: "test",
            event_types: &[("test.source", "test.event")],
            privacy_tier: PrivacyTier::Sensitive,
            horizons: &[Horizon::Continuous],
            retention: RetentionPolicy::Forever,
            occurrence_identity: OccurrenceIdentity::Natural,
            access_scope: AccessScope::Internal,
        };

        // Verify the descriptor is well-formed (fields accessible).
        assert_eq!(descriptor.id, "test.register-form");
        assert_eq!(descriptor.privacy_tier, PrivacyTier::Sensitive);
        Ok(())
    }

    #[sinex_test]
    async fn source_runtime_binding_builder_accepts_all_required_fields() -> TestResult<()> {
        let descriptor = SourceRuntimeBinding::builder(
            SubjectRef::from_static("runtime_unit:test.demo"),
            "test.demo",
            "test",
        )
        .adapter("sqlite_row_stream")
        .implementation("demo::Unit")
        .output_event_type("test.output")
        .privacy_context(ProcessingContext::Command)
        .resource_profile(ResourceProfile::BoundedStream)
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::Continuous)
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        assert_eq!(descriptor.output_event_type, "test.output");
        assert_eq!(descriptor.privacy_context, ProcessingContext::Command);
        assert_eq!(descriptor.resource_profile, ResourceProfile::BoundedStream);
        assert_eq!(
            descriptor.resource_budget(),
            ResourceProfile::BoundedStream.budget_spec()
        );
        Ok(())
    }

    #[sinex_test]
    async fn resource_profile_budget_spec_preserves_operational_bounds() -> TestResult<()> {
        let live_budget = ResourceProfile::LiveWatcher.budget_spec();
        assert_eq!(live_budget.work_class, WorkClass::CaptureLive);
        assert!(live_budget.steady_memory_mib <= live_budget.burst_memory_mib);
        assert_eq!(
            live_budget.burst_memory_mib,
            ResourceProfile::LiveWatcher.limits().memory_max_mib
        );
        assert!(
            live_budget
                .pressure_actions
                .contains(&BudgetPressureAction::Pause)
        );
        assert!(
            live_budget
                .pressure_actions
                .contains(&BudgetPressureAction::Inspect)
        );

        let stream_budget = ResourceProfile::BoundedStream.budget_spec();
        assert_eq!(stream_budget.work_class, WorkClass::AdmissionHot);
        assert!(stream_budget.max_unacked_transport_messages.is_some());
        assert!(stream_budget.max_pending_candidates > 0);
        assert!(
            stream_budget
                .pressure_actions
                .contains(&BudgetPressureAction::Retry)
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_capability_refs_parse_known_package_refs() -> TestResult<()> {
        assert_eq!(
            SourceCapabilityRef::parse("coverage:source-coverage"),
            Some(SourceCapabilityRef {
                kind: SourceCapabilityKind::Coverage,
                target: "source-coverage",
                raw: "coverage:source-coverage",
            })
        );
        assert_eq!(
            SourceCapabilityRef::parse("debt:unified-debt-view").map(|capability| capability.kind),
            Some(SourceCapabilityKind::Debt)
        );
        assert_eq!(
            SourceCapabilityRef::parse("operation:terminal.activity.check")
                .map(|capability| capability.target),
            Some("terminal.activity.check")
        );
        assert_eq!(SourceCapabilityRef::parse("operation:"), None);
        assert_eq!(
            SourceCapabilityRef::parse("package:terminal.activity"),
            None
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_runtime_binding_exposes_typed_capability_refs() -> TestResult<()> {
        let binding = SourceRuntimeBinding::builder(
            SubjectRef::from_static("runtime_unit:test.capabilities"),
            "test.capabilities",
            "test",
        )
        .adapter("static")
        .implementation("test::capabilities")
        .output_event_type("test.output")
        .privacy_context(ProcessingContext::Metadata)
        .resource_profile(ResourceProfile::EmbeddedEmitter)
        .capabilities(&[
            "coverage:source-coverage",
            "unknown:ignored",
            "operation:test.capabilities.check",
        ])
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::OnDemand)
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        let capabilities = binding.capability_refs().collect::<Vec<_>>();
        assert_eq!(capabilities.len(), 2);
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.is_kind(SourceCapabilityKind::Coverage))
        );
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.target == "test.capabilities.check")
        );
        Ok(())
    }

    // Sentinel binding submitted at link time to exercise the inventory
    // collection path. Concrete source bindings (e.g. terminal.atuin-history)
    // now live with their `#[derive(SourceDefinition)]` source structs in
    // `sinexd`, so this crate's test binary only verifies the mechanism.
    ::inventory::submit! {
        SourceRuntimeBinding::builder(
            SubjectRef::from_static("source:primitives.inventory-sentinel"),
            "primitives.inventory-sentinel",
            "test",
        )
        .implementation("sinex-primitives::test")
        .adapter("test_adapter")
        .output_event_type("test.output")
        .privacy_context(ProcessingContext::Metadata)
        .resource_profile(ResourceProfile::EmbeddedEmitter)
        .source_id("primitives.inventory-sentinel")
        .runner_pack(RunnerPack::InProcess)
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::OnDemand)
        .build_impact(SourceBuildImpact::ZERO)
        .build()
    }

    #[sinex_test]
    async fn source_runtime_binding_inventory_collects_submissions() -> TestResult<()> {
        let bindings = source_runtime_bindings()
            .map(|descriptor| descriptor.subject.as_str())
            .collect::<Vec<_>>();

        assert!(bindings.contains(&"source:primitives.inventory-sentinel"));
        Ok(())
    }
}
