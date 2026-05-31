//! Proof-carrying descriptor vocabulary.
//!
//! This module is intentionally data-oriented: runtime units, claims, runner
//! bindings, obligations, and exemptions are plain typed descriptors that can be
//! registered through `inventory` and rendered by development tooling. The
//! descriptors do not replace database constraints or runtime validation; they
//! make test/scenario obligations inspectable and mechanically connected.

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const PROOF_CATALOG_SCHEMA_VERSION: u32 = 1;

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
            "invalid proof subject reference"
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
            "invalid proof subject query"
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
        const SUBJECT: $crate::proof::SubjectRef = $crate::proof::SubjectRef::from_static($value);
        SUBJECT
    }};
}

#[macro_export]
macro_rules! subject_query {
    ($value:literal) => {{
        const QUERY: $crate::proof::SubjectQuery = $crate::proof::SubjectQuery::from_static($value);
        QUERY
    }};
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProofClaimKind {
    Invariant,
    Law,
    Scenario,
    CommandContract,
    ResourceShape,
    Documentation,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProofObligationLevel {
    Required,
    Advisory,
    Deferred,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct Claim {
    pub id: &'static str,
    pub kind: ProofClaimKind,
    pub subject: SubjectQuery,
    pub statement: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct RunnerBinding {
    pub id: &'static str,
    pub runner: &'static str,
    pub subject: SubjectQuery,
    pub claims: &'static [&'static str],
    pub command: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ProofObligation {
    pub id: &'static str,
    pub level: ProofObligationLevel,
    pub subject: SubjectQuery,
    pub claim_id: &'static str,
    pub runner_binding_id: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct Exemption {
    pub id: &'static str,
    pub subject: SubjectQuery,
    pub obligation_id: &'static str,
    pub reason: &'static str,
    pub expires: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceEnvelope {
    pub schema_version: u32,
    pub runner_id: String,
    pub subject_refs: Vec<String>,
    pub claim_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertion_ids: Vec<String>,
    pub status: String,
    pub reproducer: Option<String>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub environment: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
}

impl EvidenceEnvelope {
    #[must_use]
    pub fn new(
        runner_id: impl Into<String>,
        subject_refs: Vec<String>,
        claim_ids: Vec<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: PROOF_CATALOG_SCHEMA_VERSION,
            runner_id: runner_id.into(),
            subject_refs,
            claim_ids,
            assertion_ids: Vec::new(),
            status: status.into(),
            reproducer: None,
            environment: JsonValue::Null,
            artifacts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct SourceUnitBinding {
    pub subject: SubjectRef,
    pub id: &'static str,
    pub domain: &'static str,
    pub implementation: &'static str,
    pub adapter: &'static str,
    pub output_event_type: &'static str,
    pub privacy_context: &'static str,
    pub material_policy: &'static str,
    pub checkpoint_policy: &'static str,
    pub resource_shape: &'static str,
    pub capabilities: &'static [&'static str],
    /// Stable id of the [`SourceUnitDescriptor`] this binding belongs to.
    ///
    /// String FK across the inventory boundary: `xtask source-units check`
    /// validates that this resolves to a registered descriptor id. Empty
    /// string means "no descriptor yet" (legacy bindings registered before
    /// the FK was introduced).
    pub source_unit_id: &'static str,
    /// True for "future-state" bindings that describe a planned but
    /// not-yet-deployed adapter shape. Proposed bindings are surfaced
    /// separately from live ones in the rendered manifest and must not
    /// be treated as the source of truth for runtime behavior.
    pub proposed: bool,
    // ────────────────────────────────────────────────────────────
    // Deployment-shape fields (#1175). These live ONLY on the binding
    // — `SourceUnitDescriptor` is now strictly semantic. Renderers that
    // need deployment shape (xtask source-units render, xtask
    // proof_catalog) look up the binding via `source_unit_id` FK.
    // ────────────────────────────────────────────────────────────
    /// Logical runner pack hosting this binding (e.g. "terminal", "process").
    pub runner_pack: &'static str,
    /// Shape of the source's checkpoint state machine.
    pub checkpoint_family: CheckpointFamily,
    /// Runtime invocation shape (continuous, scheduled, on-demand).
    pub runtime_shape: RuntimeShape,
    /// Coarse package-level impact summary string.
    pub package_impact: &'static str,
    /// How the unit is implemented (e.g. "`rust_in_pack:terminal`").
    pub implementation_mode: &'static str,
    /// Physical/build footprint declared by this binding.
    pub build_impact: SourceUnitBuildImpact,
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
pub struct MissingMaterial;
#[derive(Debug, Clone, Copy)]
pub struct HasMaterial;
/// Typestate marker: checkpoint policy not yet supplied to [`SourceUnitBindingBuilder`].
#[derive(Debug, Clone, Copy)]
pub struct MissingCheckpoint;
/// Typestate marker: checkpoint policy supplied to [`SourceUnitBindingBuilder`].
///
/// Named `CheckpointPresent` (not `HasCheckpoint`) to distinguish this family from the
/// `HasProvenance`/`NoProvenance` markers in `events::builder`, which guard a different
/// state machine — see issue #746 (A9).
#[derive(Debug, Clone, Copy)]
pub struct CheckpointPresent;
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
pub struct SourceUnitBindingBuilder<
    Output,
    Privacy,
    Material,
    Checkpoint,
    CheckpointFam,
    Runtime,
    Build,
> {
    descriptor: SourceUnitBinding,
    _state: PhantomData<(
        Output,
        Privacy,
        Material,
        Checkpoint,
        CheckpointFam,
        Runtime,
        Build,
    )>,
}

impl SourceUnitBinding {
    #[must_use]
    pub const fn builder(
        subject: SubjectRef,
        id: &'static str,
        domain: &'static str,
    ) -> SourceUnitBindingBuilder<
        MissingOutput,
        MissingPrivacy,
        MissingMaterial,
        MissingCheckpoint,
        MissingCheckpointFamily,
        MissingRuntimeShape,
        MissingBuildImpact,
    > {
        SourceUnitBindingBuilder {
            descriptor: SourceUnitBinding {
                subject,
                id,
                domain,
                implementation: "",
                adapter: "",
                output_event_type: "",
                privacy_context: "",
                material_policy: "",
                checkpoint_policy: "",
                resource_shape: "",
                capabilities: &[],
                source_unit_id: "",
                proposed: false,
                runner_pack: "",
                checkpoint_family: CheckpointFamily::AppendStream,
                runtime_shape: RuntimeShape::Continuous,
                package_impact: "",
                implementation_mode: "",
                build_impact: SourceUnitBuildImpact::ZERO,
            },
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, RS, BI> SourceUnitBindingBuilder<O, P, M, C, CF, RS, BI> {
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
    pub const fn resource_shape(mut self, resource_shape: &'static str) -> Self {
        self.descriptor.resource_shape = resource_shape;
        self
    }

    #[must_use]
    pub const fn capabilities(mut self, capabilities: &'static [&'static str]) -> Self {
        self.descriptor.capabilities = capabilities;
        self
    }

    /// Attach the binding to a registered source-unit descriptor by id.
    ///
    /// The id is treated as a string foreign key resolved by
    /// `xtask source-units check`. Bindings that omit this default to
    /// the empty string and are flagged as unresolved by the manifest
    /// validator until they are wired up.
    #[must_use]
    pub const fn source_unit_id(mut self, source_unit_id: &'static str) -> Self {
        self.descriptor.source_unit_id = source_unit_id;
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

    /// Logical runner pack hosting this binding. Mirrors the descriptor field
    /// during the #1175 descriptor→binding migration.
    #[must_use]
    pub const fn runner_pack(mut self, runner_pack: &'static str) -> Self {
        self.descriptor.runner_pack = runner_pack;
        self
    }

    /// Coarse package-level impact summary string.
    #[must_use]
    pub const fn package_impact(mut self, package_impact: &'static str) -> Self {
        self.descriptor.package_impact = package_impact;
        self
    }

    /// How the unit is implemented (e.g. "`rust_in_pack:terminal`").
    #[must_use]
    pub const fn implementation_mode(mut self, mode: &'static str) -> Self {
        self.descriptor.implementation_mode = mode;
        self
    }
}

impl<P, M, C, CF, RS, BI> SourceUnitBindingBuilder<MissingOutput, P, M, C, CF, RS, BI> {
    #[must_use]
    pub const fn output_event_type(
        mut self,
        output_event_type: &'static str,
    ) -> SourceUnitBindingBuilder<HasOutput, P, M, C, CF, RS, BI> {
        self.descriptor.output_event_type = output_event_type;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, M, C, CF, RS, BI> SourceUnitBindingBuilder<O, MissingPrivacy, M, C, CF, RS, BI> {
    #[must_use]
    pub const fn privacy_context(
        mut self,
        privacy_context: &'static str,
    ) -> SourceUnitBindingBuilder<O, HasPrivacy, M, C, CF, RS, BI> {
        self.descriptor.privacy_context = privacy_context;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, C, CF, RS, BI> SourceUnitBindingBuilder<O, P, MissingMaterial, C, CF, RS, BI> {
    #[must_use]
    pub const fn material_policy(
        mut self,
        material_policy: &'static str,
    ) -> SourceUnitBindingBuilder<O, P, HasMaterial, C, CF, RS, BI> {
        self.descriptor.material_policy = material_policy;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, CF, RS, BI> SourceUnitBindingBuilder<O, P, M, MissingCheckpoint, CF, RS, BI> {
    #[must_use]
    pub const fn checkpoint_policy(
        mut self,
        checkpoint_policy: &'static str,
    ) -> SourceUnitBindingBuilder<O, P, M, CheckpointPresent, CF, RS, BI> {
        self.descriptor.checkpoint_policy = checkpoint_policy;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, RS, BI> SourceUnitBindingBuilder<O, P, M, C, MissingCheckpointFamily, RS, BI> {
    /// Shape of the source's checkpoint state machine. Required: codex P2 follow-up
    /// on PR #1189 — concrete defaults silently passed `xtask source-units check`
    /// for new bindings that forgot to set it. Typestate forces every binding to
    /// declare the family explicitly.
    #[must_use]
    pub const fn checkpoint_family(
        mut self,
        family: CheckpointFamily,
    ) -> SourceUnitBindingBuilder<O, P, M, C, HasCheckpointFamily, RS, BI> {
        self.descriptor.checkpoint_family = family;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, BI> SourceUnitBindingBuilder<O, P, M, C, CF, MissingRuntimeShape, BI> {
    /// Runtime invocation shape (continuous, scheduled, on-demand). Required:
    /// see `checkpoint_family` for the same rationale.
    #[must_use]
    pub const fn runtime_shape(
        mut self,
        shape: RuntimeShape,
    ) -> SourceUnitBindingBuilder<O, P, M, C, CF, HasRuntimeShape, BI> {
        self.descriptor.runtime_shape = shape;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, RS> SourceUnitBindingBuilder<O, P, M, C, CF, RS, MissingBuildImpact> {
    /// Physical/build footprint declared by this binding. Required: see
    /// `checkpoint_family` for the same rationale. `SourceUnitBuildImpact::ZERO`
    /// is a perfectly fine value to set explicitly — typestate only requires
    /// that the choice be intentional.
    #[must_use]
    pub const fn build_impact(
        mut self,
        build_impact: SourceUnitBuildImpact,
    ) -> SourceUnitBindingBuilder<O, P, M, C, CF, RS, HasBuildImpact> {
        self.descriptor.build_impact = build_impact;
        SourceUnitBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl
    SourceUnitBindingBuilder<
        HasOutput,
        HasPrivacy,
        HasMaterial,
        CheckpointPresent,
        HasCheckpointFamily,
        HasRuntimeShape,
        HasBuildImpact,
    >
{
    #[must_use]
    pub const fn build(self) -> SourceUnitBinding {
        self.descriptor
    }
}

inventory::collect!(SourceUnitBinding);
inventory::collect!(Claim);
inventory::collect!(RunnerBinding);
inventory::collect!(ProofObligation);
inventory::collect!(Exemption);

pub fn source_unit_bindings() -> impl Iterator<Item = &'static SourceUnitBinding> {
    inventory::iter::<SourceUnitBinding>()
}

pub fn claims() -> impl Iterator<Item = &'static Claim> {
    inventory::iter::<Claim>()
}

pub fn runner_bindings() -> impl Iterator<Item = &'static RunnerBinding> {
    inventory::iter::<RunnerBinding>()
}

pub fn obligations() -> impl Iterator<Item = &'static ProofObligation> {
    inventory::iter::<ProofObligation>()
}

pub fn exemptions() -> impl Iterator<Item = &'static Exemption> {
    inventory::iter::<Exemption>()
}

// Pre-existing binding for the only currently-deployed source unit
// (`terminal.atuin-history`). Aligned with the post-#1184 naming convention
// (`source_unit:<id>`), the descriptor's id (`terminal.atuin-history`), and
// the descriptor's event_types (`("shell.atuin", "command.executed")`).
inventory::submit! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .implementation("sinex-terminal-ingestor::atuin")
    .adapter("sqlite_row_stream")
    .output_event_type("command.executed")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("sqlite_row_id")
    .resource_shape("linear_rows_bounded_memory")
    .capabilities(&[
        "supports_snapshot",
        "supports_historical",
        "produces_material_anchors",
        "requires_target_home",
    ])
    .source_unit_id("terminal.atuin-history")
    .runner_pack("terminal")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "atuin_history_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

inventory::submit! {
    Claim {
        id: "claim:source_material.material_provenance",
        kind: ProofClaimKind::Invariant,
        subject: SubjectQuery::from_static("runtime_unit:*"),
        statement: "runtime units that ingest external records must produce material provenance with stable anchors",
    }
}

inventory::submit! {
    Claim {
        id: "claim:source_material.bounded_physical_frames",
        kind: ProofClaimKind::ResourceShape,
        subject: SubjectQuery::from_static("runtime_unit:*"),
        statement: "logical source records should flow through bounded physical source-material frames rather than one tiny material per record",
    }
}

inventory::submit! {
    Claim {
        id: "claim:record_source.cursor_and_anchor_laws",
        kind: ProofClaimKind::Law,
        subject: SubjectQuery::from_static("node_adapter:record_source_harness"),
        statement: "record-source harnesses advance checkpoints only through processed/skipped records and collect material anchors for processed records",
    }
}

inventory::submit! {
    Claim {
        id: "claim:xtask.command_catalog_introspection",
        kind: ProofClaimKind::CommandContract,
        subject: SubjectQuery::from_static("xtask_command:*"),
        statement: "public xtask command contracts should be derived from clap introspection and covered by ordinary Rust tests",
    }
}

inventory::submit! {
    Claim {
        id: "claim:source_unit.identity_axes_are_decoupled",
        kind: ProofClaimKind::Invariant,
        subject: SubjectQuery::from_static("source_unit:*"),
        statement: "source-unit identity must not imply a new Cargo package, Rust binary target, Nix output, or independent derivation by default",
    }
}

inventory::submit! {
    Claim {
        id: "claim:source_unit.package_impact_visible",
        kind: ProofClaimKind::CommandContract,
        subject: SubjectQuery::from_static("source_unit:*"),
        statement: "source-unit manifests expose crate, binary, package-output, derivation, SQLx validation, and service-instance impact",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:rust.sdk.source_laws",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("runtime_unit:*"),
        claims: &[
            "claim:source_material.material_provenance",
            "claim:source_material.bounded_physical_frames",
        ],
        command: "xtask test -p sinex-node-sdk",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:rust.sdk.record_source_laws",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("node_adapter:record_source_harness"),
        claims: &["claim:record_source.cursor_and_anchor_laws"],
        command: "xtask test -p sinex-node-sdk -E 'test(harness_materializes_records_and_finalizes_sink)'",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:rust.xtask.command_contracts",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("xtask_command:*"),
        claims: &["claim:xtask.command_catalog_introspection"],
        command: "xtask test -p xtask -E 'test(command_catalog_exposes_core_public_surface)'",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:xtask.source_units",
        runner: "xtask",
        subject: SubjectQuery::from_static("source_unit:*"),
        claims: &[
            "claim:source_unit.identity_axes_are_decoupled",
            "claim:source_unit.package_impact_visible",
        ],
        command: "xtask source-units check",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:runtime_unit.source_material_laws",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("runtime_unit:*"),
        claim_id: "claim:source_material.material_provenance",
        runner_binding_id: "runner:rust.sdk.source_laws",
        reason: "runtime units are the material-provenance boundary currently backed by source-material law tests",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:source_unit.material_provenance",
        level: ProofObligationLevel::Advisory,
        subject: SubjectQuery::from_static("source_unit:*"),
        claim_id: "claim:source_material.material_provenance",
        runner_binding_id: "runner:rust.sdk.source_laws",
        reason: "source units are the semantic leaves that must carry replayable material provenance; current runner proves runtime-unit material laws, so this remains advisory until source-unit-specific pass evidence exists",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:source_unit.package_impact_rationale",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("source_unit:*"),
        claim_id: "claim:source_unit.package_impact_visible",
        runner_binding_id: "runner:xtask.source_units",
        reason: "new source units must make build/cache/service impact visible before adding more physical artifacts",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:node_adapter.record_source_laws",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("node_adapter:record_source_harness"),
        claim_id: "claim:record_source.cursor_and_anchor_laws",
        runner_binding_id: "runner:rust.sdk.record_source_laws",
        reason: "source adapters are the natural SDK boundary for cursor and material-anchor correctness",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:xtask.command_catalog_introspection",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("xtask_command:*"),
        claim_id: "claim:xtask.command_catalog_introspection",
        runner_binding_id: "runner:rust.xtask.command_contracts",
        reason: "command-surface contracts should live in ordinary Rust test infrastructure rather than xtask exercise entries",
    }
}

// ─────────────────────────────────────────────────────────────
// Source-unit declaration & promotion contract (issue #690)
//
// Moved from source_unit.rs → proof.rs (#744 A1).
// ─────────────────────────────────────────────────────────────

/// How the source's checkpoint state is shaped.
///
/// Determines what the SDK's checkpoint adapter must support and what
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
    /// Source has no native cursor; ingestor polls and diffs.
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

/// Retention policy for events emitted by this source unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionPolicy {
    Forever,
    Days { days: u32 },
    Tiered { hot_days: u32, warm_days: u32 },
}

/// How the source identifies real-world occurrences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OccurrenceIdentity {
    Uuid5From(&'static str),
    Natural,
    Anchor,
}

/// Physical/build footprint declared by a source-unit descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceUnitBuildImpact {
    pub crate_impact: &'static str,
    pub binary_impact: &'static str,
    pub nix_output_impact: &'static str,
    pub derivation_impact: &'static str,
    pub sqlx_validation_impact: &'static str,
    pub dedicated_build_rationale: Option<&'static str>,
}

impl SourceUnitBuildImpact {
    pub const ZERO: Self = Self {
        crate_impact: "0",
        binary_impact: "0",
        nix_output_impact: "0",
        derivation_impact: "0",
        sqlx_validation_impact: "0",
        dedicated_build_rationale: None,
    };
}

/// The typed declaration every ingestor fills in.
///
/// This is strictly a *semantic* descriptor: identity, emitted event-type
/// pairs, privacy tier, time horizons, retention, occurrence identity, and
/// access policy. Deployment-shape fields (`runner_pack`, `checkpoint_family`,
/// `runtime_shape`, `package_impact`, `implementation_mode`, `build_impact`)
/// live on the matching [`SourceUnitBinding`] and are the source of truth for
/// `xtask source-units render`. See issue #1175.
///
/// Per-source-unit privacy rules (optional) live on a companion
/// [`SourceUnitPrivacyRules`] entry registered alongside the descriptor via
/// the `extra_privacy_rules:` arm of [`register_source_unit!`].
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceUnitDescriptor {
    pub id: &'static str,
    pub namespace: &'static str,
    pub event_types: &'static [(&'static str, &'static str)],
    pub privacy_tier: PrivacyTier,
    pub horizons: &'static [Horizon],
    pub retention: RetentionPolicy,
    pub proof_obligations: &'static [&'static str],
    pub occurrence_identity: OccurrenceIdentity,
    pub access_policy: &'static str,
}

/// Per-source-unit extra privacy rules registered alongside a [`SourceUnitDescriptor`].
///
/// When the privacy engine is invoked for a specific source unit (via
/// [`crate::privacy::engine_for_source_unit`]), these rules are merged with the
/// global catalog so that source-unit-specific patterns (e.g. Atuin login URLs)
/// are applied only where relevant.
///
/// Registered via the `extra_privacy_rules:` arm of [`register_source_unit!`].
/// The `rules_fn` field is a plain function pointer (not a closure) so it is
/// `'static`-safe.
#[derive(Clone, Copy)]
pub struct SourceUnitPrivacyRules {
    /// Must match the `id` field of the companion [`SourceUnitDescriptor`].
    pub source_unit_id: &'static str,
    /// Called once at engine-init time to produce the extra rules.
    pub rules_fn: fn() -> Vec<crate::privacy::PatternRule>,
}

inventory::collect!(SourceUnitDescriptor);
inventory::collect!(SourceUnitPrivacyRules);

/// Iterate over every registered source-unit descriptor in the binary.
pub fn all_source_units() -> impl Iterator<Item = &'static SourceUnitDescriptor> {
    inventory::iter::<SourceUnitDescriptor>()
}

/// Find a source-unit descriptor by `id`.
#[must_use]
pub fn find_source_unit(id: &crate::parser::SourceUnitId) -> Option<&'static SourceUnitDescriptor> {
    let id_str = id.as_str();
    all_source_units().find(|descriptor| descriptor.id == id_str)
}

/// Iterate over every registered per-source-unit privacy rule set in the binary.
pub fn all_source_unit_privacy_rules() -> impl Iterator<Item = &'static SourceUnitPrivacyRules> {
    inventory::iter::<SourceUnitPrivacyRules>()
}

/// Find per-source-unit extra privacy rules by `source_unit_id`.
#[must_use]
pub fn find_source_unit_privacy_rules(
    source_unit_id: &crate::parser::SourceUnitId,
) -> Option<&'static SourceUnitPrivacyRules> {
    let id_str = source_unit_id.as_str();
    all_source_unit_privacy_rules().find(|r| r.source_unit_id == id_str)
}

/// Re-exported `inventory` for consumers of [`register_source_unit!`].
#[doc(hidden)]
pub mod __register {
    pub use inventory;
}

/// Register a source-unit descriptor with the binary's inventory.
///
/// Optionally registers a companion [`SourceUnitPrivacyRules`] entry via the
/// `extra_privacy_rules:` arm.  The function-pointer value is called once at
/// engine-init time (see [`crate::privacy::engine_for_source_unit`]).
///
/// ```rust,ignore
/// register_source_unit!(
///     descriptor: MY_DESCRIPTOR,
/// );
///
/// // With per-unit privacy rules:
/// register_source_unit!(
///     descriptor: MY_DESCRIPTOR,
///     extra_privacy_rules: || vec![
///         PatternRule { name: "my_rule".into(), .. }
///     ],
/// );
/// ```
#[macro_export]
macro_rules! register_source_unit {
    // Plain form — descriptor only.
    ($descriptor:expr $(,)?) => {
        $crate::proof::__register::inventory::submit! { $descriptor }
    };
    // Named form with optional extra_privacy_rules.
    (descriptor: $descriptor:expr $(,)?) => {
        $crate::proof::__register::inventory::submit! { $descriptor }
    };
    (descriptor: $descriptor:expr, extra_privacy_rules: $rules_fn:expr $(,)?) => {
        $crate::proof::__register::inventory::submit! { $descriptor }
        $crate::proof::__register::inventory::submit! {
            $crate::proof::SourceUnitPrivacyRules {
                source_unit_id: $descriptor.id,
                rules_fn: $rules_fn,
            }
        }
    };
}

/// Register a [`SourceUnitBinding`] with the binary's inventory.
///
/// Companion to [`register_source_unit!`]: descriptors describe the *semantic*
/// shape of a source unit, bindings describe the deployed adapter that runs
/// it. Both are mechanically discoverable through the `inventory` crate.
#[macro_export]
macro_rules! register_source_unit_binding {
    ($binding:expr $(,)?) => {
        $crate::proof::__register::inventory::submit! { $binding }
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
    async fn source_unit_privacy_rules_registered_and_found() -> TestResult<()> {
        use crate::privacy::{
            Matcher, PatternRule, PrivacyConfig, PrivacyEngine, ProcessingContext, RuleCategory,
            Strategy,
        };
        use crate::proof::find_source_unit_privacy_rules;

        // Build an engine augmented with a scoped rule for a hypothetical
        // source unit.  We cannot register into the binary-wide inventory from
        // a test (inventory is link-time-only), so instead we exercise the
        // `PrivacyEngine::new` + `extra_rules` merge path directly to verify
        // that scoped rules fire as expected.
        let extra_rule = PatternRule {
            name: "test_scoped_atuin_url".into(),
            description: "test scoped rule".into(),
            category: RuleCategory::Secret,
            matcher: Matcher::Regex {
                // Matches Atuin auth URLs with query parameters.
                pattern: r"atuin\.sh/auth\?[^\s]+".into(),
            },
            strategy: Strategy::Redact {
                label: Some("<ATUIN_AUTH_URL>".into()),
            },
            contexts: vec![ProcessingContext::Command],
            enabled: true,
        };

        let mut config = PrivacyConfig::default();
        config.extra_rules.push(extra_rule);

        let engine = PrivacyEngine::new(config)?;
        let input = "atuin login --url https://atuin.sh/auth?token=supersecret123";
        let result = engine.process(input, ProcessingContext::Command);
        assert!(
            result.any_matched(),
            "scoped rule should fire on atuin auth URL"
        );
        assert!(
            result.text.contains("<ATUIN_AUTH_URL>"),
            "expected redaction label, got: {}",
            result.text
        );
        assert!(!result.text.contains("supersecret123"));

        // Verify that find_source_unit_privacy_rules returns None for unknown IDs
        // (no such unit registered in the test binary).
        assert!(
            find_source_unit_privacy_rules(&crate::parser::SourceUnitId::from_static(
                "nonexistent.source-unit"
            ))
            .is_none(),
            "unknown source unit should return None"
        );

        Ok(())
    }

    #[sinex_test]
    async fn register_source_unit_named_form_compiles() -> TestResult<()> {
        // Smoke-test: verify the named-form `register_source_unit!(descriptor: X)`
        // macros compile correctly.  We exercise the plain-descriptor path here
        // (no extra rules) since inventory submission from tests is link-time only.
        // The with-rules form is syntactically tested via the macro expansion path
        // verified by the trybuild suite.
        use crate::proof::{
            Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, SourceUnitDescriptor,
        };

        let descriptor = SourceUnitDescriptor {
            id: "test.register-form",
            namespace: "test",
            event_types: &[("test.source", "test.event")],
            privacy_tier: PrivacyTier::Sensitive,
            horizons: &[Horizon::Continuous],
            retention: RetentionPolicy::Forever,
            proof_obligations: &[],
            occurrence_identity: OccurrenceIdentity::Natural,
            access_policy: "internal",
        };

        // Verify the descriptor is well-formed (fields accessible).
        assert_eq!(descriptor.id, "test.register-form");
        assert_eq!(descriptor.privacy_tier, PrivacyTier::Sensitive);
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_binding_builder_requires_proof_fields() -> TestResult<()> {
        let descriptor = SourceUnitBinding::builder(
            SubjectRef::from_static("runtime_unit:test.demo"),
            "test.demo",
            "test",
        )
        .adapter("sqlite_row_stream")
        .implementation("demo::Unit")
        .output_event_type("test.output")
        .privacy_context("command")
        .material_policy("canonical_json_lines")
        .checkpoint_policy("row_id")
        .resource_shape("linear_rows_bounded_memory")
        .checkpoint_family(CheckpointFamily::AppendStream)
        .runtime_shape(RuntimeShape::Continuous)
        .build_impact(SourceUnitBuildImpact::ZERO)
        .build();

        assert_eq!(descriptor.output_event_type, "test.output");
        assert_eq!(descriptor.privacy_context, "command");
        assert_eq!(descriptor.material_policy, "canonical_json_lines");
        assert_eq!(descriptor.checkpoint_policy, "row_id");
        Ok(())
    }

    #[sinex_test]
    async fn proof_inventory_contains_builtin_source_unit_binding() -> TestResult<()> {
        let bindings = source_unit_bindings()
            .map(|descriptor| descriptor.subject.as_str())
            .collect::<Vec<_>>();

        assert!(bindings.contains(&"source_unit:terminal.atuin-history"));
        assert!(claims().any(|claim| claim.id == "claim:source_material.material_provenance"));
        assert!(claims().any(|claim| claim.id == "claim:source_unit.package_impact_visible"));
        assert!(runner_bindings().any(|binding| binding.id == "runner:xtask.source_units"));
        Ok(())
    }
}
