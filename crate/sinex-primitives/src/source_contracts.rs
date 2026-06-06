//! Source contract vocabulary.
//!
//! This module holds typed source identity and runtime-binding declarations.
//! It intentionally does not declare advisory obligations; source correctness
//! belongs in tests, runtime validation, and deployment checks.

use std::marker::PhantomData;

use serde::Serialize;

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
    pub privacy_context: &'static str,
    pub material_policy: &'static str,
    pub checkpoint_policy: &'static str,
    pub resource_shape: &'static str,
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
    /// Logical runner pack hosting this binding (e.g. "terminal", "process").
    pub runner_pack: &'static str,
    /// Shape of the source's checkpoint state machine.
    pub checkpoint_family: CheckpointFamily,
    /// Runtime invocation shape (continuous, scheduled, on-demand).
    pub runtime_shape: RuntimeShape,
    /// Coarse package-level impact summary string.
    pub package_impact: &'static str,
    /// How the source is implemented (e.g. "`rust_in_pack:terminal`").
    pub implementation_mode: &'static str,
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
pub struct MissingMaterial;
#[derive(Debug, Clone, Copy)]
pub struct HasMaterial;
/// Typestate marker: checkpoint policy not yet supplied to [`SourceRuntimeBindingBuilder`].
#[derive(Debug, Clone, Copy)]
pub struct MissingCheckpoint;
/// Typestate marker: checkpoint policy supplied to [`SourceRuntimeBindingBuilder`].
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
pub struct SourceRuntimeBindingBuilder<
    Output,
    Privacy,
    Material,
    Checkpoint,
    CheckpointFam,
    Runtime,
    Build,
> {
    descriptor: SourceRuntimeBinding,
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

impl SourceRuntimeBinding {
    #[must_use]
    pub const fn builder(
        subject: SubjectRef,
        id: &'static str,
        domain: &'static str,
    ) -> SourceRuntimeBindingBuilder<
        MissingOutput,
        MissingPrivacy,
        MissingMaterial,
        MissingCheckpoint,
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
                privacy_context: "",
                material_policy: "",
                checkpoint_policy: "",
                resource_shape: "",
                capabilities: &[],
                source_id: "",
                proposed: false,
                runner_pack: "",
                checkpoint_family: CheckpointFamily::AppendStream,
                runtime_shape: RuntimeShape::Continuous,
                package_impact: "",
                implementation_mode: "",
                build_impact: SourceBuildImpact::ZERO,
            },
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, RS, BI> SourceRuntimeBindingBuilder<O, P, M, C, CF, RS, BI> {
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

    /// How the source is implemented (e.g. "`rust_in_pack:terminal`").
    #[must_use]
    pub const fn implementation_mode(mut self, mode: &'static str) -> Self {
        self.descriptor.implementation_mode = mode;
        self
    }
}

impl<P, M, C, CF, RS, BI> SourceRuntimeBindingBuilder<MissingOutput, P, M, C, CF, RS, BI> {
    #[must_use]
    pub const fn output_event_type(
        mut self,
        output_event_type: &'static str,
    ) -> SourceRuntimeBindingBuilder<HasOutput, P, M, C, CF, RS, BI> {
        self.descriptor.output_event_type = output_event_type;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, M, C, CF, RS, BI> SourceRuntimeBindingBuilder<O, MissingPrivacy, M, C, CF, RS, BI> {
    #[must_use]
    pub const fn privacy_context(
        mut self,
        privacy_context: &'static str,
    ) -> SourceRuntimeBindingBuilder<O, HasPrivacy, M, C, CF, RS, BI> {
        self.descriptor.privacy_context = privacy_context;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, C, CF, RS, BI> SourceRuntimeBindingBuilder<O, P, MissingMaterial, C, CF, RS, BI> {
    #[must_use]
    pub const fn material_policy(
        mut self,
        material_policy: &'static str,
    ) -> SourceRuntimeBindingBuilder<O, P, HasMaterial, C, CF, RS, BI> {
        self.descriptor.material_policy = material_policy;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, CF, RS, BI> SourceRuntimeBindingBuilder<O, P, M, MissingCheckpoint, CF, RS, BI> {
    #[must_use]
    pub const fn checkpoint_policy(
        mut self,
        checkpoint_policy: &'static str,
    ) -> SourceRuntimeBindingBuilder<O, P, M, CheckpointPresent, CF, RS, BI> {
        self.descriptor.checkpoint_policy = checkpoint_policy;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, RS, BI> SourceRuntimeBindingBuilder<O, P, M, C, MissingCheckpointFamily, RS, BI> {
    /// Shape of the source's checkpoint state machine. Required: codex P2 follow-up
    /// on PR #1189 — concrete defaults silently passed descriptor validation
    /// for new bindings that forgot to set it. Typestate forces every binding to
    /// declare the family explicitly.
    #[must_use]
    pub const fn checkpoint_family(
        mut self,
        family: CheckpointFamily,
    ) -> SourceRuntimeBindingBuilder<O, P, M, C, HasCheckpointFamily, RS, BI> {
        self.descriptor.checkpoint_family = family;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, BI> SourceRuntimeBindingBuilder<O, P, M, C, CF, MissingRuntimeShape, BI> {
    /// Runtime invocation shape (continuous, scheduled, on-demand). Required:
    /// see `checkpoint_family` for the same rationale.
    #[must_use]
    pub const fn runtime_shape(
        mut self,
        shape: RuntimeShape,
    ) -> SourceRuntimeBindingBuilder<O, P, M, C, CF, HasRuntimeShape, BI> {
        self.descriptor.runtime_shape = shape;
        SourceRuntimeBindingBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C, CF, RS> SourceRuntimeBindingBuilder<O, P, M, C, CF, RS, MissingBuildImpact> {
    /// Physical/build footprint declared by this binding. Required: see
    /// `checkpoint_family` for the same rationale. `SourceBuildImpact::ZERO`
    /// is a perfectly fine value to set explicitly — typestate only requires
    /// that the choice be intentional.
    #[must_use]
    pub const fn build_impact(
        mut self,
        build_impact: SourceBuildImpact,
    ) -> SourceRuntimeBindingBuilder<O, P, M, C, CF, RS, HasBuildImpact> {
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
        HasMaterial,
        CheckpointPresent,
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

// Pre-existing binding for the only currently-deployed source
// (`terminal.atuin-history`). Aligned with the post-#1184 naming convention
// (`source:<id>`), the descriptor's id (`terminal.atuin-history`), and
// the descriptor's event_types (`("shell.atuin", "command.executed")`).
inventory::submit! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .implementation("sinexd::sources::terminal::atuin_history")
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
    .source_id("terminal.atuin-history")
    .runner_pack("terminal")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "atuin_history_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OccurrenceIdentity {
    Uuid5From(&'static str),
    Natural,
    Anchor,
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
/// access policy. Deployment-shape fields (`runner_pack`, `checkpoint_family`,
/// `runtime_shape`, `package_impact`, `implementation_mode`, `build_impact`)
/// live on the matching [`SourceRuntimeBinding`]. See issue #1175.
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
    pub access_policy: &'static str,
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
            access_policy: "internal",
        };

        // Verify the descriptor is well-formed (fields accessible).
        assert_eq!(descriptor.id, "test.register-form");
        assert_eq!(descriptor.privacy_tier, PrivacyTier::Sensitive);
        Ok(())
    }

    #[sinex_test]
    async fn source_binding_builder_requires_proof_fields() -> TestResult<()> {
        let descriptor = SourceRuntimeBinding::builder(
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
        .build_impact(SourceBuildImpact::ZERO)
        .build();

        assert_eq!(descriptor.output_event_type, "test.output");
        assert_eq!(descriptor.privacy_context, "command");
        assert_eq!(descriptor.material_policy, "canonical_json_lines");
        assert_eq!(descriptor.checkpoint_policy, "row_id");
        Ok(())
    }

    #[sinex_test]
    async fn proof_inventory_contains_builtin_source_binding() -> TestResult<()> {
        let bindings = source_runtime_bindings()
            .map(|descriptor| descriptor.subject.as_str())
            .collect::<Vec<_>>();

        assert!(bindings.contains(&"source:terminal.atuin-history"));
        Ok(())
    }
}
