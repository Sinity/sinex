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
            status: status.into(),
            reproducer: None,
            environment: JsonValue::Null,
            artifacts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct RuntimeUnitDescriptor {
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
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct SourceUnitDescriptor {
    pub subject: SubjectRef,
    pub id: &'static str,
    pub domain: &'static str,
    pub role: &'static str,
    pub modes: &'static [&'static str],
    pub acquisition_shape: &'static str,
    pub material_policy: &'static str,
    pub checkpoint_policy: &'static str,
    pub occurrence_policy: &'static str,
    pub output_event_type: &'static str,
    pub privacy_context: &'static str,
    pub resource_profile: &'static str,
    pub access_policy: &'static str,
    pub service_policy: &'static str,
    pub runner_pack: &'static str,
    pub package_impact: &'static str,
    pub implementation_mode: &'static str,
    pub proof_obligations: &'static [&'static str],
    pub crate_impact: &'static str,
    pub binary_impact: &'static str,
    pub nix_output_impact: &'static str,
    pub derivation_impact: &'static str,
    pub sqlx_validation_impact: &'static str,
    pub dedicated_build_rationale: Option<&'static str>,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceUnitDescriptorBuilder {
    descriptor: SourceUnitDescriptor,
}

impl SourceUnitDescriptor {
    #[must_use]
    pub const fn builder(
        subject: SubjectRef,
        id: &'static str,
        domain: &'static str,
    ) -> SourceUnitDescriptorBuilder {
        SourceUnitDescriptorBuilder {
            descriptor: SourceUnitDescriptor {
                subject,
                id,
                domain,
                role: "",
                modes: &[],
                acquisition_shape: "",
                material_policy: "",
                checkpoint_policy: "",
                occurrence_policy: "",
                output_event_type: "",
                privacy_context: "",
                resource_profile: "",
                access_policy: "",
                service_policy: "",
                runner_pack: "",
                package_impact: "",
                implementation_mode: "",
                proof_obligations: &[],
                crate_impact: "0",
                binary_impact: "0",
                nix_output_impact: "0",
                derivation_impact: "0",
                sqlx_validation_impact: "0",
                dedicated_build_rationale: None,
            },
        }
    }
}

impl SourceUnitDescriptorBuilder {
    #[must_use]
    pub const fn role(mut self, role: &'static str) -> Self {
        self.descriptor.role = role;
        self
    }

    #[must_use]
    pub const fn modes(mut self, modes: &'static [&'static str]) -> Self {
        self.descriptor.modes = modes;
        self
    }

    #[must_use]
    pub const fn acquisition_shape(mut self, acquisition_shape: &'static str) -> Self {
        self.descriptor.acquisition_shape = acquisition_shape;
        self
    }

    #[must_use]
    pub const fn material_policy(mut self, material_policy: &'static str) -> Self {
        self.descriptor.material_policy = material_policy;
        self
    }

    #[must_use]
    pub const fn checkpoint_policy(mut self, checkpoint_policy: &'static str) -> Self {
        self.descriptor.checkpoint_policy = checkpoint_policy;
        self
    }

    #[must_use]
    pub const fn occurrence_policy(mut self, occurrence_policy: &'static str) -> Self {
        self.descriptor.occurrence_policy = occurrence_policy;
        self
    }

    #[must_use]
    pub const fn output_event_type(mut self, output_event_type: &'static str) -> Self {
        self.descriptor.output_event_type = output_event_type;
        self
    }

    #[must_use]
    pub const fn privacy_context(mut self, privacy_context: &'static str) -> Self {
        self.descriptor.privacy_context = privacy_context;
        self
    }

    #[must_use]
    pub const fn resource_profile(mut self, resource_profile: &'static str) -> Self {
        self.descriptor.resource_profile = resource_profile;
        self
    }

    #[must_use]
    pub const fn access_policy(mut self, access_policy: &'static str) -> Self {
        self.descriptor.access_policy = access_policy;
        self
    }

    #[must_use]
    pub const fn service_policy(mut self, service_policy: &'static str) -> Self {
        self.descriptor.service_policy = service_policy;
        self
    }

    #[must_use]
    pub const fn runner_pack(mut self, runner_pack: &'static str) -> Self {
        self.descriptor.runner_pack = runner_pack;
        self
    }

    #[must_use]
    pub const fn package_impact(mut self, package_impact: &'static str) -> Self {
        self.descriptor.package_impact = package_impact;
        self
    }

    #[must_use]
    pub const fn implementation_mode(mut self, implementation_mode: &'static str) -> Self {
        self.descriptor.implementation_mode = implementation_mode;
        self
    }

    #[must_use]
    pub const fn proof_obligations(mut self, obligations: &'static [&'static str]) -> Self {
        self.descriptor.proof_obligations = obligations;
        self
    }

    #[must_use]
    pub const fn build_impact(
        mut self,
        crate_impact: &'static str,
        binary_impact: &'static str,
        nix_output_impact: &'static str,
    ) -> Self {
        self.descriptor.crate_impact = crate_impact;
        self.descriptor.binary_impact = binary_impact;
        self.descriptor.nix_output_impact = nix_output_impact;
        self
    }

    #[must_use]
    pub const fn validation_impact(
        mut self,
        derivation_impact: &'static str,
        sqlx_validation_impact: &'static str,
    ) -> Self {
        self.descriptor.derivation_impact = derivation_impact;
        self.descriptor.sqlx_validation_impact = sqlx_validation_impact;
        self
    }

    #[must_use]
    pub const fn dedicated_build_rationale(mut self, rationale: &'static str) -> Self {
        self.descriptor.dedicated_build_rationale = Some(rationale);
        self
    }

    #[must_use]
    pub const fn build(self) -> SourceUnitDescriptor {
        self.descriptor
    }
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
#[derive(Debug, Clone, Copy)]
pub struct MissingCheckpoint;
#[derive(Debug, Clone, Copy)]
pub struct HasCheckpoint;

#[derive(Debug, Clone, Copy)]
pub struct RuntimeUnitDescriptorBuilder<Output, Privacy, Material, Checkpoint> {
    descriptor: RuntimeUnitDescriptor,
    _state: PhantomData<(Output, Privacy, Material, Checkpoint)>,
}

impl RuntimeUnitDescriptor {
    #[must_use]
    pub const fn builder(
        subject: SubjectRef,
        id: &'static str,
        domain: &'static str,
    ) -> RuntimeUnitDescriptorBuilder<
        MissingOutput,
        MissingPrivacy,
        MissingMaterial,
        MissingCheckpoint,
    > {
        RuntimeUnitDescriptorBuilder {
            descriptor: RuntimeUnitDescriptor {
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
            },
            _state: PhantomData,
        }
    }
}

impl<O, P, M, C> RuntimeUnitDescriptorBuilder<O, P, M, C> {
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
}

impl<P, M, C> RuntimeUnitDescriptorBuilder<MissingOutput, P, M, C> {
    #[must_use]
    pub const fn output_event_type(
        mut self,
        output_event_type: &'static str,
    ) -> RuntimeUnitDescriptorBuilder<HasOutput, P, M, C> {
        self.descriptor.output_event_type = output_event_type;
        RuntimeUnitDescriptorBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, M, C> RuntimeUnitDescriptorBuilder<O, MissingPrivacy, M, C> {
    #[must_use]
    pub const fn privacy_context(
        mut self,
        privacy_context: &'static str,
    ) -> RuntimeUnitDescriptorBuilder<O, HasPrivacy, M, C> {
        self.descriptor.privacy_context = privacy_context;
        RuntimeUnitDescriptorBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, C> RuntimeUnitDescriptorBuilder<O, P, MissingMaterial, C> {
    #[must_use]
    pub const fn material_policy(
        mut self,
        material_policy: &'static str,
    ) -> RuntimeUnitDescriptorBuilder<O, P, HasMaterial, C> {
        self.descriptor.material_policy = material_policy;
        RuntimeUnitDescriptorBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl<O, P, M> RuntimeUnitDescriptorBuilder<O, P, M, MissingCheckpoint> {
    #[must_use]
    pub const fn checkpoint_policy(
        mut self,
        checkpoint_policy: &'static str,
    ) -> RuntimeUnitDescriptorBuilder<O, P, M, HasCheckpoint> {
        self.descriptor.checkpoint_policy = checkpoint_policy;
        RuntimeUnitDescriptorBuilder {
            descriptor: self.descriptor,
            _state: PhantomData,
        }
    }
}

impl RuntimeUnitDescriptorBuilder<HasOutput, HasPrivacy, HasMaterial, HasCheckpoint> {
    #[must_use]
    pub const fn build(self) -> RuntimeUnitDescriptor {
        self.descriptor
    }
}

inventory::collect!(RuntimeUnitDescriptor);
inventory::collect!(SourceUnitDescriptor);
inventory::collect!(Claim);
inventory::collect!(RunnerBinding);
inventory::collect!(ProofObligation);
inventory::collect!(Exemption);

#[must_use]
pub fn runtime_unit_descriptors() -> impl Iterator<Item = &'static RuntimeUnitDescriptor> {
    inventory::iter::<RuntimeUnitDescriptor>()
}

#[must_use]
pub fn source_unit_descriptors() -> impl Iterator<Item = &'static SourceUnitDescriptor> {
    inventory::iter::<SourceUnitDescriptor>()
}

#[must_use]
pub fn claims() -> impl Iterator<Item = &'static Claim> {
    inventory::iter::<Claim>()
}

#[must_use]
pub fn runner_bindings() -> impl Iterator<Item = &'static RunnerBinding> {
    inventory::iter::<RunnerBinding>()
}

#[must_use]
pub fn obligations() -> impl Iterator<Item = &'static ProofObligation> {
    inventory::iter::<ProofObligation>()
}

#[must_use]
pub fn exemptions() -> impl Iterator<Item = &'static Exemption> {
    inventory::iter::<Exemption>()
}

inventory::submit! {
    RuntimeUnitDescriptor::builder(
        SubjectRef::from_static("runtime_unit:terminal.atuin"),
        "terminal.atuin",
        "terminal",
    )
    .implementation("sinex-terminal-ingestor::atuin")
    .adapter("sqlite_row_stream")
    .output_event_type("shell.command")
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
    .build()
}

inventory::submit! {
    SourceUnitDescriptor::builder(
        SubjectRef::from_static("source_unit:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .role("source_adapter")
    .modes(&["historical", "continuous"])
    .acquisition_shape("sqlite_row_stream")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("sqlite_row_id")
    .occurrence_policy("natural_key:atuin.history.id")
    .output_event_type("shell.atuin/command.executed")
    .privacy_context("command_line_history")
    .resource_profile("bounded_sqlite_polling:256M")
    .access_policy("target_home_read:.local/share/atuin/history.db")
    .service_policy("dedicated_instance:on-failure")
    .runner_pack("terminal")
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .proof_obligations(&[
        "obligation:source_unit.material_provenance",
        "obligation:source_unit.package_impact_rationale",
    ])
    .build_impact("0", "0", "0")
    .validation_impact("0", "0")
    .build()
}

inventory::submit! {
    SourceUnitDescriptor::builder(
        SubjectRef::from_static("source_unit:terminal.zsh-history"),
        "terminal.zsh-history",
        "terminal",
    )
    .role("source_adapter")
    .modes(&["historical", "continuous"])
    .acquisition_shape("append_only_file")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("byte_offset_and_line")
    .occurrence_policy("material_anchor")
    .output_event_type("shell.history/command.imported")
    .privacy_context("command_line_history")
    .resource_profile("bounded_file_polling:256M")
    .access_policy("target_home_read:.zsh_history")
    .service_policy("dedicated_instance:on-failure")
    .runner_pack("terminal")
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .proof_obligations(&[
        "obligation:source_unit.material_provenance",
        "obligation:source_unit.package_impact_rationale",
    ])
    .build_impact("0", "0", "0")
    .validation_impact("0", "0")
    .build()
}

inventory::submit! {
    SourceUnitDescriptor::builder(
        SubjectRef::from_static("source_unit:terminal.fish-history"),
        "terminal.fish-history",
        "terminal",
    )
    .role("source_adapter")
    .modes(&["historical", "continuous"])
    .acquisition_shape("sqlite_row_stream")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("sqlite_row_id")
    .occurrence_policy("sqlite_row_id")
    .output_event_type("shell.history.fish/command.executed")
    .privacy_context("command_line_history")
    .resource_profile("bounded_sqlite_polling:256M")
    .access_policy("target_home_read:.local/share/fish/fish_history")
    .service_policy("dedicated_instance:on-failure")
    .runner_pack("terminal")
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .proof_obligations(&[
        "obligation:source_unit.material_provenance",
        "obligation:source_unit.package_impact_rationale",
    ])
    .build_impact("0", "0", "0")
    .validation_impact("0", "0")
    .build()
}

inventory::submit! {
    SourceUnitDescriptor::builder(
        SubjectRef::from_static("source_unit:terminal.bash-history"),
        "terminal.bash-history",
        "terminal",
    )
    .role("source_adapter")
    .modes(&["historical", "continuous"])
    .acquisition_shape("append_only_file")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("byte_offset_and_line")
    .occurrence_policy("material_anchor")
    .output_event_type("shell.history/command.imported")
    .privacy_context("command_line_history")
    .resource_profile("bounded_file_polling:256M")
    .access_policy("target_home_read:.bash_history")
    .service_policy("dedicated_instance:on-failure")
    .runner_pack("terminal")
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .proof_obligations(&[
        "obligation:source_unit.material_provenance",
        "obligation:source_unit.package_impact_rationale",
    ])
    .build_impact("0", "0", "0")
    .validation_impact("0", "0")
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
        id: "claim:scenario.evidence_envelope",
        kind: ProofClaimKind::Scenario,
        subject: SubjectQuery::from_static("scenario:*"),
        statement: "proof-carrying scenarios emit subject refs, claim ids, runner id, reproducer, and artifact references",
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
        statement: "public xtask command contracts should be derived from clap introspection and covered by ordinary Rust scenarios",
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
        id: "runner:rust.nextest.scenario",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("scenario:*"),
        claims: &["claim:scenario.evidence_envelope"],
        command: "xtask test --scenario-tag <tag>",
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
        command: "xtask test -p sinex-node-sdk --scenario-category source_material",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:rust.sdk.record_source_laws",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("node_adapter:record_source_harness"),
        claims: &["claim:record_source.cursor_and_anchor_laws"],
        command: "xtask test -p sinex-node-sdk --scenario-tag source_adapter_laws",
    }
}

inventory::submit! {
    RunnerBinding {
        id: "runner:rust.xtask.command_contracts",
        runner: "cargo-nextest",
        subject: SubjectQuery::from_static("xtask_command:*"),
        claims: &["claim:xtask.command_catalog_introspection"],
        command: "xtask test -p xtask --scenario-tag command_contract",
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
        reason: "source units are the material-provenance boundary for replay and audit",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:source_unit.material_provenance",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("source_unit:*"),
        claim_id: "claim:source_material.material_provenance",
        runner_binding_id: "runner:rust.sdk.source_laws",
        reason: "source units are the semantic leaves that must carry replayable material provenance",
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
        id: "obligation:scenario.evidence_envelope",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("scenario:*"),
        claim_id: "claim:scenario.evidence_envelope",
        runner_binding_id: "runner:rust.nextest.scenario",
        reason: "scenario failures must carry enough context to avoid host forensics",
    }
}

inventory::submit! {
    ProofObligation {
        id: "obligation:xtask.command_catalog_introspection",
        level: ProofObligationLevel::Required,
        subject: SubjectQuery::from_static("xtask_command:*"),
        claim_id: "claim:xtask.command_catalog_introspection",
        runner_binding_id: "runner:rust.xtask.command_contracts",
        reason: "command-surface contracts should live in ordinary test/scenario infrastructure rather than xtask exercise entries",
    }
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
    async fn runtime_unit_descriptor_builder_requires_proof_fields() -> TestResult<()> {
        let descriptor = RuntimeUnitDescriptor::builder(
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
        .build();

        assert_eq!(descriptor.output_event_type, "test.output");
        assert_eq!(descriptor.privacy_context, "command");
        assert_eq!(descriptor.material_policy, "canonical_json_lines");
        assert_eq!(descriptor.checkpoint_policy, "row_id");
        Ok(())
    }

    #[sinex_test]
    async fn source_unit_descriptor_builder_records_physical_impact() -> TestResult<()> {
        let descriptor = SourceUnitDescriptor::builder(
            SubjectRef::from_static("source_unit:test.demo-source"),
            "test.demo-source",
            "test",
        )
        .role("source_adapter")
        .modes(&["historical", "continuous"])
        .acquisition_shape("sqlite_row_stream")
        .material_policy("canonical_json_lines")
        .checkpoint_policy("row_id")
        .occurrence_policy("natural_key:test.id")
        .output_event_type("test.output")
        .privacy_context("metadata")
        .resource_profile("bounded")
        .access_policy("none")
        .service_policy("dedicated_instance")
        .runner_pack("test")
        .package_impact("no_new_output")
        .implementation_mode("rust_in_pack:test")
        .proof_obligations(&["obligation:source_unit.package_impact_rationale"])
        .build_impact("0", "0", "0")
        .validation_impact("0", "0")
        .build();

        assert_eq!(descriptor.runner_pack, "test");
        assert_eq!(descriptor.crate_impact, "0");
        assert_eq!(descriptor.binary_impact, "0");
        assert_eq!(descriptor.nix_output_impact, "0");
        assert_eq!(descriptor.derivation_impact, "0");
        assert_eq!(descriptor.sqlx_validation_impact, "0");
        Ok(())
    }

    #[sinex_test]
    async fn proof_inventory_contains_builtin_runtime_unit() -> TestResult<()> {
        let runtime_units = runtime_unit_descriptors()
            .map(|descriptor| descriptor.subject.as_str())
            .collect::<Vec<_>>();
        let source_units = source_unit_descriptors()
            .map(|descriptor| descriptor.subject.as_str())
            .collect::<Vec<_>>();

        assert!(runtime_units.contains(&"runtime_unit:terminal.atuin"));
        assert!(source_units.contains(&"source_unit:terminal.atuin-history"));
        assert!(source_units.contains(&"source_unit:terminal.zsh-history"));
        assert!(source_units.contains(&"source_unit:terminal.fish-history"));
        assert!(claims().any(|claim| claim.id == "claim:source_material.material_provenance"));
        assert!(claims().any(|claim| claim.id == "claim:source_unit.package_impact_visible"));
        assert!(runner_bindings().any(|binding| binding.id == "runner:rust.nextest.scenario"));
        assert!(runner_bindings().any(|binding| binding.id == "runner:xtask.source_units"));
        Ok(())
    }
}
