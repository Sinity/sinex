use std::marker::PhantomData;

use serde::Serialize;

use crate::privacy::ProcessingContext;
use crate::source_contracts::{
    CheckpointFamily, MaterialLifecyclePolicy, ResourceBudgetSpec, ResourceProfile, RunnerPack,
    RuntimeShape, SourceCapabilityRef, SubjectRef, TransportSemantics,
};

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
    /// RawMaterial/blob/artifact/DLQ lifecycle behavior for this binding.
    pub material_lifecycle: MaterialLifecyclePolicy,
    /// Transport, delivery, replay, DLQ, and backpressure semantics.
    pub transport_semantics: TransportSemantics,
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
                material_lifecycle: MaterialLifecyclePolicy::RetainRaw,
                transport_semantics: TransportSemantics::DIRECT_APPEND_STREAM,
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

    #[must_use]
    pub const fn material_lifecycle(mut self, policy: MaterialLifecyclePolicy) -> Self {
        self.descriptor.material_lifecycle = policy;
        self
    }

    #[must_use]
    pub const fn transport_semantics(mut self, semantics: TransportSemantics) -> Self {
        self.descriptor.transport_semantics = semantics;
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
    /// Shape of the source's checkpoint state machine. Concrete defaults once
    /// passed descriptor validation silently for new bindings that forgot to set
    /// this field; typestate now forces every binding to declare the family
    /// explicitly.
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
