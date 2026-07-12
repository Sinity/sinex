//! Attention stream automaton -- [`Transducer`] implementation.
//!
//! This first slice formalizes the existing multi-source activity-window layer
//! as `attention.span` events. Richer interval lifting can enrich the recipe
//! later without making recall consume raw analytics windows directly.

use crate::runtime::automaton::{DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonContext, AutomatonLogicError, InputProvenanceFilter, Transducer};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{ActivityWindowSummaryPayload, AttentionSpanPayload};

/// Derivation control-plane declaration for `attention-stream` (sinex-0vx.1/0vx.3).
pub const ATTENTION_STREAM_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "attention-stream.attention.span",
        owner: "attention-stream",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("derived.attention-stream"),
        output_event_type: Some("attention.span"),
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
        verification_command: "xtask test -p sinexd -E 'test(attention_stream)'",
    }];

#[derive(Debug, Clone, Default)]
pub struct AttentionStream;

impl Transducer for AttentionStream {
    type State = ();
    type Input = ActivityWindowSummaryPayload;
    type Output = AttentionSpanPayload;

    fn name(&self) -> &'static str {
        "attention-stream"
    }

    fn input_event_type(&self) -> &'static str {
        ActivityWindowSummaryPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        AttentionSpanPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        AttentionSpanPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::SynthesizedOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        ATTENTION_STREAM_OUTPUT_DECLARATIONS;

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let span_id = format!("attention-span:{}", input.window_id);
        let payload = AttentionSpanPayload {
            span_id: span_id.clone(),
            start_time: input.window_start,
            end_time: input.window_end,
            duration_secs: input.duration_secs,
            event_count: input.event_count,
            source_count: input.source_count,
            sources: input.sources,
            activity_sources: input.activity_sources,
            activity_source_counts: input.activity_source_counts,
            primary_source: input.primary_source,
            source_window_id: input.window_id,
            source_window_close_reason: input.close_reason,
        };

        Ok(Some(
            DerivedOutput::transduced(payload, input.window_end, context.trigger_uuid())
                .with_temporal_policy(SyntheticTemporalPolicy::InheritParent)
                .with_semantics_version("1.0.0")
                .with_equivalence_key(span_id),
        ))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type AttentionStreamRuntime = TransducerAdapter<AttentionStream>;

// --- Source descriptor ------------------------------------------------------

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "attention-stream",
        namespace: "derived",
        event_types: &[
            ("derived.attention-stream", "attention.span"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source_window_occurrence_key)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:attention-stream"),
        "attention-stream",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("attention.span")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("attention-stream")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}

#[cfg(test)]
#[path = "attention_test.rs"]
mod tests;
