//! Email capture source — `email.mailbox` (#1469).

use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SourceContract,
    SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// register_source_contract!: escape-hatch pending #1761 (proposed source with
// two independent runtime bindings and no parser; SourceMeta requires exactly
// one (id, adapter, occurrence_identity) triple).
register_source_contract! {
    SourceContract {
        id: "email.mailbox",
        namespace: "email",
        event_types: &[
            ("email", "email.message.received"),
            ("email", "email.message.sent"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical, Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(message_id, folder)"),
        access_scope: AccessScope::StagedExport,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:email.mailbox"),
        "email.mailbox",
        "email",
    )
    .implementation("staged-parser")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("email.message.received")
    .privacy_context(ProcessingContext::Document)
    .resource_profile(ResourceProfile::BoundedFile)
    .source_id("email.mailbox")
    .runner_pack(RunnerPack::Staged)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .build_impact(SourceBuildImpact::ZERO)
    .proposed(true)
    .build()
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:email.mailbox.sent"),
        "email.mailbox",
        "email",
    )
    .implementation("staged-parser")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("email.message.sent")
    .privacy_context(ProcessingContext::Document)
    .resource_profile(ResourceProfile::BoundedFile)
    .source_id("email.mailbox")
    .runner_pack(RunnerPack::Staged)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .build_impact(SourceBuildImpact::ZERO)
    .proposed(true)
    .build()
}
