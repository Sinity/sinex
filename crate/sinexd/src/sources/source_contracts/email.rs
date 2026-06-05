//! Email capture source — `email.mailbox` (#1469).

use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceBuildImpact, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

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
        access_policy: "personal_email",
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
    .privacy_context("Document")
    .material_policy("per_message_source_material")
    .checkpoint_policy("cursor_per_folder")
    .resource_shape("file_scanner")
    .source_id("email.mailbox")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .package_impact("email_mailbox_source")
    .implementation_mode("parser:staged")
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
    .privacy_context("Document")
    .material_policy("per_message_source_material")
    .checkpoint_policy("cursor_per_folder")
    .resource_shape("file_scanner")
    .source_id("email.mailbox")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .package_impact("email_mailbox_source")
    .implementation_mode("parser:staged")
    .build_impact(SourceBuildImpact::ZERO)
    .proposed(true)
    .build()
}
