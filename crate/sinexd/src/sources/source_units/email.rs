//! Email capture source unit — `email.mailbox` (#1469).

use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

register_source_unit! {
    SourceUnitDescriptor {
        id: "email.mailbox",
        namespace: "email",
        event_types: &[
            ("email", "email.message.received"),
            ("email", "email.message.sent"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical, Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "staged_mbox_eml_parser",
            "body_encryption_at_rest",
            "attachment_document_dispatch",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(message_id, folder)"),
        access_policy: "personal_email",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:email.mailbox"),
        "email.mailbox",
        "email",
    )
    .implementation("staged-parser")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("email.message.received")
    .privacy_context("Sensitive")
    .material_policy("per_message_source_material")
    .checkpoint_policy("cursor_per_folder")
    .resource_shape("file_scanner")
    .source_unit_id("email.mailbox")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .package_impact("email_mailbox_source_unit")
    .implementation_mode("parser:staged")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .proposed(true)
    .build()
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:email.mailbox.sent"),
        "email.mailbox",
        "email",
    )
    .implementation("staged-parser")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("email.message.sent")
    .privacy_context("Sensitive")
    .material_policy("per_message_source_material")
    .checkpoint_policy("cursor_per_folder")
    .resource_shape("file_scanner")
    .source_unit_id("email.mailbox")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Scheduled)
    .package_impact("email_mailbox_source_unit")
    .implementation_mode("parser:staged")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .proposed(true)
    .build()
}
