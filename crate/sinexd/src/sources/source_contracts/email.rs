//! Email capture source — `email.mailbox` (#1469).

use sinex_macros::SourceMeta;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

#[derive(SourceMeta)]
#[source_meta(
    id = "email.mailbox",
    namespace = "email",
    event_type = "email.message.received",
    event_types = "email.message.sent",
    event_source = "email",
    adapter = "AppendOnlyFileAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical, Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(message_id, folder)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
    proposed = true,
    factory = "none",
    binding(
        subject = "source:email.mailbox.sent",
        event_type = "email.message.sent"
    )
)]
pub struct EmailMailboxProposal;
