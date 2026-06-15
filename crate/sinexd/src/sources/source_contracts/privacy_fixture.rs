//! Privacy coverage fixture source.
//!
//! This source is proposed metadata only. It exists so the privacy coverage
//! matrix has a stable catalog/parser fixture carrying the three leak-prone
//! field classes audited by #1790: source paths, free text, and credentials.

use sinex_macros::{SourceMeta, SourceRecord};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

#[derive(SourceRecord, SourceMeta, Default, Debug, Clone)]
#[source_record(
    id = "privacy-fixture-sensitive-record",
    source_id = "privacy.fixture.sensitive-record",
    input_shape = "tab_separated",
    event_type = "privacy.fixture.record",
    default_privacy_context = "Metadata"
)]
#[source_meta(
    id = "privacy.fixture.sensitive-record",
    namespace = "privacy",
    event_source = "privacy.fixture",
    event_type = "privacy.fixture.record",
    adapter = "AppendOnlyFileAdapter",
    implementation = "fixture-metadata",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Days { days: 1 },
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
    proposed = true,
    factory = "parser"
)]
pub struct PrivacyFixtureSensitiveRecord {
    #[source(column_index = 0)]
    #[required]
    #[privacy(context = "Metadata", sensitivity = "source_path")]
    pub source_path: String,

    #[source(column_index = 1)]
    #[required]
    #[privacy(context = "Document", sensitivity = "free_text, potentially_sensitive")]
    pub free_text: String,

    #[source(column_index = 2)]
    #[required]
    #[privacy(context = "Command", sensitivity = "credential_bearing")]
    pub credential_material: String,
}
