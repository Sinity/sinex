//! Media capture source contracts — audio transcription + screen OCR (#1043).

use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SourceContract,
    SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// register_source_contract!: escape-hatch pending #1761 (proposed stub sources
// with proposed(true) builder flag not yet in SourceMeta/SourceDefinition DSL;
// two contracts per file).

// ── audio.transcription ────────────────────────────────────────────────

register_source_contract! {
    SourceContract {
        id: "media.audio",
        namespace: "media",
        event_types: &[("media.audio", "media.audio.transcription")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(device_id, audio_chunk_hash)"),
        access_scope: AccessScope::StagedExport,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:media.audio"),
        "media.audio",
        "media",
    )
    .implementation("proposed")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("media.audio.transcription")
    .privacy_context(ProcessingContext::Document)
    .resource_profile(ResourceProfile::LiveWatcher)
    .source_id("media.audio")
    .runner_pack(RunnerPack::Staged)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .proposed(true)
    .build()
}

// ── media.screen ────────────────────────────────────────────────────────

register_source_contract! {
    SourceContract {
        id: "media.screen",
        namespace: "media",
        event_types: &[("media.screen", "media.screen.ocr")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(display_id, capture_hash)"),
        access_scope: AccessScope::StagedExport,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:media.screen"),
        "media.screen",
        "media",
    )
    .implementation("proposed")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("media.screen.ocr")
    .privacy_context(ProcessingContext::Document)
    .resource_profile(ResourceProfile::LiveWatcher)
    .source_id("media.screen")
    .runner_pack(RunnerPack::Staged)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .proposed(true)
    .build()
}
