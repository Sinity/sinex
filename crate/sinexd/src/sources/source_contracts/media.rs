//! Media capture source contracts — audio transcription + screen OCR (#1043).

use sinex_macros::SourceMeta;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

// Proposed metadata-only sources: the contracts and runtime bindings document
// planned source surfaces, but no parser/source factories are registered until
// the corresponding runtime exists.

// ── audio.transcription ────────────────────────────────────────────────

#[derive(SourceMeta)]
#[source_meta(
    id = "media.audio",
    namespace = "media",
    event_type = "media.audio.transcription",
    event_source = "media.audio",
    adapter = "AppendOnlyFileAdapter",
    implementation = "proposed",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(device_id, audio_chunk_hash)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Continuous,
    proposed = true,
    factory = "none"
)]
pub struct MediaAudioProposal;

// ── media.screen ────────────────────────────────────────────────────────

#[derive(SourceMeta)]
#[source_meta(
    id = "media.screen",
    namespace = "media",
    event_type = "media.screen.ocr",
    event_source = "media.screen",
    adapter = "AppendOnlyFileAdapter",
    implementation = "proposed",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(display_id, capture_hash)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Continuous,
    proposed = true,
    factory = "none"
)]
pub struct MediaScreenProposal;
