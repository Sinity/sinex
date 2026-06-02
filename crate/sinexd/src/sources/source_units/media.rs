//! Media capture source units — audio transcription + screen OCR (#1043).

use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ── audio.transcription ────────────────────────────────────────────────

register_source_unit! {
    SourceUnitDescriptor {
        id: "media.audio",
        namespace: "media",
        event_types: &[("media.audio", "media.audio.transcription")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "local_whisper_only",
            "continuous_capture",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(device_id, audio_chunk_hash)"),
        access_policy: "personal_audio",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:media.audio"),
        "media.audio",
        "media",
    )
    .implementation("proposed")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("media.audio.transcription")
    .privacy_context("Document")
    .material_policy("audio_chunk_material")
    .checkpoint_policy("continuous_audio_stream")
    .resource_shape("audio_capture")
    .source_unit_id("media.audio")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("media_audio_source_unit")
    .implementation_mode("parser:staged")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .proposed(true)
    .build()
}

// ── media.screen ────────────────────────────────────────────────────────

register_source_unit! {
    SourceUnitDescriptor {
        id: "media.screen",
        namespace: "media",
        event_types: &[("media.screen", "media.screen.ocr")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "periodic_full_screen_capture",
            "local_ocr_processing",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(display_id, capture_hash)"),
        access_policy: "personal_screen",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:media.screen"),
        "media.screen",
        "media",
    )
    .implementation("proposed")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("media.screen.ocr")
    .privacy_context("Document")
    .material_policy("screenshot_chunk_material")
    .checkpoint_policy("periodic_screen_capture")
    .resource_shape("screen_capture")
    .source_unit_id("media.screen")
    .runner_pack("staged")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("media_screen_source_unit")
    .implementation_mode("parser:staged")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .proposed(true)
    .build()
}
