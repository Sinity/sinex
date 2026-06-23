use super::{
    EventContract, EventContractId, EventOccurrenceContract, EventProvenanceRequirement,
    EventTemporalContract, PayloadSchemaContract,
};
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;

pub const MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/recording.observed@v1";
pub const MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/capture_session.started@v1";
pub const MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/capture_session.ended@v1";
pub const MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/transcript_segment.observed@v1";
pub const MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/transcription_run.observed@v1";
pub const MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/screenshot.observed@v1";
pub const MEDIA_SCREEN_CAPTURE_SESSION_STARTED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/capture_session.started@v1";
pub const MEDIA_SCREEN_CAPTURE_SESSION_ENDED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/capture_session.ended@v1";
pub const MEDIA_SCREEN_VIDEO_SEGMENT_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/video_segment.observed@v1";
pub const MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/ocr_segment.observed@v1";
pub const MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/ocr_run.observed@v1";

const MEDIA_AUDIO_TRANSCRIPT_PACKAGES: &[&str] = &["media.audio-transcript"];
const MEDIA_SCREEN_OCR_PACKAGES: &[&str] = &["media.screen-ocr"];

const MEDIA_AUDIO_RECORDING_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(raw_material_id, capture_session_id, observed_at)",
    )];
const MEDIA_AUDIO_CAPTURE_STARTED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, started_at)",
    )];
const MEDIA_AUDIO_CAPTURE_ENDED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, ended_at)",
    )];
const MEDIA_AUDIO_TRANSCRIPT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(material_id, segment_index, start_ms, end_ms)",
    )];
const MEDIA_AUDIO_TRANSCRIPTION_RUN_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(producer_run_id, model_id, input_material_ids)",
    )];
const MEDIA_SCREEN_SCREENSHOT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(raw_material_id, capture_session_id, display_id, region)",
    )];
const MEDIA_SCREEN_CAPTURE_STARTED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, started_at)",
    )];
const MEDIA_SCREEN_CAPTURE_ENDED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, ended_at)",
    )];
const MEDIA_SCREEN_VIDEO_SEGMENT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(raw_material_id, capture_session_id, display_id, region, duration_ms)",
    )];
const MEDIA_SCREEN_OCR_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(material_id, segment_index, bbox)",
    )];
const MEDIA_SCREEN_OCR_RUN_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(producer_run_id, engine_id, input_material_ids)",
    )];

const MEDIA_AUDIO_RECORDING_OCCURRENCE_FIELDS: &[&str] =
    &["raw_material_id", "capture_session_id", "observed_at"];
const MEDIA_AUDIO_CAPTURE_STARTED_OCCURRENCE_FIELDS: &[&str] =
    &["capture_session_id", "started_at"];
const MEDIA_AUDIO_CAPTURE_ENDED_OCCURRENCE_FIELDS: &[&str] = &["capture_session_id", "ended_at"];
const MEDIA_AUDIO_TRANSCRIPTION_RUN_OCCURRENCE_FIELDS: &[&str] =
    &["producer_run_id", "model_id", "input_material_ids"];
const MEDIA_SCREEN_SCREENSHOT_OCCURRENCE_FIELDS: &[&str] = &[
    "raw_material_id",
    "capture_session_id",
    "display_id",
    "region",
];
const MEDIA_SCREEN_CAPTURE_STARTED_OCCURRENCE_FIELDS: &[&str] =
    &["capture_session_id", "started_at"];
const MEDIA_SCREEN_CAPTURE_ENDED_OCCURRENCE_FIELDS: &[&str] = &["capture_session_id", "ended_at"];
const MEDIA_SCREEN_VIDEO_SEGMENT_OCCURRENCE_FIELDS: &[&str] = &[
    "raw_material_id",
    "capture_session_id",
    "display_id",
    "region",
    "duration_ms",
];
const MEDIA_SCREEN_OCR_RUN_OCCURRENCE_FIELDS: &[&str] =
    &["producer_run_id", "engine_id", "input_material_ids"];

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.recording_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.recording_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_RECORDING_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_RECORDING_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.capture_session_started",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.capture_session_started",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_CAPTURE_STARTED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_CAPTURE_STARTED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.capture_session_ended",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.capture_session_ended",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_CAPTURE_ENDED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_CAPTURE_ENDED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.transcript_segment_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.transcript_segment_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: MEDIA_AUDIO_TRANSCRIPT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::MaterialOrDerived,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.transcription_run_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.transcription_run_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_TRANSCRIPTION_RUN_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_TRANSCRIPTION_RUN_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Derived,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.screenshot_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.screenshot_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_SCREENSHOT_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_SCREENSHOT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_CAPTURE_SESSION_STARTED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.capture_session_started",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.capture_session_started",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_CAPTURE_STARTED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_CAPTURE_STARTED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_CAPTURE_SESSION_ENDED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.capture_session_ended",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.capture_session_ended",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_CAPTURE_ENDED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_CAPTURE_ENDED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_VIDEO_SEGMENT_OBSERVED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.video_segment_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.video_segment_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_VIDEO_SEGMENT_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_VIDEO_SEGMENT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.ocr_segment_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.ocr_segment_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: MEDIA_SCREEN_OCR_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::MaterialOrDerived,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.ocr_run_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.ocr_run_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_OCR_RUN_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_OCR_RUN_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Derived,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}
