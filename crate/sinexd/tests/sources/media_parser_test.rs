use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    events::payloads::media::{
        MediaScreenCaptureSessionEndedPayload, MediaScreenCaptureSessionStartedPayload,
        ScreenVideoSegmentObservedPayload,
    },
    ids::Id,
    parser::{MaterialAnchor, OccurrenceKey, ParserContext, SourceId, SourceRecord},
    temporal::Timestamp,
};
use sinexd::{
    runtime::parser::MaterialParser, sources::source_contracts::media::MediaScreenOcrParser,
};
use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("media.screen-ocr"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn source_record(bytes: Vec<u8>) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from("screens/session-a.webm.json")),
        source_ts_hint: None,
        metadata: serde_json::json!({}),
    }
}

fn occurrence_field<'a>(
    intent: &'a sinex_primitives::parser::ParsedEventIntent,
    key: &str,
) -> Option<&'a str> {
    let OccurrenceKey { fields, .. } = intent.occurrence_key.as_ref()?;
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value.as_str()))
}

#[sinex_test]
async fn screen_video_manifest_emits_contract_occurrence_fields() -> xtask::sandbox::TestResult<()>
{
    let mut parser = MediaScreenOcrParser;
    let record = source_record(
        br#"{
          "video_segment": {
            "format": "webm",
            "codec": "vp9",
            "duration_ms": 1800,
            "frame_rate_fps": 30.0,
            "width": 800,
            "height": 600,
            "display_id": "DP-2",
            "region": [0, 0, 800, 600],
            "capture_session_id": "capture-session-a",
            "source_file": "screens/session-a.webm",
            "policy_posture": "operator-controlled-video-material"
          }
        }"#
        .to_vec(),
    );
    let material_id = record.material_id.to_string();

    let intents = parser.parse_record(record, &test_ctx()).await?;

    assert_eq!(intents.len(), 1);
    let intent = &intents[0];
    assert_eq!(
        intent.event_type.as_str(),
        "media.screen.video_segment_observed"
    );
    assert_eq!(
        occurrence_field(intent, "raw_material_id"),
        Some(material_id.as_str())
    );
    assert_eq!(
        occurrence_field(intent, "capture_session_id"),
        Some("capture-session-a")
    );
    assert_eq!(occurrence_field(intent, "display_id"), Some("DP-2"));
    assert_eq!(occurrence_field(intent, "region"), Some("0,0,800,600"));
    assert_eq!(occurrence_field(intent, "duration_ms"), Some("1800"));

    let payload: ScreenVideoSegmentObservedPayload =
        serde_json::from_value(intent.payload.clone())?;
    assert_eq!(payload.raw_material_id, material_id);
    assert_eq!(payload.file_format.as_deref(), Some("webm"));
    assert_eq!(payload.codec.as_deref(), Some("vp9"));
    assert_eq!(payload.duration_ms, Some(1800));
    assert_eq!(payload.display_id.as_deref(), Some("DP-2"));
    assert_eq!(
        payload.capture_session_id.as_deref(),
        Some("capture-session-a")
    );
    Ok(())
}

#[sinex_test]
async fn screen_capture_manifest_emits_session_lifecycle_and_video()
-> xtask::sandbox::TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = source_record(
        br#"{
          "capture_session_started": {
            "capture_session_id": "screen-session-a",
            "scope": "focused-window",
            "reason": "operator_requested",
            "operator_binding_id": "source:media.screen-ocr.on-demand-video",
            "display_id": "DP-2",
            "region": [0, 0, 800, 600],
            "started_at": "2026-06-23T10:00:00Z",
            "policy_posture": "operator-controlled-video-material"
          },
          "video_segment": {
            "format": "webm",
            "codec": "vp9",
            "duration_ms": 1800,
            "display_id": "DP-2",
            "capture_session_id": "screen-session-a",
            "source_file": "screens/session-a.webm"
          },
          "capture_session_ended": {
            "capture_session_id": "screen-session-a",
            "reason": "operator_stopped",
            "duration_ms": 1800,
            "final_state": "completed",
            "ended_at": "2026-06-23T10:00:02Z",
            "policy_posture": "operator-controlled-video-material"
          }
        }"#
        .to_vec(),
    );

    let intents = parser.parse_record(record, &test_ctx()).await?;

    assert_eq!(intents.len(), 3);
    assert_eq!(
        intents
            .iter()
            .map(|intent| intent.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "media.screen.capture_session_started",
            "media.screen.video_segment_observed",
            "media.screen.capture_session_ended"
        ]
    );
    let started = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "media.screen.capture_session_started")
        .expect("capture start event");
    let video = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "media.screen.video_segment_observed")
        .expect("video segment event");
    let ended = intents
        .iter()
        .find(|intent| intent.event_type.as_str() == "media.screen.capture_session_ended")
        .expect("capture end event");

    let started_payload: MediaScreenCaptureSessionStartedPayload =
        serde_json::from_value(started.payload.clone())?;
    assert_eq!(started_payload.capture_session_id, "screen-session-a");
    assert_eq!(started_payload.scope, "focused-window");
    assert_eq!(
        started_payload.operator_binding_id,
        "source:media.screen-ocr.on-demand-video"
    );
    assert_eq!(started_payload.display_id.as_deref(), Some("DP-2"));
    assert_eq!(
        occurrence_field(started, "capture_session_id"),
        Some("screen-session-a")
    );

    let video_payload: ScreenVideoSegmentObservedPayload =
        serde_json::from_value(video.payload.clone())?;
    assert_eq!(
        video_payload.capture_session_id.as_deref(),
        Some("screen-session-a")
    );

    let ended_payload: MediaScreenCaptureSessionEndedPayload =
        serde_json::from_value(ended.payload.clone())?;
    assert_eq!(ended_payload.capture_session_id, "screen-session-a");
    assert_eq!(ended_payload.reason.as_deref(), Some("operator_stopped"));
    assert_eq!(ended_payload.duration_ms, Some(1800));
    assert_eq!(ended_payload.final_state, "completed");
    assert_eq!(
        occurrence_field(ended, "capture_session_id"),
        Some("screen-session-a")
    );
    Ok(())
}
