use super::*;
use camino::Utf8PathBuf;
use sinex_primitives::source_contracts::{
    RunnerPack, RuntimeShape, all_source_contracts, source_runtime_bindings,
};
use sinex_primitives::{Uuid, ids::Id};
use xtask::sandbox::prelude::*;

fn test_ctx(source_id: &'static str) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static(source_id),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn record_for(bytes: &[u8], logical_path: &str) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes: bytes.to_vec(),
        logical_path: Some(Utf8PathBuf::from(logical_path)),
        source_ts_hint: None,
        metadata: Value::Null,
    }
}

#[sinex_test]
async fn media_contracts_declare_capture_and_model_event_surface() -> TestResult<()> {
    let audio = all_source_contracts()
        .find(|contract| contract.id == "media.audio-transcript")
        .expect("media.audio-transcript contract registered");
    let audio_types = audio
        .event_types
        .iter()
        .map(|(_, event_type)| *event_type)
        .collect::<Vec<_>>();
    for event_type in [
        "media.audio.recording_observed",
        "media.audio.capture_session_started",
        "media.audio.capture_session_ended",
        "media.audio.transcript_segment_observed",
        "media.audio.transcription_run_observed",
    ] {
        assert!(
            audio_types.contains(&event_type),
            "audio contract missing {event_type}"
        );
    }

    let screen = all_source_contracts()
        .find(|contract| contract.id == "media.screen-ocr")
        .expect("media.screen-ocr contract registered");
    let screen_types = screen
        .event_types
        .iter()
        .map(|(_, event_type)| *event_type)
        .collect::<Vec<_>>();
    for event_type in [
        "media.screen.screenshot_observed",
        "media.screen.capture_session_started",
        "media.screen.capture_session_ended",
        "media.screen.video_segment_observed",
        "media.screen.ocr_segment_observed",
        "media.screen.ocr_run_observed",
    ] {
        assert!(
            screen_types.contains(&event_type),
            "screen contract missing {event_type}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn media_runtime_bindings_cover_staged_model_on_demand_and_live_modes() -> TestResult<()> {
    let bindings = source_runtime_bindings()
        .filter(|binding| {
            binding.source_id == "media.audio-transcript" || binding.source_id == "media.screen-ocr"
        })
        .collect::<Vec<_>>();

    let binding = |subject: &str| {
        bindings
            .iter()
            .copied()
            .find(|binding| binding.subject.as_str() == subject)
            .unwrap_or_else(|| panic!("missing media binding {subject}"))
    };

    assert!(!binding("source:media.audio-transcript").proposed);
    assert!(
        !binding("source:media.audio-transcript.audio-bundle-staged").proposed,
        "staged audio bundle parser is implemented and should be an accepted package mode"
    );
    assert_eq!(
        binding("source:media.audio-transcript.local-model-batch").runtime_shape,
        RuntimeShape::OnDemand
    );
    assert!(
        !binding("source:media.audio-transcript.local-model-batch").proposed,
        "audio local model worker output is executable through media operations"
    );
    assert!(binding("source:media.audio-transcript.on-demand-session").proposed);
    assert_eq!(
        binding("source:media.audio-transcript.live-session").runner_pack,
        RunnerPack::Live
    );
    assert!(binding("source:media.audio-transcript.live-session").proposed);
    assert!(
        binding("source:media.audio-transcript.audio-bundle-staged")
            .capabilities
            .contains(&"operation:media.audio-transcript.import-bundle")
    );

    assert!(!binding("source:media.screen-ocr").proposed);
    assert!(
        !binding("source:media.screen-ocr.screenshot-ocr-staged").proposed,
        "staged screenshot/OCR bundle parser is implemented and should be an accepted package mode"
    );
    assert!(
        !binding("source:media.screen-ocr.video-staged").proposed,
        "staged screen-video bundle parser is implemented and should be an accepted package mode"
    );
    assert_eq!(
        binding("source:media.screen-ocr.on-demand-region").runtime_shape,
        RuntimeShape::OnDemand
    );
    assert!(
        !binding("source:media.screen-ocr.local-model-batch").proposed,
        "screen OCR local model worker output is executable through media operations"
    );
    assert!(
        !binding("source:media.screen-ocr.on-demand-region").proposed,
        "on-demand screen region capture is executable through bounded worker output"
    );
    assert!(
        !binding("source:media.screen-ocr.on-demand-video").proposed,
        "on-demand screen video capture is executable through bounded worker output"
    );
    assert_eq!(
        binding("source:media.screen-ocr.live-session").runner_pack,
        RunnerPack::Live
    );
    assert!(binding("source:media.screen-ocr.live-session").proposed);
    assert!(
        binding("source:media.screen-ocr.screenshot-ocr-staged")
            .capabilities
            .contains(&"operation:media.screen-ocr.import-screenshots")
    );
    assert!(
        binding("source:media.screen-ocr.video-staged")
            .capabilities
            .contains(&"operation:media.screen-ocr.import-video")
    );
    Ok(())
}

#[sinex_test]
async fn transcript_json_segments_emit_observations() -> TestResult<()> {
    let mut parser = MediaAudioTranscriptParser;
    let record = record_for(
            br#"{"segments":[{"text":"hello world","start_ms":1200,"end_ms":2500,"speaker_label":"speaker-1","language":"en","confidence":0.91,"model_id":"fixture-transcriber"}]}"#,
            "transcripts/session.json",
        );

    let intents = parser
        .parse_record(record, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].event_type.as_str(),
        "media.audio.transcript_segment_observed"
    );
    assert_eq!(intents[0].payload["text"], "hello world");
    assert_eq!(intents[0].payload["speaker_label"], "speaker-1");
    assert_eq!(intents[0].payload["model_id"], "fixture-transcriber");
    assert!(intents[0].occurrence_key.is_some());
    Ok(())
}

#[sinex_test]
async fn transcript_whisper_json_seconds_are_normalized_to_millis() -> TestResult<()> {
    let mut parser = MediaAudioTranscriptParser;
    let record = record_for(
            br#"{"segments":[{"start":1.25,"end":3.5,"text":"second-shaped cue","speaker":"SPEAKER_00","model":"whisper-large"}]}"#,
            "transcripts/whisper.json",
        );

    let intents = parser
        .parse_record(record, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["text"], "second-shaped cue");
    assert_eq!(intents[0].payload["start_ms"], 1250);
    assert_eq!(intents[0].payload["end_ms"], 3500);
    assert_eq!(intents[0].payload["speaker_label"], "SPEAKER_00");
    assert_eq!(intents[0].payload["model_id"], "whisper-large");
    Ok(())
}

#[sinex_test]
async fn transcript_plain_text_emits_one_segment_per_nonempty_line() -> TestResult<()> {
    let mut parser = MediaAudioTranscriptParser;
    let record = record_for(b"first line\n\nsecond line\n", "transcripts/session.txt");

    let intents = parser
        .parse_record(record, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].payload["text"], "first line");
    assert_eq!(intents[1].payload["text"], "second line");
    Ok(())
}

#[sinex_test]
async fn transcript_vtt_and_srt_cues_preserve_timing() -> TestResult<()> {
    let mut parser = MediaAudioTranscriptParser;
    let vtt = record_for(
            b"WEBVTT\n\ncue-1\n00:00:01.200 --> 00:00:03.400\nfirst cue\n\n00:03.500 --> 00:04.000\nsecond cue\n",
            "transcripts/session.vtt",
        );
    let srt = record_for(
        b"1\n00:00:05,000 --> 00:00:06,250\nthird cue\n",
        "transcripts/session.srt",
    );

    let vtt_intents = parser
        .parse_record(vtt, &test_ctx("media.audio-transcript"))
        .await?;
    let srt_intents = parser
        .parse_record(srt, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(vtt_intents.len(), 2);
    assert_eq!(vtt_intents[0].payload["text"], "first cue");
    assert_eq!(vtt_intents[0].payload["start_ms"], 1200);
    assert_eq!(vtt_intents[0].payload["end_ms"], 3400);
    assert_eq!(vtt_intents[1].payload["start_ms"], 3500);
    assert_eq!(srt_intents[0].payload["text"], "third cue");
    assert_eq!(srt_intents[0].payload["start_ms"], 5000);
    assert_eq!(srt_intents[0].payload["end_ms"], 6250);
    Ok(())
}

#[sinex_test]
async fn audio_bundle_manifest_emits_recording_and_transcript_events() -> TestResult<()> {
    let mut parser = MediaAudioTranscriptParser;
    let record = record_for(
        br#"{
              "recording": {
                "format": "wav",
                "codec": "pcm_s16le",
                "duration_ms": 3200,
                "channels": 1,
                "sample_rate_hz": 16000,
                "capture_session_id": "session-a",
                "source_file": "audio/session-a.wav",
                "policy_posture": "explicit-raw-material-policy"
              },
              "segments": [
                {"text":"bundle segment","start_ms":0,"end_ms":3200,"model_id":"local-whisper"}
              ]
            }"#,
        "audio/session-a/manifest.json",
    );

    let intents = parser
        .parse_record(record, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(intents.len(), 2);
    assert_eq!(
        intents[0].event_type.as_str(),
        "media.audio.recording_observed"
    );
    assert_eq!(intents[0].payload["file_format"], "wav");
    assert_eq!(intents[0].payload["duration_ms"], 3200);
    assert_eq!(intents[0].payload["channel_count"], 1);
    assert_eq!(
        intents[0].payload["policy_posture"],
        "explicit-raw-material-policy"
    );
    assert!(intents[0].occurrence_key.is_some());
    assert_eq!(
        intents[1].event_type.as_str(),
        "media.audio.transcript_segment_observed"
    );
    assert_eq!(intents[1].payload["text"], "bundle segment");
    Ok(())
}

#[sinex_test]
async fn audio_manifest_emits_transcription_run_and_propagates_segment_provenance() -> TestResult<()>
{
    let mut parser = MediaAudioTranscriptParser;
    let record = record_for(
        br#"{
              "recording": {
                "format": "flac",
                "duration_ms": 4100
              },
              "transcription_run": {
                "producer_run_id": "transcribe-run-a",
                "model_id": "whisper-large-v3",
                "model_version": "2026-06",
                "input_material_ids": ["raw-audio-a"],
                "output_refs": ["artifact:media.audio.transcript/run-a"],
                "duration_ms": 980,
                "resource_posture": "bounded-local-worker"
              },
              "segments": [
                {"text":"run-backed segment","start_ms":0,"end_ms":4100}
              ]
            }"#,
        "audio/session-a/manifest.json",
    );

    let intents = parser
        .parse_record(record, &test_ctx("media.audio-transcript"))
        .await?;

    assert_eq!(intents.len(), 3);
    assert_eq!(
        intents[1].event_type.as_str(),
        "media.audio.transcription_run_observed"
    );
    assert_eq!(intents[1].payload["producer_run_id"], "transcribe-run-a");
    assert_eq!(intents[1].payload["model_id"], "whisper-large-v3");
    assert_eq!(intents[1].payload["input_material_ids"][0], "raw-audio-a");
    assert_eq!(
        intents[1].payload["resource_posture"],
        "bounded-local-worker"
    );
    assert!(intents[1].occurrence_key.is_some());
    assert_eq!(
        intents[2].event_type.as_str(),
        "media.audio.transcript_segment_observed"
    );
    assert_eq!(intents[2].payload["producer_run_id"], "transcribe-run-a");
    assert_eq!(intents[2].payload["model_id"], "whisper-large-v3");
    Ok(())
}

#[sinex_test]
async fn transcription_rerun_with_different_model_keeps_recording_occurrence_but_new_producer_run()
-> TestResult<()> {
    // Issue #1043 identity invariant: a model rerun with a different
    // model/version produces a new ProducerRun (and new derived identity) but
    // does NOT create a new raw-material occurrence for the recording itself.
    // Two manifests describe the SAME recording material (same material id +
    // duration) but different transcription runs/models.
    let material_id = Id::new();
    let recording_block = r#""recording": { "format": "flac", "duration_ms": 4100 }"#;
    let manifest = |run_id: &str, model: &str| {
        format!(
            "{{ {recording_block},
               \"transcription_run\": {{
                 \"producer_run_id\": \"{run_id}\",
                 \"model_id\": \"{model}\",
                 \"input_material_ids\": [\"raw-audio-shared\"]
               }},
               \"segments\": [ {{\"text\":\"shared audio\",\"start_ms\":0,\"end_ms\":4100}} ]
             }}"
        )
        .into_bytes()
    };
    let record_with_shared_material = |bytes: Vec<u8>| SourceRecord {
        material_id,
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from("audio/shared/manifest.json")),
        source_ts_hint: None,
        metadata: Value::Null,
    };

    let mut parser = MediaAudioTranscriptParser;
    let first = parser
        .parse_record(
            record_with_shared_material(manifest("transcribe-run-a", "whisper-large-v3")),
            &test_ctx("media.audio-transcript"),
        )
        .await?;
    let second = parser
        .parse_record(
            record_with_shared_material(manifest("transcribe-run-b", "whisper-medium")),
            &test_ctx("media.audio-transcript"),
        )
        .await?;

    let occurrence_of = |intents: &[ParsedEventIntent], event_type: &str| {
        intents
            .iter()
            .find(|intent| intent.event_type.as_str() == event_type)
            .and_then(|intent| intent.occurrence_key.clone())
            .map(|key| key.fields)
    };

    // The recording is the same real-world occurrence across both runs.
    let first_recording = occurrence_of(&first, "media.audio.recording_observed")
        .expect("first parse should emit a recording observation");
    let second_recording = occurrence_of(&second, "media.audio.recording_observed")
        .expect("second parse should emit a recording observation");
    assert_eq!(
        first_recording, second_recording,
        "a model rerun must not change the raw recording occurrence identity"
    );

    // The transcription runs are distinct producer-run identities.
    let first_run = occurrence_of(&first, "media.audio.transcription_run_observed")
        .expect("first parse should emit a transcription run observation");
    let second_run = occurrence_of(&second, "media.audio.transcription_run_observed")
        .expect("second parse should emit a transcription run observation");
    assert_ne!(
        first_run, second_run,
        "a different model/run must produce a distinct ProducerRun occurrence"
    );
    let segment_run_id = |intents: &[ParsedEventIntent]| {
        intents
            .iter()
            .find(|intent| intent.event_type.as_str() == "media.audio.transcript_segment_observed")
            .map(|intent| intent.payload["producer_run_id"].clone())
    };
    assert_ne!(
        segment_run_id(&first),
        segment_run_id(&second),
        "derived segments must carry the rerun's producer_run_id"
    );
    Ok(())
}

#[sinex_test]
async fn ocr_json_segments_emit_bbox_observations() -> TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = record_for(
            br#"[{"text":"Visible title","bbox":[10,20,300,60],"confidence":0.87,"display_id":"DP-1","window_title":"Report.pdf","engine":"fixture-ocr"}]"#,
            "ocr/screen.json",
        );

    let intents = parser
        .parse_record(record, &test_ctx("media.screen-ocr"))
        .await?;

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].event_type.as_str(),
        "media.screen.ocr_segment_observed"
    );
    assert_eq!(intents[0].payload["text"], "Visible title");
    assert_eq!(intents[0].payload["bbox"][0], 10);
    assert_eq!(intents[0].payload["window_title"], "Report.pdf");
    assert!(intents[0].occurrence_key.is_some());
    Ok(())
}

#[sinex_test]
async fn screenshot_bundle_manifest_emits_screenshot_and_ocr_events() -> TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = record_for(
        br#"{
              "screenshot": {
                "display_id": "DP-1",
                "window_title": "Editor",
                "region": [10, 20, 640, 360],
                "width": 640,
                "height": 360,
                "capture_session_id": "screen-session-a",
                "source_file": "screens/screen-session-a.png",
                "policy_posture": "explicit-image-material-policy"
              },
              "segments": [
                {"text":"visible code","bbox":[12,24,120,30],"confidence":0.93,"engine":"local-ocr"}
              ]
            }"#,
        "screens/session-a/manifest.json",
    );

    let intents = parser
        .parse_record(record, &test_ctx("media.screen-ocr"))
        .await?;

    assert_eq!(intents.len(), 2);
    assert_eq!(
        intents[0].event_type.as_str(),
        "media.screen.screenshot_observed"
    );
    assert_eq!(intents[0].payload["display_id"], "DP-1");
    assert_eq!(intents[0].payload["width_px"], 640);
    assert_eq!(intents[0].payload["height_px"], 360);
    assert_eq!(
        intents[0].payload["policy_posture"],
        "explicit-image-material-policy"
    );
    assert!(intents[0].occurrence_key.is_some());
    assert_eq!(
        intents[1].event_type.as_str(),
        "media.screen.ocr_segment_observed"
    );
    assert_eq!(intents[1].payload["text"], "visible code");
    Ok(())
}

#[sinex_test]
async fn screen_manifest_emits_ocr_run_and_propagates_segment_provenance() -> TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = record_for(
        br#"{
              "screenshot": {
                "display_id": "DP-2",
                "region": [0, 0, 800, 600],
                "width": 800,
                "height": 600
              },
              "ocr_run": {
                "producer_run_id": "ocr-run-a",
                "engine_id": "tesseract",
                "engine_version": "5.5",
                "input_material_ids": ["raw-screen-a"],
                "output_refs": ["artifact:media.screen.ocr/run-a"],
                "duration_ms": 330,
                "resource_posture": "bounded-local-worker"
              },
              "segments": [
                {"text":"run-backed OCR","bbox":[4,8,160,24],"confidence":0.95}
              ]
            }"#,
        "screens/session-a/manifest.json",
    );

    let intents = parser
        .parse_record(record, &test_ctx("media.screen-ocr"))
        .await?;

    assert_eq!(intents.len(), 3);
    assert_eq!(
        intents[1].event_type.as_str(),
        "media.screen.ocr_run_observed"
    );
    assert_eq!(intents[1].payload["producer_run_id"], "ocr-run-a");
    assert_eq!(intents[1].payload["engine_id"], "tesseract");
    assert_eq!(intents[1].payload["input_material_ids"][0], "raw-screen-a");
    assert_eq!(
        intents[1].payload["resource_posture"],
        "bounded-local-worker"
    );
    assert!(intents[1].occurrence_key.is_some());
    assert_eq!(
        intents[2].event_type.as_str(),
        "media.screen.ocr_segment_observed"
    );
    assert_eq!(intents[2].payload["producer_run_id"], "ocr-run-a");
    assert_eq!(intents[2].payload["engine"], "tesseract");
    Ok(())
}

#[sinex_test]
async fn ocr_tesseract_tsv_rows_emit_bbox_observations() -> TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = record_for(
            b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n5\t1\t1\t1\t1\t1\t10\t20\t300\t60\t87\tVisible\n5\t1\t1\t1\t1\t2\t320\t20\t120\t60\t93\ttitle\n",
            "ocr/screen.tsv",
        );

    let intents = parser
        .parse_record(record, &test_ctx("media.screen-ocr"))
        .await?;

    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].payload["text"], "Visible");
    assert_eq!(intents[0].payload["bbox"], json!([10, 20, 300, 60]));
    assert_eq!(intents[0].payload["confidence"], 0.87);
    assert_eq!(intents[0].payload["engine"], "tesseract-tsv");
    assert_eq!(intents[1].payload["text"], "title");
    Ok(())
}

#[sinex_test]
async fn invalid_media_utf8_is_rejected() -> TestResult<()> {
    let mut parser = MediaScreenOcrParser;
    let record = record_for(&[0xff, 0xfe], "ocr/bad.txt");

    let err = parser
        .parse_record(record, &test_ctx("media.screen-ocr"))
        .await
        .expect_err("invalid UTF-8 should be rejected");

    assert!(err.to_string().contains("not UTF-8"));
    Ok(())
}
