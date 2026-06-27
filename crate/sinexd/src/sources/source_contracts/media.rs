//! Media capture source contracts (#1043).
//!
//! The implemented parsers consume transcript/OCR text material and staged
//! bundle manifests that anchor raw recording/screenshot/video observations.
//! Worker-backed local model and on-demand capture modes consume bounded
//! operation output. Long-lived session control bindings stay proposed until a
//! durable live runner owns the capture process.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingEvidence,
};
use sinex_primitives::privacy::{ProcessingContext, SensitivityHint};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, MaterialLifecyclePolicy, OccurrenceIdentity,
    PrivacyTier, ResourceProfile, RetentionPolicy, RunnerPack, RuntimeShape, TransportSemantics,
};

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "media.audio-transcript",
    namespace = "media",
    event_type = "media.audio.transcript_segment_observed",
    event_types = "media.audio.recording_observed, media.audio.capture_session_started, media.audio.capture_session_ended, media.audio.transcription_run_observed",
    event_source = "media.audio",
    adapter = "FileContentDropAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(material_id, segment_index, start_ms, end_ms)"),
    access_scope = AccessScope::StagedExport,
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.check, operation:media.audio-transcript.import-transcript, operation:media.audio-transcript.inspect, operation:media.audio-transcript.replay, operation:media.audio-transcript.export",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
    material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
    transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
    binding(
        subject = "source:media.audio-transcript.audio-bundle-staged",
        event_type = "media.audio.recording_observed",
        implementation = "staged-audio-bundle",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.import-bundle, operation:media.audio-transcript.inspect, operation:media.audio-transcript.delete-material, operation:media.audio-transcript.export"
    ),
    binding(
        subject = "source:media.audio-transcript.local-model-batch",
        event_type = "media.audio.transcription_run_observed",
        implementation = "local-transcription-worker",
        adapter = "LocalProcessWorker",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::SinexdSource,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::OnDemand,
        material_lifecycle = MaterialLifecyclePolicy::DerivedOnly,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.run-model, operation:media.audio-transcript.retry, operation:media.audio-transcript.rebuild-artifact, operation:media.audio-transcript.inspect"
    ),
    binding(
        subject = "source:media.audio-transcript.on-demand-session",
        event_type = "media.audio.capture_session_started",
        implementation = "live-capture",
        adapter = "AudioSessionCaptureAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::OnDemand,
        material_lifecycle = MaterialLifecyclePolicy::EphemeralRaw,
        transport_semantics = TransportSemantics::LOCAL_LIVE_QUEUE,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.enable-session, operation:media.audio-transcript.disable-session, operation:media.audio-transcript.pause, operation:media.audio-transcript.resume, operation:media.audio-transcript.inspect",
        proposed = true
    ),
    binding(
        subject = "source:media.audio-transcript.live-session",
        event_type = "media.audio.capture_session_ended",
        implementation = "live-capture",
        adapter = "AudioSessionCaptureAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::Continuous,
        material_lifecycle = MaterialLifecyclePolicy::EphemeralRaw,
        transport_semantics = TransportSemantics::LOCAL_LIVE_QUEUE,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.enable-session, operation:media.audio-transcript.disable-session, operation:media.audio-transcript.pause, operation:media.audio-transcript.resume, operation:media.audio-transcript.retry, operation:media.audio-transcript.inspect",
        proposed = true
    )
)]
pub struct MediaAudioTranscriptParser;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "media.screen-ocr",
    namespace = "media",
    event_type = "media.screen.ocr_segment_observed",
    event_types = "media.screen.screenshot_observed, media.screen.capture_session_started, media.screen.capture_session_ended, media.screen.video_segment_observed, media.screen.ocr_run_observed",
    event_source = "media.screen",
    adapter = "FileContentDropAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(material_id, segment_index, bbox)"),
    access_scope = AccessScope::StagedExport,
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.check, operation:media.screen-ocr.import-ocr, operation:media.screen-ocr.inspect, operation:media.screen-ocr.replay, operation:media.screen-ocr.export",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
    material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
    transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
    binding(
        subject = "source:media.screen-ocr.screenshot-ocr-staged",
        event_type = "media.screen.screenshot_observed",
        implementation = "staged-screenshot-bundle",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.import-screenshots, operation:media.screen-ocr.inspect, operation:media.screen-ocr.delete-material, operation:media.screen-ocr.export"
    ),
    binding(
        subject = "source:media.screen-ocr.video-staged",
        event_type = "media.screen.video_segment_observed",
        implementation = "staged-screen-video-bundle",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::RetainRaw,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.import-video, operation:media.screen-ocr.inspect, operation:media.screen-ocr.delete-material, operation:media.screen-ocr.export"
    ),
    binding(
        subject = "source:media.screen-ocr.local-model-batch",
        event_type = "media.screen.ocr_run_observed",
        implementation = "local-ocr-worker",
        adapter = "LocalProcessWorker",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::SinexdSource,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::OnDemand,
        material_lifecycle = MaterialLifecyclePolicy::DerivedOnly,
        transport_semantics = TransportSemantics::DIRECT_APPEND_STREAM,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.run-ocr, operation:media.screen-ocr.retry, operation:media.screen-ocr.rebuild-artifact, operation:media.screen-ocr.inspect"
    ),
    binding(
        subject = "source:media.screen-ocr.on-demand-region",
        event_type = "media.screen.screenshot_observed",
        implementation = "live-capture",
        adapter = "ScreenRegionCaptureAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::OnDemand,
        material_lifecycle = MaterialLifecyclePolicy::EphemeralRaw,
        transport_semantics = TransportSemantics::LOCAL_LIVE_QUEUE,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.capture-region, operation:media.screen-ocr.pause, operation:media.screen-ocr.resume, operation:media.screen-ocr.inspect"
    ),
    binding(
        subject = "source:media.screen-ocr.on-demand-video",
        event_type = "media.screen.video_segment_observed",
        implementation = "live-capture",
        adapter = "ScreenVideoCaptureAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::OnDemand,
        material_lifecycle = MaterialLifecyclePolicy::EphemeralRaw,
        transport_semantics = TransportSemantics::LOCAL_LIVE_QUEUE,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.record-video, operation:media.screen-ocr.pause, operation:media.screen-ocr.resume, operation:media.screen-ocr.inspect"
    ),
    binding(
        subject = "source:media.screen-ocr.live-session",
        event_type = "media.screen.ocr_segment_observed",
        implementation = "live-capture",
        adapter = "ScreenRegionCaptureAdapter",
        resource_profile = ResourceProfile::LiveWatcher,
        runner_pack = RunnerPack::Live,
        checkpoint_family = CheckpointFamily::LiveObservation,
        runtime_shape = RuntimeShape::Continuous,
        material_lifecycle = MaterialLifecyclePolicy::EphemeralRaw,
        transport_semantics = TransportSemantics::LOCAL_LIVE_QUEUE,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.enable-session, operation:media.screen-ocr.disable-session, operation:media.screen-ocr.pause, operation:media.screen-ocr.resume, operation:media.screen-ocr.retry, operation:media.screen-ocr.inspect"
    )
)]
pub struct MediaScreenOcrParser;

#[derive(Debug, Clone)]
struct TranscriptSegment {
    text: String,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    speaker_label: Option<String>,
    language: Option<String>,
    confidence: Option<f64>,
    model_id: Option<String>,
    producer_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct OcrSegment {
    text: String,
    #[serde(default)]
    bbox: Option<Vec<i64>>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    display_id: Option<String>,
    #[serde(default)]
    window_title: Option<String>,
    #[serde(default)]
    engine: Option<String>,
    #[serde(default)]
    producer_run_id: Option<String>,
}

#[derive(Debug, Clone)]
struct AudioRecording {
    file_format: Option<String>,
    codec: Option<String>,
    duration_ms: Option<u64>,
    channel_count: Option<u32>,
    sample_rate_hz: Option<u32>,
    capture_session_id: Option<String>,
    source_file: Option<String>,
    policy_posture: Option<String>,
}

#[derive(Debug, Clone)]
struct ScreenshotObservation {
    display_id: Option<String>,
    window_title: Option<String>,
    region: Option<Vec<i64>>,
    width_px: u32,
    height_px: u32,
    capture_session_id: Option<String>,
    source_file: Option<String>,
    policy_posture: Option<String>,
}

#[derive(Debug, Clone)]
struct ScreenCaptureSessionStarted {
    capture_session_id: String,
    scope: Option<String>,
    reason: Option<String>,
    operator_binding_id: Option<String>,
    display_id: Option<String>,
    region: Option<Vec<i64>>,
    policy_posture: Option<String>,
    started_at: Option<String>,
}

#[derive(Debug, Clone)]
struct ScreenCaptureSessionEnded {
    capture_session_id: String,
    reason: Option<String>,
    duration_ms: Option<u64>,
    final_state: Option<String>,
    policy_posture: Option<String>,
    ended_at: Option<String>,
}

#[derive(Debug, Clone)]
struct ScreenVideoSegment {
    file_format: Option<String>,
    codec: Option<String>,
    duration_ms: Option<u64>,
    frame_rate_fps: Option<f64>,
    width_px: Option<u32>,
    height_px: Option<u32>,
    display_id: Option<String>,
    window_title: Option<String>,
    region: Option<Vec<i64>>,
    capture_session_id: Option<String>,
    source_file: Option<String>,
    policy_posture: Option<String>,
}

#[derive(Debug, Clone)]
struct AudioTranscriptionRun {
    producer_run_id: String,
    model_id: String,
    model_version: Option<String>,
    input_material_ids: Option<Vec<String>>,
    output_refs: Option<Vec<String>>,
    duration_ms: Option<u64>,
    resource_posture: Option<String>,
    failure_class: Option<String>,
}

#[derive(Debug, Clone)]
struct ScreenOcrRun {
    producer_run_id: String,
    engine_id: String,
    engine_version: Option<String>,
    input_material_ids: Option<Vec<String>>,
    output_refs: Option<Vec<String>>,
    duration_ms: Option<u64>,
    resource_posture: Option<String>,
    failure_class: Option<String>,
}

#[derive(Debug, Clone)]
struct AudioTranscriptMaterial {
    recording: Option<AudioRecording>,
    transcription_run: Option<AudioTranscriptionRun>,
    segments: Vec<TranscriptSegment>,
}

#[derive(Debug, Clone)]
struct ScreenOcrMaterial {
    screenshot: Option<ScreenshotObservation>,
    capture_session_started: Option<ScreenCaptureSessionStarted>,
    capture_session_ended: Option<ScreenCaptureSessionEnded>,
    video_segment: Option<ScreenVideoSegment>,
    ocr_run: Option<ScreenOcrRun>,
    segments: Vec<OcrSegment>,
}

#[async_trait]
impl MaterialParser for MediaAudioTranscriptParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("media-audio-transcript-staged"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("media.audio-transcript"),
            declared_event_types: vec![
                (
                    EventSource::from_static("media.audio"),
                    EventType::from_static("media.audio.recording_observed"),
                ),
                (
                    EventSource::from_static("media.audio"),
                    EventType::from_static("media.audio.transcript_segment_observed"),
                ),
                (
                    EventSource::from_static("media.audio"),
                    EventType::from_static("media.audio.transcription_run_observed"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description: "Parses staged transcript text, VTT/SRT, or JSON segment exports into transcript segment observations.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("transcript material is not UTF-8: {e}")))?;
        let mut material = parse_audio_transcript_material(text)?;
        if let Some(run) = material.transcription_run.as_ref() {
            apply_audio_run_defaults(run, &mut material.segments);
        }
        let AudioTranscriptMaterial {
            recording,
            transcription_run,
            segments,
        } = material;
        let mut intents = Vec::new();
        if let Some(recording) = recording {
            intents.push(audio_recording_intent(recording, &record, ctx));
        }
        if let Some(run) = transcription_run {
            intents.push(audio_transcription_run_intent(run, &record, ctx));
        }
        intents.extend(
            segments
                .into_iter()
                .enumerate()
                .map(|(index, segment)| transcript_intent(index, segment, &record, ctx)),
        );
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["transcript text, VTT/SRT, or JSON segment export".to_string()]
    }
}

#[async_trait]
impl MaterialParser for MediaScreenOcrParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("media-screen-ocr-staged"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("media.screen-ocr"),
            declared_event_types: vec![
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.screenshot_observed"),
                ),
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.capture_session_started"),
                ),
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.capture_session_ended"),
                ),
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.video_segment_observed"),
                ),
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.ocr_segment_observed"),
                ),
                (
                    EventSource::from_static("media.screen"),
                    EventType::from_static("media.screen.ocr_run_observed"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
            ],
            description:
                "Parses staged OCR text, Tesseract TSV, or JSON segment exports into OCR segment observations."
                    .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("OCR material is not UTF-8: {e}")))?;
        let mut material = parse_screen_ocr_material(text)?;
        if let Some(run) = material.ocr_run.as_ref() {
            apply_ocr_run_defaults(run, &mut material.segments);
        }
        let ScreenOcrMaterial {
            screenshot,
            capture_session_started,
            capture_session_ended,
            video_segment,
            ocr_run,
            segments,
        } = material;
        let mut intents = Vec::new();
        if let Some(session_started) = capture_session_started {
            intents.push(screen_capture_session_started_intent(
                session_started,
                &record,
                ctx,
            ));
        }
        if let Some(screenshot) = screenshot {
            intents.push(screenshot_intent(screenshot, &record, ctx));
        }
        if let Some(video_segment) = video_segment {
            intents.push(screen_video_segment_intent(video_segment, &record, ctx));
        }
        if let Some(run) = ocr_run {
            intents.push(screen_ocr_run_intent(run, &record, ctx));
        }
        intents.extend(
            segments
                .into_iter()
                .enumerate()
                .map(|(index, segment)| ocr_intent(index, segment, &record, ctx)),
        );
        if let Some(session_ended) = capture_session_ended {
            intents.push(screen_capture_session_ended_intent(
                session_ended,
                &record,
                ctx,
            ));
        }
        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["OCR text, Tesseract TSV, or JSON segment export".to_string()]
    }
}

fn parse_audio_transcript_material(text: &str) -> ParserResult<AudioTranscriptMaterial> {
    if let Some(value) = parse_json_value(text)? {
        return match value {
            Value::Array(values) => Ok(AudioTranscriptMaterial {
                recording: None,
                transcription_run: None,
                segments: values
                    .into_iter()
                    .map(parse_transcript_json_segment)
                    .collect::<ParserResult<Vec<_>>>()?,
            }),
            Value::Object(mut object) => {
                let recording = object
                    .remove("recording")
                    .map(parse_audio_recording)
                    .transpose()?;
                let transcription_run = remove_first_field(
                    &mut object,
                    &["transcription_run", "transcription", "model_run"],
                )
                .map(parse_audio_transcription_run)
                .transpose()?;
                let segments = match object.remove("segments") {
                    Some(Value::Array(values)) => values
                        .into_iter()
                        .map(parse_transcript_json_segment)
                        .collect::<ParserResult<Vec<_>>>()?,
                    Some(_) => {
                        return Err(ParserError::Parse(
                            "media transcript manifest segments field must be an array".into(),
                        ));
                    }
                    None if recording.is_some() || transcription_run.is_some() => Vec::new(),
                    None => {
                        return Err(ParserError::Parse(
                            "media transcript JSON object must contain recording, transcription_run, or segments[]"
                                .into(),
                        ));
                    }
                };
                Ok(AudioTranscriptMaterial {
                    recording,
                    transcription_run,
                    segments,
                })
            }
            _ => Err(ParserError::Parse(
                "media transcript JSON must be an array or manifest object".into(),
            )),
        };
    }

    let segments = if text.contains("-->") {
        parse_timed_text_segments(text)?
    } else {
        nonempty_lines(text)
            .into_iter()
            .map(|text| TranscriptSegment {
                text,
                start_ms: None,
                end_ms: None,
                speaker_label: None,
                language: None,
                confidence: None,
                model_id: None,
                producer_run_id: None,
            })
            .collect()
    };
    Ok(AudioTranscriptMaterial {
        recording: None,
        transcription_run: None,
        segments,
    })
}

fn parse_transcript_json_segment(value: Value) -> ParserResult<TranscriptSegment> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "transcript segment JSON must be an object".into(),
        ));
    };

    let text = string_field(&object, "text")?
        .ok_or_else(|| ParserError::Parse("transcript segment JSON missing text field".into()))?;

    Ok(TranscriptSegment {
        text,
        start_ms: time_ms_field(&object, "start_ms", "start")?,
        end_ms: time_ms_field(&object, "end_ms", "end")?,
        speaker_label: string_field(&object, "speaker_label")?
            .or(string_field(&object, "speaker")?),
        language: string_field(&object, "language")?,
        confidence: number_field(&object, "confidence")?,
        model_id: string_field(&object, "model_id")?
            .or(string_field(&object, "model")?)
            .or(string_field(&object, "model_name")?),
        producer_run_id: string_field(&object, "producer_run_id")?
            .or(string_field(&object, "run_id")?),
    })
}

fn parse_audio_transcription_run(value: Value) -> ParserResult<AudioTranscriptionRun> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "audio transcription_run manifest entry must be an object".into(),
        ));
    };
    Ok(AudioTranscriptionRun {
        producer_run_id: required_any_string_field(&object, &["producer_run_id", "run_id"])?,
        model_id: required_any_string_field(&object, &["model_id", "model", "model_name"])?,
        model_version: any_string_field(&object, &["model_version", "version"])?,
        input_material_ids: any_string_array_field(&object, &["input_material_ids", "inputs"])?,
        output_refs: any_string_array_field(&object, &["output_refs", "outputs"])?,
        duration_ms: u64_field(&object, "duration_ms")?,
        resource_posture: string_field(&object, "resource_posture")?,
        failure_class: string_field(&object, "failure_class")?,
    })
}

fn parse_screen_ocr_run(value: Value) -> ParserResult<ScreenOcrRun> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "screen OCR ocr_run manifest entry must be an object".into(),
        ));
    };
    Ok(ScreenOcrRun {
        producer_run_id: required_any_string_field(&object, &["producer_run_id", "run_id"])?,
        engine_id: required_any_string_field(&object, &["engine_id", "engine", "model_id"])?,
        engine_version: any_string_field(&object, &["engine_version", "version"])?,
        input_material_ids: any_string_array_field(&object, &["input_material_ids", "inputs"])?,
        output_refs: any_string_array_field(&object, &["output_refs", "outputs"])?,
        duration_ms: u64_field(&object, "duration_ms")?,
        resource_posture: string_field(&object, "resource_posture")?,
        failure_class: string_field(&object, "failure_class")?,
    })
}

fn remove_first_field(object: &mut Map<String, Value>, keys: &[&str]) -> Option<Value> {
    keys.iter().find_map(|key| object.remove(*key))
}

fn string_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ParserError::Parse(format!(
            "transcript segment field {key:?} must be a string"
        ))),
    }
}

fn any_string_field(object: &Map<String, Value>, keys: &[&str]) -> ParserResult<Option<String>> {
    for key in keys {
        if let Some(value) = string_field(object, key)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn required_any_string_field(object: &Map<String, Value>, keys: &[&str]) -> ParserResult<String> {
    any_string_field(object, keys)?.ok_or_else(|| {
        ParserError::Parse(format!(
            "media manifest missing one of required string fields: {}",
            keys.join(", ")
        ))
    })
}

fn string_array_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<Vec<String>>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                Value::String(value) => Ok(value.clone()),
                _ => Err(ParserError::Parse(format!(
                    "media manifest field {key:?} array entries must be strings"
                ))),
            })
            .collect::<ParserResult<Vec<_>>>()
            .map(Some),
        Some(_) => Err(ParserError::Parse(format!(
            "media manifest field {key:?} must be an array"
        ))),
    }
}

fn any_string_array_field(
    object: &Map<String, Value>,
    keys: &[&str],
) -> ParserResult<Option<Vec<String>>> {
    for key in keys {
        if let Some(value) = string_array_field(object, key)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn number_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<f64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value.as_f64().map(Some).ok_or_else(|| {
            ParserError::Parse(format!("transcript segment field {key:?} is not finite"))
        }),
        Some(_) => Err(ParserError::Parse(format!(
            "transcript segment field {key:?} must be a number"
        ))),
    }
}

fn u64_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<u64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| ParserError::Parse(format!("media manifest field {key:?} must be u64"))),
        Some(_) => Err(ParserError::Parse(format!(
            "media manifest field {key:?} must be a number"
        ))),
    }
}

fn u32_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<u32>> {
    let Some(value) = u64_field(object, key)? else {
        return Ok(None);
    };
    u32::try_from(value)
        .map(Some)
        .map_err(|_| ParserError::Parse(format!("media manifest field {key:?} exceeds u32")))
}

fn required_u32_field(object: &Map<String, Value>, key: &str) -> ParserResult<u32> {
    u32_field(object, key)?
        .ok_or_else(|| ParserError::Parse(format!("media manifest missing {key:?} field")))
}

fn i64_array_field(object: &Map<String, Value>, key: &str) -> ParserResult<Option<Vec<i64>>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                Value::Number(number) => number.as_i64().ok_or_else(|| {
                    ParserError::Parse(format!(
                        "media manifest field {key:?} array entries must be i64"
                    ))
                }),
                _ => Err(ParserError::Parse(format!(
                    "media manifest field {key:?} array entries must be numbers"
                ))),
            })
            .collect::<ParserResult<Vec<_>>>()
            .map(Some),
        Some(_) => Err(ParserError::Parse(format!(
            "media manifest field {key:?} must be an array"
        ))),
    }
}

fn time_ms_field(
    object: &Map<String, Value>,
    millis_key: &str,
    seconds_key: &str,
) -> ParserResult<Option<u64>> {
    if let Some(value) = object.get(millis_key) {
        return match value {
            Value::Null => Ok(None),
            Value::Number(number) => number_to_u64_millis(number, millis_key).map(Some),
            _ => Err(ParserError::Parse(format!(
                "transcript segment field {millis_key:?} must be a number"
            ))),
        };
    }

    let Some(seconds) = number_field(object, seconds_key)? else {
        return Ok(None);
    };
    if seconds.is_sign_negative() {
        return Err(ParserError::Parse(format!(
            "transcript segment field {seconds_key:?} must not be negative"
        )));
    }
    Ok(Some((seconds * 1000.0).round() as u64))
}

fn number_to_u64_millis(number: &serde_json::Number, key: &str) -> ParserResult<u64> {
    if let Some(value) = number.as_u64() {
        return Ok(value);
    }
    let Some(value) = number.as_f64() else {
        return Err(ParserError::Parse(format!(
            "transcript segment field {key:?} is not finite"
        )));
    };
    if value.is_sign_negative() || value.fract() != 0.0 {
        return Err(ParserError::Parse(format!(
            "transcript segment field {key:?} must be an unsigned integer millisecond value"
        )));
    }
    Ok(value as u64)
}

fn parse_timed_text_segments(text: &str) -> ParserResult<Vec<TranscriptSegment>> {
    let mut segments = Vec::new();
    let normalized = text.replace("\r\n", "\n");

    for block in normalized.split("\n\n") {
        let mut lines = block
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && *line != "WEBVTT" && !line.starts_with("NOTE"));

        let Some(first) = lines.next() else {
            continue;
        };
        let timing_line = if first.contains("-->") {
            first
        } else {
            let candidate = lines.next().ok_or_else(|| {
                ParserError::Parse("timed transcript cue missing timing line".into())
            })?;
            if !candidate.contains("-->") {
                continue;
            }
            candidate
        };

        let (start_ms, end_ms) = parse_timing_line(timing_line)?;
        let body = lines.collect::<Vec<_>>().join("\n");
        if body.trim().is_empty() {
            continue;
        }

        segments.push(TranscriptSegment {
            text: body,
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
            speaker_label: None,
            language: None,
            confidence: None,
            model_id: None,
            producer_run_id: None,
        });
    }

    if segments.is_empty() {
        return Err(ParserError::Parse(
            "timed transcript contained no text cues".into(),
        ));
    }
    Ok(segments)
}

fn parse_timing_line(line: &str) -> ParserResult<(u64, u64)> {
    let (start, rest) = line
        .split_once("-->")
        .ok_or_else(|| ParserError::Parse("timed transcript cue missing --> separator".into()))?;
    let end = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| ParserError::Parse("timed transcript cue missing end timestamp".into()))?;
    Ok((parse_time_ms(start.trim())?, parse_time_ms(end.trim())?))
}

fn parse_time_ms(raw: &str) -> ParserResult<u64> {
    let normalized = raw.replace(',', ".");
    let (time, millis) = normalized
        .rsplit_once('.')
        .ok_or_else(|| ParserError::Parse(format!("timestamp {raw:?} missing milliseconds")))?;
    let millis = millis
        .parse::<u64>()
        .map_err(|e| ParserError::Parse(format!("invalid timestamp milliseconds {raw:?}: {e}")))?;
    if millis > 999 {
        return Err(ParserError::Parse(format!(
            "timestamp {raw:?} has millisecond component above 999"
        )));
    }

    let parts = time
        .split(':')
        .map(|part| {
            part.parse::<u64>()
                .map_err(|e| ParserError::Parse(format!("invalid timestamp {raw:?}: {e}")))
        })
        .collect::<ParserResult<Vec<_>>>()?;

    let seconds = match parts.as_slice() {
        [minutes, seconds] => minutes * 60 + seconds,
        [hours, minutes, seconds] => hours * 3600 + minutes * 60 + seconds,
        _ => {
            return Err(ParserError::Parse(format!(
                "timestamp {raw:?} must be MM:SS.mmm or HH:MM:SS.mmm"
            )));
        }
    };

    Ok(seconds * 1000 + millis)
}

fn parse_screen_ocr_material(text: &str) -> ParserResult<ScreenOcrMaterial> {
    if let Some(value) = parse_json_value(text)? {
        return match value {
            Value::Array(values) => Ok(ScreenOcrMaterial {
                screenshot: None,
                capture_session_started: None,
                capture_session_ended: None,
                video_segment: None,
                ocr_run: None,
                segments: values
                    .into_iter()
                    .map(parse_ocr_json_segment)
                    .collect::<ParserResult<Vec<_>>>()?,
            }),
            Value::Object(mut object) => {
                let screenshot = object
                    .remove("screenshot")
                    .map(parse_screenshot)
                    .transpose()?;
                let capture_session_started = remove_first_field(
                    &mut object,
                    &[
                        "capture_session_started",
                        "session_started",
                        "screen_capture_session_started",
                    ],
                )
                .map(parse_screen_capture_session_started)
                .transpose()?;
                let capture_session_ended = remove_first_field(
                    &mut object,
                    &[
                        "capture_session_ended",
                        "session_ended",
                        "screen_capture_session_ended",
                    ],
                )
                .map(parse_screen_capture_session_ended)
                .transpose()?;
                let video_segment = remove_first_field(
                    &mut object,
                    &["video_segment", "screen_video", "recording"],
                )
                .map(parse_screen_video_segment)
                .transpose()?;
                let ocr_run = remove_first_field(&mut object, &["ocr_run", "ocr", "model_run"])
                    .map(parse_screen_ocr_run)
                    .transpose()?;
                let segments = match object.remove("segments") {
                    Some(Value::Array(values)) => values
                        .into_iter()
                        .map(parse_ocr_json_segment)
                        .collect::<ParserResult<Vec<_>>>()?,
                    Some(_) => {
                        return Err(ParserError::Parse(
                            "screen OCR manifest segments field must be an array".into(),
                        ));
                    }
                    None if screenshot.is_some()
                        || capture_session_started.is_some()
                        || capture_session_ended.is_some()
                        || video_segment.is_some()
                        || ocr_run.is_some() =>
                    {
                        Vec::new()
                    }
                    None => {
                        return Err(ParserError::Parse(
                            "screen OCR JSON object must contain screenshot, capture_session_started, capture_session_ended, video_segment, ocr_run, or segments[]"
                                .into(),
                        ));
                    }
                };
                Ok(ScreenOcrMaterial {
                    screenshot,
                    capture_session_started,
                    capture_session_ended,
                    video_segment,
                    ocr_run,
                    segments,
                })
            }
            _ => Err(ParserError::Parse(
                "screen OCR JSON must be an array or manifest object".into(),
            )),
        };
    }

    if looks_like_tesseract_tsv(text) {
        return Ok(ScreenOcrMaterial {
            screenshot: None,
            capture_session_started: None,
            capture_session_ended: None,
            video_segment: None,
            ocr_run: None,
            segments: parse_tesseract_tsv_segments(text)?,
        });
    }

    Ok(ScreenOcrMaterial {
        screenshot: None,
        capture_session_started: None,
        capture_session_ended: None,
        video_segment: None,
        ocr_run: None,
        segments: nonempty_lines(text)
            .into_iter()
            .map(|text| OcrSegment {
                text,
                bbox: None,
                confidence: None,
                display_id: None,
                window_title: None,
                engine: None,
                producer_run_id: None,
            })
            .collect(),
    })
}

fn parse_ocr_json_segment(value: Value) -> ParserResult<OcrSegment> {
    serde_json::from_value(value)
        .map_err(|e| ParserError::Parse(format!("invalid OCR segment JSON: {e}")))
}

fn parse_audio_recording(value: Value) -> ParserResult<AudioRecording> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "audio recording manifest entry must be an object".into(),
        ));
    };
    Ok(AudioRecording {
        file_format: string_field(&object, "file_format")?.or(string_field(&object, "format")?),
        codec: string_field(&object, "codec")?,
        duration_ms: u64_field(&object, "duration_ms")?,
        channel_count: u32_field(&object, "channel_count")?.or(u32_field(&object, "channels")?),
        sample_rate_hz: u32_field(&object, "sample_rate_hz")?,
        capture_session_id: string_field(&object, "capture_session_id")?,
        source_file: string_field(&object, "source_file")?,
        policy_posture: string_field(&object, "policy_posture")?,
    })
}

fn parse_screenshot(value: Value) -> ParserResult<ScreenshotObservation> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "screenshot manifest entry must be an object".into(),
        ));
    };
    Ok(ScreenshotObservation {
        display_id: string_field(&object, "display_id")?,
        window_title: string_field(&object, "window_title")?,
        region: i64_array_field(&object, "region")?,
        width_px: required_u32_field(&object, "width_px")
            .or_else(|_| required_u32_field(&object, "width"))?,
        height_px: required_u32_field(&object, "height_px")
            .or_else(|_| required_u32_field(&object, "height"))?,
        capture_session_id: string_field(&object, "capture_session_id")?,
        source_file: string_field(&object, "source_file")?,
        policy_posture: string_field(&object, "policy_posture")?,
    })
}

fn parse_screen_capture_session_started(value: Value) -> ParserResult<ScreenCaptureSessionStarted> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "screen capture session_started manifest entry must be an object".into(),
        ));
    };
    Ok(ScreenCaptureSessionStarted {
        capture_session_id: required_any_string_field(
            &object,
            &["capture_session_id", "session_id"],
        )?,
        scope: string_field(&object, "scope")?,
        reason: string_field(&object, "reason")?,
        operator_binding_id: any_string_field(
            &object,
            &["operator_binding_id", "binding_id", "mode_id"],
        )?,
        display_id: string_field(&object, "display_id")?,
        region: i64_array_field(&object, "region")?,
        policy_posture: string_field(&object, "policy_posture")?,
        started_at: string_field(&object, "started_at")?,
    })
}

fn parse_screen_capture_session_ended(value: Value) -> ParserResult<ScreenCaptureSessionEnded> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "screen capture session_ended manifest entry must be an object".into(),
        ));
    };
    Ok(ScreenCaptureSessionEnded {
        capture_session_id: required_any_string_field(
            &object,
            &["capture_session_id", "session_id"],
        )?,
        reason: string_field(&object, "reason")?,
        duration_ms: u64_field(&object, "duration_ms")?,
        final_state: string_field(&object, "final_state")?,
        policy_posture: string_field(&object, "policy_posture")?,
        ended_at: string_field(&object, "ended_at")?,
    })
}

fn parse_screen_video_segment(value: Value) -> ParserResult<ScreenVideoSegment> {
    let Value::Object(object) = value else {
        return Err(ParserError::Parse(
            "screen video manifest entry must be an object".into(),
        ));
    };
    Ok(ScreenVideoSegment {
        file_format: string_field(&object, "file_format")?
            .or(string_field(&object, "format")?)
            .or(string_field(&object, "container")?),
        codec: string_field(&object, "codec")?,
        duration_ms: u64_field(&object, "duration_ms")?,
        frame_rate_fps: number_field(&object, "frame_rate_fps")?,
        width_px: u32_field(&object, "width_px")?.or(u32_field(&object, "width")?),
        height_px: u32_field(&object, "height_px")?.or(u32_field(&object, "height")?),
        display_id: string_field(&object, "display_id")?,
        window_title: string_field(&object, "window_title")?,
        region: i64_array_field(&object, "region")?,
        capture_session_id: string_field(&object, "capture_session_id")?,
        source_file: string_field(&object, "source_file")?.or(string_field(&object, "path")?),
        policy_posture: string_field(&object, "policy_posture")?,
    })
}

fn looks_like_tesseract_tsv(text: &str) -> bool {
    let Some(header) = text.lines().find(|line| !line.trim().is_empty()) else {
        return false;
    };
    let columns = header
        .split('\t')
        .map(|column| column.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    ["left", "top", "width", "height", "conf", "text"]
        .iter()
        .all(|required| columns.iter().any(|column| column == required))
}

fn parse_tesseract_tsv_segments(text: &str) -> ParserResult<Vec<OcrSegment>> {
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| ParserError::Parse("OCR TSV missing header".into()))?;
    let columns = header
        .split('\t')
        .map(|column| column.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();

    let index_of = |name: &str| {
        columns
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| ParserError::Parse(format!("OCR TSV missing {name:?} column")))
    };
    let left = index_of("left")?;
    let top = index_of("top")?;
    let width = index_of("width")?;
    let height = index_of("height")?;
    let confidence = index_of("conf")?;
    let text_col = index_of("text")?;

    let mut segments = Vec::new();
    for (line_number, line) in lines.enumerate() {
        let values = line.split('\t').collect::<Vec<_>>();
        let text = values
            .get(text_col)
            .map(|value| value.trim())
            .unwrap_or_default();
        if text.is_empty() {
            continue;
        }

        let parse_i64 = |index: usize, name: &str| {
            values
                .get(index)
                .ok_or_else(|| {
                    ParserError::Parse(format!(
                        "OCR TSV row {} missing {name:?} column",
                        line_number + 2
                    ))
                })?
                .trim()
                .parse::<i64>()
                .map_err(|e| {
                    ParserError::Parse(format!(
                        "invalid OCR TSV {name:?} value on row {}: {e}",
                        line_number + 2
                    ))
                })
        };
        let confidence = values
            .get(confidence)
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| *value >= 0.0)
            .map(|value| value / 100.0);

        segments.push(OcrSegment {
            text: text.to_string(),
            bbox: Some(vec![
                parse_i64(left, "left")?,
                parse_i64(top, "top")?,
                parse_i64(width, "width")?,
                parse_i64(height, "height")?,
            ]),
            confidence,
            display_id: None,
            window_title: None,
            engine: Some("tesseract-tsv".to_string()),
            producer_run_id: None,
        });
    }

    if segments.is_empty() {
        return Err(ParserError::Parse("OCR TSV contained no text rows".into()));
    }
    Ok(segments)
}

fn apply_audio_run_defaults(run: &AudioTranscriptionRun, segments: &mut [TranscriptSegment]) {
    for segment in segments {
        if segment.producer_run_id.is_none() {
            segment.producer_run_id = Some(run.producer_run_id.clone());
        }
        if segment.model_id.is_none() {
            segment.model_id = Some(run.model_id.clone());
        }
    }
}

fn apply_ocr_run_defaults(run: &ScreenOcrRun, segments: &mut [OcrSegment]) {
    for segment in segments {
        if segment.producer_run_id.is_none() {
            segment.producer_run_id = Some(run.producer_run_id.clone());
        }
        if segment.engine.is_none() {
            segment.engine = Some(run.engine_id.clone());
        }
    }
}

fn parse_json_value(text: &str) -> ParserResult<Option<Value>> {
    let trimmed = text.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Ok(None);
    }
    serde_json::from_str(trimmed)
        .map(Some)
        .map_err(|e| ParserError::Parse(format!("invalid staged media JSON: {e}")))
}

fn nonempty_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn audio_recording_intent(
    recording: AudioRecording,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let material_id = record.material_id.to_string();
    let source_file = recording.source_file.or_else(|| logical_path(record));
    let payload = json!({
        "raw_material_id": material_id,
        "file_format": recording.file_format,
        "codec": recording.codec,
        "duration_ms": recording.duration_ms,
        "channel_count": recording.channel_count,
        "sample_rate_hz": recording.sample_rate_hz,
        "capture_session_id": recording.capture_session_id,
        "source_file": source_file,
        "policy_posture": recording.policy_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.audio-transcript"),
        fields: vec![
            ("material_id".into(), record.material_id.to_string()),
            (
                "duration_ms".into(),
                recording
                    .duration_ms
                    .map_or_else(String::new, |value| value.to_string()),
            ),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-audio-transcript-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.audio"))
        .event_type(EventType::from_static("media.audio.recording_observed"))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn audio_transcription_run_intent(
    run: AudioTranscriptionRun,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let producer_run_id = run.producer_run_id;
    let model_id = run.model_id;
    let input_material_ids = run
        .input_material_ids
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec![record.material_id.to_string()]);
    let occurrence_inputs = input_material_ids.join(",");
    let payload = json!({
        "producer_run_id": producer_run_id.clone(),
        "model_id": model_id.clone(),
        "model_version": run.model_version,
        "input_material_ids": input_material_ids,
        "output_refs": run.output_refs.unwrap_or_default(),
        "duration_ms": run.duration_ms,
        "resource_posture": run.resource_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "failure_class": run.failure_class,
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.audio-transcript"),
        fields: vec![
            ("producer_run_id".into(), producer_run_id),
            ("model_id".into(), model_id),
            ("input_material_ids".into(), occurrence_inputs),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-audio-transcript-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.audio"))
        .event_type(EventType::from_static(
            "media.audio.transcription_run_observed",
        ))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn screenshot_intent(
    screenshot: ScreenshotObservation,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let material_id = record.material_id.to_string();
    let source_file = screenshot.source_file.or_else(|| logical_path(record));
    let region_key = screenshot
        .region
        .as_ref()
        .map(|region| {
            region
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let payload = json!({
        "raw_material_id": material_id,
        "display_id": screenshot.display_id,
        "window_title": screenshot.window_title,
        "region": screenshot.region,
        "width_px": screenshot.width_px,
        "height_px": screenshot.height_px,
        "capture_session_id": screenshot.capture_session_id,
        "source_file": source_file,
        "policy_posture": screenshot.policy_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("material_id".into(), record.material_id.to_string()),
            ("region".into(), region_key),
            ("width_px".into(), screenshot.width_px.to_string()),
            ("height_px".into(), screenshot.height_px.to_string()),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static("media.screen.screenshot_observed"))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn screen_capture_session_started_intent(
    session: ScreenCaptureSessionStarted,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = timestamp_or_acquisition(session.started_at.as_deref(), ctx);
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let capture_session_id = session.capture_session_id;
    let region_key = session
        .region
        .as_ref()
        .map(|region| {
            region
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let payload = json!({
        "capture_session_id": capture_session_id.clone(),
        "scope": session.scope.unwrap_or_else(|| "screen".to_string()),
        "reason": session.reason.unwrap_or_else(|| "operator_requested".to_string()),
        "operator_binding_id": session.operator_binding_id.unwrap_or_else(|| "source:media.screen-ocr.on-demand-region".to_string()),
        "display_id": session.display_id,
        "region": session.region,
        "policy_posture": session.policy_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "started_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("capture_session_id".into(), capture_session_id),
            ("started_at".into(), observed_at.format_rfc3339()),
            ("region".into(), region_key),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static(
            "media.screen.capture_session_started",
        ))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn screen_capture_session_ended_intent(
    session: ScreenCaptureSessionEnded,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = timestamp_or_acquisition(session.ended_at.as_deref(), ctx);
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let capture_session_id = session.capture_session_id;
    let payload = json!({
        "capture_session_id": capture_session_id.clone(),
        "reason": session.reason,
        "ended_at": observed_at,
        "duration_ms": session.duration_ms,
        "final_state": session.final_state.unwrap_or_else(|| "completed".to_string()),
        "policy_posture": session.policy_posture.unwrap_or_else(|| "operator_controlled".to_string()),
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("capture_session_id".into(), capture_session_id),
            ("ended_at".into(), observed_at.format_rfc3339()),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static("media.screen.capture_session_ended"))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn screen_video_segment_intent(
    video: ScreenVideoSegment,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let material_id = record.material_id.to_string();
    let source_file = video.source_file.or_else(|| logical_path(record));
    let display_id = video.display_id.clone();
    let capture_session_id = video.capture_session_id.clone();
    let region_key = video
        .region
        .as_ref()
        .map(|region| {
            region
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let payload = json!({
        "raw_material_id": material_id,
        "file_format": video.file_format,
        "codec": video.codec,
        "duration_ms": video.duration_ms,
        "frame_rate_fps": video.frame_rate_fps,
        "width_px": video.width_px,
        "height_px": video.height_px,
        "display_id": display_id.clone(),
        "window_title": video.window_title,
        "region": video.region,
        "capture_session_id": capture_session_id.clone(),
        "source_file": source_file,
        "policy_posture": video.policy_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("raw_material_id".into(), record.material_id.to_string()),
            (
                "capture_session_id".into(),
                capture_session_id.unwrap_or_default(),
            ),
            ("display_id".into(), display_id.unwrap_or_default()),
            ("region".into(), region_key),
            (
                "duration_ms".into(),
                video
                    .duration_ms
                    .map_or_else(String::new, |value| value.to_string()),
            ),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static(
            "media.screen.video_segment_observed",
        ))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn timestamp_or_acquisition(value: Option<&str>, ctx: &ParserContext) -> Timestamp {
    value
        .and_then(|value| {
            time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()
        })
        .and_then(|value| Timestamp::from_unix_timestamp_nanos(value.unix_timestamp_nanos()))
        .unwrap_or(ctx.acquisition_time)
}

fn screen_ocr_run_intent(
    run: ScreenOcrRun,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let producer_run_id = run.producer_run_id;
    let engine_id = run.engine_id;
    let input_material_ids = run
        .input_material_ids
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec![record.material_id.to_string()]);
    let occurrence_inputs = input_material_ids.join(",");
    let payload = json!({
        "producer_run_id": producer_run_id.clone(),
        "engine_id": engine_id.clone(),
        "engine_version": run.engine_version,
        "input_material_ids": input_material_ids,
        "output_refs": run.output_refs.unwrap_or_default(),
        "duration_ms": run.duration_ms,
        "resource_posture": run.resource_posture.unwrap_or_else(|| "operator_controlled".to_string()),
        "failure_class": run.failure_class,
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("producer_run_id".into(), producer_run_id),
            ("engine_id".into(), engine_id),
            ("input_material_ids".into(), occurrence_inputs),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static("media.screen.ocr_run_observed"))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: 0,
            len: record.bytes.len() as u64,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn transcript_intent(
    index: usize,
    segment: TranscriptSegment,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let material_id = record.material_id.to_string();
    let source_file = logical_path(record);
    let payload = json!({
        "segment_index": index as u32,
        "text": segment.text,
        "start_ms": segment.start_ms,
        "end_ms": segment.end_ms,
        "speaker_label": segment.speaker_label,
        "language": segment.language,
        "confidence": segment.confidence,
        "source_file": source_file,
        "raw_material_id": material_id,
        "model_id": segment.model_id,
        "producer_run_id": segment.producer_run_id,
        "timestamp_quality": null,
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.audio-transcript"),
        fields: vec![
            ("material_id".into(), record.material_id.to_string()),
            ("segment_index".into(), index.to_string()),
            (
                "start_ms".into(),
                segment
                    .start_ms
                    .map_or_else(String::new, |value| value.to_string()),
            ),
            (
                "end_ms".into(),
                segment
                    .end_ms
                    .map_or_else(String::new, |value| value.to_string()),
            ),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-audio-transcript-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.audio"))
        .event_type(EventType::from_static(
            "media.audio.transcript_segment_observed",
        ))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: index as u64,
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn ocr_intent(
    index: usize,
    segment: OcrSegment,
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParsedEventIntent {
    let observed_at = ctx.acquisition_time;
    let timing = record
        .source_ts_hint
        .clone()
        .unwrap_or(TimingEvidence::StagedAtFallback);
    let material_id = record.material_id.to_string();
    let source_file = logical_path(record);
    let bbox_key = segment
        .bbox
        .as_ref()
        .map(|bbox| {
            bbox.iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let payload = json!({
        "segment_index": index as u32,
        "text": segment.text,
        "bbox": segment.bbox,
        "confidence": segment.confidence,
        "display_id": segment.display_id,
        "window_title": segment.window_title,
        "source_file": source_file,
        "raw_material_id": material_id,
        "engine": segment.engine,
        "producer_run_id": segment.producer_run_id,
        "timestamp_quality": null,
        "observed_at": observed_at,
    });
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("media.screen-ocr"),
        fields: vec![
            ("material_id".into(), record.material_id.to_string()),
            ("segment_index".into(), index.to_string()),
            ("bbox".into(), bbox_key),
        ],
    };

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("media-screen-ocr-staged"))
        .parser_version("1.0.0")
        .event_source(EventSource::from_static("media.screen"))
        .event_type(EventType::from_static("media.screen.ocr_segment_observed"))
        .payload(payload)
        .ts_orig(observed_at)
        .timing(timing)
        .anchor(MaterialAnchor::ByteRange {
            start: index as u64,
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

fn logical_path(record: &SourceRecord) -> Option<String> {
    record.logical_path.as_ref().map(|path| path.to_string())
}

#[cfg(test)]
mod tests;
