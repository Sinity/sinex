//! Media capture source contracts (#1043).
//!
//! The implemented parsers consume transcript/OCR text material and staged
//! bundle manifests that anchor raw recording/screenshot observations.
//! Additional proposed bindings keep the full media package shape visible to
//! package-completeness, coverage, and deployment inventory without claiming
//! that the corresponding live/model runner is executable today.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingEvidence,
};
use sinex_primitives::privacy::{ProcessingContext, SensitivityHint};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
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
    binding(
        subject = "source:media.audio-transcript.audio-bundle-staged",
        event_type = "media.audio.recording_observed",
        implementation = "staged-audio-bundle",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.import-bundle, operation:media.audio-transcript.inspect, operation:media.audio-transcript.delete-material, operation:media.audio-transcript.export",
        proposed = true
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
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.run-model, operation:media.audio-transcript.retry, operation:media.audio-transcript.rebuild-artifact, operation:media.audio-transcript.inspect",
        proposed = true
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
    event_types = "media.screen.screenshot_observed, media.screen.ocr_run_observed",
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
    binding(
        subject = "source:media.screen-ocr.screenshot-ocr-staged",
        event_type = "media.screen.screenshot_observed",
        implementation = "staged-screenshot-bundle",
        adapter = "FileContentDropAdapter",
        resource_profile = ResourceProfile::Oneshot,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.import-screenshots, operation:media.screen-ocr.inspect, operation:media.screen-ocr.delete-material, operation:media.screen-ocr.export",
        proposed = true
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
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.run-ocr, operation:media.screen-ocr.retry, operation:media.screen-ocr.rebuild-artifact, operation:media.screen-ocr.inspect",
        proposed = true
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
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.capture-region, operation:media.screen-ocr.pause, operation:media.screen-ocr.resume, operation:media.screen-ocr.inspect",
        proposed = true
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
        capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.enable-session, operation:media.screen-ocr.disable-session, operation:media.screen-ocr.pause, operation:media.screen-ocr.resume, operation:media.screen-ocr.retry, operation:media.screen-ocr.inspect",
        proposed = true
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
struct AudioTranscriptMaterial {
    recording: Option<AudioRecording>,
    segments: Vec<TranscriptSegment>,
}

#[derive(Debug, Clone)]
struct ScreenOcrMaterial {
    screenshot: Option<ScreenshotObservation>,
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
            declared_event_types: vec![(
                EventSource::from_static("media.audio"),
                EventType::from_static("media.audio.transcript_segment_observed"),
            )],
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
        let material = parse_audio_transcript_material(text)?;
        let mut intents = Vec::new();
        if let Some(recording) = material.recording {
            intents.push(audio_recording_intent(recording, &record, ctx));
        }
        intents.extend(
            material
                .segments
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
            declared_event_types: vec![(
                EventSource::from_static("media.screen"),
                EventType::from_static("media.screen.ocr_segment_observed"),
            )],
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
        let material = parse_screen_ocr_material(text)?;
        let mut intents = Vec::new();
        if let Some(screenshot) = material.screenshot {
            intents.push(screenshot_intent(screenshot, &record, ctx));
        }
        intents.extend(
            material
                .segments
                .into_iter()
                .enumerate()
                .map(|(index, segment)| ocr_intent(index, segment, &record, ctx)),
        );
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
                    None if recording.is_some() => Vec::new(),
                    None => {
                        return Err(ParserError::Parse(
                            "media transcript JSON object must contain recording or segments[]"
                                .into(),
                        ));
                    }
                };
                Ok(AudioTranscriptMaterial {
                    recording,
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
            })
            .collect()
    };
    Ok(AudioTranscriptMaterial {
        recording: None,
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
    })
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
                    None if screenshot.is_some() => Vec::new(),
                    None => {
                        return Err(ParserError::Parse(
                            "screen OCR JSON object must contain screenshot or segments[]".into(),
                        ));
                    }
                };
                Ok(ScreenOcrMaterial {
                    screenshot,
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
            segments: parse_tesseract_tsv_segments(text)?,
        });
    }

    Ok(ScreenOcrMaterial {
        screenshot: None,
        segments: nonempty_lines(text)
            .into_iter()
            .map(|text| OcrSegment {
                text,
                bbox: None,
                confidence: None,
                display_id: None,
                window_title: None,
                engine: None,
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
        });
    }

    if segments.is_empty() {
        return Err(ParserError::Parse("OCR TSV contained no text rows".into()));
    }
    Ok(segments)
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
mod tests {
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
    async fn media_runtime_bindings_cover_staged_model_on_demand_and_live_modes() -> TestResult<()>
    {
        let bindings = source_runtime_bindings()
            .filter(|binding| {
                binding.source_id == "media.audio-transcript"
                    || binding.source_id == "media.screen-ocr"
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
        assert_eq!(
            binding("source:media.audio-transcript.local-model-batch").runtime_shape,
            RuntimeShape::OnDemand
        );
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
        assert_eq!(
            binding("source:media.screen-ocr.on-demand-region").runtime_shape,
            RuntimeShape::OnDemand
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
}
