//! Media capture source contracts — staged transcripts + OCR text (#1043).

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

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
    event_source = "media.audio",
    adapter = "FileContentDropAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(material_id, segment_index, start_ms, end_ms)"),
    access_scope = AccessScope::StagedExport,
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.audio-transcript.check, operation:media.audio-transcript.import, operation:media.audio-transcript.inspect, operation:media.audio-transcript.replay",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
)]
pub struct MediaAudioTranscriptParser;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "media.screen-ocr",
    namespace = "media",
    event_type = "media.screen.ocr_segment_observed",
    event_source = "media.screen",
    adapter = "FileContentDropAdapter",
    implementation = "staged-parser",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(material_id, segment_index, bbox)"),
    access_scope = AccessScope::StagedExport,
    capabilities = "coverage:source-coverage, debt:unified-debt-view, operation:media.screen-ocr.check, operation:media.screen-ocr.import, operation:media.screen-ocr.inspect, operation:media.screen-ocr.replay",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::Staged,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Scheduled,
)]
pub struct MediaScreenOcrParser;

#[derive(Debug, Clone, Deserialize)]
struct TranscriptSegment {
    text: String,
    #[serde(default)]
    start_ms: Option<u64>,
    #[serde(default)]
    end_ms: Option<u64>,
    #[serde(default)]
    speaker_label: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
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
            description: "Parses staged transcript text or JSON segment exports into transcript segment observations.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("transcript material is not UTF-8: {e}")))?;
        let segments = parse_transcript_segments(text)?;
        Ok(segments
            .into_iter()
            .enumerate()
            .map(|(index, segment)| transcript_intent(index, segment, &record, ctx))
            .collect())
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["transcript text or JSON segment export".to_string()]
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
                "Parses staged OCR text or JSON segment exports into OCR segment observations."
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
        let segments = parse_ocr_segments(text)?;
        Ok(segments
            .into_iter()
            .enumerate()
            .map(|(index, segment)| ocr_intent(index, segment, &record, ctx))
            .collect())
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["OCR text or JSON segment export".to_string()]
    }
}

fn parse_transcript_segments(text: &str) -> ParserResult<Vec<TranscriptSegment>> {
    if let Some(value) = parse_json_value(text)? {
        let segment_values = segment_values(value)?;
        return segment_values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).map_err(|e| {
                    ParserError::Parse(format!("invalid transcript segment JSON: {e}"))
                })
            })
            .collect();
    }

    Ok(nonempty_lines(text)
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
        .collect())
}

fn parse_ocr_segments(text: &str) -> ParserResult<Vec<OcrSegment>> {
    if let Some(value) = parse_json_value(text)? {
        let segment_values = segment_values(value)?;
        return segment_values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|e| ParserError::Parse(format!("invalid OCR segment JSON: {e}")))
            })
            .collect();
    }

    Ok(nonempty_lines(text)
        .into_iter()
        .map(|text| OcrSegment {
            text,
            bbox: None,
            confidence: None,
            display_id: None,
            window_title: None,
            engine: None,
        })
        .collect())
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

fn segment_values(value: Value) -> ParserResult<Vec<Value>> {
    match value {
        Value::Array(values) => Ok(values),
        Value::Object(mut object) => match object.remove("segments") {
            Some(Value::Array(values)) => Ok(values),
            _ => Err(ParserError::Parse(
                "media JSON object must contain segments[]".into(),
            )),
        },
        _ => Err(ParserError::Parse(
            "media JSON must be an array or object with segments[]".into(),
        )),
    }
}

fn nonempty_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
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
