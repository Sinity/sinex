//! Explicit transcript imports. Relative offsets are never receipt timestamps.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::imported_text::TranscriptSegment;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat, Settings};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::Path;

pub mod framing;
pub mod metadata;

pub const CUE_FRAMING: Framing = Framing::Custom {
    name: "cue-blocks",
    frame: framing::frame,
};
pub const JSON_FRAMING: Framing = Framing::JsonArrayWithMetadata {
    name: "imported-text",
    pointer: "/segments",
    project: metadata::project,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Whisper,
    Text,
    WebVtt,
    Srt,
}

pub const fn spec(
    parser: &'static str,
    framing: Framing,
    frame: fn(&[u8]) -> (Vec<(usize, usize)>, usize),
) -> SourceSpec {
    SourceSpec {
        name: "media.transcripts",
        driver: Driver::ExportArchive,
        excisable: false,
        purpose: "imported timed speech text",
        class: Class::Guarded,
        parser,
        parser_version: 1,
        event_types: &["transcript.segment"],
        framer: sinex_source_core::Framer::export(frame, framing),
    }
}
pub const JSON: SourceSpec = spec(
    "transcript-json",
    Framing::JsonArray(""),
    sinex_source_core::drivers::export_archive::frame_json_array,
);
pub const WHISPER: SourceSpec = spec(
    "whisper-json",
    Framing::JsonArray("/segments"),
    sinex_source_core::drivers::export_archive::frame_json_array,
);
pub const TEXT: SourceSpec = spec(
    "transcript-text",
    Framing::Lines,
    sinex_source_core::drivers::export_archive::frame_lines,
);
pub const WEBVTT: SourceSpec = spec("webvtt", CUE_FRAMING, framing::whole);
pub const SRT: SourceSpec = spec("srt", CUE_FRAMING, framing::whole);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Carry {
    pub member: String,
    pub next: u64,
    pub header: Option<Vec<String>>,
}
impl Carry {
    pub fn at(&self, context: &RecordContext<'_>) -> Self {
        let member = format!(
            "{}:{}:{}",
            context.manifest.path.as_deref().unwrap_or_default(),
            context.member.map_or("", |m| m.path.as_str()),
            context.manifest.detail("archive_member")
        );
        if self.member == member {
            self.clone()
        } else {
            Self {
                member,
                next: context
                    .member
                    .and_then(|m| m.details.get("record_start"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                header: None,
            }
        }
    }
    pub fn advance(&mut self) -> Result<(), RecordError> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| RecordError::new("segment index exceeds u64"))?;
        Ok(())
    }
}

pub struct TranscriptParser {
    pub format: Format,
    pub input: String,
}
impl Parser for TranscriptParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = TranscriptSegment;
    const READS_RECORD_INDEX: bool = true;
    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        hash_with_metadata(raw, context)
    }
    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<TranscriptSegment>, Carry), RecordError> {
        let text =
            std::str::from_utf8(record).map_err(|_| RecordError::new("transcript is not UTF-8"))?;
        let mut carry = carry.at(context);
        let index = record_index(context).unwrap_or(carry.next);
        carry.next = index;
        carry.advance()?;
        let mut payload = TranscriptSegment {
            input: self.input.clone(),
            segment_index: index,
            ..Default::default()
        };
        match self.format {
            Format::Json | Format::Whisper => {
                let mut object: Map<String, Value> = serde_json::from_str(text)
                    .map_err(|e| RecordError::new(format!("transcript object: {e}")))?;
                let defaults = context.manifest.detail("file_metadata").as_object();
                for key in [
                    "language",
                    "model",
                    "external_run",
                    "recording_started_at",
                    "confidence_scale",
                ] {
                    if !object.contains_key(key)
                        && let Some(value) = defaults.and_then(|m| m.get(key))
                    {
                        object.insert(key.into(), value.clone());
                    }
                }
                payload.text = required_string(&mut object, "text")?;
                payload.provider_segment_id = identifier(&mut object, "id")?;
                payload.speaker = optional_string(&mut object, "speaker")?;
                payload.language = optional_string(&mut object, "language")?;
                payload.model = optional_string(&mut object, "model")?;
                payload.external_run = optional_string(&mut object, "external_run")?;
                payload.confidence = optional_number(&mut object, "confidence")?;
                payload.confidence_scale = optional_string(&mut object, "confidence_scale")?;
                if payload.confidence.is_some() && payload.confidence_scale.is_none() {
                    payload.confidence_scale = Some("provider".into());
                }
                let (start, end) = if self.format == Format::Whisper {
                    ("start", "end")
                } else {
                    ("start_ms", "end_ms")
                };
                let raw: std::collections::HashMap<String, &serde_json::value::RawValue> =
                    serde_json::from_str(text)
                        .map_err(|e| RecordError::new(format!("transcript object: {e}")))?;
                payload.start_ms = raw_offset(
                    raw.get(start).map(|v| v.get()),
                    self.format == Format::Whisper,
                )?;
                payload.end_ms = raw_offset(
                    raw.get(end).map(|v| v.get()),
                    self.format == Format::Whisper,
                )?;
                object.remove(start);
                object.remove(end);
                let recording_start = optional_string(&mut object, "recording_started_at")?
                    .map(|s| {
                        time::OffsetDateTime::parse(
                            &s,
                            &time::format_description::well_known::Rfc3339,
                        )
                        .map(Timestamp)
                        .map_err(|_| RecordError::new("invalid recording_started_at"))
                    })
                    .transpose()?;
                if let (Some(at), Some(ms)) = (recording_start, payload.start_ms) {
                    payload.at = at
                        .unix_ns()
                        .checked_add(i128::from(ms) * 1_000_000)
                        .and_then(|ns| Timestamp::from_unix_ns(ns).ok());
                }
                payload.provider_id_stable = payload.provider_segment_id.is_some()
                    && (self.format == Format::Json
                        || (payload.external_run.is_some()
                            && matches!(
                                context.manifest.detail("source_container").as_str(),
                                Some("stateless" | "gzip")
                            )));
                payload.extra = object;
            }
            Format::Text => {
                payload.text = text.trim_end_matches(['\r', '\n']).to_owned();
                if payload.text.is_empty() {
                    return Ok((vec![], carry));
                }
            }
            Format::WebVtt | Format::Srt => {
                let block = text
                    .trim_matches(['\r', '\n'])
                    .trim_start_matches('\u{feff}');
                let heading = block.lines().next().unwrap_or_default();
                let word = heading.split_ascii_whitespace().next().unwrap_or_default();
                if self.format == Format::WebVtt
                    && ((index == 0 && word == "WEBVTT")
                        || word == "NOTE"
                        || matches!(heading.trim_end(), "STYLE" | "REGION"))
                {
                    return Ok((vec![], carry));
                }
                let mut lines = block.lines();
                let first = lines.next().ok_or_else(|| RecordError::new("empty cue"))?;
                let timing = if first.contains("-->") {
                    first
                } else {
                    payload.provider_segment_id = None;
                    payload
                        .extra
                        .insert("cue_id".into(), Value::String(first.into()));
                    lines
                        .next()
                        .ok_or_else(|| RecordError::new("cue has no timing"))?
                };
                let (start, end) = timing
                    .split_once("-->")
                    .ok_or_else(|| RecordError::new("cue has no timing arrow"))?;
                let mut end = end.split_whitespace();
                payload.start_ms = Some(cue_time(start.trim(), self.format == Format::Srt)?);
                payload.end_ms = Some(cue_time(
                    end.next()
                        .ok_or_else(|| RecordError::new("cue has no end"))?,
                    self.format == Format::Srt,
                )?);
                let settings = end.collect::<Vec<_>>().join(" ");
                if !settings.is_empty() {
                    payload
                        .extra
                        .insert("cue_settings".into(), Value::String(settings));
                }
                payload.text = lines.collect::<Vec<_>>().join("\n");
            }
        }
        if let (Some(start), Some(end)) = (payload.start_ms, payload.end_ms)
            && end < start
        {
            return Err(RecordError::new("segment ends before it starts"));
        }
        Ok((vec![payload], carry))
    }
}

pub fn optional_string(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<String>, RecordError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        _ => Err(RecordError::new(format!("{key} must be text"))),
    }
}
pub fn required_string(object: &mut Map<String, Value>, key: &str) -> Result<String, RecordError> {
    optional_string(object, key)?.ok_or_else(|| RecordError::new(format!("missing {key}")))
}
pub fn identifier(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<String>, RecordError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if !s.is_empty() => Ok(Some(s)),
        Some(Value::Number(n)) if n.is_u64() || n.is_i64() => Ok(Some(n.to_string())),
        _ => Err(RecordError::new(format!("invalid {key}"))),
    }
}
pub fn optional_number(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<f64>, RecordError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_f64()
            .filter(|n| n.is_finite())
            .map(Some)
            .ok_or_else(|| RecordError::new(format!("invalid {key}"))),
        _ => Err(RecordError::new(format!("{key} must be numeric"))),
    }
}
fn raw_offset(value: Option<&str>, seconds: bool) -> Result<Option<u64>, RecordError> {
    match value {
        None | Some("null") => Ok(None),
        Some(text) => decimal_ms(text, if seconds { 3 } else { 0 }).map(Some),
    }
}
/// Whisper seconds round to the nearest millisecond without floating point arithmetic.
/// Explicit millisecond fields must be integral.
pub fn decimal_ms(text: &str, places: u32) -> Result<u64, RecordError> {
    let invalid = || RecordError::new("offset must resolve to nonnegative milliseconds within u64");
    if text.starts_with('-') {
        return Err(invalid());
    }
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(i) => (
            &text[..i],
            text[i + 1..].parse::<i64>().map_err(|_| invalid())?,
        ),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(0);
    }
    let shift = exponent
        .checked_add(i64::from(places))
        .and_then(|n| n.checked_sub(fraction.len() as i64))
        .ok_or_else(invalid)?;
    let value = if shift < 0 {
        let cut = usize::try_from(shift.unsigned_abs()).map_err(|_| invalid())?;
        let split = digits.len().saturating_sub(cut);
        let discarded = &digits[split..];
        if places == 0 && !discarded.bytes().all(|b| b == b'0') {
            return Err(invalid());
        }
        let whole = if split == 0 {
            0
        } else {
            digits[..split].parse::<u64>().map_err(|_| invalid())?
        };
        let round_up = places > 0 && cut <= digits.len() && discarded.as_bytes()[0] >= b'5';
        whole.checked_add(u64::from(round_up)).ok_or_else(invalid)?
    } else {
        let shift = u32::try_from(shift).map_err(|_| invalid())?;
        digits
            .parse::<u64>()
            .ok()
            .and_then(|n| {
                10u64
                    .checked_pow(shift)
                    .and_then(|scale| n.checked_mul(scale))
            })
            .ok_or_else(invalid)?
    };
    Ok(value)
}
fn cue_time(text: &str, srt: bool) -> Result<u64, RecordError> {
    let parts: Vec<_> = text.split(':').collect();
    if !(parts.len() == 3 || (!srt && parts.len() == 2)) {
        return Err(RecordError::new("invalid cue timestamp"));
    }
    let mut total = 0u64;
    for (index, part) in parts.iter().enumerate() {
        let last = index + 1 == parts.len();
        let value = if last {
            let (seconds, fraction) = part
                .split_once(if srt { ',' } else { '.' })
                .ok_or_else(|| RecordError::new("cue needs milliseconds"))?;
            if fraction.len() != 3 || seconds.len() != 2 {
                return Err(RecordError::new("invalid cue seconds"));
            }
            decimal_ms(&format!("{seconds}.{fraction}"), 3)?
        } else {
            if part.len() < 2 || !part.bytes().all(|b| b.is_ascii_digit()) {
                return Err(RecordError::new("invalid cue time"));
            }
            part.parse::<u64>()
                .map_err(|_| RecordError::new("invalid cue time"))?
                .checked_mul(1000)
                .ok_or_else(|| RecordError::new("cue time overflow"))?
        };
        if (index > 0 || parts.len() == 2) && value >= 60_000 {
            return Err(RecordError::new("cue minute or second exceeds 59"));
        }
        total = total
            .checked_mul(60)
            .and_then(|n| n.checked_add(value))
            .ok_or_else(|| RecordError::new("cue time overflow"))?;
    }
    Ok(total)
}

pub fn archive(binding: &Binding, framing: Framing) -> sinex_source_core::binding::Reading {
    sinex_source_core::binding::shared(ExportArchive {
        path: binding.input.clone(),
        host: binding.settings.host.clone(),
        select: |_| true,
        framing,
        part_bytes: PART_BYTES,
    })
}
pub(super) fn plan(
    spec: &SourceSpec,
    input: &Path,
    settings: &Settings,
) -> Result<Vec<Binding>, String> {
    if input.as_os_str().is_empty() {
        return Err("an explicit import path is required".into());
    }
    if settings.option("input").is_none_or(str::is_empty) {
        return Err("an explicit input identity is required".into());
    }
    if input.is_dir() {
        return Err("imports require an explicit file or archive".into());
    }
    let input = std::path::absolute(input).map_err(|error| format!("import path: {error}"))?;
    let identity = serde_json::to_vec(&(spec.parser, settings.option("input")))
        .expect("input identity serializes");
    let name = blake3::hash(&identity).to_hex().to_string();
    Ok(vec![
        Binding::of(spec, &input, settings.clone()).named(&name),
    ])
}
fn source(
    binding: &Binding,
    format: Format,
    spec: SourceSpec,
) -> Result<Box<dyn sinex_source_core::Source>, String> {
    Ok(Box::new(DeclaredSource {
        spec,
        parser: TranscriptParser {
            format,
            input: binding
                .settings
                .option("input")
                .ok_or("an explicit input identity is required")?
                .into(),
        },
    }))
}
macro_rules! format {
    ($name:ident, $spec:ident, $format:ident, $framing:expr) => {
        pub const $name: InputFormat = InputFormat {
            name: $spec.parser,
            specs: || vec![&$spec],
            plan: Some(|p, s| plan(&$spec, p, s)),
            source: |b| source(b, Format::$format, $spec),
            reader: Some(|b| Ok(archive(b, $framing))),
            capture: None,
        };
    };
}
format!(JSON_FORMAT, JSON, Json, Framing::JsonArray(""));
format!(WHISPER_FORMAT, WHISPER, Whisper, JSON_FRAMING);
format!(TEXT_FORMAT, TEXT, Text, Framing::Lines);
format!(WEBVTT_FORMAT, WEBVTT, WebVtt, CUE_FRAMING);
format!(SRT_FORMAT, SRT, Srt, CUE_FRAMING);

#[cfg(test)]
mod tests;

/// File metadata supplies part of a segment's meaning even when its text is unchanged.
pub fn hash_with_metadata(raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
    let metadata = context.manifest.detail("file_metadata");
    let metadata = (!metadata.is_null() && !metadata.as_object().is_some_and(Map::is_empty))
        .then_some(metadata);
    let index = record_index(context);
    if metadata.is_none() && index.is_none() {
        return *blake3::hash(raw).as_bytes();
    }
    let values = serde_json::to_vec(&(metadata, index)).expect("JSON metadata serializes");
    let mut hash = blake3::Hasher::new();
    hash.update(&(raw.len() as u64).to_le_bytes());
    hash.update(raw);
    hash.update(&values);
    *hash.finalize().as_bytes()
}
pub(super) fn record_index(context: &RecordContext<'_>) -> Option<u64> {
    context
        .member
        .and_then(|member| member.details.get("record_index"))
        .and_then(Value::as_u64)
}
