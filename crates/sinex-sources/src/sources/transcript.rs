//! Explicit transcript imports. Relative offsets are never receipt timestamps.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::imported_text::TranscriptCue;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat, Settings};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::Path;

pub(super) mod discovery;
pub mod framing;
mod decimal;

pub const CUE_FRAMING: Framing = Framing::Custom {
    name: "cue-blocks",
    frame: framing::frame,
};
pub const JSON_FRAMING: Framing = Framing::Whole;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Whisper,
    Text,
    WebVtt,
    Srt,
}

type Frame = fn(&[u8]) -> (Vec<(usize, usize)>, usize);

pub const fn spec(parser: &'static str, framing: Framing, frame: Frame) -> SourceSpec {
    SourceSpec {
        name: "media.transcripts",
        driver: Driver::ExportArchive,
        excisable: false,
        purpose: "imported timed speech text",
        class: Class::Guarded,
        parser,
        parser_version: 1,
        event_types: &["transcript.cue"],
        framer: sinex_source_core::Framer::export(frame, framing),
    }
}
pub const JSON: SourceSpec = spec(
    "transcript-json",
    Framing::Whole,
    sinex_source_core::drivers::export_archive::frame_whole,
);
pub const WHISPER: SourceSpec = spec(
    "whisper-json",
    Framing::Whole,
    sinex_source_core::drivers::export_archive::frame_whole,
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
            .ok_or_else(|| RecordError::new("output index exceeds u64"))?;
        Ok(())
    }
}

pub struct TranscriptParser {
    pub format: Format,
    pub input: String,
}
impl TranscriptParser {
    fn parse_item(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<TranscriptCue>, Carry), RecordError> {
        let text =
            std::str::from_utf8(record).map_err(|_| RecordError::new("transcript is not UTF-8"))?;
        let mut carry = carry.at(context);
        let index = carry.next;
        carry.next = index;
        carry.advance()?;
        let mut payload = TranscriptCue {
            input: input_identity(&self.input, context),
            ..Default::default()
        };
        match self.format {
            Format::Json | Format::Whisper => {
                let (start, end) = if self.format == Format::Whisper {
                    ("start", "end")
                } else {
                    ("start_ms", "end_ms")
                };
                let raw: std::collections::HashMap<String, &serde_json::value::RawValue> =
                    serde_json::from_str(text)
                        .map_err(|e| RecordError::new(format!("transcript object: {e}")))?;
                let mut object = Map::new();
                for (key, value) in &raw {
                    if key != start && key != end {
                        object.insert(
                            key.clone(),
                            serde_json::from_str(value.get()).map_err(|e| {
                                RecordError::new(format!("transcript field {key}: {e}"))
                            })?,
                        );
                    }
                }
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
                payload.provider_id = identifier(&mut object, "id")?;
                payload.speaker = optional_string(&mut object, "speaker")?;
                payload.language = optional_string(&mut object, "language")?;
                payload.model = optional_string(&mut object, "model")?;
                payload.external_run = optional_string(&mut object, "external_run")?;
                payload.confidence = optional_number(&mut object, "confidence")?;
                payload.confidence_scale = optional_string(&mut object, "confidence_scale")?;
                if payload.confidence.is_some() && payload.confidence_scale.is_none() {
                    payload.confidence_scale = Some("provider".into());
                }
                if self.format == Format::Whisper {
                    payload.start_seconds = decimal_text(raw.get(start).map(|v| v.get()))?;
                    payload.end_seconds = decimal_text(raw.get(end).map(|v| v.get()))?;
                }
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
                let offset_ns = if self.format == Format::Whisper {
                    payload
                        .start_seconds
                        .as_deref()
                        .and_then(|text| scaled_offset(text, 9, false).ok().and_then(|ns| i128::try_from(ns).ok()))
    
                } else {
                    payload.start_ms.map(|ms| i128::from(ms) * 1_000_000)
                };
                if let (Some(at), Some(ns)) = (recording_start, offset_ns) {
                    payload.at = at
                        .unix_ns()
                        .checked_add(ns)
                        .and_then(|ns| Timestamp::from_unix_ns(ns).ok());
                }
                payload.provider_id_stable = payload.provider_id.is_some()
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
                    payload.provider_id = None;
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
        if let (Some(start), Some(end)) = (&payload.start_seconds, &payload.end_seconds)
            && decimal::compare(start, end).is_gt()
        {
            return Err(RecordError::new("cue ends before it starts"));
        }
        if let (Some(start), Some(end)) = (payload.start_ms, payload.end_ms)
            && end < start
        {
            return Err(RecordError::new("cue ends before it starts"));
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
        Some(text) if seconds => {
            decimal_text(Some(text))?;
            Ok(decimal_ms(text, 3).ok())
        }
        Some(text) => decimal_ms(text, 0).map(Some),
    }
}
/// Whisper seconds round to the nearest millisecond without floating point arithmetic.
/// Explicit millisecond fields must be integral.
pub fn decimal_ms(text: &str, places: u32) -> Result<u64, RecordError> {
    u64::try_from(scaled_offset(text, places, true)?).map_err(|_| RecordError::new("offset exceeds u64 milliseconds"))
}
fn scaled_offset(text: &str, places: u32, round: bool) -> Result<u128, RecordError> {
    let invalid = || RecordError::new("offset cannot be represented at the requested precision");
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
        if (!round || places == 0) && !discarded.bytes().all(|b| b == b'0') {
            return Err(invalid());
        }
        let whole = if split == 0 {
            0
        } else {
            digits[..split].parse::<u128>().map_err(|_| invalid())?
        };
        let round_up = round && places > 0 && cut <= digits.len() && discarded.as_bytes()[0] >= b'5';
        whole.checked_add(u128::from(round_up)).ok_or_else(invalid)?
    } else {
        let shift = u32::try_from(shift).map_err(|_| invalid())?;
        digits
            .parse::<u128>()
            .ok()
            .and_then(|n| {
                10u128
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
    discovery::plan(spec, input, settings)
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
            input: binding.input.display().to_string(),
        },
    }))
}
macro_rules! format {
    ($name:ident, $spec:ident, $format:ident, $framing:expr) => {
        pub const $name: InputFormat = InputFormat {
            name: $spec.parser,
            discovery: discovery::spec(true),
            discover: discovery::discover,
            specs: || vec![&$spec],
            plan: Some(|p, s| plan(&$spec, p, s)),
            source: |b| source(b, Format::$format, $spec),
            reader: Some(|b| Ok(archive(b, $framing))),
            capture: None,
        };
    };
}
format!(JSON_FORMAT, JSON, Json, Framing::Whole);
format!(WHISPER_FORMAT, WHISPER, Whisper, JSON_FRAMING);
format!(TEXT_FORMAT, TEXT, Text, Framing::Lines);
format!(WEBVTT_FORMAT, WEBVTT, WebVtt, CUE_FRAMING);
format!(SRT_FORMAT, SRT, Srt, CUE_FRAMING);

#[cfg(test)]
mod tests;

impl Parser for TranscriptParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = TranscriptCue;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<TranscriptCue>, Carry), RecordError> {
        if !matches!(self.format, Format::Json | Format::Whisper) {
            return self.parse_item(record, carry, context);
        }
        let (records, defaults) = json_records(record, self.format == Format::Whisper)?;
        let mut manifest = context.manifest.clone();
        manifest.details.insert("file_metadata".into(), defaults);
        let context = RecordContext {
            manifest: &manifest,
            member: context.member,
        };
        let mut output = Vec::with_capacity(records.len());
        for (index, record) in records.iter().enumerate() {
            let carry = Carry {
                next: index as u64,
                member: Carry::default().at(&context).member,
                header: None,
            };
            output.extend(self.parse_item(&record.as_slice(), &carry, &context)?.0);
        }
        Ok((output, carry.clone()))
    }
}

/// JSON members remain one record, so output indexes and defaults replay together.
pub(super) fn json_records(
    bytes: &[u8],
    nested: bool,
) -> Result<(Vec<Vec<u8>>, Value), RecordError> {
    use serde_json::value::RawValue;
    let error = |e| RecordError::new(format!("imported JSON: {e}"));
    let (rows, defaults) = if nested {
        let mut object: Map<String, Value> = Map::new();
        let mut raw: std::collections::BTreeMap<String, Box<RawValue>> =
            serde_json::from_slice(bytes).map_err(error)?;
        let rows = raw
            .remove("segments")
            .ok_or_else(|| RecordError::new("imported JSON needs segments"))?;
        for (key, value) in raw {
            object.insert(key, serde_json::from_str(value.get()).map_err(error)?);
        }
        (
            serde_json::from_str::<Vec<Box<RawValue>>>(rows.get()).map_err(error)?,
            Value::Object(object),
        )
    } else {
        (
            serde_json::from_slice::<Vec<Box<RawValue>>>(bytes).map_err(error)?,
            Value::Null,
        )
    };
    Ok((
        rows.into_iter()
            .map(|row| row.get().as_bytes().to_vec())
            .collect(),
        defaults,
    ))
}

fn decimal_text(value: Option<&str>) -> Result<Option<String>, RecordError> {
    match value {
        None | Some("null") => Ok(None),
        Some(text) if text.as_bytes().first().is_some_and(u8::is_ascii_digit) => {
            Ok(Some(text.into()))
        }
        _ => Err(RecordError::new("offset must be nonnegative")),
    }
}

/// An archive member is a separate imported input even when its provider reuses IDs.
pub(super) fn input_identity(input: &str, context: &RecordContext<'_>) -> String {
    if matches!(
        context.manifest.detail("source_container").as_str(),
        Some("zip" | "tar" | "tar-gzip")
    ) {
        serde_json::to_string(&(input, context.member.map(|member| member.path.as_str())))
            .expect("paths serialize")
    } else {
        input.into()
    }
}
