//! Explicit OCR imports preserve provider confidence scales and geometry.
use super::transcript::{Carry, identifier, optional_number, optional_string, required_string};
use crate::binding::{Binding, InputFormat, Settings};
use crate::drivers::export_archive::Framing;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::imported_text::{BoundingBox, OcrSegment};
use std::path::Path;

#[derive(Clone, Copy)]
pub enum Format {
    Json,
    Tsv,
    Text,
}
pub struct OcrParser {
    pub input: String,
    pub format: Format,
}
pub const fn spec(
    parser: &'static str,
    framing: Framing,
    frame: fn(&[u8]) -> (Vec<(usize, usize)>, usize),
) -> SourceSpec {
    SourceSpec {
        name: "media.ocr-imports",
        driver: Driver::ExportArchive,
        excisable: false,
        purpose: "imported OCR text and geometry",
        class: Class::Guarded,
        parser,
        parser_version: 1,
        event_types: &["ocr.segment"],
        framer: crate::Framer::export(frame, framing),
    }
}
pub const JSON: SourceSpec = spec(
    "ocr-json",
    Framing::JsonArray("/segments"),
    crate::drivers::export_archive::frame_json_array,
);
pub const TSV: SourceSpec = spec(
    "ocr-tsv",
    Framing::Lines,
    crate::drivers::export_archive::frame_lines,
);
pub const TEXT: SourceSpec = spec(
    "ocr-text",
    Framing::Lines,
    crate::drivers::export_archive::frame_lines,
);

impl Parser for OcrParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = OcrSegment;
    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        super::transcript::hash_with_metadata(raw, context)
    }
    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<OcrSegment>, Carry), RecordError> {
        let text =
            std::str::from_utf8(record).map_err(|_| RecordError::new("OCR input is not UTF-8"))?;
        let mut carry = carry.at(context);
        let mut payload = OcrSegment {
            input: self.input.clone(),
            segment_index: carry.next,
            ..Default::default()
        };
        carry.advance()?;
        let mut object = match self.format {
            Format::Text => {
                payload.text = text.trim_end_matches(['\r', '\n']).into();
                return Ok((
                    if payload.text.is_empty() {
                        vec![]
                    } else {
                        vec![payload]
                    },
                    carry,
                ));
            }
            Format::Json => {
                let mut object: Map<String, Value> = serde_json::from_str(text)
                    .map_err(|e| RecordError::new(format!("OCR object: {e}")))?;
                for key in [
                    "engine",
                    "external_run",
                    "confidence_scale",
                    "coordinate_units",
                    "display",
                    "window",
                ] {
                    if !object.contains_key(key)
                        && let Some(value) = context.manifest.detail("imported_text").get(key)
                    {
                        object.insert(key.into(), value.clone());
                    }
                }
                object
            }
            Format::Tsv => {
                let fields: Vec<String> = text
                    .trim_end_matches(['\r', '\n'])
                    .split('\t')
                    .map(str::to_owned)
                    .collect();
                if carry.header.is_none() {
                    for name in [
                        "level",
                        "page_num",
                        "block_num",
                        "par_num",
                        "line_num",
                        "word_num",
                        "left",
                        "top",
                        "width",
                        "height",
                        "conf",
                        "text",
                    ] {
                        if !fields.iter().any(|s| s == name) {
                            return Err(RecordError::new(format!(
                                "OCR TSV header is missing {name}"
                            )));
                        }
                    }
                    if fields
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != fields.len()
                    {
                        return Err(RecordError::new("duplicate OCR TSV column"));
                    }
                    carry.header = Some(fields);
                    return Ok((vec![], carry));
                }
                let header = carry.header.as_ref().expect("header checked");
                if fields.len() != header.len() {
                    return Err(RecordError::new("OCR TSV column count differs from header"));
                }
                let mut values: Map<String, Value> = header
                    .iter()
                    .cloned()
                    .zip(fields.into_iter().map(Value::String))
                    .collect();
                let level = tsv_unsigned(&mut values, "level")?;
                if !(1..=5).contains(&level) {
                    return Err(RecordError::new("unknown Tesseract hierarchy level"));
                }
                if level < 5 {
                    return Ok((vec![], carry));
                }
                for (from, to) in [
                    ("page_num", "page"),
                    ("block_num", "block"),
                    ("par_num", "paragraph"),
                    ("line_num", "line"),
                    ("word_num", "word"),
                ] {
                    let n = tsv_unsigned(&mut values, from)?;
                    values.insert(to.into(), Value::from(n));
                }
                for key in ["left", "top", "width", "height", "conf"] {
                    let text = required_string(&mut values, key)?;
                    let number: serde_json::Number = text
                        .parse()
                        .map_err(|_| RecordError::new(format!("invalid TSV {key}")))?;
                    if key == "conf" && number.as_i64() == Some(-1) {
                        continue;
                    }
                    values.insert(
                        if key == "conf" { "confidence" } else { key }.into(),
                        Value::Number(number),
                    );
                }
                values.insert("coordinate_units".into(), Value::String("pixels".into()));
                values.insert(
                    "confidence_scale".into(),
                    Value::String("tesseract-0-100".into()),
                );
                values.insert("engine".into(), Value::String("tesseract".into()));
                values
            }
        };
        payload.text = required_string(&mut object, "text")?;
        payload.provider_segment_id = identifier(&mut object, "id")?;
        payload.external_run = optional_string(&mut object, "external_run")?;
        payload.engine = optional_string(&mut object, "engine")?;
        payload.display = optional_string(&mut object, "display")?;
        payload.window = optional_string(&mut object, "window")?;
        payload.confidence = optional_number(&mut object, "confidence")?;
        payload.confidence_scale = optional_string(&mut object, "confidence_scale")?;
        if payload.confidence.is_some() && payload.confidence_scale.is_none() {
            payload.confidence_scale = Some("provider".into());
        }
        payload.page = unsigned(&mut object, "page")?;
        payload.block = unsigned(&mut object, "block")?;
        payload.paragraph = unsigned(&mut object, "paragraph")?;
        payload.line = unsigned(&mut object, "line")?;
        payload.word = unsigned(&mut object, "word")?;
        if let Some(value) = object.remove("bbox").filter(|v| !v.is_null()) {
            let values = value
                .as_array()
                .filter(|v| v.len() == 4)
                .ok_or_else(|| RecordError::new("bbox must have left, top, width, height"))?;
            for (key, value) in ["left", "top", "width", "height"].into_iter().zip(values) {
                if object.insert(key.into(), value.clone()).is_some() {
                    return Err(RecordError::new("duplicate bounding box coordinates"));
                }
            }
        }
        let left = optional_number(&mut object, "left")?;
        let top = optional_number(&mut object, "top")?;
        let width = optional_number(&mut object, "width")?;
        let height = optional_number(&mut object, "height")?;
        let units = optional_string(&mut object, "coordinate_units")?;
        payload.bounding_box = match (left, top, width, height) {
            (None, None, None, None) => {
                if let Some(units) = units {
                    object.insert("coordinate_units".into(), Value::String(units));
                }
                None
            }
            (Some(left), Some(top), Some(width), Some(height)) if width >= 0.0 && height >= 0.0 => {
                Some(BoundingBox {
                    left,
                    top,
                    width,
                    height,
                    units: units
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| RecordError::new("bounding box needs coordinate_units"))?,
                })
            }
            _ => return Err(RecordError::new("incomplete or negative bounding box size")),
        };
        payload.extra = object;
        Ok((vec![payload], carry))
    }
}
fn unsigned(object: &mut Map<String, Value>, key: &str) -> Result<Option<u64>, RecordError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| RecordError::new(format!("invalid {key}"))),
    }
}
fn tsv_unsigned(object: &mut Map<String, Value>, key: &str) -> Result<u64, RecordError> {
    required_string(object, key)?
        .parse()
        .map_err(|_| RecordError::new(format!("invalid TSV {key}")))
}
fn plan(spec: &SourceSpec, input: &Path, settings: &Settings) -> Result<Vec<Binding>, String> {
    if input.as_os_str().is_empty() || settings.option("input").is_none_or(str::is_empty) {
        return Err("an explicit import path and input identity are required".into());
    }
    Ok(crate::binding::single(spec, input, settings))
}
fn source(
    binding: &Binding,
    format: Format,
    spec: SourceSpec,
) -> Result<Box<dyn crate::Source>, String> {
    Ok(Box::new(DeclaredSource {
        spec,
        parser: OcrParser {
            input: binding
                .settings
                .option("input")
                .ok_or("an explicit input identity is required")?
                .into(),
            format,
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
            reader: Some(|b| Ok(super::transcript::archive(b, $framing))),
            capture: None,
        };
    };
}
format!(JSON_FORMAT, JSON, Json, Framing::ImportedJson("/segments"));
format!(TSV_FORMAT, TSV, Tsv, Framing::Lines);
format!(TEXT_FORMAT, TEXT, Text, Framing::Lines);
#[cfg(test)]
mod tests;
