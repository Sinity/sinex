//! Additional document roots store file bytes independently of text extraction.
use serde_json::Value;
use sinex_model::event_types::documents::DocumentCaptured;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat, Settings};
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::{Path, PathBuf};

mod file;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.documents",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "configured document bytes for recall and text extraction",
    class: Class::Guarded,
    parser: "documents",
    parser_version: 1,
    event_types: &["document.captured"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
};
pub struct DocumentParser;
impl Parser for DocumentParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = DocumentCaptured;
    const READS_BLANK_RECORDS: bool = true;
    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<DocumentCaptured>, ()), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("document needs its directory member"))?;
        let time = |key: &str| -> Result<Option<Timestamp>, RecordError> {
            member
                .details
                .get(key)
                .map(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<i128>().ok())
                        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
                        .ok_or_else(|| RecordError::new(format!("invalid document {key}")))
                })
                .transpose()
        };
        let removed_at = time("removed_at_ns")?;
        let content = removed_at
            .is_none()
            .then(|| format!("blake3:{}", blake3::hash(record).to_hex()));
        if let Some(expected) = member.details.get("hash").and_then(Value::as_str)
            && removed_at.is_none()
            && expected != blake3::hash(record).to_hex().as_str()
        {
            return Err(RecordError::new(
                "document content differs from member digest",
            ));
        }
        Ok((
            vec![DocumentCaptured {
                host: context
                    .manifest
                    .detail("host")
                    .as_str()
                    .ok_or_else(|| RecordError::new("document needs its recorded host"))?
                    .into(),
                root: context
                    .manifest
                    .path
                    .clone()
                    .ok_or_else(|| RecordError::new("document needs its recorded root"))?,
                path: member.path.clone(),
                size: if removed_at.is_some() {
                    0
                } else {
                    record.len() as u64
                },
                modified_at: time("mtime_ns")?,
                media_type: if removed_at.is_some() {
                    "application/octet-stream"
                } else {
                    media_type(record)
                }
                .into(),
                content,
                removed_at,
            }],
            (),
        ))
    }
}
fn media_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"%PDF-") {
        "application/pdf"
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"PK\x03\x04") {
        "application/zip"
    } else if !bytes.contains(&0) && std::str::from_utf8(bytes).is_ok() {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}
pub fn select(path: &str) -> bool {
    !path.split('/').any(|s| s.starts_with('.'))
        && !path.to_ascii_lowercase().ends_with(".md")
        && !path.to_ascii_lowercase().ends_with(".markdown")
}
pub fn driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        root,
        host,
        select,
        part_bytes: 64 << 20,
    }
}
fn validate(input: &Path, settings: &Settings) -> Result<(), String> {
    if settings
        .options
        .get("capture_documents")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err("document acquisition requires capture_documents=true".into());
    }
    if input.as_os_str().is_empty() {
        return Err("document acquisition requires an explicit root".into());
    }
    if std::fs::symlink_metadata(input).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err("a document root must not be a symbolic link".into());
    }
    let roots = settings.options.get("existing_roots").and_then(Value::as_array).ok_or("documents need existing_roots listing notes and library roots (an empty list is explicit)")?;
    let root = std::fs::canonicalize(input).unwrap_or_else(|_| input.to_path_buf());
    for other in roots {
        let other = other.as_str().ok_or("existing_roots must contain paths")?;
        let other = std::fs::canonicalize(other).unwrap_or_else(|_| PathBuf::from(other));
        if root.starts_with(&other) || other.starts_with(&root) {
            return Err("document roots must not overlap notes or library roots".into());
        }
    }
    Ok(())
}
pub const FORMAT: InputFormat = InputFormat {
    name: "documents",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        validate(input, settings)?;
        let mut settings = settings.clone();
        if input.is_file() {
            settings.options["file"] = Value::Bool(true);
        }
        Ok(sinex_source_core::binding::single(&SPEC, input, &settings))
    }),
    source: |_| {
        Ok(Box::new(DeclaredSource {
            spec: SPEC,
            parser: DocumentParser,
        }))
    },
    reader: Some(|binding: &Binding| {
        validate(&binding.input, &binding.settings)?;
        if binding
            .settings
            .options
            .get("file")
            .and_then(Value::as_bool)
            == Some(true)
        {
            if !select(
                binding
                    .input
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or("document file needs a UTF-8 name")?,
            ) {
                return Err("the selected file belongs to notes or is hidden".into());
            }
            Ok(sinex_source_core::binding::polled(file::FileReader::new(
                binding.input.clone(),
                binding.settings.host.clone(),
            )))
        } else {
            Ok(sinex_source_core::binding::walked(driver(
                binding.input.clone(),
                binding.settings.host.clone(),
            )))
        }
    }),
    capture: None,
};
#[cfg(test)]
mod tests;
