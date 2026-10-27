//! Additional document roots store file bytes independently of text extraction.
use serde_json::Value;
use sinex_model::event_types::documents::DocumentCaptured;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat, Settings};
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::{Component, Path, PathBuf};

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
        let digest = removed_at.is_none().then(|| blake3::hash(record));
        let content = digest
            .as_ref()
            .map(|hash| format!("blake3:{}", hash.to_hex()));
        if let Some(expected) = member.details.get("hash").and_then(Value::as_str)
            && let Some(digest) = digest
            && expected != digest.to_hex().as_str()
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
    let root = resolved_path(input)?;
    for other in roots {
        let other = other.as_str().ok_or("existing_roots must contain paths")?;
        if other.is_empty() {
            return Err("existing_roots must contain nonempty paths".into());
        }
        let other = resolved_path(Path::new(other))?;
        if root.starts_with(&other) || other.starts_with(&root) {
            return Err("document roots must not overlap notes or library roots".into());
        }
    }
    Ok(())
}
fn resolved_path(path: &Path) -> Result<PathBuf, String> {
    let absolute = std::path::absolute(path).map_err(|error| format!("document path: {error}"))?;
    for parent in absolute.ancestors() {
        if let Ok(mut resolved) = std::fs::canonicalize(parent) {
            for part in absolute
                .strip_prefix(parent)
                .expect("an ancestor is a prefix")
                .components()
            {
                match part {
                    Component::Normal(name) => resolved.push(name),
                    Component::ParentDir => {
                        resolved.pop();
                    }
                    Component::CurDir => {}
                    _ => return Err("document path has an invalid root".into()),
                }
            }
            return Ok(resolved);
        }
    }
    Err("document path needs an accessible parent".into())
}
pub const FORMAT: InputFormat = InputFormat {
    name: "documents",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        if input.as_os_str().is_empty() {
            return Err("document acquisition requires an explicit root".into());
        }
        let input =
            std::path::absolute(input).map_err(|error| format!("document path: {error}"))?;
        validate(&input, settings)?;
        let mut settings = settings.clone();
        let file =
            settings.options.get("file").and_then(Value::as_bool) == Some(true) || input.is_file();
        settings.options["file"] = Value::Bool(file);
        Ok(vec![Binding::of(&SPEC, &input, settings).named(if file {
            "file"
        } else {
            "directory"
        })])
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
                return Err("document acquisition excludes Markdown and hidden files".into());
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
