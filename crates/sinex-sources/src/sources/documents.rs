//! Additional document roots store file bytes independently of text extraction.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::event_types::documents::DocumentCaptured;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat};
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::PathBuf;

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
/// Subtrees assigned to another source are excluded before this reader walks.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub excluded_roots: Vec<PathBuf>,
}

impl Options {
    fn from_settings(settings: &sinex_source_core::Settings) -> Result<Self, String> {
        if settings.options.is_null() {
            Ok(Self::default())
        } else {
            serde_json::from_value(settings.options.clone()).map_err(|error| error.to_string())
        }
    }
}

pub struct DocumentParser;
impl Parser for DocumentParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = DocumentCaptured;
    const READS_BLANK_RECORDS: bool = true;

    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        context.hash_with_path(raw)
    }

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
                size: removed_at.is_none().then_some(record.len() as u64),
                modified_at: time("mtime_ns")?,
                media_type: removed_at.is_none().then(|| media_type(record).into()),
                native_path_bytes: member
                    .details
                    .get("native_path_bytes")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
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
pub fn driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        root,
        host,
        select: |_| true,
        part_bytes: 64 << 20,
    }
}

use sinex_source_core::discovery::{
    Availability, DiscoveredInput, DiscoveryContext, DiscoverySpec, Location, Owner,
};

fn discover(format: &InputFormat, context: &DiscoveryContext) -> Vec<DiscoveredInput> {
    let mut found = crate::discovery::discover_format(format, context);
    // Declared roots retain their owner when that owner's reader is excluded.
    let mut owners = context.clone();
    owners.exclude.remove(super::notes::SPEC.name);
    owners.exclude.remove(super::library::SCAN.name);
    let mut owned = Vec::new();
    for format in [super::notes::FORMAT, super::library::FORMAT] {
        for input in (format.discover)(&format, &owners) {
            if let Some(path) = input.input {
                owned.push(std::fs::canonicalize(&path).unwrap_or(path));
            }
        }
    }
    for input in &mut found {
        if let Some(path) = &input.input {
            let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
            if owned.iter().any(|other| resolved.starts_with(other)) {
                input.bindings.clear();
                input.availability = Availability::Excluded;
                input.reason = Some("root is owned by notes or the library".into());
                continue;
            }
            if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                input.reason = Some(format!(
                    "symbolic-link root resolves to {}",
                    resolved.display()
                ));
            }
            let excluded: Vec<_> = owned.iter().filter(|p| p.starts_with(&resolved)).collect();
            for binding in &mut input.bindings {
                let mut options = match Options::from_settings(&binding.settings) {
                    Ok(options) => options,
                    Err(reason) => {
                        input.availability = Availability::Unavailable;
                        input.reason = Some(reason);
                        continue;
                    }
                };
                options
                    .excluded_roots
                    .extend(excluded.iter().map(|path| path.as_path().to_path_buf()));
                options.excluded_roots.sort();
                options.excluded_roots.dedup();
                binding.settings.options =
                    serde_json::to_value(options).expect("document options serialize");
            }
        }
    }
    found
}

pub const FORMAT: InputFormat = InputFormat {
    name: "documents",
    discovery: DiscoverySpec {
        owner: Owner::System,
        locations: &[
            Location {
                root: "declared",
                path: "documents",
            },
            Location {
                root: "declared",
                path: "downloads",
            },
        ],
        capabilities: &[],
        credentials: &[],
        request_only: false,
        requests: &[],
        streams: &[],
    },
    discover,
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Options::from_settings(settings)?;
        if input.as_os_str().is_empty() {
            return Err("document acquisition needs a root".into());
        }
        let input = std::path::absolute(input).map_err(|e| e.to_string())?;
        if input.is_file() {
            return Err("document acquisition needs a directory root".into());
        }
        Ok(vec![
            Binding::of(&SPEC, &input, settings.clone()).named("directory"),
        ])
    }),
    source: |_| {
        Ok(Box::new(DeclaredSource {
            spec: SPEC,
            parser: DocumentParser,
        }))
    },
    reader: Some(|binding| {
        let excluded = Options::from_settings(&binding.settings)?.excluded_roots;
        Ok(sinex_source_core::binding::walked_excluding(
            driver(binding.input.clone(), binding.settings.host.clone()),
            excluded,
        ))
    }),
    capture: None,
};
#[cfg(test)]
mod tests;
