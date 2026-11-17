//! Additional document roots store file bytes independently of text extraction.
use serde::{Deserialize, Serialize};
use sinex_model::event_types::documents::DocumentCaptured;
use sinex_model::json::JsonExt;
use sinex_model::{Class, Timestamp};
use sinex_source_core::binding::{Binding, InputFormat};
use sinex_source_core::discovery::{
    Availability, Cause, DiscoveredInput, DiscoveryContext, DiscoverySpec, Location,
};
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::drivers::export_archive::{Framing, frame_whole};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{
    Framer, RecordContext, RecordError, RecordResult, Stateless, StatelessParser, binding,
};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.documents",
    origin: Origin::Input(Shape::DirSnapshot),
    expect: Expectation::cadence_hours(1),
    purpose: "configured document bytes for recall and text extraction",
    class: Class::Guarded,
    parser: "documents",
    parser_version: 2,
    event_types: &["document.captured"],
    framer: Framer::export(frame_whole, Framing::Whole),
    ..SourceSpec::DEFAULT
};
/// Subtrees assigned to another source are excluded before this reader walks.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub excluded_roots: Vec<PathBuf>,
    /// Video and audio keep their hash and file facts instead of copied bytes.
    pub locate_media: bool,
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
impl StatelessParser for DocumentParser {
    type Record<'a> = &'a [u8];
    type Payload = DocumentCaptured;
    const READS_BLANK_RECORDS: bool = true;

    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        if let Some(member) = context.member
            && member.details.bool("located") == Some(true)
            && let Some(node) = &member.node
        {
            return context.hash_with_path(&node.content_hash);
        }
        context.hash_with_path(raw)
    }

    fn parse(
        &self,
        record: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<DocumentCaptured>, RecordError> {
        let member = context
            .member
            .record("document needs its directory member")?;
        let time = |key: &str| -> Result<Option<Timestamp>, RecordError> {
            member
                .details
                .get(key)
                .map(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<i128>().ok())
                        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
                        .record_with(|| format!("invalid document {key}"))
                })
                .transpose()
        };
        let removed_at = time("removed_at_ns")?;
        let located = member.details.bool("located").unwrap_or(false);
        let located_node = if located {
            if removed_at.is_some() || !record.is_empty() || member.length != 0 {
                return Err(RecordError::new(
                    "located file needs an empty listing member",
                ));
            }
            Some(
                member
                    .node
                    .as_ref()
                    .record("located file needs recorded file facts")?,
            )
        } else {
            None
        };
        let digest = if let Some(node) = located_node {
            Some(blake3::Hash::from(node.content_hash))
        } else {
            removed_at.is_none().then(|| blake3::hash(record))
        };
        let content = digest
            .as_ref()
            .map(|hash| format!("blake3:{}", hash.to_hex()));
        if let Some(expected) = member.details.str("hash")
            && let Some(digest) = digest
            && expected != digest.to_hex().as_str()
        {
            return Err(RecordError::new(
                "document content differs from member digest",
            ));
        }
        Ok(vec![DocumentCaptured {
            host: context
                .manifest
                .detail("host")
                .as_str()
                .record("document needs its recorded host")?
                .into(),
            root: context
                .manifest
                .path
                .clone()
                .record("document needs its recorded root")?,
            path: member.path.clone(),
            size: located_node
                .map(|node| node.size)
                .or_else(|| removed_at.is_none().then_some(record.len() as u64)),
            modified_at: time("mtime_ns")?,
            media_type: if located {
                Some(
                    member
                        .details
                        .str("media_type")
                        .record("located file needs its media type")?
                        .into(),
                )
            } else {
                removed_at.is_none().then(|| media_type(record).into())
            },
            native_path_bytes: member.details.str("native_path_bytes").map(str::to_owned),
            content,
            removed_at,
            located,
        }])
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
        locate: None,
        removed_at_source: false,
    }
}

fn locate_audio_video(head: &[u8]) -> Option<&'static str> {
    let kind = crate::media_type::sniff(head);
    (kind.starts_with("audio/") || kind.starts_with("video/")).then_some(kind)
}

fn discover(format: &InputFormat, context: &DiscoveryContext) -> Vec<DiscoveredInput> {
    let mut found = crate::discovery::discover_format(format, context);
    // Roots that notes or the library read stay theirs even when that reader is excluded.
    let mut readers = context.clone();
    readers.exclude.remove(super::notes::SPEC.name);
    readers.exclude.remove(super::library::SCAN.name);
    let mut claimed = Vec::new();
    for format in [super::notes::FORMAT, super::library::FORMAT] {
        for input in (format.discover)(&format, &readers) {
            if let Some(path) = input.input {
                claimed.push(std::fs::canonicalize(&path).unwrap_or(path));
            }
        }
    }
    for input in &mut found {
        if let Some(path) = &input.input {
            let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
            if claimed.iter().any(|other| resolved.starts_with(other)) {
                input.bindings.clear();
                input.availability = Availability::Excluded;
                input.reason = Some("root is read by notes or the library".into());
                continue;
            }
            if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                input.reason = Some(format!(
                    "symbolic-link root resolves to {}",
                    resolved.display()
                ));
            }
            let excluded: Vec<_> = claimed
                .iter()
                .filter(|p| p.starts_with(&resolved))
                .collect();
            for binding in &mut input.bindings {
                let mut options = match Options::from_settings(&binding.settings) {
                    Ok(options) => options,
                    Err(reason) => {
                        input.availability = Availability::Unavailable(Cause::Failed);
                        input.reason = Some(reason);
                        continue;
                    }
                };
                options
                    .excluded_roots
                    .extend(excluded.iter().map(|path| path.as_path().to_path_buf()));
                options.excluded_roots.sort();
                options.excluded_roots.dedup();
                options.locate_media = context.input_roots.get("downloads").is_some_and(|roots| {
                    roots.iter().any(|root| {
                        std::fs::canonicalize(root).unwrap_or_else(|_| root.clone()) == resolved
                    })
                });
                let suffix = if options.locate_media {
                    "located"
                } else {
                    "directory"
                };
                binding.settings.options =
                    serde_json::to_value(options).expect("document options serialize");
                *binding = Binding::of(&SPEC, path, binding.settings.clone()).named(suffix);
            }
        }
    }
    found
}

super::input_format! { pub FORMAT = "documents" {
    specs: [SPEC],
    parser: |_b| Stateless(DocumentParser),
    discovery: DiscoverySpec::system(&[
        Location::new("declared", "documents"),
        Location::new("declared", "downloads"),
    ]),
    discover: discover,
    plan: |input, settings| {
        let options = Options::from_settings(settings)?;
        if input.as_os_str().is_empty() {
            return Err("document acquisition needs a root".into());
        }
        let input = std::path::absolute(input).map_err(|e| e.to_string())?;
        if input.is_file() {
            return Err("document acquisition needs a directory root".into());
        }
        Ok(vec![
            Binding::of(&SPEC, &input, settings.clone()).named(if options.locate_media { "located" } else { "directory" }),
        ])
    },
    reader: |binding| {
        let options = Options::from_settings(&binding.settings)?;
        let mut driver = driver(binding.input.clone(), binding.settings.host.clone());
        driver.locate = options.locate_media.then_some(locate_audio_video);
        Ok(binding::walked_excluding(
            driver,
            options.excluded_roots,
        ))
    },
} }
#[cfg(test)]
mod tests;
