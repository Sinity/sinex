//! A notes vault (Obsidian or plain Markdown folders), on `DirSnapshot`: each
//! Markdown file is a `note.document` keyed by the vault and its path, and a
//! file missing from a completed scan is removed at the source.
//!
//! The vault's own folders (`.obsidian`, `.trash`, `.git`) are not notes.
//!
//! A note whose front matter declares an `id` is identified by that ID, so a
//! renamed or moved note is a newer reading of the same note. Directory
//! removals cite stored record locations, including when the content is sealed.
use serde_json::Value;
use sinex_model::event_types::media::NoteDocument;
use sinex_model::{Class, Timestamp};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

mod metadata;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.notes-vault",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "note recall and the library",
    class: Class::Open,
    parser: "notes-vault",
    parser_version: 6,
    event_types: &["note.document"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
};

/// The vault is named by its folder, from the material's path, so a replay
/// reads it the same way.
pub struct NotesParser;

fn nanos(member: &sinex_model::Member, field: &str) -> Option<Timestamp> {
    member
        .details
        .get(field)
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<i128>().ok())
        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
}

impl Parser for NotesParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = NoteDocument;

    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        context.hash_with_path(raw)
    }

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<NoteDocument>, ()), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("a note needs its directory snapshot member"))?;
        if member.details.contains_key("removed_at_ns") {
            return Ok((Vec::new(), ()));
        }
        let path = member.path.clone();
        let text = Some(
            String::from_utf8(record.to_vec())
                .map_err(|_| RecordError::new(format!("{path} is not UTF-8 text")))?,
        );
        let metadata::Metadata {
            title,
            note_id,
            tags,
            wikilinks,
            front_matter,
        } = text
            .as_deref()
            .map(|text| metadata::parse(text, &path))
            .unwrap_or_default();
        let payload = NoteDocument {
            vault: context
                .manifest
                .path
                .as_deref()
                .and_then(|p| p.rsplit('/').find(|s| !s.is_empty()))
                .unwrap_or_default()
                .to_owned(),
            host: context.manifest.detail("host").as_str().map(str::to_owned),
            vault_path: context.manifest.path.clone(),
            native_vault_path_bytes: context
                .manifest
                .detail("native_path_bytes")
                .as_str()
                .map(str::to_owned),
            native_path_bytes: member
                .details
                .get("native_path_bytes")
                .and_then(Value::as_str)
                .map(str::to_owned),
            path,
            size: text.as_ref().map_or(0, |t| t.len() as u64),
            text,
            modified_at: nanos(member, "mtime_ns"),
            removed_at: None,
            title,
            note_id,
            tags,
            wikilinks,
            front_matter,
        };
        Ok((vec![payload], ()))
    }
}

pub fn source() -> DeclaredSource<NotesParser> {
    DeclaredSource {
        spec: SPEC,
        parser: NotesParser,
    }
}

pub fn select(path: &str) -> bool {
    let hidden = path
        .split('/')
        .any(|part| matches!(part, ".obsidian" | ".trash" | ".git"));
    !hidden && (path.ends_with(".md") || path.ends_with(".markdown"))
}

pub fn driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        removed_at_source: true,
        root,
        host,
        select,
        part_bytes: 64 << 20,
    }
}

/// `--read notes=PATH`: the note vault at PATH, walked as one directory.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "notes",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "declared",
                path: "notes",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, input, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::walked(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
