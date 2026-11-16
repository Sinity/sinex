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
use sinex_model::Timestamp;
use sinex_model::event_types::media::NoteDocument;
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::drivers::export_archive::{Framing, frame_whole};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, RecordError, RecordResult, Stateless, StatelessParser};
use std::path::PathBuf;

mod metadata;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.notes-vault",
    origin: Origin::Input(Shape::DirSnapshot),
    expect: Expectation::cadence_hours(1),
    purpose: "note recall and the library",
    parser: "notes-vault",
    parser_version: 7,
    event_types: &["note.document"],
    framer: Framer::export(frame_whole, Framing::Whole),
    ..SourceSpec::DEFAULT
};

/// The vault is named by its folder, from the material's path, so a replay
/// reads it the same way.
pub struct NotesParser;

fn nanos(member: &sinex_model::Member, field: &str) -> Option<Timestamp> {
    member
        .details
        .str(field)
        .and_then(|s| s.parse::<i128>().ok())
        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
}

impl StatelessParser for NotesParser {
    type Record<'a> = &'a [u8];
    type Payload = NoteDocument;

    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        context.hash_with_path(raw)
    }

    fn parse(
        &self,
        record: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<NoteDocument>, RecordError> {
        let member = context
            .member
            .record("a note needs its directory snapshot member")?;
        if member.details.contains_key("removed_at_ns") {
            return Ok(Vec::new());
        }
        let path = member.path.clone();
        let text = Some(
            String::from_utf8(record.to_vec())
                .record_with(|| format!("{path} is not UTF-8 text"))?,
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
            native_path_bytes: member.details.str("native_path_bytes").map(str::to_owned),
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
        Ok(vec![payload])
    }
}

pub fn source() -> DeclaredSource<Stateless<NotesParser>> {
    DeclaredSource {
        spec: SPEC,
        parser: Stateless(NotesParser),
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

/// `stage --parser notes PATH`: the note vault at PATH, walked as one directory.
super::input_format! { pub FORMAT = "notes" {
    specs: [SPEC],
    source: |_b| Ok(Box::new(source())),
    discovery: DiscoverySpec::system(&[Location::new("declared", "notes")]),
    plan: single,
    reader: walked(driver),
} }

#[cfg(test)]
mod tests;
