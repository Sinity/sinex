//! A notes vault (Obsidian or plain Markdown folders), on `DirSnapshot`: each
//! Markdown file is a `note.document` keyed by the vault and its path, and a
//! file missing from a completed scan is removed at the source.
//!
//! The vault's own folders (`.obsidian`, `.trash`, `.git`) are not notes.
//!
//! A note whose front matter declares an `id` is identified by that ID, so a
//! renamed or moved note is a newer reading of the same note, not a removal
//! and a new one. The parser carries which ID each path last held: a scan
//! reads the note at its new path before it records the old path missing,
//! and a missing path whose ID now lives at another path records nothing.
use crate::drain::RecordContext;
use crate::drivers::dir_snapshot::DirSnapshot;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::event_types::media::NoteDocument;
use sinex_model::{Class, Timestamp};
use std::collections::BTreeMap;
use std::path::PathBuf;

mod metadata;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.notes-vault",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "note recall and the library",
    class: Class::Open,
    parser: "notes-vault",
    parser_version: 4,
    event_types: &["note.document"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_whole,
        crate::drivers::export_archive::Framing::Whole,
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

/// Which note ID each path last held, and where each ID lives now.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NoteIds {
    by_path: BTreeMap<String, String>,
    path_of: BTreeMap<String, String>,
}

impl NoteIds {
    /// Record that `path` now holds `id` (or no ID).
    fn read(&mut self, path: &str, id: Option<&str>) {
        if let Some(old) = self.by_path.remove(path)
            && self.path_of.get(&old).is_some_and(|p| p == path)
        {
            self.path_of.remove(&old);
        }
        if let Some(id) = id.filter(|id| !id.trim().is_empty()) {
            self.by_path.insert(path.to_owned(), id.to_owned());
            self.path_of.insert(id.to_owned(), path.to_owned());
        }
    }

    /// `path` is gone. The ID it held, unless that ID now lives at another
    /// path (the note was renamed): `Err(())` then, and nothing is removed.
    fn remove(&mut self, path: &str) -> Result<Option<String>, ()> {
        let Some(id) = self.by_path.remove(path) else {
            return Ok(None);
        };
        if self.path_of.get(&id).is_some_and(|p| p == path) {
            self.path_of.remove(&id);
            Ok(Some(id))
        } else {
            Err(())
        }
    }
}

impl Parser for NotesParser {
    type Record<'a> = &'a [u8];
    type Carry = NoteIds;
    type Payload = NoteDocument;

    fn parse(
        &self,
        record: &&[u8],
        ids: &NoteIds,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<NoteDocument>, NoteIds), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("a note needs its directory snapshot member"))?;
        let path = member.path.clone();
        let removed_at = nanos(member, "removed_at_ns");
        let text = if removed_at.is_some() {
            None
        } else {
            Some(
                String::from_utf8(record.to_vec())
                    .map_err(|_| RecordError::new(format!("{path} is not UTF-8 text")))?,
            )
        };
        let metadata::Metadata {
            title,
            mut note_id,
            tags,
            wikilinks,
            front_matter,
        } = text
            .as_deref()
            .map(|text| metadata::parse(text, &path))
            .unwrap_or_default();
        let mut ids = ids.clone();
        if removed_at.is_some() {
            match ids.remove(&path) {
                Ok(id) => note_id = id,
                Err(()) => return Ok((vec![], ids)),
            }
        } else {
            ids.read(&path, note_id.as_deref());
        }
        let payload = NoteDocument {
            vault: context
                .manifest
                .path
                .as_deref()
                .and_then(|p| p.rsplit('/').find(|s| !s.is_empty()))
                .unwrap_or_default()
                .to_owned(),
            path,
            size: text.as_ref().map_or(0, |t| t.len() as u64),
            text,
            modified_at: nanos(member, "mtime_ns"),
            removed_at,
            title,
            note_id,
            tags,
            wikilinks,
            front_matter,
        };
        Ok((vec![payload], ids))
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
        root,
        host,
        select,
        part_bytes: 64 << 20,
    }
}

/// `--read notes=PATH`: the note vault at PATH, walked as one directory.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "notes",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::walked(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
