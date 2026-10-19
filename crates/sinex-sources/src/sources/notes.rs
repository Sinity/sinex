//! A notes vault (Obsidian or plain Markdown folders), on `DirSnapshot`: each
//! Markdown file is a `note.document` keyed by the vault and its path, and a
//! file missing from a completed scan is removed at the source.
//!
//! The vault's own folders (`.obsidian`, `.trash`, `.git`) are not notes. A
//! note renamed in the vault is a removal and a new path; following an
//! Obsidian note ID across renames (`on0h`) needs the ID in the note, which is
//! not read yet.
use crate::drain::RecordContext;
use crate::drivers::dir_snapshot::DirSnapshot;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::Value;
use sinex_model::event_types::media::NoteDocument;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "knowledge.notes-vault",
    driver: Driver::DirSnapshot,
    consumer: "note recall and the library",
    class: Class::Open,
    parser: "notes-vault",
    parser_version: 1,
    event_types: &["note.document"],
    framer: crate::drivers::export_archive::frame_whole,
};

/// The vault is named by its folder, from the material's path, so a replay
/// reads it the same way.
pub struct NotesParser;

fn nanos(member: &Value, field: &str) -> Option<Timestamp> {
    member
        .get(field)
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<i128>().ok())
        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
}

impl Parser for NotesParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = NoteDocument;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<NoteDocument>, ()), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("a note needs its directory snapshot member"))?;
        let path = member
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("snapshot member has no path"))?
            .to_owned();
        let vanished_at = nanos(member, "vanished_at_ns");
        let text = if vanished_at.is_some() {
            None
        } else {
            Some(
                String::from_utf8(record.to_vec())
                    .map_err(|_| RecordError::new(format!("{path} is not UTF-8 text")))?,
            )
        };
        let payload = NoteDocument {
            vault: context
                .manifest
                .get("path")
                .and_then(Value::as_str)
                .and_then(|p| p.rsplit('/').find(|s| !s.is_empty()))
                .unwrap_or_default()
                .to_owned(),
            path,
            size: text.as_ref().map_or(0, |t| t.len() as u64),
            text,
            modified_at: nanos(member, "mtime_ns"),
            vanished_at,
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
        root,
        host,
        select,
        part_bytes: 64 << 20,
    }
}

/// `--source notes=PATH`: the note vault at PATH, walked as one directory.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
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
