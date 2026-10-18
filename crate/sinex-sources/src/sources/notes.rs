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
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::Value;
use sinex_model::kinds::media::NoteDocument;
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

    fn parse(&self, _: &&[u8], _: &()) -> Result<(Vec<NoteDocument>, ()), RecordError> {
        Err(RecordError::new(
            "a note needs its directory snapshot member",
        ))
    }

    fn parse_record(
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

pub fn source() -> Declared<NotesParser> {
    Declared {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::dir_snapshot::{DirMemory, vanished_marker};
    use crate::{DrainInput, Limits, drain};
    use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

    #[test]
    fn notes_and_their_removal() {
        let root = std::env::temp_dir().join(format!("sinex-vault-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(root.join(".obsidian")).unwrap();
        std::fs::write(root.join("a.md"), "# A\n").unwrap();
        std::fs::write(root.join(".obsidian/app.json"), "{}").unwrap();
        let d = driver(root.clone(), "h".into());
        let mut memory = DirMemory::default();
        let parser = NotesParser;
        let run = |a: &crate::drivers::Acquired| {
            let m = a.material.as_ref().unwrap();
            let out = drain(
                &parser,
                DrainInput {
                    source: "t",
                    class: Class::Open,
                    material: MaterialId(uuid::Uuid::nil()),
                    bytes: &m.bytes,
                    records: &m.records,
                    carry: Value::Null,
                    reading_order: &|_| ReadingOrder(0),
                    limits: Limits::default(),
                    manifest: &m.manifest,
                },
            )
            .unwrap();
            out.into_iter()
                .flat_map(|c| c.outcomes)
                .map(|o| match o {
                    RecordOutcome::Events { events, .. } => events[0].payload.clone(),
                    RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
                })
                .collect::<Vec<_>>()
        };
        let first = run(&d.poll(&Value::Null, &mut memory).unwrap());
        assert_eq!(first.len(), 1, ".obsidian is not a note");
        assert_eq!(first[0]["path"], "a.md");
        assert_eq!(first[0]["text"], "# A\n");
        assert!(first[0]["modified_at"].is_string());
        std::fs::remove_file(root.join("a.md")).unwrap();
        let second = run(&d.poll(&Value::Null, &mut memory).unwrap());
        assert_eq!(second[0]["path"], "a.md");
        assert!(second[0]["vanished_at"].is_string());
        assert!(second[0]["text"].is_null());
        assert!(!vanished_marker("a.md").is_empty());
    }
}
