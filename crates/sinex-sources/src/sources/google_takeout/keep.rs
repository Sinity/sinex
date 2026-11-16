//! Google Keep (`Keep/*.json`, one note per file), on `ExportArchive`. Keep
//! writes no note ID and names files after titles, so the creation time is the
//! identity; a later edit is a newer reading, and a trashed note is removed at
//! the source.
use crate::sources::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::account::{NoteItem, NoteObserved};
use sinex_model::json::JsonExt;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Framer, RecordError, Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.keep",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "notes and checklists kept in Keep",
    parser: "google-keep",
    event_types: &["note.observed"],
    framer: Framer::export(frame, Framing::Whole),
    ..SourceSpec::DEFAULT
};

const KNOWN: &[&str] = &[
    "title",
    "textContent",
    "listContent",
    "labels",
    "color",
    "isPinned",
    "isArchived",
    "isTrashed",
    "createdTimestampUsec",
    "userEditedTimestampUsec",
    "attachments",
];

pub struct KeepParser;

impl StatelessParser for KeepParser {
    type Record<'a> = Map<String, Value>;
    type Payload = NoteObserved;

    fn parse(
        &self,
        note: &Map<String, Value>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<NoteObserved>, RecordError> {
        let created_at = fields::from_unix_us(fields::int(note, "createdTimestampUsec")?)?;
        let edited_at = note
            .i64("userEditedTimestampUsec")
            .map(fields::from_unix_us)
            .transpose()?;
        let items = note
            .array("listContent")
            .map(|items| {
                items
                    .iter()
                    .map(|i| NoteItem {
                        text: i.str("text").unwrap_or("").to_owned(),
                        checked: i.bool("isChecked").unwrap_or(false),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let names = |field: &str, key: &str| -> Vec<String> {
            note.array(field)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.str(key).map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let trashed = fields::optional_bool(note, "isTrashed") == Some(true);
        Ok(vec![NoteObserved {
            service: "google-keep".into(),
            created_at,
            edited_at,
            title: fields::optional_text(note, "title"),
            text: fields::optional_text(note, "textContent"),
            items,
            labels: names("labels", "name"),
            color: fields::optional_text(note, "color"),
            pinned: fields::optional_bool(note, "isPinned"),
            archived: fields::optional_bool(note, "isArchived"),
            trashed_at: if trashed {
                edited_at.or(Some(created_at))
            } else {
                None
            },
            attachments: names("attachments", "filePath"),
            // `textContentHtml` restates `textContent`; it is kept here,
            // with any field this parser does not know.
            extra: fields::extra(note, KNOWN),
        }])
    }
}

pub fn source() -> DeclaredSource<Stateless<KeepParser>> {
    DeclaredSource {
        spec: SPEC,
        parser: Stateless(KeepParser),
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.contains("/Keep/") && member.ends_with(".json")
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::Whole,
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests;
