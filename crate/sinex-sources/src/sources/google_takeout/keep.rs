//! Google Keep (`Keep/*.json`, one note per file), on `ExportArchive`. Keep
//! writes no note ID and names files after titles, so the creation time is the
//! identity; a later edit is a newer reading, and a trashed note is removed at
//! the source.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::account::{NoteItem, NoteObserved};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.keep",
    driver: Driver::ExportArchive,
    consumer: "notes and checklists kept in Keep",
    class: Class::Open,
    parser: "google-keep",
    parser_version: 1,
    event_types: &["note.observed"],
    framer: frame,
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

impl Parser for KeepParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = NoteObserved;

    fn parse(&self, note: &Map<String, Value>, _: &()) -> Result<(Vec<NoteObserved>, ()), RecordError> {
        let created_at = fields::from_unix_us(fields::int(note, "createdTimestampUsec")?)?;
        let edited_at = note
            .get("userEditedTimestampUsec")
            .and_then(Value::as_i64)
            .map(fields::from_unix_us)
            .transpose()?;
        let items = note
            .get("listContent")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|i| NoteItem {
                        text: i.get("text").and_then(Value::as_str).unwrap_or("").to_owned(),
                        checked: i.get("isChecked").and_then(Value::as_bool).unwrap_or(false),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let names = |field: &str, key: &str| -> Vec<String> {
            note.get(field)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.get(key).and_then(Value::as_str).map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let trashed = fields::optional_bool(note, "isTrashed") == Some(true);
        Ok((
            vec![NoteObserved {
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
                trashed_at: if trashed { edited_at.or(Some(created_at)) } else { None },
                attachments: names("attachments", "filePath"),
                // `textContentHtml` restates `textContent`; it is kept here,
                // with any field this parser does not know.
                extra: fields::extra(note, KNOWN),
            }],
            (),
        ))
    }
}

pub fn source() -> Declared<KeepParser> {
    Declared {
        spec: SPEC,
        parser: KeepParser,
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn notes_lists_and_trash() {
        let note = json!({"color": "DEFAULT", "isTrashed": true, "isPinned": false, "isArchived": false,
            "title": "Shopping", "createdTimestampUsec": 1_739_887_192_500_000i64,
            "userEditedTimestampUsec": 1_739_887_200_000_000i64,
            "listContent": [{"text": "milk", "isChecked": true}, {"text": "eggs", "isChecked": false}],
            "labels": [{"name": "home"}], "textContentHtml": "<p>x</p>"});
        let (out, ()) = KeepParser.parse(note.as_object().unwrap(), &()).unwrap();
        let n = &out[0];
        assert_eq!(n.items.len(), 2);
        assert!(n.items[0].checked);
        assert_eq!(n.labels, vec!["home"]);
        assert_eq!(n.trashed_at, n.edited_at);
        assert!(n.extra.contains_key("textContentHtml"));
        assert!(KeepParser.parse(json!({"title": "x"}).as_object().unwrap(), &()).is_err());
    }
}
