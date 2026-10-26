//! Google Assistant notes and lists (`Assistant Notes and Lists/*.csv`, one
//! note or list per file), on `ExportArchive`. A note's file holds a header
//! row and its title, content and state; a list's file its title and state,
//! then an item header row and one row per item.
//!
//! The file carries no time and no ID: its name ends with the time the note
//! or list was made (`Shopping list2019-02-10T20_35_18.476.csv`), which is its
//! identity. Exports from 2019 and 2020 write that time in UTC; one from 2022
//! wrote the same notes an hour later, in the operator's zone, so its
//! readings are separate notes. The time is read as UTC.
use super::super::fields;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::account::{NoteItem, NoteObserved};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.assistant",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "notes and lists kept through Google Assistant",
    class: Class::Open,
    parser: "google-assistant-notes",
    parser_version: 1,
    event_types: &["note.observed"],
    framer: crate::Framer::export(frame, Framing::Whole),
};

pub struct AssistantParser;

/// The time a file's name ends with, read as UTC.
fn made_at(path: &str) -> Result<Timestamp, RecordError> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name.strip_suffix(".csv").unwrap_or(name);
    // `YYYY-MM-DDTHH_MM_SS.mmm` is 23 characters.
    let time = stem
        .char_indices()
        .rev()
        .nth(22)
        .map(|(i, _)| &stem[i..])
        .ok_or_else(|| RecordError::new(format!("{name}: no time in the file name")))?;
    let civil = jiff::civil::DateTime::strptime("%Y-%m-%dT%H_%M_%S%.f", time)
        .map_err(|e| RecordError::new(format!("{name}: time {time:?}: {e}")))?;
    let at = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|e| RecordError::new(format!("{name}: {e}")))?
        .timestamp();
    Timestamp::from_unix_ns(at.as_nanosecond())
        .map_err(|_| RecordError::new(format!("{name}: time out of range")))
}

fn rows(bytes: &[u8]) -> Result<Vec<Vec<String>>, RecordError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(bytes);
    reader
        .records()
        .map(|row| {
            row.map(|r| r.iter().map(str::to_owned).collect())
                .map_err(|e| RecordError::new(format!("not CSV: {e}")))
        })
        .collect()
}

fn cell(row: Option<&Vec<String>>, index: usize) -> Option<String> {
    row.and_then(|r| r.get(index))
        .filter(|v| !v.is_empty())
        .cloned()
}

impl Parser for AssistantParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = NoteObserved;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<NoteObserved>, ()), RecordError> {
        let created_at = made_at(context.path().unwrap_or_default())?;
        let rows = rows(record)?;
        let header = rows
            .first()
            .ok_or_else(|| RecordError::new("an empty file"))?;
        let mut extra = Map::new();
        let (title, text, state, items) = match header.first().map(String::as_str) {
            Some("Note title") => {
                let row = rows.get(1);
                (cell(row, 0), cell(row, 1), cell(row, 2), vec![])
            }
            Some("List title") => {
                let row = rows.get(1);
                let mut items = Vec::new();
                let mut states = Vec::new();
                // Row 2 is the item header.
                for item in rows.iter().skip(3) {
                    let state = cell(Some(item), 1);
                    items.push(NoteItem {
                        text: cell(Some(item), 0).unwrap_or_default(),
                        checked: state.as_deref().is_some_and(|s| s != "Active"),
                    });
                    states.push(state.map_or(Value::Null, Value::String));
                }
                if !states.is_empty() {
                    extra.insert("item_states".into(), Value::Array(states));
                }
                (cell(row, 0), None, cell(row, 1), items)
            }
            other => {
                return Err(RecordError::new(format!(
                    "{other:?} is not a note or list header"
                )));
            }
        };
        if let Some(state) = state {
            extra.insert("state".into(), Value::String(state));
        }
        Ok((
            vec![NoteObserved {
                service: "google-assistant".into(),
                created_at,
                edited_at: None,
                title,
                text,
                items,
                labels: vec![],
                color: None,
                pinned: None,
                archived: None,
                trashed_at: None,
                attachments: vec![],
                extra,
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<AssistantParser> {
    DeclaredSource {
        spec: SPEC,
        parser: AssistantParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.contains("/Assistant Notes and Lists/") && member.ends_with(".csv")
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
