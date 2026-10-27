//! A Beads issue store, read from its JSONL export (what `bd export` writes,
//! or the store's own `.beads/issues.jsonl` with `export.auto` on), on
//! `ApiCursor`. Sinex reads only the export: it never runs `bd` and never
//! writes to the store.
//!
//! Each issue is a `task.revised` event identified by its Beads ID across
//! exports: a changed issue is a newer reading, and an issue gone from the
//! export is removed at the source. A poll reads the file when its size or
//! modification time changed and records each issue whose line changed. The
//! reader keeps each issue's last line in memory; after a restart it takes
//! them from the file when the file is unchanged since the cursor, and
//! otherwise records every issue again (an unchanged one is a repeat at
//! admission), so an issue deleted while Sinex was stopped and the file
//! changed is not seen as removed.
use crate::drivers::DriverError;
use crate::drivers::api_cursor::{Api, ApiCursor, Page};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::account::TaskRevised;
use sinex_model::{Class, InputRef, Timestamp};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

pub const SPEC: SourceSpec = SourceSpec {
    name: "work.beads",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "the issues work was planned and tracked in, beside the commits and jobs that did it",
    class: Class::Open,
    parser: "beads-issues",
    parser_version: 1,
    event_types: &["task.revised"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

/// Fields mapped to the payload's own; the rest are kept in `extra`.
const KNOWN: &[&str] = &[
    "_type",
    "id",
    "title",
    "description",
    "status",
    "priority",
    "assignee",
    "labels",
    "created_at",
    "updated_at",
    "closed_at",
    "due_at",
    "removed_at",
];

pub struct BeadsParser;

impl Parser for BeadsParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = TaskRevised;

    fn parse(
        &self,
        issue: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<TaskRevised>, ()), RecordError> {
        if let Some(line) = issue.get("unreadable") {
            return Err(RecordError::new(format!("not a JSON line: {line}")));
        }
        // Exports with `--all` carry memories and other records too.
        if issue.get("_type").and_then(Value::as_str) != Some("issue") {
            return Ok((Vec::new(), ()));
        }
        let id = fields::text(issue, "id")?;
        let store = id
            .rsplit_once('-')
            .map(|(prefix, _)| prefix.to_owned())
            .ok_or_else(|| RecordError::new(format!("issue ID {id:?} has no prefix")))?;
        let time = |name: &str| -> Result<Option<Timestamp>, RecordError> {
            fields::optional_text(issue, name)
                .map(|t| fields::rfc3339(&t))
                .transpose()
        };
        let parent = issue
            .get("dependencies")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|d| d.get("type").and_then(Value::as_str) == Some("parent-child"))
            .and_then(|d| d.get("depends_on_id").and_then(Value::as_str))
            .map(str::to_owned);
        let task = TaskRevised {
            service: "beads".into(),
            task_id: id,
            list_id: store,
            list_title: None,
            title: fields::optional_text(issue, "title").filter(|t| !t.is_empty()),
            // The issue's description; Beads' own `notes` stay in `extra`.
            notes: fields::optional_text(issue, "description").filter(|t| !t.is_empty()),
            status: fields::optional_text(issue, "status"),
            parent,
            created_at: time("created_at")?,
            updated_at: time("updated_at")?,
            due: fields::optional_text(issue, "due_at"),
            completed_at: time("closed_at")?,
            deleted_at: time("removed_at")?,
            priority: issue.get("priority").and_then(|p| match p {
                Value::Number(n) => Some(n.to_string()),
                Value::String(s) => Some(s.clone()),
                _ => None,
            }),
            labels: issue
                .get("labels")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|l| l.as_str().map(str::to_owned))
                .collect(),
            assignee: fields::optional_text(issue, "assignee").filter(|a| !a.is_empty()),
            extra: fields::extra(issue, KNOWN),
        };
        Ok((vec![task], ()))
    }
}

pub fn source() -> DeclaredSource<BeadsParser> {
    DeclaredSource {
        spec: SPEC,
        parser: BeadsParser,
    }
}

/// The export file as last read.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    pub size: u64,
    pub modified_ns: i128,
}

/// Each issue's line, by ID.
type Issues = BTreeMap<String, Map<String, Value>>;

pub struct Export {
    pub path: PathBuf,
    pub host: String,
    /// The file as last read, with each issue's line by ID: what the cursor
    /// naming that reading stands for. Empty until the first read.
    issues: Mutex<Option<(Seen, Issues)>>,
}

impl Export {
    pub fn new(path: PathBuf, host: String) -> Self {
        Self {
            path,
            host,
            issues: Mutex::new(None),
        }
    }
}

fn io(path: &std::path::Path, e: impl std::fmt::Display) -> DriverError {
    DriverError::Io(format!("{}: {e}", path.display()))
}

impl Api for Export {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "file".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError> {
        let previous: Option<Seen> = if cursor.is_null() {
            None
        } else {
            Some(
                serde_json::from_value(cursor.clone())
                    .map_err(|e| DriverError::Cursor(e.to_string()))?,
            )
        };
        let metadata = std::fs::metadata(&self.path).map_err(|e| io(&self.path, e))?;
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX));
        let seen = Seen {
            size: metadata.len(),
            modified_ns,
        };
        let mut remembered = self.issues.lock().expect("lock");
        let unchanged = previous.as_ref() == Some(&seen);
        if unchanged && remembered.as_ref().is_some_and(|(read, _)| *read == seen) {
            return Ok(Page {
                records: Vec::new(),
                next: cursor.clone(),
            });
        }
        let bytes = std::fs::read(&self.path).map_err(|e| io(&self.path, e))?;
        let mut current = Issues::new();
        let mut unreadable = Vec::new();
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            match serde_json::from_slice::<Map<String, Value>>(line) {
                Ok(issue) => match issue.get("id").and_then(Value::as_str) {
                    Some(id) => {
                        current.insert(id.to_owned(), issue);
                    }
                    None => unreadable.push(issue),
                },
                // A line that is not JSON is kept as text, so the parser
                // records it as a failure with its bytes.
                Err(_) => unreadable.push(Map::from_iter([(
                    "unreadable".into(),
                    json!(String::from_utf8_lossy(line)),
                )])),
            }
        }
        let mut records = Vec::new();
        match (previous, remembered.take()) {
            // The file is as it was when the cursor was recorded.
            (Some(previous), _) if previous == seen => {}
            // What the cursor stands for is remembered: only what changed.
            (Some(previous), Some((read, before))) if read == previous => {
                for (id, issue) in &current {
                    if before.get(id) != Some(issue) {
                        records.push(issue.clone());
                    }
                }
                let removed_at =
                    serde_json::to_value(Timestamp::now()).map_err(|e| io(&self.path, e))?;
                for (id, issue) in before {
                    if !current.contains_key(&id) {
                        let mut removed = issue;
                        removed.insert("removed_at".into(), removed_at.clone());
                        records.push(removed);
                    }
                }
                records.extend(unreadable);
            }
            // Nothing known about the cursor's reading: every issue.
            _ => {
                records.extend(current.values().cloned());
                records.extend(unreadable);
            }
        }
        *remembered = Some((seen.clone(), current));
        Ok(Page {
            records,
            next: json!(seen),
        })
    }
}

pub fn driver(path: PathBuf, host: String) -> ApiCursor<Export> {
    ApiCursor {
        api: Export::new(path, host),
    }
}

/// `--read beads=PATH`: a Beads JSONL export.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "beads",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
