//! A Beads issue store, read from its JSONL export (what `bd export` writes,
//! or the store's own `.beads/issues.jsonl` with `export.auto` on), on
//! `ApiCursor`. Sinex reads only the export: it never runs `bd` and never
//! writes to the store.
//!
//! Each issue is a `task.revised` event identified by its Beads ID across
//! exports: a changed issue is a newer reading, and an issue gone from the
//! export is removed at the source. A poll reads the file when its size or
//! modification time changed and records each issue whose line changed. The
//! cursor keeps each issue's hash and ID, so a retry or restart compares the
//! new export with exactly the export whose records were admitted. Missing
//! issues become removed readings even when the file changed while stopped.
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

/// The admitted export's file facts and issue hashes, without issue text.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub size: u64,
    pub modified_ns: i128,
    pub issues: BTreeMap<String, String>,
}

pub struct Export {
    pub path: PathBuf,
    pub host: String,
}

impl Export {
    pub fn new(path: PathBuf, host: String) -> Self {
        Self { path, host }
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
        let previous: Option<Position> = if cursor.is_null() {
            None
        } else {
            Some(
                serde_json::from_value(cursor.clone())
                    .map_err(|error| DriverError::Cursor(error.to_string()))?,
            )
        };
        let facts = |metadata: &std::fs::Metadata| -> Result<(u64, i128), DriverError> {
            let modified = metadata.modified().map_err(|error| io(&self.path, error))?;
            let modified_ns = modified
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| io(&self.path, error))?
                .as_nanos();
            Ok((
                metadata.len(),
                i128::try_from(modified_ns).map_err(|error| io(&self.path, error))?,
            ))
        };
        let mut file = std::fs::File::open(&self.path).map_err(|error| io(&self.path, error))?;
        let metadata = file.metadata().map_err(|error| io(&self.path, error))?;
        let (size, modified_ns) = facts(&metadata)?;
        if previous
            .as_ref()
            .is_some_and(|position| position.size == size && position.modified_ns == modified_ns)
        {
            return Ok(Page {
                records: Vec::new(),
                next: cursor.clone(),
            });
        }
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut bytes).map_err(|error| io(&self.path, error))?;
        let after = file.metadata().map_err(|error| io(&self.path, error))?;
        if facts(&after)? != (size, modified_ns) || bytes.len() as u64 != size {
            return Err(io(&self.path, "export changed while reading"));
        }
        let mut issues = BTreeMap::new();
        let mut records = Vec::new();
        let mut unreadable = Vec::new();
        let mut complete = true;
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            match serde_json::from_slice::<Map<String, Value>>(line) {
                Ok(issue) => match issue.get("id").and_then(Value::as_str) {
                    Some(id) if issue.get("_type").and_then(Value::as_str) == Some("issue") => {
                        let hash = blake3::hash(line).to_hex().to_string();
                        if previous.as_ref().and_then(|p| p.issues.get(id)) != Some(&hash) {
                            records.push(issue.clone());
                        }
                        if issues.insert(id.to_owned(), hash).is_some() {
                            return Err(io(&self.path, format!("duplicate issue ID {id:?}")));
                        }
                    }
                    _ => {
                        if issue.get("_type").and_then(Value::as_str) == Some("issue") {
                            complete = false;
                        }
                        unreadable.push(issue);
                    }
                },
                Err(_) => {
                    complete = false;
                    unreadable.push(Map::from_iter([(
                        "unreadable".into(),
                        json!(String::from_utf8_lossy(line)),
                    )]));
                }
            }
        }
        if let Some(previous) = previous {
            let removed_at =
                serde_json::to_value(Timestamp::now()).map_err(|error| io(&self.path, error))?;
            for id in previous.issues.keys() {
                if !issues.contains_key(id) && !complete {
                    issues.insert(id.clone(), previous.issues[id].clone());
                } else if !issues.contains_key(id) {
                    records.push(
                        serde_json::from_value(json!({
                            "_type": "issue", "id": id, "removed_at": removed_at,
                        }))
                        .expect("JSON object"),
                    );
                }
            }
        }
        records.extend(unreadable);
        Ok(Page {
            records,
            next: json!(Position {
                size,
                modified_ns,
                issues
            }),
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
        Ok(crate::binding::polled(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
