//! Taskwarrior exports (`task export` writes a JSON array of tasks), on
//! `ExportArchive`. A task is identified by its UUID across exports; a later
//! export's reading replaces an earlier one. A deleted task (status
//! `deleted`) is removed at the source, at its `end`.
//!
//! The working-set number (`id`) and `urgency` change without the task
//! changing, so they are left out of the record hash.
use crate::sources::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::account::TaskRevised;
use sinex_model::{Class, Timestamp};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "tasks.taskwarrior",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "tasks and their completion",
    class: Class::Open,
    parser: "taskwarrior-export",
    parser_version: 1,
    event_types: &["task.revised"],
    framer: sinex_source_core::Framer::export(frame, Framing::JsonArray("")),
};

const KNOWN: &[&str] = &[
    "uuid",
    "description",
    "status",
    "project",
    "parent",
    "entry",
    "modified",
    "due",
    "end",
    "priority",
    "tags",
];

/// Taskwarrior's time format, `20260101T120000Z`.
fn time(raw: &str) -> Result<Timestamp, RecordError> {
    let civil = jiff::civil::DateTime::strptime("%Y%m%dT%H%M%SZ", raw)
        .map_err(|e| RecordError::new(format!("time {raw:?}: {e}")))?;
    let at = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|e| RecordError::new(format!("time {raw:?}: {e}")))?;
    Timestamp::from_unix_ns(at.timestamp().as_nanosecond())
        .map_err(|_| RecordError::new(format!("time {raw:?} out of range")))
}

pub struct TaskwarriorParser;

impl Parser for TaskwarriorParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = TaskRevised;

    const VOLATILE: &'static [&'static str] = &["id", "urgency"];

    fn parse(
        &self,
        task: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<TaskRevised>, ()), RecordError> {
        let at = |name: &str| -> Result<Option<Timestamp>, RecordError> {
            fields::optional_text(task, name)
                .map(|t| time(&t))
                .transpose()
        };
        let status = fields::optional_text(task, "status");
        let end = at("end")?;
        Ok((
            vec![TaskRevised {
                service: "taskwarrior".into(),
                task_id: fields::text(task, "uuid")?,
                list_id: fields::optional_text(task, "project").unwrap_or_default(),
                list_title: None,
                title: fields::optional_text(task, "description"),
                notes: None,
                parent: fields::optional_text(task, "parent"),
                created_at: at("entry")?,
                updated_at: at("modified")?,
                due: at("due")?.and_then(|due| match serde_json::to_value(due) {
                    Ok(Value::String(text)) => Some(text),
                    _ => None,
                }),
                completed_at: end.filter(|_| status.as_deref() == Some("completed")),
                deleted_at: end.filter(|_| status.as_deref() == Some("deleted")),
                status,
                priority: fields::optional_text(task, "priority"),
                labels: task
                    .get("tags")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|t| t.as_str().map(str::to_owned))
                    .collect(),
                assignee: None,
                extra: fields::extra(task, KNOWN),
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<TaskwarriorParser> {
    DeclaredSource {
        spec: SPEC,
        parser: TaskwarriorParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("").frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".json"),
        framing: Framing::JsonArray(""),
        part_bytes: PART_BYTES,
    }
}

/// `--read taskwarrior=PATH`: each `task export` file in PATH.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "taskwarrior",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "input",
                path: "taskwarrior",
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
            Ok(sinex_source_core::binding::per_file(
                &SPEC,
                input,
                settings,
                false,
                |path| path.extension().is_some_and(|e| e == "json"),
            ))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::shared(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
