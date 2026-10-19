//! Google Tasks (`Tasks/Tasks.json`, or `Tasks/<number>.Tasks`, JSON despite
//! the extension), on `ExportArchive`: one document of task lists, each with
//! its tasks. A task is identified by its ID; a later export with a newer
//! `updated` is a newer reading, and a deleted task is removed at the source.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::account::TaskRevised;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "google.tasks",
    driver: Driver::ExportArchive,
    consumer: "tasks and their completion",
    class: Class::Open,
    parser: "google-tasks",
    parser_version: 1,
    event_types: &["task.revised"],
    framer: frame,
};

const KNOWN: &[&str] = &[
    "id",
    "title",
    "notes",
    "status",
    "parent",
    "created",
    "updated",
    "due",
    "completed",
    "deleted",
    "kind",
    "selfLink",
];

pub struct TasksParser;

impl Parser for TasksParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = TaskRevised;

    fn parse(&self, doc: &Map<String, Value>, _: &()) -> Result<(Vec<TaskRevised>, ()), RecordError> {
        let lists = doc
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| RecordError::new("no task lists (`items`)"))?;
        let mut out = Vec::new();
        for list in lists {
            let list = list
                .as_object()
                .ok_or_else(|| RecordError::new("a task list is not an object"))?;
            let list_id = fields::text(list, "id")?;
            let list_title = fields::optional_text(list, "title");
            for task in list.get("items").and_then(Value::as_array).into_iter().flatten() {
                let task = task
                    .as_object()
                    .ok_or_else(|| RecordError::new("a task is not an object"))?;
                let time = |name: &str| -> Result<Option<sinex_model::Timestamp>, RecordError> {
                    fields::optional_text(task, name)
                        .map(|t| fields::rfc3339(&t))
                        .transpose()
                };
                let updated_at = time("updated")?;
                let created_at = time("created")?;
                let deleted = fields::optional_bool(task, "deleted") == Some(true);
                out.push(TaskRevised {
                    service: "google-tasks".into(),
                    task_id: fields::text(task, "id")?,
                    list_id: list_id.clone(),
                    list_title: list_title.clone(),
                    title: fields::optional_text(task, "title"),
                    notes: fields::optional_text(task, "notes"),
                    status: fields::optional_text(task, "status"),
                    parent: fields::optional_text(task, "parent"),
                    created_at,
                    updated_at,
                    due: fields::optional_text(task, "due"),
                    completed_at: time("completed")?,
                    // Removed at the source; the export gives no time of
                    // deletion, so the last update stands for it.
                    deleted_at: if deleted {
                        updated_at
                            .or(created_at)
                            .or_else(|| sinex_model::Timestamp::from_unix_secs(0).ok())
                    } else {
                        None
                    },
                    extra: fields::extra(task, KNOWN),
                });
            }
        }
        Ok((out, ()))
    }
}

pub fn source() -> Declared<TasksParser> {
    Declared {
        spec: SPEC,
        parser: TasksParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.contains("/Tasks/") && (member.ends_with(".json") || member.ends_with(".Tasks"))
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
    fn tasks_in_lists() {
        let doc = json!({"kind": "tasks#taskLists", "items": [{"id": "L1", "title": "Mine",
            "items": [{"id": "T1", "title": "Call", "status": "completed",
                "updated": "2019-06-13T15:33:45.074110Z", "completed": "2019-06-13T15:33:45Z",
                "task_type": "PERSONAL_TASK"},
                {"id": "T2", "title": "", "status": "needsAction", "deleted": true,
                 "updated": "2019-06-14T10:00:00Z"}]}]});
        let (out, ()) = TasksParser.parse(doc.as_object().unwrap(), &()).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].list_title.as_deref(), Some("Mine"));
        assert!(out[0].completed_at.is_some());
        assert!(out[0].extra.contains_key("task_type"));
        assert_eq!(out[1].title, None, "empty title is unknown");
        assert!(out[1].deleted_at.is_some());
    }
}
