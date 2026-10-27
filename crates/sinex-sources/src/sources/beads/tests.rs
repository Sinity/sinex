use super::*;
use sinex_model::{EventPayload, RecordState};
use std::path::Path;

fn line(id: &str, title: &str, status: &str) -> String {
    json!({"_type": "issue", "id": id, "title": title, "description": "what to do",
           "design": "how", "notes": "progress", "status": status, "priority": 2,
           "issue_type": "task", "assignee": "lane-a", "labels": ["area:code"],
           "created_at": "2026-10-01T10:00:00Z", "updated_at": "2026-10-02T10:00:00Z",
           "dependencies": [{"issue_id": id, "depends_on_id": "demo-epic",
                             "type": "parent-child"}]})
    .to_string()
}

fn export(path: &Path, lines: &[String]) {
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(path, text).unwrap();
}

fn parse(record: &Map<String, Value>) -> Result<Vec<TaskRevised>, RecordError> {
    BeadsParser
        .parse(record, &(), &crate::RecordContext::none())
        .map(|(out, ())| out)
}

fn records(a: &crate::drivers::Acquired) -> Vec<Map<String, Value>> {
    let Some(m) = a.material.as_ref() else {
        return Vec::new();
    };
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
        .collect()
}

fn cursor(a: &crate::drivers::Acquired) -> Value {
    match a.material.as_ref() {
        Some(m) => (m.cursor_at)(m.bytes.len()).unwrap(),
        None => a.cursor_without_material.clone(),
    }
}

fn ids(records: &[Map<String, Value>]) -> Vec<&str> {
    records.iter().map(|r| r["id"].as_str().unwrap()).collect()
}

fn file() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-beads-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("issues.jsonl")
}

#[devkit::test]
fn an_issue_is_a_task_identified_by_its_id() {
    let issue: Map<String, Value> =
        serde_json::from_str(&line("demo-a1.2", "Fix it", "open")).unwrap();
    let task = parse(&issue).unwrap().remove(0);
    assert_eq!(task.service, "beads");
    assert_eq!(task.task_id, "demo-a1.2");
    assert_eq!(task.list_id, "demo", "the store's prefix");
    assert_eq!(task.title.as_deref(), Some("Fix it"));
    assert_eq!(task.notes.as_deref(), Some("what to do"));
    assert_eq!(task.parent.as_deref(), Some("demo-epic"));
    assert_eq!(task.priority.as_deref(), Some("2"));
    assert_eq!(task.labels, ["area:code"]);
    assert_eq!(task.assignee.as_deref(), Some("lane-a"));
    assert_eq!(task.extra["notes"], json!("progress"));
    assert_eq!(task.extra["design"], json!("how"));
    assert!(!task.extra.contains_key("description"));
    assert_eq!(task.record_state(), RecordState::Present);
    let closed: Map<String, Value> = serde_json::from_value(json!({
        "_type": "issue", "id": "plnext-x", "status": "closed",
        "closed_at": "2026-10-03T00:00:00Z"}))
    .unwrap();
    let closed = parse(&closed).unwrap().remove(0);
    assert_eq!(closed.list_id, "plnext");
    assert!(closed.completed_at.is_some());
}

#[devkit::test]
fn other_records_and_unreadable_lines() {
    let memory: Map<String, Value> =
        serde_json::from_value(json!({"_type": "memory", "key": "k"})).unwrap();
    assert!(parse(&memory).unwrap().is_empty());
    let unreadable: Map<String, Value> =
        serde_json::from_value(json!({"unreadable": "{not json"})).unwrap();
    assert!(parse(&unreadable).unwrap_err().0.contains("not json"));
}

#[devkit::test]
fn a_changed_issue_is_read_again_and_a_missing_one_is_removed() {
    let path = file();
    export(
        &path,
        &[line("demo-a", "A", "open"), line("demo-b", "B", "open")],
    );
    let d = driver(path.clone(), "h".into());
    let first = d.poll(&Value::Null).unwrap();
    assert_eq!(ids(&records(&first)), ["demo-a", "demo-b"]);
    let at_first = cursor(&first);
    assert!(d.poll(&at_first).unwrap().material.is_none(), "unchanged");

    export(
        &path,
        &[
            line("demo-a", "A, renamed", "open"),
            line("demo-c", "C", "open"),
        ],
    );
    let second = d.poll(&at_first).unwrap();
    let read = records(&second);
    assert_eq!(ids(&read), ["demo-a", "demo-c", "demo-b"]);
    assert!(read[0].get("removed_at").is_none());
    let removed = parse(&read[2]).unwrap().remove(0);
    assert!(removed.deleted_at.is_some());
    assert_eq!(removed.record_state(), RecordState::Removed);
}

#[devkit::test]
fn after_a_restart_an_unchanged_file_is_only_remembered() {
    let path = file();
    export(
        &path,
        &[line("demo-a", "A", "open"), line("demo-b", "B", "open")],
    );
    let first = driver(path.clone(), "h".into()).poll(&Value::Null).unwrap();
    let at_first = cursor(&first);

    let restarted = driver(path.clone(), "h".into());
    let again = restarted.poll(&at_first).unwrap();
    assert!(records(&again).is_empty());
    export(&path, &[line("demo-a", "A", "open")]);
    let read = records(&restarted.poll(&cursor(&again)).unwrap());
    assert_eq!(ids(&read), ["demo-b"]);
    assert!(read[0].contains_key("removed_at"));
}

#[devkit::test]
fn asked_again_from_an_older_cursor_every_issue_is_read() {
    // A failed admission keeps the older cursor; the reader has moved on, so
    // it cannot tell what changed since then and reads everything.
    let path = file();
    export(&path, &[line("demo-a", "A", "open")]);
    let d = driver(path.clone(), "h".into());
    let at_first = cursor(&d.poll(&Value::Null).unwrap());
    export(
        &path,
        &[line("demo-a", "A", "closed"), line("demo-b", "B", "open")],
    );
    assert_eq!(
        ids(&records(&d.poll(&at_first).unwrap())),
        ["demo-a", "demo-b"]
    );
    assert_eq!(
        ids(&records(&d.poll(&at_first).unwrap())),
        ["demo-a", "demo-b"]
    );
}

#[devkit::test]
fn removal_survives_a_failed_admission_and_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("issues.jsonl");
    export(&path, &[line("demo-a", "A", "open"), line("demo-b", "B", "open")]);
    let reader = driver(path.clone(), "h".into());
    let admitted = cursor(&reader.poll(&Value::Null).unwrap());
    assert!(!admitted.to_string().contains("what to do"), "cursor holds no issue text");
    export(&path, &[line("demo-a", "A", "open")]);
    for reader in [reader, driver(path.clone(), "h".into())] {
        for _ in 0..2 {
            let acquired = reader.poll(&admitted).unwrap();
            let records = records(&acquired);
            assert_eq!(ids(&records), ["demo-b"]);
            assert_eq!(parse(&records[0]).unwrap()[0].record_state(), RecordState::Removed);
        }
    }
}
