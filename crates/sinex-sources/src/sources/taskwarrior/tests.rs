use super::*;
use serde_json::json;
use sinex_model::{EventPayload, RecordState};

const EXPORT: &[u8] = br#"[
{"id":1,"description":"Write the report","entry":"20260101T090000Z","modified":"20260102T100000Z","due":"20260110T170000Z","project":"work","priority":"H","status":"pending","tags":["writing"],"urgency":9.1,"uuid":"6f1c0f6e-0000-4000-8000-000000000001","annotations":[{"entry":"20260101T091000Z","description":"draft first"}]},
{"id":0,"description":"Call","end":"20260103T120000Z","entry":"20260101T090000Z","modified":"20260103T120000Z","status":"completed","urgency":0,"uuid":"6f1c0f6e-0000-4000-8000-000000000002"},
{"id":0,"description":"Old idea","end":"20260104T080000Z","entry":"20260101T090000Z","modified":"20260104T080000Z","status":"deleted","urgency":0,"uuid":"6f1c0f6e-0000-4000-8000-000000000003"}
]"#;

fn tasks() -> Vec<TaskRevised> {
    let (records, _) = frame(EXPORT);
    records
        .iter()
        .map(|(start, end)| {
            let record: Map<String, Value> = serde_json::from_slice(&EXPORT[*start..*end]).unwrap();
            TaskwarriorParser
                .parse(&record, &(), &crate::RecordContext::none())
                .unwrap()
                .0
                .remove(0)
        })
        .collect()
}

#[devkit::test]
fn tasks_by_uuid_with_their_times() {
    let tasks = tasks();
    assert_eq!(tasks.len(), 3);
    let pending = &tasks[0];
    assert_eq!(pending.service, "taskwarrior");
    assert_eq!(pending.task_id, "6f1c0f6e-0000-4000-8000-000000000001");
    assert_eq!(pending.list_id, "work");
    assert_eq!(pending.title.as_deref(), Some("Write the report"));
    assert_eq!(pending.priority.as_deref(), Some("H"));
    assert_eq!(pending.labels, ["writing"]);
    assert_eq!(pending.due.as_deref(), Some("2026-01-10T17:00:00Z"));
    assert_eq!(pending.created_at, Some(time("20260101T090000Z").unwrap()));
    assert!(pending.extra.contains_key("annotations"));
    assert_eq!(pending.completed_at, None);
    let completed = &tasks[1];
    assert_eq!(
        completed.completed_at,
        Some(time("20260103T120000Z").unwrap())
    );
    assert_eq!(completed.record_state(), RecordState::Present);
    let deleted = &tasks[2];
    assert_eq!(deleted.deleted_at, Some(time("20260104T080000Z").unwrap()));
    assert_eq!(deleted.completed_at, None);
    assert_eq!(deleted.record_state(), RecordState::Removed);
}

#[devkit::test]
fn the_working_set_number_and_urgency_do_not_change_a_task() {
    let one = json!({"id": 1, "urgency": 9.1, "uuid": "u", "status": "pending"}).to_string();
    let two = json!({"id": 4, "urgency": 2.0, "uuid": "u", "status": "pending"}).to_string();
    let other = json!({"id": 1, "urgency": 9.1, "uuid": "u", "status": "completed"}).to_string();
    let hash = |raw: &str| crate::drain::record_hash::<TaskwarriorParser>(raw.as_bytes());
    assert_eq!(hash(&one), hash(&two));
    assert_ne!(hash(&one), hash(&other));
}

#[devkit::test]
fn a_time_not_in_taskwarrior_form_is_refused() {
    assert!(time("2026-01-01T09:00:00Z").is_err());
}
