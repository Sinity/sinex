use super::*;
use serde_json::json;

#[devkit::test]
fn tasks_in_lists() {
    let doc = json!({"kind": "tasks#taskLists", "items": [{"id": "L1", "title": "Mine",
        "items": [{"id": "T1", "title": "Call", "status": "completed",
            "updated": "2019-06-13T15:33:45.074110Z", "completed": "2019-06-13T15:33:45Z",
            "task_type": "PERSONAL_TASK"},
            {"id": "T2", "title": "", "status": "needsAction", "deleted": true,
             "updated": "2019-06-14T10:00:00Z"}]}]});
    let (out, ()) = TasksParser
        .parse(doc.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].list_title.as_deref(), Some("Mine"));
    assert!(out[0].completed_at.is_some());
    assert!(out[0].extra.contains_key("task_type"));
    assert_eq!(out[1].title, None, "empty title is unknown");
    assert!(out[1].deleted_at.is_some());
}
