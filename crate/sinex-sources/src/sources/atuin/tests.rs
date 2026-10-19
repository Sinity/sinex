use super::*;
use serde_json::json;

fn row(duration: i64, exit: i64) -> Map<String, Value> {
    json!({"_rowid": 1, "id": "019", "timestamp": 1_743_722_259_001_000_000i64, "duration": duration,
           "exit": exit, "command": "ls", "cwd": "/", "session": "s", "hostname": "h:u",
           "deleted_at": null, "author": "sinity", "intent": null, "shell": "zsh", "future_column": 3})
    .as_object()
    .unwrap()
    .clone()
}

#[test]
fn running_commands_have_unknown_duration_and_exit() {
    let (p, ()) = AtuinParser.parse(&row(-1, -1), &()).unwrap();
    assert_eq!((p[0].duration_ns, p[0].exit_code), (None, None));
    let (p, ()) = AtuinParser.parse(&row(5, -9), &()).unwrap();
    assert_eq!(
        (p[0].duration_ns, p[0].exit_code),
        (Some(5), Some(-9)),
        "exit codes are signed"
    );
}

#[test]
fn unknown_columns_are_kept_and_deletion_is_removal() {
    let mut r = row(5, 0);
    r.insert("deleted_at".into(), json!(1_743_722_300_000_000_000i64));
    let (p, ()) = AtuinParser.parse(&r, &()).unwrap();
    assert_eq!(p[0].extra.get("future_column"), Some(&json!(3)));
    assert!(p[0].deleted_at.is_some());
    assert_eq!(
        sinex_model::EventPayload::record_state(&p[0]),
        sinex_model::RecordState::Removed
    );
}

#[test]
fn wrong_types_fail_the_record() {
    let mut r = row(5, 0);
    r.insert("command".into(), json!(7));
    assert!(AtuinParser.parse(&r, &()).is_err());
}
