use super::*;
use serde_json::json;

fn parse(line: Value) -> Result<AgentctlEvent, RecordError> {
    AgentctlParser
        .parse(
            line.as_object().unwrap(),
            &(),
            &crate::RecordContext::none(),
        )
        .map(|(mut out, ())| out.remove(0))
}

#[devkit::test]
fn a_finished_job_with_its_checkout_heads() {
    let event = parse(json!({
        "attempt": 1, "emitted_at": "2026-10-26T21:01:00Z", "exit_code": 0,
        "job_id": "demo-check-f575e943", "job_kind": "declared-operation",
        "kind": "queue-task", "label": "demo:check", "operation": "check",
        "outcome": "success", "phase": "finished", "pool": "normal", "project": "demo",
        "task_id": 1386, "working_directory": "/work/lane",
        "execution_receipt": {"start": {"head": "aaa", "checkout_root": "/work/lane",
                                        "files": ["a", "b"]},
                              "end": {"head": "bbb"}},
        "execution_evidence": {"selector": ["cargo", "test"]}
    }))
    .unwrap();
    assert_eq!(event.record_kind, "queue-task");
    assert_eq!(event.phase.as_deref(), Some("finished"));
    assert_eq!(event.job_id.as_deref(), Some("demo-check-f575e943"));
    assert_eq!(event.outcome.as_deref(), Some("success"));
    assert_eq!(event.exit_code, Some(0));
    assert_eq!(event.attempt, Some(1));
    assert_eq!(event.working_directory.as_deref(), Some("/work/lane"));
    assert_eq!(event.start_head.as_deref(), Some("aaa"));
    assert_eq!(event.end_head.as_deref(), Some("bbb"));
    assert!(event.at.is_some());
    assert_eq!(event.extra["label"], json!("demo:check"));
    assert!(!event.extra.contains_key("execution_receipt"));
    assert!(!event.extra.contains_key("execution_evidence"));
}

#[devkit::test]
fn older_and_other_lines_keep_what_they_have() {
    let old = parse(
        json!({"checkout": "default", "completed_at": "2026-08-25T21:35:30.264107+00:00",
                           "job_id": "68228212", "kind": "declared-operation",
                           "phase": "succeeded", "project": "demo"}),
    )
    .unwrap();
    assert_eq!(
        old.at,
        Some(fields::rfc3339("2026-08-25T21:35:30.264107Z").unwrap())
    );
    assert_eq!(old.extra["checkout"], json!("default"));
    let untimed =
        parse(json!({"kind": "campaign", "transition": "opened", "wave_id": "w1"})).unwrap();
    assert_eq!(
        untimed.at, None,
        "a line without a time has an unknown time"
    );
    assert_eq!(untimed.extra["wave_id"], json!("w1"));
    assert!(
        parse(json!({"phase": "started"})).is_err(),
        "a line names its kind"
    );
}
