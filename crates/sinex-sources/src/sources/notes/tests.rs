use super::*;
use crate::drivers::dir_snapshot::{DirState, vanished_marker};
use crate::{DrainInput, Limits, drain};
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

#[test]
fn notes_and_their_removal() {
    let root = std::env::temp_dir().join(format!("sinex-vault-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join(".obsidian")).unwrap();
    std::fs::write(root.join("a.md"), "# A\n").unwrap();
    std::fs::write(root.join(".obsidian/app.json"), "{}").unwrap();
    let d = driver(root.clone(), "h".into());
    let mut state = DirState::default();
    let parser = NotesParser;
    let run = |a: &crate::drivers::Acquired| {
        let m = a.material.as_ref().unwrap();
        let out = drain(
            &parser,
            DrainInput {
                source: "t",
                class: Class::Open,
                material: MaterialId(uuid::Uuid::nil()),
                bytes: m.record_bytes(),
                records: &m.records,
                carry: Value::Null,
                reading_order: &|_| ReadingOrder(0),
                limits: Limits::default(),
                manifest: &m.manifest,
            },
        )
        .unwrap();
        out.into_iter()
            .flat_map(|c| c.outcomes)
            .map(|o| match o {
                RecordOutcome::Events { events, .. } => events[0].payload.clone(),
                RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
            })
            .collect::<Vec<_>>()
    };
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1);
    let first = run(&acquired);
    assert_eq!(first.len(), 1, ".obsidian is not a note");
    assert_eq!(first[0]["path"], "a.md");
    assert_eq!(first[0]["text"], "# A\n");
    assert!(first[0]["modified_at"].is_string());
    std::fs::remove_file(root.join("a.md")).unwrap();
    let second = run(&d.poll(&cursor, &mut state).unwrap());
    assert_eq!(second[0]["path"], "a.md");
    assert!(second[0]["vanished_at"].is_string());
    assert!(second[0]["text"].is_null());
    assert!(!vanished_marker("a.md").is_empty());
}
