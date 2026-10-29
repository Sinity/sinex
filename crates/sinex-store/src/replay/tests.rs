//! The replay diff: archived readings against the new live readings.
use super::*;
use crate::admission::NoDerivations;
use sinex_model::event_types::CommandExecuted;
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder,
    RecordLocation, RecordOutcome, ReplayScope, RunId, Timestamp,
};

fn reading(material: MaterialId, key: &str, command: &str) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: key.into(),
            command: command.into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            duration_ns: None,
            exit_code: None,
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn batch(events: Vec<Event>, archive: Vec<ArchiveRequest>, operation: OperationId) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Replay {
            operation,
            source: None,
            host: None,
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive,
    }
}

#[devkit::test]
async fn diff_names_changed_added_removed_and_counts_unchanged(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let m = MaterialId::new();
    let first = OperationId::new();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &batch(
            vec![
                reading(m, "same", "ls"),
                reading(m, "edit", "old"),
                reading(m, "gone", "rm"),
            ],
            vec![],
            first,
        ),
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let operation = OperationId::new();
    let mut tx = conn.begin().await.unwrap();
    let out = tx
        .admit(
            &batch(
                vec![
                    reading(m, "same", "ls"),
                    reading(m, "edit", "new"),
                    reading(m, "born", "make"),
                ],
                vec![ArchiveRequest::Replay {
                    scope: ReplayScope {
                        materials: vec![m],
                        ..ReplayScope::default()
                    },
                    operation,
                }],
                operation,
            ),
            &NoDerivations,
        )
        .await
        .unwrap();
    assert_eq!((out.replay_archived, out.admitted), (3, 3));
    let ids: Vec<Uuid> = out.admitted_ids.iter().map(|i| i.0).collect();
    let diff = diff(&mut tx, operation, &ids).await.unwrap();
    assert_eq!(diff.unchanged, 1);
    // Records are named by their equivalence key, a hash of the key fields.
    let id = |v: &Option<serde_json::Value>| v.as_ref().unwrap()["native_id"].clone();
    assert_eq!(diff.changed.len(), 1);
    assert_eq!(id(&diff.changed[0].before), "edit");
    assert_eq!(diff.changed[0].before.as_ref().unwrap()["command"], "old");
    assert_eq!(diff.changed[0].after.as_ref().unwrap()["command"], "new");
    assert_eq!(id(&diff.added[0].after), "born");
    assert_eq!(id(&diff.removed[0].before), "gone");
}
