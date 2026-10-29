use super::*;
use serde_json::Value;
use sinex_model::event_types::CommandImported;

/// A zsh command stated at `secs`.
fn stated(command: &str, secs: i64) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "kestrel".into(),
            started_at: Some(Timestamp::from_unix_secs(secs).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// An implausible stated time is kept as stated and labelled, never refused
/// or filtered out for its value.
#[devkit::test]
async fn implausible_times_are_kept_and_labelled(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![
                stated("at epoch", 0),
                stated("in 1901", -2_177_452_800),
                stated("in 2023", 1_700_000_000),
                stated("in 2200", 7_258_118_400),
            ],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(tx.admit(&batch, &NoDerivations).await.unwrap().admitted, 4);
    tx.commit().await.unwrap();

    let all = ask(&mut conn, "events fields when, time_suspect sort when").await;
    assert_eq!(
        column(&all, "time_suspect"),
        [
            json!(null),
            json!("epoch zero"),
            json!(null),
            json!("future")
        ]
    );
    let suspect = ask(&mut conn, "events where time_suspect exists sort when").await;
    assert_eq!(suspect.total, 2);
    let future = ask(&mut conn, "events where time_suspect = future").await;
    assert_eq!(future.total, 1);
}

/// Each event says which version of its producer wrote it, so outputs of two
/// derivation versions live side by side during a recompute are told apart.
#[devkit::test]
async fn events_name_their_producer_version(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    for (version, command) in [("1", "first"), ("2", "second")] {
        let run = crate::runs::start(
            &mut conn,
            &crate::runs::Manifest {
                name: "terminal.zsh",
                kind: "source",
                version,
                build: "test",
                emits: &["command.imported"],
            },
            "kestrel",
        )
        .await
        .unwrap();
        let batch = Batch {
            run,
            kind: BatchKind::Api {
                actor: "test".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: vec![stated(command, 1_700_000_000)],
            }],
            archive: vec![],
        };
        let mut tx = conn.begin().await.unwrap();
        tx.admit(&batch, &NoDerivations).await.unwrap();
        tx.commit().await.unwrap();
    }
    let a = ask(
        &mut conn,
        "events fields when, producer_version sort producer_version",
    )
    .await;
    assert_eq!(column(&a, "producer_version"), [json!("1"), json!("2")]);
    let a = ask(&mut conn, "events where producer_version = 2").await;
    assert_eq!(a.total, 1);
}

#[devkit::test]
async fn telemetry_payloads_use_the_callers_class_grants(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = Event::build(
        &sinex_model::event_types::telemetry::SinexTelemetry {
            component: "source".into(),
            name: "example".into(),
            sampled_at: Timestamp::now(),
            measures: [("events".into(), 3.0)].into(),
        },
        "sinex.telemetry",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Replay {
            operation: OperationId::new(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(tx.admit(&batch, &NoDerivations).await.unwrap().admitted, 1);
    tx.commit().await.unwrap();
    let answer = ask(&mut conn, "telemetry fields payload").await;
    assert_eq!(column(&answer, "payload"), [json!({"withheld": "guarded"})]);
    assert_eq!(
        crate::telemetry::latest(&mut conn, "sinex.telemetry")
            .await
            .unwrap(),
        [json!({"withheld": "guarded"})]
    );
    let answer = ask(&mut conn, "telemetry where payload.name = example").await;
    assert_eq!(answer.total, 0);
    sqlx::query("SELECT set_config('sinex.grants', 'guarded', false)")
        .execute(&mut conn)
        .await
        .unwrap();
    let payload: Value = sqlx::query_scalar(
        "SELECT payload FROM core.visible_events WHERE event_type = 'sinex.telemetry'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(payload["name"], "example");
    assert_eq!(
        crate::telemetry::latest(&mut conn, "sinex.telemetry")
            .await
            .unwrap()[0]["name"],
        "example"
    );
}

#[devkit::test]
async fn telemetry_uses_ordinary_admission_commit_order_and_visibility(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = Event::build(
        &sinex_model::event_types::telemetry::SinexTelemetry {
            component: "writer".into(),
            name: "".into(),
            sampled_at: Timestamp::now(),
            measures: Default::default(),
        },
        "sinex.telemetry",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    let event_id = event.id;
    let mut batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    for admitted in [1, 0] {
        let mut tx = conn.begin().await.unwrap();
        assert_eq!(
            tx.admit(&batch, &NoDerivations).await.unwrap().admitted,
            admitted
        );
        tx.commit().await.unwrap();
    }
    assert_eq!(
        ask(&mut conn, "events where event_type = sinex.telemetry")
            .await
            .total,
        1
    );
    assert_eq!(ask(&mut conn, "telemetry").await.total, 1);
    let committed: i64 =
        sqlx::query_scalar("SELECT commit_seq FROM core.commit_log WHERE event_id = $1")
            .bind(event_id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(committed > 0);
    batch.records.clear();
    batch.archive.push(sinex_model::ArchiveRequest::Hide {
        target: event_id,
        operation: OperationId::new(),
    });
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(ask(&mut conn, "telemetry").await.total, 0);
    assert!(
        crate::telemetry::latest(&mut conn, "sinex.telemetry")
            .await
            .unwrap()
            .is_empty()
    );
}
