use super::*;
use sqlx::Executor;
use std::time::Duration;

#[devkit::test]
async fn a_sample_keeps_one_snapshot_while_admission_commits(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut writer = PgConnection::connect(&url).await.unwrap();
    writer
        .execute("INSERT INTO core.source_counts (source, events, last_seen) VALUES ('test.source', 1, now())")
        .await
        .unwrap();
    writer
        .execute("UPDATE core.counters SET value = 1 WHERE name = 'commit_seq'")
        .await
        .unwrap();
    let mut reader = PgConnection::connect(&url).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut reader)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&url).await.unwrap();
    let mut transaction = writer.begin().await.unwrap();
    transaction
        .execute("LOCK TABLE core.source_counts IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let sample = tokio::spawn(async move { read(&mut reader).await.unwrap() });

    // Admission changes the counts after the sample read its commit position.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE pid = $1 AND wait_event_type = 'Lock'
                   AND query LIKE '%FROM core.source_counts%')",
            )
            .bind(pid)
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the sample did not reach its source counts");
    transaction
        .execute("UPDATE core.counters SET value = 2 WHERE name = 'commit_seq'")
        .await
        .unwrap();
    transaction
        .execute("UPDATE core.source_counts SET events = 2 WHERE source = 'test.source'")
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let sample = sample.await.unwrap();
    let measures = |sample: &[Reading], component, name| {
        sample
            .iter()
            .find(|reading| reading.component == component && reading.name == name)
            .unwrap()
            .measures
            .clone()
    };
    assert_eq!(measures(&sample, "writer", "")["commit_seq"], 1.0);
    assert_eq!(measures(&sample, "source", "test.source")["events"], 1.0);
    let next = read(&mut writer).await.unwrap();
    assert_eq!(measures(&next, "writer", "")["commit_seq"], 2.0);
    assert_eq!(measures(&next, "source", "test.source")["events"], 2.0);
}

#[devkit::test]
async fn recent_samples_keep_history_within_the_window(db: devkit::pg::TestDatabase) {
    use sinex_model::{
        Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
        RecordOutcome, RunId, Timestamp,
    };
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let now = Timestamp::now();
    let mut events = Vec::new();
    for (minutes, position) in [(20, 10.0), (10, 20.0), (5, 30.0)] {
        let sampled_at = Timestamp(now.0 - time::Duration::minutes(minutes));
        events.push(
            Event::build(
                &Sample {
                    component: "search index".into(),
                    name: String::new(),
                    sampled_at,
                    measures: [
                        ("position".into(), position),
                        ("behind".into(), 40.0 - position),
                    ]
                    .into(),
                },
                "sinex.telemetry",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        record_offset: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(0),
                [0; 32],
                Class::Open,
            )
            .unwrap(),
        );
    }
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch, &crate::NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    let samples = recent(&mut conn, "sinex.telemetry", Duration::from_secs(15 * 60))
        .await
        .unwrap();
    assert_eq!(
        samples,
        [
            Sample {
                component: "search index".into(),
                name: String::new(),
                sampled_at: Timestamp(now.0 - time::Duration::minutes(10)),
                measures: [("position".into(), 20.0), ("behind".into(), 20.0)].into()
            },
            Sample {
                component: "search index".into(),
                name: String::new(),
                sampled_at: Timestamp(now.0 - time::Duration::minutes(5)),
                measures: [("position".into(), 30.0), ("behind".into(), 10.0)].into()
            },
        ]
    );
    for kind in ["per_event", "keyed"] {
        crate::registry::register_derivation(
            &mut conn,
            &crate::registry::Derivation {
                name: format!("example.{kind}"),
                kind,
                version: 1,
                inputs: vec![],
                outputs: vec![],
            },
            "test",
        )
        .await
        .unwrap();
    }
    let readings = read(&mut conn).await.unwrap();
    for component in ["publisher", "search index"] {
        let reading = readings
            .iter()
            .find(|reading| reading.component == component)
            .unwrap();
        assert_eq!(reading.measures["position"], 0.0);
        assert_eq!(reading.measures["behind"], 3.0);
    }
    let initial = readings
        .iter()
        .find(|reading| reading.name == "example.per_event")
        .unwrap();
    assert_eq!(initial.measures["position"], 0.0);
    assert_eq!(initial.measures["behind"], 3.0);
    let keyed = readings
        .iter()
        .find(|reading| reading.name == "example.keyed")
        .unwrap();
    assert_eq!(
        keyed.measures,
        [("dirty_keys".into(), 0.0), ("waiting_keys".into(), 0.0)].into()
    );
}
