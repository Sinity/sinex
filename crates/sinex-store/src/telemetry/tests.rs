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
async fn latest_samples_lookup_only_their_own_archive_membership(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::telemetry::SinexTelemetry as Sample;
    use sinex_model::{
        Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
        RecordOutcome, RunId, Timestamp,
    };
    const SAMPLES: usize = 4096;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let now = Timestamp::now();
    let mut events = Vec::new();
    let mut expected = Vec::new();
    let mut archived = Vec::new();
    for index in 0..SAMPLES {
        let outside = index % 11 == 0;
        let sample = Sample {
            component: "source".into(),
            name: format!("sample.{}", index % 8),
            sampled_at: Timestamp(
                now.0
                    - if outside {
                        time::Duration::hours(2)
                    } else {
                        time::Duration::seconds(60 + (index / 16) as i64)
                    },
            ),
            measures: [("position".into(), (index / 16) as f64)].into(),
        };
        let class = [Class::Open, Class::Guarded][index % 2];
        // Two source identities can report the same consumer and sample time.
        // Equal-time peers carry identical payloads: latest keeps its existing
        // timestamp tie semantics without adding an ID ordering key.
        let source = format!("telemetry.host{}", (index / 8) % 2);
        let event = Event::build(
            &sample,
            &source,
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    record_offset: 0,
                },
                output_index: 0,
            },
            ReadingOrder(0),
            *blake3::hash(&index.to_le_bytes()).as_bytes(),
            class,
        )
        .unwrap();
        if index % 7 == 0 {
            archived.push(event.id.0);
        } else if !outside {
            expected.push((event.id.0, sample, class));
        }
        events.push(event);
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
    // Unrelated old archive entries outnumber the recent samples, as on a
    // retained store. They must not be visited to answer this window.
    conn.execute(
        "INSERT INTO audit.archived(event_id,archived_seq,reason)
        SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,1,'archive'
        FROM generate_series(1,32768)n",
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO audit.archived(event_id,archived_seq,reason)
        SELECT unnest($1::uuid[]),1,'archive'",
    )
    .bind(&archived)
    .execute(&mut *conn)
    .await
    .unwrap();
    conn.execute("ANALYZE core.events; ANALYZE audit.archived")
        .await
        .unwrap();
    expected.sort_by(|a, b| {
        a.1.component
            .cmp(&b.1.component)
            .then(a.1.name.cmp(&b.1.name))
            .then(a.1.sampled_at.0.cmp(&b.1.sampled_at.0))
            .then(a.0.cmp(&b.0))
    });
    for (grants, classes) in [
        ("", vec![Class::Open]),
        ("guarded", vec![Class::Open, Class::Guarded]),
    ] {
        crate::query::set_grants(&mut conn, grants).await.unwrap();
        let mut newest = std::collections::BTreeMap::new();
        let mut withheld = false;
        for (_, sample, class) in &expected {
            if classes.contains(class) {
                newest.insert(
                    (sample.component.clone(), sample.name.clone()),
                    serde_json::to_value(sample).unwrap(),
                );
            } else {
                withheld = true;
            }
        }
        let mut rows: Vec<_> = newest.into_values().collect();
        if withheld {
            // Legacy latest returns one redacted null-key group, last in its
            // existing ordering. It does not expose a hidden sample's fields.
            rows.push(serde_json::json!({"withheld":"guarded"}));
        }
        assert_eq!(latest(&mut conn, "sinex.telemetry").await.unwrap(), rows);
    }

    // Execute plans only in this isolated test database. Count archive tuples
    // visited, rather than imposing a machine-dependent time budget.
    let statement = include_str!("latest.sql");
    fn archive_visits(value: &serde_json::Value) -> f64 {
        match value {
            serde_json::Value::Object(fields) => {
                let own =
                    if fields.get("Relation Name").and_then(|v| v.as_str()) == Some("archived") {
                        fields["Actual Rows"].as_f64().unwrap()
                            * fields["Actual Loops"].as_f64().unwrap()
                    } else {
                        0.0
                    };
                own + fields.values().map(archive_visits).sum::<f64>()
            }
            serde_json::Value::Array(values) => values.iter().map(archive_visits).sum(),
            _ => 0.0,
        }
    }
    let mut visits = Vec::new();
    let old = statement.replace(
        "(SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL",
        "NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)",
    );
    for sql in [statement, &old] {
        let plan: serde_json::Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {sql}"))
                .bind("sinex.telemetry")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        visits.push(archive_visits(&plan));
    }
    assert!(
        visits[0] <= SAMPLES as f64,
        "archive visits must be bounded by samples: {visits:?}"
    );
    assert!(
        visits[1] > SAMPLES as f64,
        "old query must expose the unrelated-history scan: {visits:?}"
    );
}
