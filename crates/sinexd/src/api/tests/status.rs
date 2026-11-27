use super::*;
use sinex_model::event_types::telemetry::SinexTelemetry;

fn sample_event(sample: SinexTelemetry) -> Event {
    Event::build(
        &sample,
        "sinex.telemetry",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&sample).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn sample(at: Timestamp, position: i64, behind: i64) -> Event {
    sample_event(SinexTelemetry {
        component: "search index".into(),
        name: String::new(),
        sampled_at: at,
        measures: [
            ("position".into(), position as f64),
            ("behind".into(), behind as f64),
        ]
        .into(),
    })
}

async fn status(w: &World) -> Value {
    let (status, body) = call_unbounded(w, Some(READ), json!({"method":"status"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["result"].clone()
}

fn search(status: &Value) -> &Value {
    status["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|consumer| consumer["name"] == "search index")
        .unwrap()
}

#[devkit::test]
async fn status_degraded_when_search_position_stalls(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let now = Timestamp::now();
    admit_events(
        &world,
        [600, 200, 1]
            .into_iter()
            .map(|seconds| {
                sample(
                    Timestamp(now.0 - time::Duration::seconds(seconds)),
                    100,
                    2_620_985,
                )
            })
            .collect(),
    )
    .await;
    let status = status(&world).await;
    assert_eq!(status["verdict"]["state"], "degraded", "{status}");
    assert_eq!(search(&status)["state"], "stalled", "{status}");
    assert_eq!(search(&status)["position"], 100);
    assert_eq!(search(&status)["last_advanced"], Value::Null);
    let reason = status["verdict"]["reason"].as_str().unwrap();
    assert!(
        reason.starts_with("search index: 2,620,985 behind, not advanced since "),
        "{reason}"
    );
    assert_eq!(status["reasons"][0], reason);
}

#[devkit::test]
async fn status_derivation_backlog_counts_only_unprocessed_inputs(db: devkit::pg::TestDatabase) {
    use sinex_model::{EventPayload, event_types::CommandImported};
    const NAME: &str = "synthetic.per-event";
    let world = world(db).await;
    let mut connection = world.reads.acquire().await.unwrap();
    sinex_store::registry::register_derivation(
        &mut connection,
        &sinex_store::registry::Derivation {
            name: NAME.into(),
            kind: "per_event",
            version: 1,
            inputs: vec![CommandExecuted::EVENT_TYPE.into()],
            outputs: vec![],
        },
    )
    .await
    .unwrap();
    let input = |text: &str| {
        Event::build(
            &CommandImported {
                command: text.into(),
                hostname: "fixture".into(),
                started_at: None,
                header_elapsed: None,
                paths: vec![],
                n: 0,
            },
            "synthetic.history",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    record_offset: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            *blake3::hash(text.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    let processed = input("processed input");
    let id = processed.id.0;
    admit_events(&world, vec![processed]).await;
    let position: i64 = sqlx::query_scalar("SELECT commit_seq FROM core.events WHERE id = $1")
        .bind(id)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO derivation.positions (derivation, commit_seq) VALUES ($1, $2)")
        .bind(NAME)
        .bind(position)
        .execute(&mut *connection)
        .await
        .unwrap();
    admit(&world, &["unrelated reading"], 0).await;

    let now = Timestamp::now();
    let legacy = sample_event(SinexTelemetry {
        component: "derivation".into(),
        name: NAME.into(),
        sampled_at: Timestamp(now.0 - time::Duration::minutes(12)),
        measures: [
            ("position".into(), position as f64),
            ("behind".into(), 99.0),
        ]
        .into(),
    });
    admit_events(&world, vec![legacy]).await;
    let sample_series = |reading: sinex_store::telemetry::Reading, seconds: &[i64]| {
        seconds
            .iter()
            .map(|seconds| {
                sample_event(SinexTelemetry {
                    component: reading.component.into(),
                    name: reading.name.clone(),
                    sampled_at: Timestamp(now.0 - time::Duration::seconds(*seconds)),
                    measures: reading.measures.clone(),
                })
            })
            .collect()
    };
    let derivation = |readings: Vec<sinex_store::telemetry::Reading>| {
        readings
            .into_iter()
            .find(|reading| reading.name == NAME)
            .unwrap()
    };
    let running = [(NAME, &[CommandImported::EVENT_TYPE][..])];
    let caught_up = derivation(
        sinex_store::telemetry::read(&mut connection, None, &running)
            .await
            .unwrap(),
    );
    admit_events(&world, sample_series(caught_up, &[710, 700])).await;
    let consumer = |status: &Value| {
        status["consumers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|consumer| consumer["name"] == format!("derivation {NAME}"))
            .unwrap()
            .clone()
    };
    let current = consumer(&status(&world).await);
    assert_eq!(current["position"], position);
    assert_eq!(current["behind"], 0);
    assert_eq!(current["state"], "current");

    admit_events(
        &world,
        vec![
            input("waiting input"),
            command("another unrelated reading", 2),
        ],
    )
    .await;
    let waiting = derivation(
        sinex_store::telemetry::read(&mut connection, None, &running)
            .await
            .unwrap(),
    );
    admit_events(&world, sample_series(waiting, &[600, 300, 1])).await;
    let report = status(&world).await;
    let stalled = consumer(&report);
    assert_eq!(stalled["behind"], 1);
    assert_eq!(stalled["state"], "stalled");
    assert!(report["reasons"].as_array().unwrap().iter().any(|reason| {
        reason
            .as_str()
            .unwrap()
            .starts_with(&format!("derivation {NAME}: 1 behind, not advanced since "))
    }));
    let paused = derivation(
        sinex_store::telemetry::read(&mut connection, None, &[])
            .await
            .unwrap(),
    );
    admit_events(&world, sample_series(paused, &[0])).await;
    let paused = consumer(&status(&world).await);
    assert_eq!(paused["behind"], Value::Null);
    assert_eq!(paused["state"], "unknown");
}

#[devkit::test]
async fn status_reports_rate_while_catching_up(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let now = Timestamp::now();
    admit_events(
        &world,
        [(601, 100, 900), (301, 400, 600), (1, 700, 300)]
            .into_iter()
            .map(|(seconds, position, behind)| {
                sample(
                    Timestamp(now.0 - time::Duration::seconds(seconds)),
                    position,
                    behind,
                )
            })
            .collect(),
    )
    .await;
    let status = status(&world).await;
    assert_eq!(status["verdict"]["state"], "healthy", "{status}");
    let consumer = search(&status);
    assert_eq!(consumer["state"], "catching_up");
    assert_eq!(consumer["rate"], 1.0);
    assert_eq!(consumer["estimate_seconds"], 300.0);
    let last = jiff::Timestamp::from_nanosecond(
        (now.0 - time::Duration::seconds(1)).unix_timestamp_nanos(),
    )
    .unwrap()
    .to_zoned(jiff::tz::TimeZone::UTC)
    .strftime("%Y-%m-%d %H:%M:%S%:z")
    .to_string();
    assert_eq!(consumer["last_advanced"], last);
}

#[devkit::test]
async fn status_import_only_source_not_dead(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let source = sinex_store::registry::Source {
        name: "synthetic.export".into(),
        shapes: vec!["snapshot".into()],
        version: 1,
        event_types: vec!["command.executed".into()],
        purpose: "Synthetic import".into(),
        expect: sinex_model::Expectation::Export,
        expires_after: None,
    };
    sinex_store::registry::write_sources(&mut world.reads.acquire().await.unwrap(), &[source])
        .await
        .unwrap();
    let status = status(&world).await;
    assert_eq!(status["sources"][0]["binding"], "synthetic.export");
    assert_eq!(status["sources"][0]["state"], "unavailable");
    assert_eq!(status["sources"][0]["reason"], "not_present");
    assert_eq!(status["source_states"]["unavailable"], 1);
    assert_eq!(status["verdict"]["state"], "unknown");
}

#[devkit::test]
async fn status_reads_every_source_page(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let sources: Vec<_> = (0..1003)
        .map(|index| sinex_store::registry::Source {
            name: format!("synthetic.export.{index:04}"),
            shapes: vec!["snapshot".into()],
            version: 1,
            event_types: vec![],
            purpose: "Synthetic import".into(),
            expect: sinex_model::Expectation::Export,
            expires_after: None,
        })
        .collect();
    sinex_store::registry::write_sources(&mut world.reads.acquire().await.unwrap(), &sources)
        .await
        .unwrap();
    let status = status(&world).await;
    assert_eq!(status["sources"].as_array().unwrap().len(), 1003);
    assert_eq!(status["sources"][1002]["binding"], "synthetic.export.1002");
    assert_eq!(status["source_states"]["unavailable"], 1003);
}

#[devkit::test]
async fn status_reports_advancing_consumer_falling_behind(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let now = Timestamp::now();
    admit_events(
        &world,
        [(301, 100, 100), (1, 200, 200)]
            .into_iter()
            .map(|(seconds, position, behind)| {
                sample(
                    Timestamp(now.0 - time::Duration::seconds(seconds)),
                    position,
                    behind,
                )
            })
            .collect(),
    )
    .await;
    let status = status(&world).await;
    assert_eq!(status["verdict"]["state"], "degraded", "{status}");
    assert_eq!(search(&status)["state"], "falling_behind");
    assert_eq!(search(&status)["estimate_seconds"], Value::Null);
}

#[devkit::test]
async fn status_keeps_unmeasured_keyed_consumers_unknown(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let sample = SinexTelemetry {
        component: "derivation".into(),
        name: "synthetic.keyed".into(),
        sampled_at: Timestamp::now(),
        measures: [("dirty_keys".into(), 7.0), ("waiting_keys".into(), 2.0)].into(),
    };
    let event = Event::build(
        &sample,
        "sinex.telemetry",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&sample).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap();
    admit_events(&world, vec![event]).await;
    let status = status(&world).await;
    assert_eq!(status["verdict"]["state"], "unknown", "{status}");
    assert_eq!(
        status["consumers"],
        json!([{
            "name":"derivation synthetic.keyed", "position":null, "behind":null,
            "rate":null, "estimate_seconds":null, "last_advanced":null, "state":"unknown",
        }])
    );
}

#[test]
fn publisher_status_uses_consecutive_samples_from_the_current_stream() {
    let context = sinex_query::Context::utc(jiff::Timestamp::now());
    let at = Timestamp::now();
    let publisher = |seconds, name: &str, position: Option<i64>| SinexTelemetry {
        component: "publisher".into(),
        name: name.into(),
        sampled_at: Timestamp(at.0 + time::Duration::seconds(seconds)),
        measures: position
            .map(|p| {
                [
                    ("position".into(), p as f64),
                    ("behind".into(), (100 - p) as f64),
                ]
                .into()
            })
            .unwrap_or_default(),
    };
    let report = |samples: &[SinexTelemetry]| {
        let status = crate::api::status_fields(samples, &[], &context).unwrap();
        let rows = status["consumers"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "publisher");
        rows[0].clone()
    };
    let old = "publisher/TEST_CONFIRMED/1";
    let active = "publisher/TEST_CONFIRMED/2";
    let legacy = publisher(-600, "", Some(1));
    assert_eq!(report(std::slice::from_ref(&legacy))["behind"], Value::Null);
    // The store orders by sample name before time.
    let mut samples = vec![
        publisher(-120, active, Some(80)),
        legacy,
        publisher(-180, old, Some(90)),
    ];
    let first = report(&samples);
    assert_eq!(first["position"], 80);
    assert_eq!(first["rate"], Value::Null);
    samples.push(publisher(-60, active, Some(100)));
    let current = report(&samples);
    assert_eq!(current["behind"], 0);
    assert_eq!(current["state"], "current");
    for name in [active, ""] {
        let mut missing = samples.clone();
        missing.push(publisher(-30, name, None));
        let unknown = report(&missing);
        assert_eq!(unknown["state"], "unknown");
        assert_eq!(unknown["position"], Value::Null);
        assert_eq!(unknown["behind"], Value::Null);
        missing.push(publisher(-1, active, Some(100)));
        assert_eq!(report(&missing)["rate"], Value::Null);
    }
}
