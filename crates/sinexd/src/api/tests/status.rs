use super::*;
use sinex_model::event_types::telemetry::SinexTelemetry;

fn sample(at: Timestamp, position: i64, behind: i64) -> Event {
    let sample = SinexTelemetry {
        component: "search index".into(),
        name: String::new(),
        sampled_at: at,
        measures: [
            ("position".into(), position as f64),
            ("behind".into(), behind as f64),
        ]
        .into(),
    };
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
