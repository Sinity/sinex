use super::*;
use crate::registry::{Source as RegisteredSource, write_exclusions, write_sources};
use sinex_model::Expectation;
use sinex_model::event_types::coverage::{CoverageCompleteness, CoverageState, SourceCoverage};
use sinex_model::event_types::telemetry::SinexTelemetry;
use std::time::Duration;

fn source(name: &str, expect: Expectation) -> RegisteredSource {
    RegisteredSource {
        name: name.into(),
        shapes: match expect {
            Expectation::Export => vec!["export-archive".into()],
            Expectation::OnDemand => vec![],
            _ => vec!["live-stream".into()],
        },
        version: 1,
        event_types: vec!["sinex.telemetry".into()],
        purpose: "synthetic source status".into(),
        expect,
        expires_after: None,
    }
}

fn origin() -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            record_offset: 0,
        },
        output_index: 0,
    }
}

fn reading(source: &str, at: Timestamp) -> Event {
    let mut event = Event::build(
        &SinexTelemetry {
            component: "source".into(),
            name: source.into(),
            sampled_at: at,
            measures: Default::default(),
        },
        source,
        origin(),
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    event.id = EventId(Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        at.0.unix_timestamp() as u64,
        0,
    )));
    event
}

fn coverage(source: &str, state: CoverageState, reason: Option<&str>, at: Timestamp) -> Event {
    let mut event = Event::build(
        &SourceCoverage {
            observed_run: None,
            observed_class: None,
            observed_source: source.into(),
            binding: source.into(),
            host: "example".into(),
            started_at: at,
            ended_at: at,
            state,
            reason: reason.map(str::to_owned),
            completeness: CoverageCompleteness::Unknown,
            bounded_by: "synthetic read".into(),
            events: 0,
            failures: 0,
        },
        "test.coverage",
        origin(),
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    event.id = EventId(Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        at.0.unix_timestamp() as u64,
        0,
    )));
    event
}

async fn admit(conn: &mut crate::AdmissionContext, events: Vec<Event>) {
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
    tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn sources_report_three_states(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    write_sources(
        &mut conn,
        &[
            source(
                "test.continuous",
                Expectation::Continuous {
                    silent_after: Duration::from_secs(300),
                },
            ),
            source("test.quiet", Expectation::OnDemand),
            source("test.excluded", Expectation::Export),
        ],
    )
    .await
    .unwrap();
    let old = Timestamp::from_unix_secs(1_700_000_000).unwrap();
    admit(
        &mut conn,
        vec![reading("test.continuous", old), reading("test.quiet", old)],
    )
    .await;
    write_exclusions(
        &mut conn,
        &[(
            "test.excluded".into(),
            "input intentionally excluded".into(),
        )]
        .into(),
    )
    .await
    .unwrap();
    let answer = ask(
        &mut conn,
        "sources | fields source,state,reason,last_error,exclusion | sort by source",
    )
    .await;
    assert_eq!(
        answer.rows,
        [
            json!({"source":"test.continuous","state":"stalled","reason":"no reading within 00:05:00","last_error":null,"exclusion":null}),
            json!({"source":"test.excluded","state":"excluded","reason":"input intentionally excluded","last_error":null,"exclusion":"input intentionally excluded"}),
            json!({"source":"test.quiet","state":"quiet","reason":null,"last_error":null,"exclusion":null}),
        ]
    );
    write_exclusions(&mut conn, &Default::default())
        .await
        .unwrap();
    let answer = ask(
        &mut conn,
        "sources where source = test.excluded | fields state,reason,exclusion",
    )
    .await;
    assert_eq!(
        answer.rows,
        [json!({"state":"unavailable","reason":"not_present","exclusion":null})]
    );
}

#[devkit::test]
async fn sources_classify_coverage_before_recent_activity(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    write_sources(
        &mut conn,
        &[
            source(
                "test.cadence",
                Expectation::Cadence {
                    every: Duration::from_secs(300),
                },
            ),
            source("test.excluded", Expectation::OnDemand),
            source("test.missing", Expectation::OnDemand),
            source("test.empty", Expectation::OnDemand),
            source("test.export", Expectation::Export),
            source("test.capturing", Expectation::OnDemand),
        ],
    )
    .await
    .unwrap();
    let old = Timestamp::from_unix_secs(1_700_000_000).unwrap();
    let now = Timestamp::now();
    admit(
        &mut conn,
        vec![
            coverage("test.cadence", CoverageState::Observed, None, old),
            coverage(
                "test.excluded",
                CoverageState::Missing,
                Some("synthetic unreadable input"),
                now,
            ),
            coverage(
                "test.missing",
                CoverageState::Missing,
                Some("synthetic read failure"),
                now,
            ),
            coverage("test.empty", CoverageState::Observed, None, now),
            reading("test.capturing", now),
        ],
    )
    .await;
    write_exclusions(
        &mut conn,
        &[("test.excluded".into(), "deliberate exclusion".into())].into(),
    )
    .await
    .unwrap();
    let answer = ask(&mut conn, "sources where source = (test.cadence | test.excluded | test.missing | test.empty | test.export | test.capturing) | fields source,state,reason,last_error | sort by source").await;
    assert_eq!(
        answer.rows,
        [
            json!({"source":"test.cadence","state":"stalled","reason":"no coverage read within 00:05:00","last_error":null}),
            json!({"source":"test.capturing","state":"capturing","reason":null,"last_error":null}),
            json!({"source":"test.empty","state":"capturing","reason":null,"last_error":null}),
            json!({"source":"test.excluded","state":"excluded","reason":"deliberate exclusion","last_error":"synthetic unreadable input"}),
            json!({"source":"test.export","state":"unavailable","reason":"not_present","last_error":null}),
            json!({"source":"test.missing","state":"stalled","reason":"synthetic read failure","last_error":"synthetic read failure"}),
        ]
    );
    let mut recovered = coverage(
        "test.missing",
        CoverageState::Observed,
        None,
        Timestamp::now(),
    );
    recovered.reading_order = ReadingOrder(1);
    recovered.record_hash = [1; 32];
    admit(&mut conn, vec![recovered]).await;
    let answer = ask(
        &mut conn,
        "sources where source = test.missing | fields state,reason,last_error",
    )
    .await;
    assert_eq!(
        answer.rows,
        [json!({"state":"capturing","reason":null,"last_error":null})]
    );
}

#[devkit::test]
async fn answer_times_share_the_context_zone(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit(
        &mut conn,
        vec![
            reading(
                "test.zone",
                Timestamp::from_unix_secs(1_792_888_200).unwrap(),
            ),
            reading(
                "test.zone",
                Timestamp::from_unix_secs(1_792_891_800).unwrap(),
            ),
        ],
    )
    .await;
    let context = Context::new(
        "2026-10-25T02:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let answer = run(&mut conn,
        "events where source = test.zone AND when >= 2026-10-25T00:30:00Z | fields when | sort by when",
        None, &context, &Page::default(),
    ).await.unwrap();
    assert_eq!(answer.resolved.zone, "Europe/Warsaw");
    assert_eq!(answer.resolved.now, "2026-10-25T02:00:00Z");
    assert!(
        answer.read_as.contains("2026-10-25 02:30:00+02:00"),
        "{}",
        answer.read_as
    );
    assert_eq!(
        column(&answer, "when"),
        [
            json!("2026-10-25T02:30:00+02:00"),
            json!("2026-10-25T02:30:00+01:00")
        ]
    );
}
