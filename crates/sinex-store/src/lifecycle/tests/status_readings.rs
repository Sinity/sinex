use super::*;
use sinex_model::Expectation;
use sinex_model::event_types::coverage::{CoverageCompleteness, CoverageState, SourceCoverage};
use sinex_model::event_types::fused::{ConsideredPair, PairState, Strategy};

async fn parts(conn: &mut PgConnection) -> (i64, i64, String) {
    let fusion =
        crate::diagnostics::fusion_ambiguity(conn, &[("command".into(), "command.fused".into())])
            .await
            .unwrap();
    let source =
        crate::query::source_rows(conn, &sinex_query::Context::utc(jiff::Timestamp::now()))
            .await
            .unwrap()
            .into_iter()
            .find(|row| row["binding"] == "fixture.inputs")
            .unwrap();
    (
        fusion[0].undecided,
        fusion[0].decided,
        source["state"].as_str().unwrap().into(),
    )
}

#[devkit::test]
async fn status_readings_follow_admission_archive_restore_and_replay(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    crate::registry::write_sources(
        &mut conn,
        &[crate::registry::Source {
            name: "fixture.inputs".into(),
            shapes: vec![],
            version: 1,
            event_types: vec![],
            purpose: "Status fixture".into(),
            expect: Expectation::OnDemand,
            expires_after: None,
        }],
    )
    .await
    .unwrap();
    let material = material(&mut conn, &blob(59)).await;
    let at = Timestamp::from_unix_secs(1_600_000_000).unwrap();
    let origin = |record_offset| Origin::Record {
        location: RecordLocation {
            material,
            record_offset,
        },
        output_index: 0,
    };
    let pair = |state, hash| {
        Event::build(
            &ConsideredPair {
                family: "command".into(),
                left: "left".into(),
                right: "right".into(),
                left_source: "fixture.left".into(),
                right_source: "fixture.right".into(),
                at,
                strategy: Strategy::Window,
                candidates: 2,
                distance_ns: 0,
                chosen: false,
                state,
                decided_by: None,
                other_decisions: vec![],
            },
            "fixture.fusion",
            origin(0),
            ReadingOrder(hash.into()),
            [hash; 32],
            Class::Open,
        )
        .unwrap()
    };
    let coverage = |state, hash| {
        Event::build(
            &SourceCoverage {
                observed_run: None,
                observed_class: None,
                observed_source: "fixture.inputs".into(),
                binding: "fixture.inputs".into(),
                host: "synthetic".into(),
                started_at: at,
                ended_at: at,
                state,
                reason: Some("fixture read outcome".into()),
                completeness: CoverageCompleteness::Unknown,
                bounded_by: "synthetic".into(),
                events: 0,
                failures: 0,
            },
            "fixture.coverage",
            origin(1),
            ReadingOrder(hash.into()),
            [hash; 32],
            Class::Open,
        )
        .unwrap()
    };
    let waiting = pair(PairState::Undecided, 1);
    let missing = coverage(CoverageState::Missing, 1);
    admit(&mut conn, &batch(vec![waiting.clone(), missing.clone()])).await;
    assert_eq!(parts(&mut conn).await, (1, 0, "stalled".into()));
    let selected = events(&[&waiting, &missing]);
    assert!(matches!(
        operate(&mut conn, Act::Archive, selected.clone(), 16).await,
        Ended::Ok { .. }
    ));
    assert_eq!(parts(&mut conn).await, (0, 0, "unavailable".into()));
    assert!(matches!(
        operate(&mut conn, Act::Restore, selected, 16).await,
        Ended::Ok { .. }
    ));
    assert_eq!(parts(&mut conn).await, (1, 0, "stalled".into()));
    let (_, changed) = replay(
        &mut conn,
        material,
        &[
            pair(PairState::Same, 2),
            coverage(CoverageState::Observed, 2),
        ],
    )
    .await;
    assert_eq!(changed.admitted, 2);
    let (waiting, decided, source) = parts(&mut conn).await;
    assert_eq!((waiting, decided), (0, 1));
    assert!(matches!(source.as_str(), "capturing" | "quiet"), "{source}");
}
