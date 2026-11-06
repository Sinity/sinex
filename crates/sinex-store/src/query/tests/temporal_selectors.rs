//! Actual state endpoints are independent of an event's occurrence clock.
use super::*;

async fn marks(conn: &mut crate::AdmissionContext) {
    let events = [1, 2]
        .into_iter()
        .map(|offset| {
            Event::build(
                &sinex_model::event_types::query::Mark {
                    host: "synthetic".into(),
                    at: Timestamp::from_unix_secs(BASE + offset).unwrap(),
                    text: Some(format!("mark-{offset}")),
                    cwd: None,
                    command: None,
                    app: None,
                    title: None,
                    by: "synthetic".into(),
                    written_at: Timestamp::from_unix_secs(BASE + offset).unwrap(),
                    command_record: None,
                    focus_record: None,
                },
                "synthetic.marks",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                *blake3::hash(format!("mark-{offset}").as_bytes()).as_bytes(),
                Class::Open,
            )
            .unwrap()
        })
        .collect();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
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
async fn matched_endpoints_keep_actual_spans_and_half_open_membership(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands(&mut conn, &[("work", "/synthetic")]).await;
    marks(&mut conn).await;
    let start = ask(
        &mut conn,
        "commands | join near (commands) within 0s at match.start | fields match.distance",
    )
    .await;
    assert_eq!(start.rows[0]["match.distance"].as_f64(), Some(0.));
    let end = ask(
        &mut conn,
        "commands | join near (commands) within 2s at match.end | fields match.distance",
    )
    .await;
    assert_eq!(end.rows[0]["match.distance"].as_f64(), Some(2.));
    let contained=ask(&mut conn,"commands | join containing (events where event_type = operator.mark) at match.when | fields match.payload.text").await;
    assert_eq!(column(&contained, "match.payload.text"), [json!("mark-1")]);
    let during=ask(&mut conn,"events where event_type = operator.mark AND during (commands) at match.end | fields payload.text").await;
    assert_eq!(column(&during, "payload.text"), [json!("mark-1")]);
}

#[devkit::test]
async fn missing_actual_span_is_unknown_and_known_matches_win(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_commands(&mut conn, &[("work", "/synthetic")]).await;
    marks(&mut conn).await;
    let missing=ask(&mut conn,"commands where NOT near (events where event_type = operator.mark) within 1h at match.start").await;
    assert_eq!(
        missing.total, 0,
        "negation does not turn absent span evidence into a match"
    );
    let mixed=ask(&mut conn,"commands where near (events where event_type:(command.fused|operator.mark)) within 0s at match.start").await;
    assert_eq!(
        mixed.total, 1,
        "a known matching candidate wins over unknown candidates"
    );
    let no_match=ask(&mut conn,"events where event_type = operator.mark AND NOT near (events where event_type:(command.fused|operator.mark)) within 0s at match.start").await;
    assert_eq!(
        no_match.total, 0,
        "without a known match an unknown candidate remains unknown"
    );
}
