use super::*;
fn state_keys() -> Vec<Box<dyn AnyKeyed>> {
    vec![Box::new(sinex_derivations::state_interval::StateIntervals)]
}
#[devkit::test]
async fn state_closer_after_more_than_sixty_four_refreshes_is_not_lost(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, state_keys()).await;
    let mut records = vec![focus("window-a", 0)];
    records.extend((0..130).map(|index| {
        let mut event = focus("window-a", 18 * 3600 + index);
        event.payload["title"] = json!(format!("refresh-{index}"));
        event.record_hash = *blake3::hash(event.payload.to_string().as_bytes()).as_bytes();
        event
    }));
    w.admit(records).await;
    let mut runner = w.driver_with(state_keys(), vec![]).await;
    runner.clock = Some(at(4 * 86400));
    runner.catch_up().await.unwrap();
    let before = w.live::<StateInterval>().await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].ended_at, None);
    w.admit(vec![focus("window-b", 3 * 86400)]).await;
    runner.catch_up().await.unwrap();
    let after = w.live::<StateInterval>().await;
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].ended_at, Some(at(3 * 86400)));
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let parents:i64=sqlx::query_scalar("SELECT cardinality(parents)::bigint FROM core.events e WHERE event_type='state.interval' AND payload->>'interval_id'=$1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)")
        .bind(&after[0].interval_id).fetch_one(&mut conn).await.unwrap();
    assert_eq!(parents, 132);
    runner.end_runs().await.unwrap();
}

mod bounds;

#[devkit::test]
async fn interval_boundary_evidence_survives_derivation_admission_and_readback(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::TimeQuality;
    use sinex_model::event_types::{IntervalTimeBoundary, IntervalTimeEvidence};
    use sqlx::Row;

    let w = World::with_keys(db, state_keys()).await;
    let opening = focus("window-a", 0);
    let mut refresh = focus("window-a", 5);
    refresh.payload["title"] = json!("refreshed title");
    refresh.record_hash = *blake3::hash(refresh.payload.to_string().as_bytes()).as_bytes();
    w.admit(vec![opening.clone(), refresh.clone()]).await;
    let mut runner = w.driver_with(state_keys(), vec![]).await;
    runner.clock = Some(at(30));
    runner.catch_up().await.unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let before = sqlx::query("SELECT id,ts_orig,ts_quality::text AS quality,payload FROM core.events e WHERE event_type='state.interval' AND payload->>'state_kind'='desktop.focus' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)")
        .fetch_one(&mut conn).await.unwrap();
    let old_id: uuid::Uuid = before.get("id");
    let open: StateInterval = serde_json::from_value(before.get("payload")).unwrap();
    assert_eq!(before.get::<time::OffsetDateTime, _>("ts_orig"), at(0).0);
    assert_eq!(before.get::<String, _>("quality"), "captured");
    assert_eq!(
        open.started_at_evidence,
        Some(IntervalTimeEvidence {
            parent: opening.id,
            boundary: IntervalTimeBoundary::Transition,
            quality: TimeQuality::Captured,
        })
    );
    assert_eq!(open.ended_at, None);
    assert_eq!(open.ended_at_evidence, None);

    // An intrinsic AFK timestamp closes the captured focus; a same-kind label
    // refresh is a parent but supplies neither boundary.
    let closing = Event::build(
        &sinex_model::event_types::desktop::PresenceSpan {
            scope: Some("synthetic-compositor".into()),
            host: Some("box".into()),
            bucket: "synthetic-afk".into(),
            row: 1,
            started_at: at(10),
            ended_at: at(20),
            status: "afk".into(),
        },
        "desktop.activitywatch",
        location(10),
        ReadingOrder(0),
        [41; 32],
        Class::Open,
    )
    .unwrap();
    let later = focus("window-b", 25);
    let mut later_refresh = focus("window-b", 27);
    later_refresh.payload["title"] = json!("another refresh");
    later_refresh.record_hash =
        *blake3::hash(later_refresh.payload.to_string().as_bytes()).as_bytes();
    let next = focus("window-c", 40);
    runner.clock = Some(at(60));
    w.admit(vec![
        closing.clone(),
        later.clone(),
        later_refresh.clone(),
        next.clone(),
    ])
    .await;
    runner.catch_up().await.unwrap();
    let rows = sqlx::query("SELECT id,ts_orig,ts_quality::text AS quality,payload,parents FROM core.events e WHERE event_type='state.interval' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY ts_orig")
        .fetch_all(&mut conn).await.unwrap();
    let intervals: Vec<StateInterval> = rows
        .iter()
        .map(|row| serde_json::from_value(row.get("payload")).unwrap())
        .collect();
    let index = intervals
        .iter()
        .position(|interval| interval.interval_id == open.interval_id)
        .unwrap();
    let closed = &intervals[index];
    assert_eq!(closed.started_at, at(0));
    assert_eq!(closed.started_at_evidence, open.started_at_evidence);
    assert_eq!(closed.ended_at, Some(at(10)));
    assert_eq!(
        closed.ended_at_evidence,
        Some(IntervalTimeEvidence {
            parent: closing.id,
            boundary: IntervalTimeBoundary::Transition,
            quality: TimeQuality::Intrinsic,
        })
    );
    assert_eq!(closed.label.as_deref(), Some("kitty: refreshed title"));
    assert_eq!(rows[index].get::<String, _>("quality"), "captured");
    assert_eq!(
        rows[index].get::<time::OffsetDateTime, _>("ts_orig"),
        at(0).0
    );
    let parents: Vec<uuid::Uuid> = rows[index].get("parents");
    for parent in [opening.id, refresh.id, closing.id] {
        assert!(parents.contains(&parent.0));
    }
    assert_ne!(closed.ended_at_evidence.unwrap().parent, refresh.id);
    let archived: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
            .bind(old_id)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(
        archived,
        "closing replaces the same interval's earlier open reading"
    );

    let span = intervals
        .iter()
        .find(|interval| interval.state_kind == "desktop.activitywatch.afk")
        .unwrap();
    assert_eq!(span.ended_at, Some(at(20)));
    assert_eq!(
        span.ended_at_evidence,
        Some(IntervalTimeEvidence {
            parent: closing.id,
            boundary: IntervalTimeBoundary::ObservedEnd,
            quality: TimeQuality::Intrinsic,
        })
    );
    let next_closed = intervals
        .iter()
        .find(|interval| interval.subject_id.as_deref() == Some("window-b"))
        .unwrap();
    assert_eq!(next_closed.started_at_evidence.unwrap().parent, later.id);
    assert_eq!(
        next_closed.ended_at_evidence,
        Some(IntervalTimeEvidence {
            parent: next.id,
            boundary: IntervalTimeBoundary::Transition,
            quality: TimeQuality::Captured,
        })
    );
    assert_ne!(
        next_closed.ended_at_evidence.unwrap().parent,
        later_refresh.id
    );
    let still_open = intervals
        .iter()
        .find(|interval| interval.subject_id.as_deref() == Some("window-c"))
        .unwrap();
    assert_eq!(still_open.started_at_evidence.unwrap().parent, next.id);
    assert_eq!(still_open.ended_at, None);
    assert_eq!(still_open.ended_at_evidence, None);
    runner.end_runs().await.unwrap();
}
