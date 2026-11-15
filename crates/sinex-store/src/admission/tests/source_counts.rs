use super::*;
use sinex_model::lifecycle::Act;

type Time = time::OffsetDateTime;
type Counts = Vec<(String, i64, Option<Time>, Option<Time>, Time)>;

async fn kept(conn: &mut PgConnection) -> Counts {
    sqlx::query_as(
        "SELECT source, events, first_event, last_event, last_seen
         FROM core.source_counts ORDER BY source",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

/// The same counts computed from every stored event.
async fn counted(conn: &mut PgConnection) -> Counts {
    sqlx::query_as(
        "SELECT e.source,
           count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)),
           min(e.ts_orig), max(e.ts_orig), max(e.ts_coided)
         FROM core.events e GROUP BY e.source ORDER BY e.source",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

async fn change(conn: &mut PgConnection, act: Act, target: sinex_model::EventId) {
    let ended = crate::lifecycle::move_events(conn, act, vec![target], &NoDerivations).await;
    assert!(
        matches!(ended, crate::lifecycle::Ended::Ok { .. }),
        "{ended:?}"
    );
}

#[devkit::test]
async fn counts_follow_admission_replacement_archiving_and_restoring(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first: Vec<Event> = (0..4).map(|key| keyed("a", key, 1, 1)).collect();
    admit_one(&mut conn, &batch(first.clone())).await;
    admit_one(&mut conn, &batch(vec![keyed("b", 9, 1, 1)])).await;
    // A newer reading of key 0 archives the first one.
    admit_one(&mut conn, &batch(vec![keyed("a", 0, 2, 5)])).await;
    change(&mut conn, Act::Archive, first[1].id).await;
    change(&mut conn, Act::Archive, first[2].id).await;
    change(&mut conn, Act::Restore, first[2].id).await;

    let kept = kept(&mut conn).await;
    assert_eq!(kept, counted(&mut conn).await);
    let live: Vec<(&str, i64)> = kept.iter().map(|c| (c.0.as_str(), c.1)).collect();
    assert_eq!(live, [("a", 3), ("b", 1)]);
}
