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
