//! The old join can read all live keys under a merge plan. The replacement
//! must agree on every row while visiting only the offered exact keys.
use super::*;
use sqlx::Executor;

const OLD: &str = include_str!("classification/predecessor.sql");
const NEW: &str = include_str!("../classification.sql");
type Row = (i64, String, Option<Uuid>);

async fn rows(conn: &mut PgConnection, sql: &str) -> Vec<Row> {
    sqlx::query_as(sql)
        .persistent(false)
        .fetch_all(conn)
        .await
        .unwrap()
}
async fn stage(conn: &mut PgConnection, events: &[Event]) {
    conn.execute("TRUNCATE core.admission_stage").await.unwrap();
    let numbered = events
        .iter()
        .enumerate()
        .map(|(i, e)| (i as i64, e))
        .collect::<Vec<_>>();
    crate::stage::copy(conn, &numbered).await.unwrap();
}
async fn table_work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid IN ('core.live_equivalence'::regclass,'core.live_locations'::regclass)")
        .fetch_one(conn).await.unwrap()
}
async fn encoder_calls(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce((SELECT calls FROM pg_stat_xact_user_functions WHERE funcid='core.encode_equivalence_key(text)'::regprocedure),0)::bigint")
        .fetch_one(conn).await.unwrap()
}
fn plan_contains(plan: &serde_json::Value, field: &str, text: &str) -> bool {
    match plan {
        Value::Object(fields) => {
            fields
                .get(field)
                .is_some_and(|v| v.as_str().is_some_and(|s| s.contains(text)))
                || fields.values().any(|v| plan_contains(v, field, text))
        }
        Value::Array(values) => values.iter().any(|v| plan_contains(v, field, text)),
        _ => false,
    }
}
async fn plan(conn: &mut PgConnection, sql: &str) -> Value {
    sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {sql}"))
        .persistent(false)
        .fetch_one(conn)
        .await
        .unwrap()
}
fn input(key: Option<&str>, material: MaterialId, anchor: u64, hash: u8, order: i64) -> Event {
    let mut e = keyed("synthetic-classifier", 1, hash, order);
    e.equivalence_key = key.map(|key| sinex_model::EquivalenceKey(key.to_owned()));
    e.origin = Origin::Record {
        location: RecordLocation { material, anchor },
        output_index: 0,
    };
    e.record_hash = [hash; 32];
    e
}

#[devkit::test]
async fn classification_exact_probes_preserve_rank_removal_and_location_semantics(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let material = MaterialId::new();
    let live = Uuid::now_v7();
    sqlx::query("INSERT INTO core.live_equivalence VALUES ('synthetic-classifier','command.executed',core.encode_equivalence_key('key'),$1,10,$2)")
        .bind(live).bind(&[1u8;32][..]).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.live_locations VALUES ($1,7,'command.executed',0,$2,10,$3)")
        .bind(material.0)
        .bind(live)
        .bind(&[1u8; 32][..])
        .execute(&mut conn)
        .await
        .unwrap();
    // Each case has its own stage, so the expected decision tests live
    // comparison independently of the duplicate-ranking test below.
    let cases = [
        (Some("key"), 7, 1, 10, "repeat"),
        (Some("key"), 7, 2, 11, "newer"),
        (Some("key"), 7, 2, 9, "older"),
        (Some("key"), 7, 2, 10, "newer"),
        (Some("key"), 7, 0, 10, "older"),
        (Some("missing"), 7, 2, 11, "new"),
        (None, 7, 1, 10, "repeat"),
        (None, 7, 2, 11, "newer"),
        (None, 8, 1, 10, "new"),
    ];
    for (key, anchor, hash, order, state) in cases {
        let event = input(key, material, anchor, hash, order);
        stage(&mut conn, &[event]).await;
        let expected = vec![(
            0,
            state.to_owned(),
            if state == "new" { None } else { Some(live) },
        )];
        assert_eq!(rows(&mut conn, OLD).await, expected);
        assert_eq!(rows(&mut conn, NEW).await, expected);
    }
    let mut other_source = input(Some("key"), material, 7, 1, 10);
    other_source.source = "other-source".into();
    stage(&mut conn, &[other_source]).await;
    assert_eq!(rows(&mut conn, NEW).await, vec![(0, "new".into(), None)]);
    let mut no_location = input(None, material, 7, 1, 10);
    no_location.origin = Origin::Derived {
        parents: vec![sinex_model::EventId::new()],
    };
    stage(&mut conn, &[no_location]).await;
    assert_eq!(rows(&mut conn, OLD).await, rows(&mut conn, NEW).await);
    assert_eq!(rows(&mut conn, NEW).await, vec![(0, "new".into(), None)]);
    let mut suppressed = input(Some("key"), material, 7, 2, 11);
    suppressed.replace = Some(sinex_model::ReplaceRule::SuppressRepeat);
    stage(&mut conn, &[suppressed.clone()]).await;
    assert_eq!(
        rows(&mut conn, NEW).await,
        vec![(0, "repeat".into(), Some(live))]
    );
    suppressed.record_state = sinex_model::RecordState::Removed;
    stage(&mut conn, &[suppressed]).await;
    assert_eq!(rows(&mut conn, OLD).await, rows(&mut conn, NEW).await);
    assert_eq!(
        rows(&mut conn, NEW).await,
        vec![(0, "newer".into(), Some(live))]
    );
    let events = vec![
        input(Some("key"), material, 7, 1, 10),
        input(Some("key"), material, 7, 2, 11),
        input(Some("key"), material, 7, 2, 11),
        input(None, material, 7, 1, 10),
        input(None, material, 7, 2, 11),
    ];
    stage(&mut conn, &events).await;
    let actual = rows(&mut conn, NEW).await;
    // The caller discards live_id for superseded rows; rank-one decisions
    // retain the exact predecessor identity without probing discarded rows.
    let old = rows(&mut conn, OLD).await;
    assert_eq!(
        actual.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>(),
        old.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>()
    );
    assert_eq!(
        actual.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
        ["superseded", "superseded", "newer", "superseded", "newer"]
    );
    let mut outcome = Outcome::default();
    let classified = classify::stage_and_classify(&mut conn, &events, &mut outcome)
        .await
        .unwrap();
    assert_eq!(classified.admit, [2, 4]);
    assert_eq!(classified.replaced, [(2, live), (4, live)]);
    assert_eq!(
        classified.resolved,
        [None, None, Some(events[2].id.0), None, Some(events[4].id.0)]
    );
    assert_eq!(outcome.superseded, 3);
}

#[devkit::test]
async fn classification_bounds_live_key_work_under_merge_plans(db: devkit::pg::TestDatabase) {
    // Compile-time binding/result validation uses another connection; it
    // cannot change the measured session's prepared statements or GUCs.
    let mut checked = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query_file!("src/admission/classification.sql")
        .fetch_all(&mut checked)
        .await
        .unwrap();
    checked.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let material = MaterialId::new();
    sqlx::query("INSERT INTO core.live_equivalence SELECT 'synthetic-classifier','command.executed',core.encode_equivalence_key('key-'||g),uuidv7(),10,decode(repeat('01',32),'hex') FROM generate_series(1,8192)g")
        .execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.live_locations SELECT $1,g,'command.executed',0,uuidv7(),10,decode(repeat('01',32),'hex') FROM generate_series(1,8192)g")
        .bind(material.0).execute(&mut conn).await.unwrap();
    let mut events = Vec::new();
    for i in 1..=64 {
        for _ in 0..2 {
            events.push(input(Some(&format!("key-{i}")), material, i, 1, 10));
        }
        events.push(input(None, material, i, 1, 10));
    }
    for i in 8193..8201 {
        events.push(input(
            Some(&format!("zzzz-missing-{i}")),
            material,
            i,
            1,
            10,
        ));
        events.push(input(None, material, i, 1, 10));
    }
    for e in &mut events {
        e.payload["command"] = json!("wide\t\\\nλ".repeat(8192));
    }
    stage(&mut conn, &events).await;
    conn.execute(
        "ANALYZE core.live_equivalence; ANALYZE core.live_locations; ANALYZE core.admission_stage",
    )
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_hashjoin=off; SET LOCAL enable_nestloop=off; SET LOCAL track_functions='all'; SET LOCAL jit=off").await.unwrap();
    let old_plan = plan(&mut tx, OLD).await;
    assert!(
        plan_contains(&old_plan, "Node Type", "Merge Join"),
        "{old_plan}"
    );
    let new_plan = plan(&mut tx, NEW).await;
    assert!(
        plan_contains(&new_plan, "Index Cond", "equivalence_key"),
        "{new_plan}"
    );
    let before = table_work(&mut tx).await;
    let fn_before = encoder_calls(&mut tx).await;
    let expected = rows(&mut tx, OLD).await;
    for _ in 1..8 {
        assert_eq!(rows(&mut tx, OLD).await, expected);
    }
    let old_work = table_work(&mut tx).await - before;
    let old_calls = encoder_calls(&mut tx).await - fn_before;
    let before = table_work(&mut tx).await;
    let fn_before = encoder_calls(&mut tx).await;
    for _ in 0..8 {
        let actual = rows(&mut tx, NEW).await;
        assert_eq!(
            actual.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>(),
            expected.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>()
        );
        for (a, b) in actual.iter().zip(&expected) {
            if a.1 != "superseded" {
                assert_eq!(a, b);
            }
        }
    }
    let new_work = table_work(&mut tx).await - before;
    let new_calls = encoder_calls(&mut tx).await - fn_before;
    eprintln!(
        "classification tuple visits over8: old={old_work} new={new_work}; encoder calls old={old_calls} new={new_calls}"
    );
    assert!(
        old_work >= 8192 * 8,
        "old query must read unrelated live history: {old_work}"
    );
    assert!(
        new_work <= events.len() as i64 * 8 * 2,
        "new probes exceed offered rows: {new_work}"
    );
    assert!(old_work > new_work * 4, "old={old_work} new={new_work}");
    assert!(
        new_calls
            <= events
                .iter()
                .filter(|e| e.equivalence_key.is_some())
                .count() as i64
                * 8,
        "encoder ran more than once per keyed row: {new_calls}"
    );
    tx.rollback().await.unwrap();
}
