//! Classification compares each offered row with the live row for its key
//! or record location.
use super::*;
use sqlx::Executor;

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
    // Superseded rows are not compared with live rows, so they carry none.
    assert_eq!(
        actual.iter().map(|r| r.2).collect::<Vec<_>>(),
        [None, None, Some(live), None, Some(live)]
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
async fn classification_matches_offered_rows_among_many_live_keys(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let material = MaterialId::new();
    sqlx::query("INSERT INTO core.live_equivalence SELECT 'synthetic-classifier','command.executed',core.encode_equivalence_key('key-'||g),uuidv7(),10,decode(repeat('01',32),'hex') FROM generate_series(1,8192)g")
        .execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.live_locations SELECT $1,g,'command.executed',0,uuidv7(),10,decode(repeat('01',32),'hex') FROM generate_series(1,8192)g")
        .bind(material.0).execute(&mut conn).await.unwrap();
    let mut events = Vec::new();
    let mut expected = Vec::new();
    for i in 1..=64 {
        for _ in 0..2 {
            events.push(input(Some(&format!("key-{i}")), material, i, 1, 10));
        }
        events.push(input(None, material, i, 1, 10));
        expected.extend(["superseded", "repeat", "repeat"]);
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
        expected.extend(["new", "new"]);
    }
    stage(&mut conn, &events).await;
    conn.execute(
        "ANALYZE core.live_equivalence; ANALYZE core.live_locations; ANALYZE core.admission_stage",
    )
    .await
    .unwrap();
    let actual = sqlx::query_file!("src/admission/classification.sql")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        actual.iter().map(|r| r.ord).collect::<Vec<_>>(),
        (0..events.len() as i64).collect::<Vec<_>>()
    );
    assert_eq!(
        actual.iter().map(|r| r.state.as_str()).collect::<Vec<_>>(),
        expected
    );
    for row in &actual {
        assert_eq!(row.live_id.is_some(), row.state == "repeat", "{}", row.ord);
    }
}
