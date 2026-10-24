//! Admission laws (architecture §11): one live reading per location and key;
//! newer replaces, older suppresses; hidden stays hidden across re-admission;
//! events and progress commit together. Run against a real database.
use super::*;
use crate::testdb;
use proptest::prelude::*;
use serde_json::json;
use sinex_model::event_types::{CommandExecuted, CommandImported};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, OperationId, Origin, ReadingOrder,
    RecordLocation, RunId,
};
use sinex_model::{RecordOutcome, RevisionChange, SourceProgress, Timestamp};
use sqlx::Connection;
use std::collections::BTreeMap;

fn keyed(source: &str, key: u8, content: u8, order: i64) -> Event {
    let command = format!("cmd-{content}");
    Event::build(
        &CommandExecuted {
            native_id: format!("k{key}"),
            command: command.clone(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000 + i64::from(key)).unwrap(),
            duration_ns: Some(1),
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(order),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

async fn admit_one(conn: &mut PgConnection, b: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let out = admit(&mut tx, b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    out
}

async fn live(conn: &mut PgConnection, source: &str) -> BTreeMap<String, String> {
    sqlx::query!(
        r#"SELECT e.payload->>'native_id' AS "k!", e.payload->>'command' AS "c!"
           FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id WHERE l.source = $1"#,
        source
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.k, r.c))
    .collect()
}

/// The content hash `keyed` gives a reading.
fn content_hash(c: u8) -> [u8; 32] {
    *blake3::hash(format!("cmd-{c}").as_bytes()).as_bytes()
}

/// The admission rules, written plainly, as the oracle: per key, the reading
/// with the greatest (reading order, content hash) wins, within a batch and
/// against the live reading; equal content is a repeat.
fn model(batches: &[Vec<(u8, u8, i64)>]) -> BTreeMap<String, (u8, i64)> {
    let mut live: BTreeMap<String, (u8, i64)> = BTreeMap::new();
    for b in batches {
        let mut best: BTreeMap<u8, (u8, i64)> = BTreeMap::new();
        for (k, c, o) in b {
            let e = best.entry(*k).or_insert((*c, *o));
            if (*o, content_hash(*c)) > (e.1, content_hash(e.0)) {
                *e = (*c, *o);
            }
        }
        for (k, (c, o)) in best {
            let key = format!("k{k}");
            match live.get(&key) {
                None => {
                    live.insert(key, (c, o));
                }
                Some((lc, _)) if *lc == c => {}
                Some((lc, lo)) if (o, content_hash(c)) > (*lo, content_hash(*lc)) => {
                    live.insert(key, (c, o));
                }
                Some(_) => {}
            }
        }
    }
    live
}

fn reading() -> impl Strategy<Value = (u8, u8, i64)> {
    (0u8..4, 0u8..3, 0i64..8)
}

#[tokio::test(flavor = "multi_thread")]
async fn newer_replaces_older_suppresses_one_live_per_key() {
    let (_db, conn) = testdb::fresh().await;
    let conn = tokio::sync::Mutex::new(conn);
    let cases = std::sync::Mutex::new(0u32);
    let rt = tokio::runtime::Handle::current();
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 48,
        ..Default::default()
    });
    runner
        .run(
            &prop::collection::vec(prop::collection::vec(reading(), 1..6), 1..5),
            |batches| {
                let n = {
                    let mut g = cases.lock().unwrap();
                    *g += 1;
                    *g
                };
                let source = format!("law-{n}");
                let got = tokio::task::block_in_place(|| {
                    rt.block_on(async {
                        let mut conn = conn.lock().await;
                        for b in &batches {
                            let events = b
                                .iter()
                                .map(|(k, c, o)| keyed(&source, *k, *c, *o))
                                .collect();
                            admit_one(&mut conn, &batch(events)).await;
                        }
                        live(&mut conn, &source).await
                    })
                });
                let want: BTreeMap<String, String> = model(&batches)
                    .into_iter()
                    .map(|(k, (c, _))| (k, format!("cmd-{c}")))
                    .collect();
                prop_assert_eq!(got, want);
                Ok(())
            },
        )
        .unwrap();
}

/// Ties on reading order are decided by content, never by arrival: the same
/// readings admitted one batch each in either order, or together in either
/// order, leave the same live readings (sxnext-9zw.24).
#[tokio::test]
async fn equal_reading_orders_are_decided_by_content_not_arrival() {
    let (_db, mut conn) = testdb::fresh().await;
    let readings = [(0u8, 1u8, 5i64), (0, 2, 5), (1, 0, 3), (1, 2, 3), (1, 1, 3)];
    let mut outcomes = Vec::new();
    for (n, plan) in [
        "separate",
        "separate-reversed",
        "together",
        "together-reversed",
    ]
    .iter()
    .enumerate()
    {
        let source = format!("tie-{n}");
        let mut ordered: Vec<_> = readings.to_vec();
        if plan.ends_with("reversed") {
            ordered.reverse();
        }
        let events: Vec<Event> = ordered
            .iter()
            .map(|(k, c, o)| keyed(&source, *k, *c, *o))
            .collect();
        if plan.starts_with("separate") {
            for e in events {
                admit_one(&mut conn, &batch(vec![e])).await;
            }
        } else {
            admit_one(&mut conn, &batch(events)).await;
        }
        outcomes.push(live(&mut conn, &source).await);
    }
    assert!(
        outcomes.windows(2).all(|w| w[0] == w[1]),
        "arrival decided a tie: {outcomes:#?}"
    );
    let want: BTreeMap<String, String> = model(&[readings.to_vec()])
        .into_iter()
        .map(|(k, (c, _))| (k, format!("cmd-{c}")))
        .collect();
    assert_eq!(outcomes[0], want);
}

#[tokio::test]
async fn replaced_readings_are_archived_with_their_successor() {
    let (_db, mut conn) = testdb::fresh().await;
    let first = keyed("s", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![first.clone()])).await;
    let second = keyed("s", 1, 2, 2);
    let out = admit_one(&mut conn, &batch(vec![second.clone()])).await;
    assert_eq!((out.admitted, out.replaced), (1, 1));
    let archived = sqlx::query!(
        "SELECT reason, replaced_by FROM audit.archived WHERE event_id = $1",
        first.id.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(archived.reason, "newer_reading");
    assert_eq!(archived.replaced_by, Some(second.id.0));
    let repeat = admit_one(&mut conn, &batch(vec![keyed("s", 1, 2, 3)])).await;
    assert_eq!((repeat.admitted, repeat.repeats), (0, 1));
    let older = admit_one(&mut conn, &batch(vec![keyed("s", 1, 3, 0)])).await;
    assert_eq!((older.admitted, older.older), (0, 1));
}

fn watched(title: Option<&str>, order: i64) -> Event {
    let payload = sinex_model::event_types::account::VideoWatched {
        service: "youtube".into(),
        video_id: Some("v1".into()),
        watched_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
        url: Some("https://www.youtube.com/watch?v=v1".into()),
        title: title.map(str::to_owned),
        channel: title.map(|_| "Chan".to_owned()),
        channel_url: None,
        products: vec![],
        details: vec![order.to_string()],
    };
    Event::build(
        &payload,
        "youtube",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(order),
        *blake3::hash(format!("{title:?}{order}").as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

#[tokio::test]
async fn a_newer_reading_keeps_known_fields_it_leaves_unknown() {
    let (_db, mut conn) = testdb::fresh().await;
    admit_one(&mut conn, &batch(vec![watched(Some("A talk"), 1)])).await;
    // The later export dropped the title and channel of a deleted video.
    let out = admit_one(&mut conn, &batch(vec![watched(None, 2)])).await;
    assert_eq!((out.admitted, out.replaced), (1, 1));
    let live = sqlx::query!(
        r#"SELECT e.payload->>'title' AS title, e.payload->>'channel' AS channel,
                  e.payload->'details'->>0 AS details
           FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
           WHERE l.source = 'youtube'"#
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(live.title.as_deref(), Some("A talk"));
    assert_eq!(live.channel.as_deref(), Some("Chan"));
    assert_eq!(
        live.details.as_deref(),
        Some("2"),
        "the rest is the newer reading"
    );
}

#[tokio::test]
async fn location_keyed_records_are_one_live_reading_and_never_replaced() {
    let (_db, mut conn) = testdb::fresh().await;
    let material = MaterialId::new();
    let at = |anchor: u64, change: &str, order: i64| {
        Event::build(
            &sinex_model::event_types::machine::FileChanged {
                received_at: Timestamp::from_unix_secs(1).unwrap(),
                change: change.into(),
                path: Some("/p".into()),
                path_hex: None,
                to: None,
                is_dir: None,
                note: None,
            },
            "file-watch",
            Origin::Record {
                location: RecordLocation { material, anchor },
                ordinal: 0,
            },
            ReadingOrder(order),
            *blake3::hash(change.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    let out = admit_one(&mut conn, &batch(vec![at(0, "a", 0), at(10, "a", 10)])).await;
    assert_eq!(
        out.admitted, 2,
        "the same change at two locations is two records"
    );
    let again = admit_one(&mut conn, &batch(vec![at(0, "a", 0), at(0, "changed", 5)])).await;
    assert_eq!(again.admitted, 0);
    let live: i64 = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.live_locations"#)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(live, 2);
}

#[tokio::test]
async fn hidden_stays_hidden_across_readmission_until_unhidden() {
    let (_db, mut conn) = testdb::fresh().await;
    let e = keyed("s", 7, 1, 1);
    admit_one(&mut conn, &batch(vec![e.clone()])).await;
    let mut hide = batch(vec![]);
    hide.archive.push(ArchiveRequest::Hide {
        target: e.id,
        operation: OperationId::new(),
    });
    admit_one(&mut conn, &hide).await;
    assert!(live(&mut conn, "s").await.is_empty());
    for (content, order) in [(1, 1), (2, 5), (3, 9)] {
        let out = admit_one(&mut conn, &batch(vec![keyed("s", 7, content, order)])).await;
        assert_eq!((out.admitted, out.hidden), (0, 1));
    }
    let mut unhide = batch(vec![]);
    unhide.archive.push(ArchiveRequest::Unhide {
        target: e.id,
        operation: OperationId::new(),
    });
    admit_one(&mut conn, &unhide).await;
    assert_eq!(
        live(&mut conn, "s").await.get("k7").map(String::as_str),
        Some("cmd-1")
    );
}

#[tokio::test]
async fn a_dropped_record_gets_a_removed_reading_once() {
    let (_db, mut conn) = testdb::fresh().await;
    let location = RecordLocation {
        material: MaterialId::new(),
        anchor: 2,
    };
    let read = Event::build(
        &CommandImported {
            command: "b".into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000).unwrap()),
            header_elapsed: Some(0),
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location,
            ordinal: 0,
        },
        ReadingOrder(2),
        *blake3::hash(b"b").as_bytes(),
        Class::Open,
    )
    .unwrap();
    admit_one(&mut conn, &batch(vec![read.clone()])).await;

    let mut dropped = batch(vec![]);
    dropped.archive.push(ArchiveRequest::RemovedAtSource {
        locations: vec![location],
    });
    let out = admit_one(&mut conn, &dropped).await;
    assert_eq!(out.replaced, 1, "{out:?}");
    let state: (String, String) = sqlx::query_as(
        "SELECT e.record_state, e.payload->>'command' FROM core.live_equivalence q
         JOIN core.events e ON e.id = q.event_id WHERE e.anchor = 2",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(state, ("removed".into(), "b".into()));
    let archived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit.archived WHERE event_id = $1")
            .bind(read.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(archived, 1, "the earlier reading is archived, not deleted");

    // Reported again: already removed, nothing new.
    let again = admit_one(&mut conn, &dropped).await;
    assert_eq!((again.admitted, again.replaced), (0, 0));
}

#[tokio::test]
async fn events_and_progress_commit_together() {
    let (_db, mut conn) = testdb::fresh().await;
    let progress = |cursor: i64| SourceProgress {
        erased_replay: None,
        excision_basis: None,
        image: None,
        image_revision: None,
        material_keys: Vec::new(),
        binding: "b".into(),
        input: InputRef {
            kind: "file".into(),
            locator: "/x".into(),
            host: "h".into(),
        },
        observation: ObservationFacts {
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Appended,
        material: None,
        cursor: json!(cursor),
        carry: json!({}),
    };
    let mut good = batch(vec![keyed("s", 1, 1, 1)]);
    good.kind = BatchKind::Source(Box::new(progress(10)));
    admit_one(&mut conn, &good).await;

    // A payload the database refuses (NUL inside jsonb) fails the whole batch:
    // neither its events nor its cursor are stored.
    let mut bad_event = keyed("s", 2, 1, 1);
    bad_event.payload["command"] = json!("a\u{0}b");
    let mut bad = batch(vec![keyed("s", 3, 1, 1), bad_event]);
    bad.kind = BatchKind::Source(Box::new(progress(20)));
    let mut tx = conn.begin().await.unwrap();
    assert!(admit(&mut tx, &bad, &NoDerivations).await.is_err());
    tx.rollback().await.unwrap();

    let cursor = crate::positions::load(&mut conn, "b")
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(cursor, json!(10));
    assert_eq!(live(&mut conn, "s").await.len(), 1);
}

#[tokio::test]
async fn commit_seq_is_dense_and_in_batch_order() {
    let (_db, mut conn) = testdb::fresh().await;
    let events: Vec<Event> = (0..5).map(|k| keyed("s", k, 1, 1)).collect();
    let out = admit_one(&mut conn, &batch(events.clone())).await;
    assert_eq!(out.commit_seq, Some((1, 5)));
    let seqs = sqlx::query!("SELECT event_id, commit_seq FROM core.commit_log ORDER BY commit_seq")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let ids: Vec<_> = seqs
        .iter()
        .map(|r| r.event_id.expect("admitted event entry"))
        .collect();
    assert_eq!(ids, events.iter().map(|e| e.id.0).collect::<Vec<_>>());
}

struct ByDay;
impl DirtyKeys for ByDay {
    fn keys(&self, e: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        e.ts_orig
            .map(|t| vec![("daily", json!(t.date().to_string()))])
            .unwrap_or_default()
    }
}

#[tokio::test]
async fn admitted_and_archived_readings_mark_dirty_keys() {
    let (_db, mut conn) = testdb::fresh().await;
    let mut tx = conn.begin().await.unwrap();
    let out = admit(
        &mut tx,
        &batch(vec![keyed("s", 1, 1, 1), keyed("s", 2, 1, 1)]),
        &ByDay,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(out.dirty_keys, 2);
    let n: i64 = sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM derivation.dirty WHERE derivation = 'daily'"#
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(n, 1, "both events fall on the same day");
}

#[tokio::test]
async fn dirty_keys_keep_each_derivation_and_advance_existing_keys() {
    let (_db, mut conn) = testdb::fresh().await;
    let first = json!({"day": "2026-01-01"});
    let second = json!({"day": "2026-01-02"});
    let mut tx = conn.begin().await.unwrap();
    let offered = derived::mark_dirty(
        &mut tx,
        vec![
            ("daily", first.clone()),
            ("daily", first.clone()),
            ("hourly", first.clone()),
            ("daily", second.clone()),
        ],
        3,
    )
    .await
    .unwrap();
    assert_eq!(offered, 4);
    let offered = derived::mark_dirty(
        &mut tx,
        vec![("daily", first.clone()), ("daily", first.clone())],
        7,
    )
    .await
    .unwrap();
    assert_eq!(offered, 2);
    let rows = sqlx::query!(
        "SELECT derivation, key, first_dirty_seq, last_dirty_seq FROM derivation.dirty ORDER BY derivation, key"
    )
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    let actual: Vec<_> = rows
        .into_iter()
        .map(|r| (r.derivation, r.key, r.first_dirty_seq, r.last_dirty_seq))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("daily".to_owned(), first.clone(), 3, 7),
            ("daily".to_owned(), second, 3, 3),
            ("hourly".to_owned(), first, 3, 3),
        ]
    );
    tx.rollback().await.unwrap();
    let count = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM derivation.dirty"#)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

mod privacy;

#[tokio::test]
async fn untimed_zsh_occurrences_coexist_and_only_same_locations_repeat() {
    let (_db, mut conn) = testdb::fresh().await;
    let first = MaterialId::new();
    let second = MaterialId::new();
    let make = |material, anchor, timed: bool| {
        Event::build(
            &CommandImported {
                command: "pwd".into(),
                hostname: "h".into(),
                started_at: timed.then(|| Timestamp::from_unix_secs(10).unwrap()),
                header_elapsed: timed.then_some(0),
                n: 0,
            },
            "zsh",
            Origin::Record {
                location: RecordLocation { material, anchor },
                ordinal: 0,
            },
            ReadingOrder(anchor as i64),
            [1; 32],
            Class::Open,
        )
        .unwrap()
    };
    let events = vec![
        make(first, 0, false),
        make(first, 4, false),
        make(second, 0, false),
        make(first, 8, true),
    ];
    assert!(
        events[..3]
            .iter()
            .all(|e| e.ts_orig.at.is_none() && e.equivalence_key.is_none())
    );
    assert!(events[3].equivalence_key.is_some());
    assert_eq!(admit_one(&mut conn, &batch(events)).await.admitted, 4);
    let repeated = admit_one(
        &mut conn,
        &batch(vec![
            make(first, 0, false),
            make(second, 0, false),
            make(second, 4, true),
        ]),
    )
    .await;
    assert_eq!(repeated.admitted, 0);
    assert_eq!(repeated.repeats, 3);
    let live: i64 = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.live_locations"#)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(live, 3);
    let timed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.live_equivalence WHERE source = 'zsh'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(timed, 1);
}

mod archive_history;

mod erased_replay;
