//! Admission laws (architecture §11): one live reading per location and key;
//! newer replaces, older suppresses; hidden stays hidden across re-admission;
//! events and progress commit together. Run against a real database.
use super::*;
use crate::testdb;
use proptest::prelude::*;
use serde_json::json;
use sinex_model::kinds::{CommandExecuted, CommandImported};
use sinex_model::{
    Class, InputRef, MaterialId, ObservationFacts, OperationId, Origin, ReadingOrder,
    RecordLocation, RunId,
};
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

/// The admission rules, written plainly, as the oracle.
fn model(batches: &[Vec<(u8, u8, i64)>]) -> BTreeMap<String, (u8, i64)> {
    let mut live: BTreeMap<String, (u8, i64)> = BTreeMap::new();
    for b in batches {
        let mut best: BTreeMap<u8, (usize, u8, i64)> = BTreeMap::new();
        for (i, (k, c, o)) in b.iter().enumerate() {
            let e = best.entry(*k).or_insert((i, *c, *o));
            if (*o, i) >= (e.2, e.0) {
                *e = (i, *c, *o);
            }
        }
        for (k, (_, c, o)) in best {
            let key = format!("k{k}");
            match live.get(&key) {
                None => {
                    live.insert(key, (c, o));
                }
                Some((lc, _)) if *lc == c => {}
                Some((_, lo)) if o > *lo => {
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

#[tokio::test]
async fn location_keyed_records_are_one_live_reading_and_never_replaced() {
    let (_db, mut conn) = testdb::fresh().await;
    let material = MaterialId::new();
    let at = |anchor: u64, command: &str, order: i64| {
        Event::build(
            &CommandImported {
                command: command.into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
            },
            "zsh",
            Origin::Record {
                location: RecordLocation { material, anchor },
                ordinal: 0,
            },
            ReadingOrder(order),
            *blake3::hash(command.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    let out = admit_one(&mut conn, &batch(vec![at(0, "a", 0), at(10, "a", 10)])).await;
    assert_eq!(
        out.admitted, 2,
        "same command at two locations is two records"
    );
    let again = admit_one(&mut conn, &batch(vec![at(0, "a", 0), at(0, "changed", 5)])).await;
    assert_eq!(again.admitted, 0);
    let live: i64 = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.live_coordinates"#)
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
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            header_elapsed: 0,
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
        "SELECT e.record_state, e.payload->>'command' FROM core.live_coordinates c
         JOIN core.events e ON e.id = c.event_id WHERE c.anchor = 2",
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
    let ids: Vec<_> = seqs.iter().map(|r| r.event_id).collect();
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
