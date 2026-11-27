//! The stored outputs an input's range touches are found from the range each
//! live output covers, kept when it is admitted and dropped when it is
//! archived, independently of how much a derivation has stored.
use super::*;
use std::collections::BTreeSet;

const NAME: &str = "fixture.dirty_ranges";
const OTHER: &str = "fixture.other_ranges";
const KIND: &str = "synthetic.interval";

fn at(second: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + second).unwrap()
}

/// `NAME`'s outputs of `KIND` cover `start` to `end`, and stay open while
/// `open` is true; an input asks about 12 to 15, twice.
struct Ranges;
impl DirtyKeys for Ranges {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(String, Value)> {
        vec![]
    }
    fn output_ranges(&self) -> Vec<crate::OutputRange> {
        vec![crate::OutputRange {
            derivation: NAME,
            output_type: KIND,
            start_field: "start",
            end_field: "end",
            open_field: Some("open"),
            fence_field: Some("fence"),
        }]
    }
    fn affected_ranges(&self, input: &KeyInput<'_>) -> Vec<AffectedRange> {
        let (start, end) = match input.payload.get("asked") {
            Some(asked) => (
                at(asked[0].as_i64().unwrap()).0,
                at(asked[1].as_i64().unwrap()).0,
            ),
            None => (at(12).0, at(15).0),
        };
        let range = AffectedRange {
            derivation: NAME,
            output_type: KIND,
            start,
            end,
        };
        vec![range.clone(), range]
    }
}

fn event(kind: &str, payload: Value) -> Event {
    Event::submitted(
        sinex_model::KindRef {
            name: kind.into(),
            version: 1,
        },
        payload.clone(),
        "synthetic".into(),
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes(),
        Class::Open,
    )
}

/// A batch of `derivation` that computes each key to its outputs.
fn computed(derivation: &str, keys: Vec<(Value, Vec<Event>)>) -> Batch {
    let (scopes, records) = keys
        .into_iter()
        .map(|(key, events)| {
            (
                ComputedKey {
                    key,
                    dirty_seq: 0,
                    waiting: false,
                    refresh_at: None,
                    woken_by: None,
                },
                RecordOutcome::Events {
                    location: None,
                    events,
                },
            )
        })
        .unzip();
    Batch {
        run: RunId::new(),
        kind: BatchKind::Derived {
            derivation: derivation.into(),
            scope: DerivedScope::Keys {
                keys: scopes,
                read: ReadMark::default(),
                replaces_at_most: None,
            }
            .to_value(),
        },
        records,
        archive: vec![],
    }
}

async fn admit_computed(conn: &mut crate::AdmissionContext, batch: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let out = tx.admit(batch, &Ranges).await.unwrap();
    tx.commit().await.unwrap();
    out
}

async fn declare_kinds(conn: &mut crate::AdmissionContext) {
    let declarations: Vec<_> = [KIND, "synthetic.other_interval"]
        .into_iter()
        .map(|name| {
            sinex_model::KindDeclaration::schemaless(
                sinex_model::KindRef {
                    name: name.into(),
                    version: 1,
                },
                "synthetic".into(),
            )
        })
        .collect();
    sinex_schema::kinds::register(conn, "synthetic", &declarations)
        .await
        .unwrap();
}

/// The labels of `NAME`'s keys an input asking about `asked` marks dirty.
async fn dirtied(conn: &mut PgConnection, asked: Option<(i64, i64)>) -> BTreeSet<String> {
    let mut tx = conn.begin().await.unwrap();
    // Marks left by the fixture's own writes are not the input's.
    sqlx::query("DELETE FROM derivation.dirty")
        .execute(&mut *tx)
        .await
        .unwrap();
    let position = crate::query::current_seq(&mut tx).await.unwrap();
    let payload = match asked {
        Some((start, end)) => json!({"asked": [start, end]}),
        None => json!({}),
    };
    let marked = derived::mark_inputs_dirty(
        &mut tx,
        &[KeyInput {
            id: uuid::Uuid::nil(),
            event_type: "synthetic.trigger",
            ts_orig: None,
            payload: &payload,
        }],
        position,
        &Ranges,
    )
    .await
    .unwrap();
    let dirty: Vec<String> =
        sqlx::query_scalar("SELECT key->>'label' FROM derivation.dirty WHERE derivation=$1")
            .bind(NAME)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(marked, dirty.len(), "each affected key is offered once");
    dirty.into_iter().collect()
}

fn labels(labels: &[&str]) -> BTreeSet<String> {
    labels.iter().map(|label| (*label).to_owned()).collect()
}

#[devkit::test]
async fn dirty_output_ranges_mark_overlapping_keys_and_their_boundaries(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    declare_kinds(&mut conn).await;
    // Events of the kind that are no key's output never reach the lookup.
    let unrelated: Vec<_> = (0..64)
        .map(|n| event(KIND, json!({"n":n,"start":at(0),"end":at(30),"open":false})))
        .collect();
    admit_one(&mut conn, &batch(unrelated)).await;
    let mut keys: BTreeMap<&str, Vec<Event>> = BTreeMap::new();
    let mut archived = None;
    for (label, start, end, open) in [
        ("overlap", 10, Some(20), false),
        ("before", 0, Some(5), false),
        ("after", 30, Some(40), false),
        ("no_end", 11, None, false),
        ("open", 0, Some(1), true),
        ("closed", 0, Some(1), false),
        ("archived", 11, Some(13), false),
        ("wrong_kind", 11, Some(13), false),
        ("start_boundary", 15, Some(20), false),
        ("end_boundary", 0, Some(12), false),
        ("future_open", 20, Some(21), true),
        ("overlap", 11, Some(14), false),
    ] {
        let reading = event(
            if label == "wrong_kind" {
                "synthetic.other_interval"
            } else {
                KIND
            },
            json!({"label":label,"start":at(start),"end":end.map(at),"open":open}),
        );
        if label == "archived" {
            archived = Some(reading.id);
        }
        keys.entry(label).or_default().push(reading);
    }
    let outputs = keys
        .into_iter()
        .map(|(label, events)| (json!({"label": label}), events))
        .collect();
    admit_computed(&mut conn, &computed(NAME, outputs)).await;
    // Another derivation's output of the same kind covers nothing asked of this one.
    let other = event(
        KIND,
        json!({"label":"other_derivation","start":at(11),"end":at(13),"open":false}),
    );
    let other = vec![(json!({"label":"other_derivation"}), vec![other])];
    admit_computed(&mut conn, &computed(OTHER, other)).await;
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![archived.unwrap()],
        &NoDerivations,
    )
    .await;

    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        dirtied(&mut probe, None).await,
        labels(&[
            "overlap",
            "no_end",
            "open",
            "start_boundary",
            "end_boundary"
        ])
    );
}

/// What bounds an output with no end, and bounds recorded or asked in the
/// wrong order: a fence stands for a missing end, an end wins over a fence,
/// an unreadable end is no end, and an output or a question that ends before
/// it starts is compared by the bounds it states.
#[devkit::test]
async fn fences_and_reversed_bounds_keep_their_meaning(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    declare_kinds(&mut conn).await;
    let outputs = [
        ("fenced_before", json!({"start":at(0),"fence":at(5)})),
        ("fenced_across", json!({"start":at(0),"fence":at(12)})),
        (
            "ended_before_fence",
            json!({"start":at(0),"end":at(5),"fence":at(40)}),
        ),
        ("unreadable_end", json!({"start":at(0),"end":"soon"})),
        ("unreadable_start", json!({"start":"soon","end":at(40)})),
        ("reversed", json!({"start":at(14),"end":at(13)})),
        ("reversed_outside", json!({"start":at(40),"end":at(13)})),
    ]
    .into_iter()
    .map(|(label, mut payload)| {
        payload["label"] = json!(label);
        (json!({"label": label}), vec![event(KIND, payload)])
    })
    .collect();
    admit_computed(&mut conn, &computed(NAME, outputs)).await;
    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        dirtied(&mut probe, None).await,
        labels(&["fenced_across", "unreadable_end", "reversed"])
    );
    // Asked from 20 back to 10: an output must start by 10 and end at or
    // after 20, as the bounds state.
    assert_eq!(
        dirtied(&mut probe, Some((20, 10))).await,
        labels(&["unreadable_end"])
    );
    assert_eq!(
        dirtied(&mut probe, Some((60, 0))).await,
        labels(&["unreadable_end"])
    );
}

/// A range is kept per key and live output: written when the output joins a
/// key, copied when an output already stored joins another key, unchanged
/// when a recompute repeats it, and dropped for every key when the output is
/// archived.
#[devkit::test]
async fn ranges_follow_membership_and_liveness(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    declare_kinds(&mut conn).await;
    let output = event(KIND, json!({"label":"shared","start":at(10),"end":at(20)}));
    let rows = "SELECT key->>'label', extract(epoch FROM ended_at - started_at)::bigint
                FROM derivation.output_ranges WHERE derivation = $1 ORDER BY 1";
    let first = vec![(json!({"label":"first"}), vec![output.clone()])];
    assert_eq!(
        admit_computed(&mut conn, &computed(NAME, first.clone()))
            .await
            .admitted,
        1
    );
    let stored: Vec<(String, i64)> = sqlx::query_as(rows)
        .bind(NAME)
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    assert_eq!(stored, [("first".to_owned(), 10)]);

    // The same reading again, under its key and then under another.
    for label in ["first", "second"] {
        let again = vec![(json!({"label": label}), vec![output.clone()])];
        let repeated = admit_computed(&mut conn, &computed(NAME, again)).await;
        assert_eq!((repeated.admitted, repeated.repeats), (0, 1));
    }
    let stored: Vec<(String, i64)> = sqlx::query_as(rows)
        .bind(NAME)
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        stored,
        [("first".to_owned(), 10), ("second".to_owned(), 10)]
    );
    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        dirtied(&mut probe, None).await,
        labels(&["first", "second"])
    );

    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![output.id],
        &NoDerivations,
    )
    .await;
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM derivation.output_ranges")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(left, 0);
    assert!(dirtied(&mut probe, None).await.is_empty());
}

/// Rows the two lookup statements read from the range table, from their plans.
async fn rows_read(conn: &mut PgConnection) -> (f64, f64) {
    fn read(node: &Value) -> f64 {
        let own = if node["Relation Name"] == "output_ranges" {
            (node["Actual Rows"].as_f64().unwrap()
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Index Recheck"]
                    .as_f64()
                    .unwrap_or(0.0))
                * node["Actual Loops"].as_f64().unwrap()
        } else {
            0.0
        };
        own + node["Plans"]
            .as_array()
            .map_or(0.0, |plans| plans.iter().map(read).sum())
    }
    let overlapping: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF) {}",
        include_str!("../../output_ranges/affected_keys.sql")
    ))
    .bind(vec![NAME.to_owned()])
    .bind(vec![KIND.to_owned()])
    .bind(vec![at(1_000).0])
    .bind(vec![at(1_025).0])
    .persistent(false)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let open: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF) {}",
        include_str!("../../output_ranges/open_keys.sql")
    ))
    .bind(NAME)
    .bind(KIND)
    .bind(at(1_025).0)
    .persistent(false)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    (read(&overlapping[0]["Plan"]), read(&open[0]["Plan"]))
}

/// The lookup reads the ended outputs that overlap the asked range and one
/// open output per key that has any, the same number whether the derivation
/// has stored a few thousand outputs or sixteen times as many.
#[devkit::test]
async fn the_lookup_reads_what_overlaps_however_much_is_stored(db: devkit::pg::TestDatabase) {
    use sqlx::Executor;
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Output `n` is one of sixteen keys' outputs. The odd ones cover ten
    // seconds from second `10 n`, so three or fewer overlap any 25 seconds;
    // the even ones are open.
    let store = "INSERT INTO derivation.output_ranges (derivation, key, event_id, event_type, started_at, ended_at)
        SELECT $1, jsonb_build_object('label', n % 16), uuidv7(), $2,
               $3::timestamptz + n * interval '10 seconds',
               CASE WHEN n % 2 = 1 THEN $3::timestamptz + n * interval '10 seconds' + interval '10 seconds' END
        FROM generate_series($4::bigint, $5::bigint) n";
    let mut read = Vec::new();
    for (from, through) in [(1, 4_096), (4_097, 65_536)] {
        sqlx::query(store)
            .bind(NAME)
            .bind(KIND)
            .bind(at(0).0)
            .bind(from)
            .bind(through)
            .execute(&mut conn)
            .await
            .unwrap();
        conn.execute("VACUUM (ANALYZE) derivation.output_ranges")
            .await
            .unwrap();
        read.push(rows_read(&mut conn).await);
    }
    let (overlapping, open) = read[0];
    assert!(
        (1.0..=3.0).contains(&overlapping),
        "{overlapping} ended outputs read for 25 seconds"
    );
    assert!(
        (8.0..=9.0).contains(&open),
        "{open} open outputs read for the eight keys that have one"
    );
    assert_eq!(read[1], read[0], "sixteen times the stored outputs");

    let mut tx = conn.begin().await.unwrap();
    let payload = json!({"asked": [1_000, 1_025]});
    derived::mark_inputs_dirty(
        &mut tx,
        &[KeyInput {
            id: uuid::Uuid::nil(),
            event_type: "synthetic.trigger",
            ts_orig: None,
            payload: &payload,
        }],
        2,
        &Ranges,
    )
    .await
    .unwrap();
    let dirty: i64 =
        sqlx::query_scalar("SELECT count(*) FROM derivation.dirty WHERE derivation=$1")
            .bind(NAME)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    // The eight even keys by an open output that started by second 1025, and
    // the keys of outputs 99 and 101, which cover 990 to 1000 and 1010 to 1020.
    assert_eq!(dirty, 8 + 2);
    tx.rollback().await.unwrap();
}
