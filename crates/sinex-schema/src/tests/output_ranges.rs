use super::*;
use serde_json::{Value, json};
use sqlx::Executor;
use sqlx::types::Uuid;
use std::collections::BTreeSet;

/// The schema before output ranges were kept.
pub(super) async fn outputs_before_ranges(conn: &mut PgConnection) {
    conn.execute("DROP TABLE derivation.output_ranges")
        .await
        .unwrap();
}

/// 2026-01-01T00:00:00Z in microseconds.
const START: i64 = 1_767_225_600_000_000;
const MINUTE: i64 = 60_000_000;

fn time(micros: i64) -> Value {
    json!(sinex_model::Timestamp::from_unix_ns(i128::from(START + micros) * 1000).unwrap())
}

/// What a stored event of a derivation is besides its payload.
#[derive(Clone, Copy)]
struct Stored<'a> {
    derivation: &'a str,
    event_type: &'a str,
    /// The keys it is an output of.
    keys: &'a [&'a str],
    live: bool,
}

async fn store(conn: &mut PgConnection, stored: Stored<'_>, payload: Value) -> Uuid {
    let id = sinex_model::EventId::new().0;
    sqlx::query(
        "INSERT INTO core.events (id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash)
         VALUES ($1,$2,$3,$1,$1,0,'unknown',(SELECT count(*) + 1 FROM core.events),$4,1,decode(repeat('01',32),'hex'))",
    )
    .bind(id)
    .bind(stored.event_type)
    .bind(stored.derivation)
    .bind(payload)
    .execute(&mut *conn)
    .await
    .unwrap();
    if stored.live {
        sqlx::query(
            "INSERT INTO core.live_equivalence (source,event_type,equivalence_key,event_id,reading_order,record_hash)
             VALUES ($1,$2,uuid_send($3),$3,1,decode(repeat('01',32),'hex'))",
        )
        .bind(stored.derivation)
        .bind(stored.event_type)
        .bind(id)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
    for key in stored.keys {
        sqlx::query("INSERT INTO derivation.outputs (derivation,key,event_id) VALUES ($1,$2,$3)")
            .bind(stored.derivation)
            .bind(json!({ "key": key }))
            .bind(id)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    id
}

type Row = (String, String, Uuid, i64, Option<i64>);

/// The stored live outputs of every derivation that declares what its
/// outputs cover get their rows, one per key: with the end, the fence when
/// there is no end, or no end for an open output. An archived output, an
/// event that is no key's output, another type, another derivation's output
/// of the same type and an output with no readable start get none.
#[devkit::test]
async fn upgrade_fills_the_ranges_of_stored_live_outputs(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    outputs_before_ranges(&mut conn).await;
    conn.execute(
        "DELETE FROM sinex_schema.applied; INSERT INTO sinex_schema.applied(version) VALUES (52)",
    )
    .await
    .unwrap();

    let state = Stored {
        derivation: "state.intervals",
        event_type: "state.interval",
        keys: &["day"],
        live: true,
    };
    let mut expected: BTreeSet<Row> = BTreeSet::new();
    let mut expect = |stored: Stored<'_>, id: Uuid, ended: Option<i64>| {
        for key in stored.keys {
            expected.insert((
                stored.derivation.into(),
                (*key).to_owned(),
                id,
                START,
                ended.map(|micros| START + micros),
            ));
        }
    };
    let ended = json!({"started_at": time(0), "ended_at": time(MINUTE)});
    let id = store(&mut conn, state, ended.clone()).await;
    expect(state, id, Some(MINUTE));
    let id = store(&mut conn, state, json!({"started_at": time(0)})).await;
    expect(state, id, None);
    let fenced = json!({"started_at": time(0), "fenced_at": time(2 * MINUTE)});
    let id = store(&mut conn, state, fenced).await;
    expect(state, id, Some(2 * MINUTE));
    let both =
        json!({"started_at": time(0), "ended_at": time(MINUTE), "fenced_at": time(2 * MINUTE)});
    let id = store(&mut conn, state, both).await;
    expect(state, id, Some(MINUTE));
    // An unreadable end reads as none; an output that ends before it starts
    // keeps the bounds its payload records.
    let unreadable = json!({"started_at": time(0), "ended_at": "soon"});
    let id = store(&mut conn, state, unreadable).await;
    expect(state, id, None);
    let inverted = json!({"started_at": time(0), "ended_at": time(-MINUTE)});
    let id = store(&mut conn, state, inverted).await;
    expect(state, id, Some(-MINUTE));
    let two_keys = Stored {
        keys: &["day", "next"],
        ..state
    };
    let id = store(&mut conn, two_keys, ended.clone()).await;
    expect(two_keys, id, Some(MINUTE));
    for absent in [
        Stored {
            live: false,
            ..state
        },
        Stored { keys: &[], ..state },
        Stored {
            event_type: "state.other",
            ..state
        },
        Stored {
            derivation: "unit.intervals",
            ..state
        },
    ] {
        store(&mut conn, absent, ended.clone()).await;
    }
    store(&mut conn, state, json!({"started_at": "soon"})).await;
    store(&mut conn, state, json!({"ended_at": time(MINUTE)})).await;

    let day = Stored {
        derivation: "activity.days",
        event_type: "activity.day",
        ..state
    };
    let provisional = json!({"started_at": time(0), "ended_at": time(MINUTE), "provisional": true});
    let id = store(&mut conn, day, provisional).await;
    expect(day, id, None);
    let settled = json!({"started_at": time(0), "ended_at": time(MINUTE), "provisional": false});
    let id = store(&mut conn, day, settled).await;
    expect(day, id, Some(MINUTE));
    let episode = Stored {
        derivation: "work.episodes",
        event_type: "work.episode",
        ..state
    };
    let active =
        json!({"started_at": time(0), "last_activity": time(MINUTE), "ended_at": time(5 * MINUTE)});
    let id = store(&mut conn, episode, active).await;
    expect(episode, id, Some(MINUTE));
    for (derivation, event_type) in [
        ("activity.sessions", "activity.session"),
        ("analysis.products", "analysis.panel"),
    ] {
        let other = Stored {
            derivation,
            event_type,
            ..state
        };
        let id = store(&mut conn, other, ended.clone()).await;
        expect(other, id, Some(MINUTE));
    }
    conn.execute("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunk")
        .await
        .unwrap();

    assert!(matches!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 52,
            to: VERSION
        }
    ));
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let stored: Vec<Row> = sqlx::query_as(
        "SELECT derivation, key->>'key', event_id,
                (extract(epoch FROM started_at) * 1000000)::bigint,
                (extract(epoch FROM ended_at) * 1000000)::bigint
         FROM derivation.output_ranges",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(stored.into_iter().collect::<BTreeSet<_>>(), expected);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT has_table_privilege('sinex_reader','derivation.output_ranges','SELECT')"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
    );
}
