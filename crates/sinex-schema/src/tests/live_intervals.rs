use super::*;
use serde_json::{Value, json};
use sinex_model::clock::{INTERVAL_KINDS, IntervalEnd};
use sqlx::Executor;
use sqlx::types::Uuid;
use std::collections::BTreeSet;

/// The schema before live intervals were kept.
pub(super) async fn events_before_live_intervals(conn: &mut PgConnection) {
    conn.execute(
        "DROP TABLE core.live_intervals;
         DROP FUNCTION core.interval_end(text, timestamptz, jsonb)",
    )
    .await
    .unwrap();
}

/// 2026-01-01T00:00:00Z in microseconds.
const START: i64 = 1_767_225_600_000_000;
const MINUTE: i64 = 60_000_000;

/// The payload of an event that ends `micros` after it starts.
fn lasting(end: IntervalEnd, micros: i64) -> Value {
    match end {
        IntervalEnd::AfterNs(field) => json!({ field: micros * 1000 }),
        IntervalEnd::At(field) | IntervalEnd::EndOrInstant(field) => json!({
            field: sinex_model::Timestamp::from_unix_ns(i128::from(START + micros) * 1000).unwrap()
        }),
    }
}

/// Store an event starting at `START`, or at no known time.
async fn store(conn: &mut PgConnection, event_type: &str, payload: Value, known: bool) -> Uuid {
    let id = sinex_model::EventId::new().0;
    sqlx::query(
        "INSERT INTO core.events (id,event_type,source,run_id,material_id,record_offset,ts_orig,ts_quality,commit_seq,payload,reading_order,record_hash)
         VALUES ($1,$2,'fixture',$1,$1,0,CASE WHEN $3 THEN to_timestamp($4::double precision / 1000000) END,
                 CASE WHEN $3 THEN 'intrinsic' ELSE 'unknown' END,
                 (SELECT count(*) + 1 FROM core.events),$5,1,decode(repeat('01',32),'hex'))",
    )
    .bind(id)
    .bind(event_type)
    .bind(known)
    .bind(START)
    .bind(payload)
    .execute(conn)
    .await
    .unwrap();
    id
}

type Row = (Uuid, String, Option<String>, i64, Option<i64>);

/// Every interval kind's stored events get their rows: the live ones that
/// last or are open, with the end the window read used to compute from the
/// payload, and none for an instant, an archived reading or an unknown time.
#[devkit::test]
async fn upgrade_fills_live_intervals_of_every_interval_kind(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    events_before_live_intervals(&mut conn).await;
    conn.execute(
        "DELETE FROM sinex_schema.applied; INSERT INTO sinex_schema.applied(version) VALUES (49)",
    )
    .await
    .unwrap();

    let mut expected: BTreeSet<Row> = BTreeSet::new();
    let mut archived = Vec::new();
    for kind in INTERVAL_KINDS {
        let open_when_absent = matches!(kind.end, IntervalEnd::At(_));
        let event_type = kind.event_type;
        let narrowing = kind.narrowed_by.map(|_| "chosen".to_owned());
        let narrowed = |mut payload: Value| {
            if let Some(field) = kind.narrowed_by {
                payload[field] = json!("chosen");
            }
            payload
        };
        let id = store(
            &mut conn,
            event_type,
            narrowed(lasting(kind.end, MINUTE)),
            true,
        )
        .await;
        expected.insert((
            id,
            event_type.into(),
            narrowing.clone(),
            START,
            Some(START + MINUTE),
        ));
        let id = store(&mut conn, event_type, narrowed(json!({})), true).await;
        if open_when_absent {
            expected.insert((id, event_type.into(), narrowing, START, None));
        }
        // A null, an unreadable value and a negative duration never raise:
        // they read as no end.
        for unusable in [json!(null), json!("soon"), json!(-5), json!({"at": 1})] {
            let payload = json!({ kind.end.field(): unusable });
            let id = store(&mut conn, event_type, payload, true).await;
            if open_when_absent {
                expected.insert((id, event_type.into(), None, START, None));
            }
        }
        store(&mut conn, event_type, lasting(kind.end, -MINUTE), true).await;
        archived.push(store(&mut conn, event_type, lasting(kind.end, MINUTE), true).await);
        store(&mut conn, event_type, lasting(kind.end, MINUTE), false).await;
    }
    // A duration is whole microseconds, rounded up.
    let payload = json!({"duration_ns": 1500});
    let id = store(&mut conn, "command.executed", payload, true).await;
    expected.insert((id, "command.executed".into(), None, START, Some(START + 2)));
    let unrelated = lasting(IntervalEnd::At("ended_at"), MINUTE);
    store(&mut conn, "fixture.unrelated", unrelated, true).await;
    sqlx::query(
        "INSERT INTO audit.archived(event_id,archived_seq,reason) SELECT id,1,'archive' FROM unnest($1::uuid[]) id",
    )
    .bind(&archived)
    .execute(&mut conn)
    .await
    .unwrap();
    conn.execute("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunk")
        .await
        .unwrap();

    assert!(matches!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 49,
            to: VERSION
        }
    ));
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let stored: Vec<Row> = sqlx::query_as(
        "SELECT event_id, event_type, narrowing,
                (extract(epoch FROM started_at) * 1000000)::bigint,
                (extract(epoch FROM ended_at) * 1000000)::bigint
         FROM core.live_intervals",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(stored.into_iter().collect::<BTreeSet<_>>(), expected);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT has_table_privilege('sinex_reader','core.live_intervals','SELECT')"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
    );
}
