//! Compressed history is never changed in place: a newer reading, a hide, a
//! re-import and a replay over compressed chunks archive through the side
//! tables, so no statement decompresses a chunk. Only excision deletes rows.
use super::*;
use crate::query::{Page, run};
use sinex_model::query::{Context, bind};

/// `keyed`'s event with an ID minted `days` ago, so it lands in an old chunk.
fn old(key: u8, days: u64) -> Event {
    let mut e = keyed("s", key, 1, 1);
    let secs = 1_700_000_000 - days * 86_400;
    e.id = sinex_model::EventId(uuid::Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::NoContext,
        secs,
        u32::from(key),
    )));
    e
}

fn material_of(e: &Event) -> MaterialId {
    match &e.origin {
        Origin::Record { location, .. } => location.material,
        Origin::Derived { .. } => unreachable!("captured"),
    }
}

async fn total(conn: &mut PgConnection, text: &str) -> i64 {
    let ctx = Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let q = bind(text, &ctx).unwrap();
    run(conn, text, Some(&q), &ctx, &Page::default())
        .await
        .unwrap()
        .total
}

#[devkit::test]
async fn operations_on_compressed_history_decompress_nothing(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
    let history: Vec<Event> = (0..20).map(|k| old(k, 30)).collect();
    admit_one(&mut conn, &batch(history.clone())).await;
    let compressed: i64 =
        sqlx::query_scalar("SELECT count(compress_chunk(c)) FROM show_chunks('core.events') AS c")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(compressed, 1, "the old history is one compressed chunk");
    // Any statement that decompresses a compressed row now fails.
    sqlx::query("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 1")
        .execute(&mut conn)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT max(commit_seq) FROM core.commit_log")
        .fetch_one(&mut conn)
        .await
        .unwrap();

    // A newer reading of k3 replaces the compressed one.
    let newer = admit_one(&mut conn, &batch(vec![keyed("s", 3, 2, 2)])).await;
    assert_eq!(newer.replaced, 1);
    // Hide k5, then read it again: it stays hidden.
    let hide = Batch {
        archive: vec![ArchiveRequest::Hide {
            target: history[5].id,
            operation: OperationId::new(),
        }],
        ..batch(vec![])
    };
    admit_one(&mut conn, &hide).await;
    let again = admit_one(&mut conn, &batch(vec![keyed("s", 5, 1, 1)])).await;
    assert_eq!(again.hidden, 1);
    // Re-import of the whole history admits nothing new.
    let reimport: Vec<Event> = (0..20).map(|k| keyed("s", k, 1, 1)).collect();
    let reimport = admit_one(&mut conn, &batch(reimport)).await;
    assert_eq!(reimport.admitted, 0, "{reimport:?}");
    // A replay of k7's material archives its compressed reading.
    let operation = OperationId::new();
    let mut read_again = keyed("s", 7, 1, 1);
    read_again.origin = history[7].origin.clone();
    let replay = Batch {
        kind: BatchKind::Replay {operation,source:None,host:None},
        archive: vec![ArchiveRequest::Replay {
            scope: sinex_model::ReplayScope {
                materials: vec![material_of(&history[7])],
                ..Default::default()
            },
            operation,
        }],
        ..batch(vec![read_again])
    };
    let replayed = admit_one(&mut conn, &replay).await;
    assert_eq!((replayed.replay_archived, replayed.admitted), (1, 1));

    let archived: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT event_id FROM audit.archived")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(archived.len(), 3, "k3, k5 and k7's compressed readings");
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE id = ANY($1)")
        .bind(&archived)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(kept, 3, "archived readings stay in core.events");
    let still_compressed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' AND is_compressed",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(still_compressed, 1);

    assert_eq!(
        total(&mut conn, "events where payload.native_id = k3").await,
        1
    );
    assert_eq!(
        total(&mut conn, "events where payload.native_id = k5").await,
        0
    );
    assert_eq!(
        total(
            &mut conn,
            &format!("events where payload.native_id = k5 as of seq {before}")
        )
        .await,
        1,
        "read as of before the hide, k5 is there"
    );
    assert_eq!(
        total(
            &mut conn,
            &format!("events where payload.command = cmd-1 as of seq {before}")
        )
        .await,
        20
    );
}
