use super::*;
use futures::TryStreamExt;
use sinex_model::lifecycle::{Act, Scope};
use uuid::Uuid;

fn events(source: &str, count: usize, prefix: &str) -> Vec<Event> {
    (1..=count)
        .map(|n| {
            let mut b = batch(&format!("synthetic-{source}-{n}"));
            let RecordOutcome::Events { events, .. } = &mut b.records[0] else {
                unreachable!()
            };
            let mut e = events.pop().unwrap();
            e.source = source.into();
            e.id = sinex_model::EventId(Uuid::parse_str(&format!("{prefix}{n:012x}")).unwrap());
            e
        })
        .collect()
}

fn with_events(events: Vec<Event>) -> Batch {
    let mut b = batch("synthetic unused");
    b.records = vec![RecordOutcome::Events {
        location: None,
        events,
    }];
    b
}

async fn layout(conn: &mut AdmissionContext, chunk: &str, ids: &[Uuid], cut: i64) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('status',c.status,'rowstore_bytes',pg_relation_size(c.relid),
      'compressed_bytes',pg_relation_size(s.compress_relid),
      'rows',(SELECT count(*) FROM core.events WHERE id=ANY($2)),
      'headers',(SELECT md5(string_agg(id::text||':'||(commit_seq-$3)::text||':'||source,',' ORDER BY id)) FROM core.events WHERE id=ANY($2)))
      FROM _timescaledb_catalog.chunk c JOIN _timescaledb_catalog.compression_settings s ON s.relid=c.relid WHERE c.relid=$1::regclass")
      .bind(chunk).bind(ids).bind(cut).fetch_one(&mut **conn).await.unwrap()
}

async fn work(conn: &mut AdmissionContext) -> i64 {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&mut **conn)
        .await
        .unwrap();
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(&mut **conn)
        .await
        .unwrap();
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_user_tables
      WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events'))
      OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))")
      .fetch_one(&mut **conn).await.unwrap()
}

async fn arm(db: &devkit::pg::TestDatabase, unrelated: usize) -> (i64, Value) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let keys = sinex_store::NoDerivations;
    let check = || Ok(());
    admit_checked(
        &mut conn,
        &with_events(events("unrelated", unrelated, "017f0000-0000-7000-8000-")),
        &keys,
        None,
        &check,
        false,
    )
    .await
    .unwrap();
    let selected = events("selected", 16, "01800000-0000-7000-8000-");
    let ids: Vec<_> = selected.iter().map(|e| e.id.0).collect();
    admit_checked(
        &mut conn,
        &with_events(selected),
        &keys,
        None,
        &check,
        false,
    )
    .await
    .unwrap();
    let chunk: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id=$1")
            .bind(ids[0])
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)")
        .execute(&mut *conn)
        .await
        .unwrap();
    // Both independent arms use the same stable production generic-plan mode.
    sqlx::query("SET plan_cache_mode=force_generic_plan")
        .execute(&mut *conn)
        .await
        .unwrap();
    let op = lifecycle::begin(
        &mut conn,
        Act::Archive,
        "sinex",
        Scope::Source {
            source: "selected".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let prepared: Vec<_> = lifecycle::source_roots(&mut conn, &op, None)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(prepared, ids);
    let before_layout = layout(&mut conn, &chunk, &ids, op.start_seq).await;
    assert_eq!(
        before_layout["status"], 1,
        "selected input must be fully compressed"
    );
    let before = work(&mut conn).await;
    let finished = super::super::lifecycle_step(
        &mut conn,
        &op,
        Progress::default(),
        lifecycle::Roots::Source {
            operation: op.id,
            ids: prepared,
            complete: true,
        },
        &keys,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(finished.step, Step::Moved { done: true, .. }));
    let after = work(&mut conn).await;
    eprintln!(
        "isolated actual writer arm unrelated={unrelated} selected16 tuple_work={}",
        after - before
    );
    std::io::Write::flush(&mut std::io::stderr()).unwrap();
    let after_layout = layout(&mut conn, &chunk, &ids, op.start_seq).await;
    assert_eq!(
        after_layout, before_layout,
        "archive changed selected physical data"
    );
    let archived: i64 = sqlx::query_scalar("SELECT count(*) FROM audit.archived")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(archived, 16);
    let kept: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM core.live_locations)+(SELECT count(*) FROM core.live_equivalence)")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(kept, unrelated as i64);
    (after - before, before_layout)
}

#[devkit::test]
async fn compressed_archive_writer_work_with_independent_unrelated_history(
    db: devkit::pg::TestDatabase,
) {
    let (small, small_layout) = arm(&db, 8192).await;
    let other = devkit::pg::test_database().unwrap();
    let (large, large_layout) = arm(&other, 16384).await;
    eprintln!(
        "isolated actual writer totals unrelated8192={small} unrelated16384={large} selected16"
    );
    std::io::Write::flush(&mut std::io::stderr()).unwrap();
    assert_eq!(
        small_layout, large_layout,
        "independent selected inputs differ"
    );
    eprintln!(
        "isolated actual writer work unrelated8192={small} unrelated16384={large} selected16 layout={small_layout}"
    );
}
