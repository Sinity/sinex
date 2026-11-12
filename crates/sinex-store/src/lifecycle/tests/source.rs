use super::*;
use futures::TryStreamExt;
use sqlx::Connection;

fn source_reading(source: &str, n: u64, prefix: &str) -> Event {
    let mut event = Event::build(
        &sinex_model::event_types::operator::LogEntry {
            written_at: None,
            label: None,
            text: format!("synthetic source {n}"),
        },
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: n,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&n.to_le_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap();
    event.id = sinex_model::EventId(Uuid::parse_str(&format!("{prefix}{n:012x}")).unwrap());
    event
}

async fn prepared_step(
    conn: &mut AdmissionContext,
    op: &Operation,
    progress: Progress,
    ids: Vec<Uuid>,
    complete: bool,
) -> (Progress, bool, i64) {
    let mut tx = conn.begin().await.unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events')) OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    select::source_batch(&mut tx, op, Phase::Archive, &ids)
        .await
        .unwrap();
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        after, before,
        "current membership must not reread immutable event headers"
    );
    let selected_count = ids.len();
    let Step::Moved { progress, done, .. } = step(
        &mut tx,
        op,
        progress,
        Roots::Source {
            operation: op.id,
            ids,
            complete,
        },
        &NoDerivations,
    )
    .await
    .unwrap() else {
        panic!("source batch refused")
    };
    let after_step: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let whole_work = after_step - after;
    eprintln!("compressed writer batch selected={selected_count} event_tuple_work={whole_work}");
    tx.commit().await.unwrap();
    (progress, done, whole_work)
}

#[devkit::test]
async fn compressed_source_stream_resumes_and_observes_restored_roots(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = context(&db).await;
    let roots: Vec<_> = (1..=96)
        .map(|n| source_reading("retired-source", n, "01800000-0000-7000-8000-"))
        .collect();
    let unrelated: Vec<_> = (1..=8192)
        .map(|n| source_reading("unrelated-source", n, "017f0000-0000-7000-8000-"))
        .collect();
    admit(&mut writer, &batch(unrelated)).await;
    admit(&mut writer, &batch(roots.clone())).await;
    let child = derived("other-child-source", 1, &[&roots[95]]);
    let child_id = child.id.0;
    admit(&mut writer, &batch(vec![child])).await;
    let archived = roots[64].id.0;
    assert!(matches!(
        operate(&mut writer, Act::Archive, events(&[&roots[64]]), 16).await,
        Ended::Ok { .. }
    ));
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)")
        .execute(&mut *writer)
        .await
        .unwrap();
    let op = begin(
        &mut writer,
        Act::Archive,
        "agent:test",
        Scope::Source {
            source: "retired-source".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    // This same-source reading is newer than the operation's fixed cut.
    let new = source_reading("retired-source", 100, "01800000-0000-7000-8000-");
    let new_id = new.id.0;
    admit(&mut writer, &batch(vec![new])).await;
    let mut read = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut stream = source_roots(&mut read, &op, None).unwrap();
    let mut first = Vec::new();
    for _ in 0..16 {
        first.push(stream.try_next().await.unwrap().unwrap());
    }
    let (saved, done, first_work) =
        prepared_step(&mut writer, &op, Progress::default(), first, false).await;
    assert!(!done);
    assert_eq!(saved.after, Some(roots[15].id.0));
    drop(stream);
    // Dropping an interrupted read leaves no separate task or saved selection.
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&mut read)
            .await
            .unwrap(),
        1
    );
    // Scale only unrelated compressed history between equally sized writer
    // batches. The selected roots retain the same existing chunk/layout.
    let more_unrelated: Vec<_> = (1..=8192)
        .map(|n| source_reading("unrelated-source", n, "017e0000-0000-7000-8000-"))
        .collect();
    let added_id = more_unrelated[0].id.0;
    admit(&mut writer, &batch(more_unrelated)).await;
    let added_chunk: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id=$1")
            .bind(added_id)
            .fetch_one(&mut *writer)
            .await
            .unwrap();
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(added_chunk)
        .execute(&mut *writer)
        .await
        .unwrap();
    let (reopened, mut progress) = stored(&mut writer, op.id).await.unwrap();
    assert_eq!(reopened.id, op.id);
    assert_eq!(reopened.start_seq, op.start_seq);
    assert_eq!(progress, saved);
    let mut read_tx = read.begin().await.unwrap();
    sqlx::query("SET LOCAL max_parallel_workers_per_gather=0")
        .execute(&mut *read_tx)
        .await
        .unwrap();
    let scans = "SELECT coalesce(sum(seq_scan+idx_scan),0)::bigint FROM pg_stat_xact_user_tables WHERE schemaname IN('core','_timescaledb_internal')";
    let before: i64 = sqlx::query_scalar(scans)
        .fetch_one(&mut *read_tx)
        .await
        .unwrap();
    let mut stream = source_roots(&mut read_tx, &reopened, progress.after).unwrap();
    let mut prepared = Vec::new();
    while let Some(id) = stream.try_next().await.unwrap() {
        prepared.push(id);
    }
    drop(stream);
    let after: i64 = sqlx::query_scalar(scans)
        .fetch_one(&mut *read_tx)
        .await
        .unwrap();
    let source_scans = after - before;
    assert_eq!(prepared.len(), 80);
    assert!(
        prepared.contains(&archived),
        "initially archived roots remain in immutable selection"
    );
    assert!(
        !prepared.contains(&new_id),
        "admissions after the cut stay outside the operation"
    );
    // Restore after the selection snapshot, before this root's writer batch.
    assert!(matches!(
        operate(&mut writer, Act::Restore, events(&[&roots[64]]), 16).await,
        Ended::Ok { .. }
    ));
    let erased = roots[80].id.0;
    let mut batches = 0;
    for chunk in prepared.chunks(16) {
        let complete = chunk.last() == prepared.last();
        let (next, done, work) =
            prepared_step(&mut writer, &reopened, progress, chunk.to_vec(), complete).await;
        if batches == 0 {
            eprintln!(
                "compressed writer work pair unrelated8192={first_work} unrelated16384={work}"
            );
            // Erasing after this pair keeps its selected chunk unchanged.
            // The later batch must still skip the disappeared membership.
            assert!(matches!(
                operate(&mut writer, Act::Archive, events(&[&roots[80]]), 16).await,
                Ended::Ok { .. }
            ));
            assert!(matches!(
                operate(&mut writer, Act::Tombstone, events(&[&roots[80]]), 16).await,
                Ended::Ok { .. }
            ));
        }
        progress = next;
        batches += 1;
        assert_eq!(done, complete);
    }
    assert_eq!(count(&mut writer,"SELECT count(*) FROM audit.archived WHERE event_id IN(SELECT id FROM core.events WHERE source='retired-source')").await,95);
    assert_eq!(
        count(
            &mut writer,
            &format!("SELECT count(*) FROM audit.archived WHERE event_id='{child_id}'")
        )
        .await,
        1
    );
    assert_eq!(count(&mut writer,"SELECT count(*) FROM core.live_locations l JOIN core.events e ON e.id=l.event_id WHERE e.source='unrelated-source'").await,16384);
    assert_eq!(
        count(
            &mut writer,
            &format!("SELECT count(*) FROM core.live_locations WHERE event_id='{new_id}'")
        )
        .await,
        1
    );
    // Positive control: the former per-batch query revisits compressed
    // relations for each page even when only the root IDs are needed.
    let before: i64 = sqlx::query_scalar(scans)
        .fetch_one(&mut *read_tx)
        .await
        .unwrap();
    for _ in 0..batches {
        select::roots(&mut read_tx, &reopened, Phase::Archive, saved.after, 8)
            .await
            .unwrap();
    }
    let after: i64 = sqlx::query_scalar(scans)
        .fetch_one(&mut *read_tx)
        .await
        .unwrap();
    assert!(source_scans > 0);
    assert!(
        after - before >= source_scans * 3,
        "old scans {}, one stream {source_scans}",
        after - before
    );
    read_tx.rollback().await.unwrap();
    assert_eq!(
        count(
            &mut writer,
            &format!("SELECT count(*) FROM core.event_tombstones WHERE event_id='{erased}'")
        )
        .await,
        1
    );
    assert_state_order(&mut writer).await;
}
