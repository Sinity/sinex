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
) -> (Progress, bool) {
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
    tx.commit().await.unwrap();
    (progress, done)
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
    let child = derived("other-child-source", 1, &[&roots[0]]);
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
    let mut read = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut stream = source_roots(&mut read, &op, None).unwrap();
    let mut first = Vec::new();
    for _ in 0..16 {
        first.push(stream.try_next().await.unwrap().unwrap());
    }
    let (saved, done) = prepared_step(&mut writer, &op, Progress::default(), first, false).await;
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
    let (reopened, mut progress) = stored(&mut writer, op.id).await.unwrap();
    assert_eq!(reopened.id, op.id);
    assert_eq!(reopened.start_seq, op.start_seq);
    assert_eq!(progress, saved);
    // This same-source reading is newer than the operation's fixed cut.
    let new = source_reading("retired-source", 100, "01800000-0000-7000-8000-");
    let new_id = new.id.0;
    admit(&mut writer, &batch(vec![new])).await;
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
    // Erasure after selection removes current membership; the source batch
    // must skip it without trying to reconstruct its old immutable headers.
    let erased = roots[80].id.0;
    assert!(matches!(
        operate(&mut writer, Act::Archive, events(&[&roots[80]]), 16).await,
        Ended::Ok { .. }
    ));
    assert!(matches!(
        operate(&mut writer, Act::Tombstone, events(&[&roots[80]]), 16).await,
        Ended::Ok { .. }
    ));
    let mut batches = 0;
    for chunk in prepared.chunks(8) {
        let complete = chunk.last() == prepared.last();
        let (next, done) =
            prepared_step(&mut writer, &reopened, progress, chunk.to_vec(), complete).await;
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
    assert_eq!(count(&mut writer,"SELECT count(*) FROM core.live_locations l JOIN core.events e ON e.id=l.event_id WHERE e.source='unrelated-source'").await,8192);
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

#[devkit::test]
async fn archive_selected_chain_does_not_scan_unrelated_archived_edges(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = context(&db).await;
    let parent = source_reading("history-parent", 1, "017f0000-0000-7000-8000-");
    admit(&mut writer, &batch(vec![parent.clone()])).await;
    let mut history: Vec<_> = (1..=8192)
        .map(|n| {
            let mut e = source_reading("history", n, "017f0001-0000-7000-8000-");
            e.origin = Origin::Derived {
                parents: vec![parent.id],
            };
            e
        })
        .collect();
    let history_ids: Vec<_> = history.iter().map(|e| e.id.0).collect();
    admit(&mut writer, &batch(std::mem::take(&mut history))).await;
    let seq = position(&mut writer).await.unwrap();
    archive_at(
        &mut writer,
        &history_ids,
        seq,
        ArchiveReason::Archive,
        None,
        None,
    )
    .await
    .unwrap();
    remove_live(&mut writer, &history_ids).await.unwrap();
    let root = source_reading("selected-source", 1, "01800000-0000-7000-8000-");
    admit(&mut writer, &batch(vec![root.clone()])).await;
    let child = derived("selected-child", 1, &[&root]);
    let child_id = child.id.0;
    admit(&mut writer, &batch(vec![child])).await;
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)")
        .execute(&mut *writer)
        .await
        .unwrap();
    let op = begin(
        &mut writer,
        Act::Archive,
        "agent:test",
        Scope::Source {
            source: "selected-source".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let mut tx = writer.begin().await.unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid IN ('audit.archived'::regclass,'core.event_children'::regclass)";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let Step::Moved { counts, .. } = step(
        &mut tx,
        &op,
        Progress::default(),
        Roots::Source {
            operation: op.id,
            ids: vec![root.id.0],
            complete: true,
        },
        &NoDerivations,
    )
    .await
    .unwrap() else {
        panic!("archive refused")
    };
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before <= 16,
        "selected chain visited {} archive/edge tuples",
        after - before
    );
    assert_eq!(counts.archived.values().sum::<u64>(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT events FROM core.source_counts WHERE source='selected-child'"
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap(),
        0
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)"
        )
        .bind(child_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap()
    );
    // The old final anti-join reads unrelated archive history when recursive
    // cardinality estimates select a hash plan, as on compressed deployments.
    sqlx::query("SET LOCAL enable_nestloop=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    sqlx::query("SELECT d.id FROM unnest($1::uuid[]) d(id) WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=d.id)")
        .bind(vec![root.id.0,child_id]).fetch_all(&mut *tx).await.unwrap();
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before >= 8192,
        "old anti-join control visited {} tuples",
        after - before
    );
    tx.commit().await.unwrap();
    assert_state_order(&mut writer).await;
}
