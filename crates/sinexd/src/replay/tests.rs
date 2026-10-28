//! Replay through the real writer and the zsh driver, on a real database.
use super::*;
use crate::reader_context::ReaderContext;
use crate::sealing::Sink;
use crate::writer::Priority;
use sinex_source_core::drivers::append_file::AppendState;
use sinex_sources::sources::zsh;
use sinex_store::positions;
use sqlx::PgConnection;
use std::path::{Path, PathBuf};

/// Hide the record `target` was read from, as the API's `hide` does.
async fn hide(writer: &crate::writer::WriterHandle, target: sinex_model::EventId) {
    writer
        .submit(
            Priority::Live,
            sinex_model::Batch {
                run: sinex_model::RunId::new(),
                kind: sinex_model::BatchKind::Api { actor: "t".into() },
                records: Vec::new(),
                archive: vec![sinex_model::ArchiveRequest::Hide {
                    target,
                    operation: sinex_model::OperationId::new(),
                }],
            },
        )
        .await
        .unwrap();
}

struct Fixture {
    _db: devkit::pg::TestDatabase,
    ctx: ReaderContext,
    dir: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn fixture(db: devkit::pg::TestDatabase) -> Fixture {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinexd-replay-{}", Uuid::now_v7()));
    let ctx = ReaderContext::open(
        &url,
        &dir.join("store"),
        sinex_model::time_settings::TimeSettings::operator_default(),
    )
    .await
    .unwrap();
    Fixture { _db: db, ctx, dir }
}

/// Read `path` through the zsh driver to the end, as backfill does.
async fn ingest(ctx: &mut ReaderContext, path: &Path, state: &mut AppendState) {
    let driver = zsh::driver(path.to_owned(), "h".into());
    ingest_with(ctx, |cursor| driver.poll(cursor, state).unwrap()).await;
}

async fn ingest_with(ctx: &mut ReaderContext, mut poll: impl FnMut(&Value) -> drivers::Acquired) {
    let source = zsh::source("h".into());
    let sink = Sink(ctx.blobs.clone());
    loop {
        let (cursor, carry) = positions::load(&mut ctx.read_conn, "zsh@h")
            .await
            .unwrap()
            .unwrap_or((Value::Null, Value::Null));
        let acquired = poll(&cursor);
        let more = acquired.material.is_some();
        for batch in drivers::batches(
            &source,
            "zsh@h",
            RunId::new(),
            acquired,
            carry,
            &sink,
            Limits::default(),
        )
        .unwrap()
        {
            ctx.writer.submit(Priority::Bulk, batch).await.unwrap();
        }
        if !more {
            break;
        }
    }
}

async fn live_commands(conn: &mut PgConnection) -> Vec<String> {
    sqlx::query_scalar!(
        r#"SELECT e.payload->>'command' AS "c!" FROM (SELECT event_id FROM core.live_locations
             UNION ALL SELECT event_id FROM core.live_equivalence) l
           JOIN core.events e ON e.id = l.event_id ORDER BY (e.payload->>'started_at'), e.id"#
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

async fn live_ids(conn: &mut PgConnection) -> Vec<Uuid> {
    sqlx::query_scalar!(
        r#"SELECT event_id AS "id!" FROM (SELECT event_id FROM core.live_locations
             UNION ALL SELECT event_id FROM core.live_equivalence) l ORDER BY event_id"#
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

fn history(dir: &Path, text: &str) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("zsh_history");
    std::fs::write(&path, text).unwrap();
    path
}

fn whole_binding() -> Scope {
    Scope {
        binding: Some("zsh@h".into()),
        ..Scope::default()
    }
}

fn options(scope: Scope, compare: bool) -> Options {
    Options {
        scope,
        compare,
        resume: None,
        actor: "t".into(),
        unlock: None,
    }
}

#[devkit::test]
async fn replay_reads_every_record_again_with_new_ids_and_the_same_content(
    db: devkit::pg::TestDatabase,
) {
    let mut f = fixture(db).await;
    let path = history(&f.dir, ": 1:0;ls\n: 2:0;echo a\\\nb\n");
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    std::fs::write(&path, ": 1:0;ls\n: 2:0;echo a\\\nb\n: 3:0;make\n").unwrap();
    ingest(&mut f.ctx, &path, &mut state).await;
    let before = live_commands(&mut f.ctx.read_conn).await;
    let ids_before = live_ids(&mut f.ctx.read_conn).await;
    assert_eq!(before, vec!["ls", "echo a\nb", "make"]);

    let compared = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), true),
    )
    .await
    .unwrap();
    assert_eq!(
        (
            compared.sequences,
            compared.materials,
            compared.archived,
            compared.admitted
        ),
        (1, 2, 3, 3)
    );
    assert_eq!(compared.diff.unchanged, 3);
    assert!(compared.diff.changed.is_empty() && compared.diff.removed.is_empty());
    assert_eq!(
        live_ids(&mut f.ctx.read_conn).await,
        ids_before,
        "compare keeps nothing"
    );
    let ops: i64 = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.operations"#)
        .fetch_one(&mut f.ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(ops, 0);

    let applied = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), false),
    )
    .await
    .unwrap();
    assert_eq!(
        (applied.archived, applied.admitted, applied.diff.unchanged),
        (3, 3, 3)
    );
    assert_eq!(live_commands(&mut f.ctx.read_conn).await, before);
    let ids_after = live_ids(&mut f.ctx.read_conn).await;
    assert!(
        ids_after.iter().all(|id| !ids_before.contains(id)),
        "replay mints new IDs"
    );
    let archived: i64 = sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM audit.archived WHERE reason = 'replay' AND operation_id = $1"#,
        applied.operation.0
    )
    .fetch_one(&mut f.ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(archived, 3);

    // Resuming the finished operation skips its sequence.
    let resumed = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        Options {
            resume: Some(applied.operation),
            ..options(Scope::default(), false)
        },
    )
    .await
    .unwrap();
    assert_eq!((resumed.sequences, resumed.sequences_skipped), (0, 1));
}

#[devkit::test]
async fn a_narrowed_replay_touches_only_its_window_and_hidden_stays_hidden(
    db: devkit::pg::TestDatabase,
) {
    let mut f = fixture(db).await;
    let path = history(&f.dir, ": 100:0;one\n: 200:0;two\n: 300:0;three\n");
    ingest(&mut f.ctx, &path, &mut AppendState::default()).await;
    let two: Uuid =
        sqlx::query_scalar!(r#"SELECT id FROM core.events WHERE payload->>'command' = 'two'"#)
            .fetch_one(&mut f.ctx.read_conn)
            .await
            .unwrap();
    hide(&f.ctx.writer, sinex_model::EventId(two)).await;
    let report = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(
            Scope {
                from: Some(Timestamp::from_unix_secs(150).unwrap()),
                ..Scope::default()
            },
            false,
        ),
    )
    .await
    .unwrap();
    // "two" is hidden, so only "three" was live in the window; the new
    // reading of "two" is suppressed as hidden.
    assert_eq!((report.archived, report.admitted), (1, 1));
    assert_eq!(
        live_commands(&mut f.ctx.read_conn).await,
        vec!["one", "three"]
    );
}

#[devkit::test]
async fn a_rewritten_history_file_is_aligned_and_replays_without_duplicates(
    db: devkit::pg::TestDatabase,
) {
    let mut f = fixture(db).await;
    let path = history(&f.dir, ": 1:0;a\n: 2:0;b\n: 3:0;a\n");
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    // zsh expires the older duplicate and appends a new command.
    std::fs::write(&path, ": 2:0;b\n: 3:0;a\n: 4:0;c\n").unwrap();
    // The driver restarts: its state comes back from the stored bytes.
    let mut restarted = sinex_source_core::binding::appended(zsh::driver(path.clone(), "h".into()))
        .into_polled()
        .unwrap();
    let (_stop, stop) = tokio::sync::watch::channel(false);
    crate::binding_reader::restore_reader(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        restarted.as_mut(),
        "zsh@h",
        None,
        &stop,
    )
    .await
    .unwrap();
    ingest_with(&mut f.ctx, |cursor| restarted.poll(cursor).unwrap()).await;
    assert_eq!(
        live_commands(&mut f.ctx.read_conn).await,
        vec!["a", "b", "a", "c"],
        "only the new command was read from the rewrite"
    );
    let report = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), true),
    )
    .await
    .unwrap();
    assert_eq!(
        report.sequences, 1,
        "the aligned rewrite continues the previous revision's parser sequence"
    );
    let revisions: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.revisions")
        .fetch_one(&mut f.ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(
        revisions, 2,
        "byte revisions stay distinct from the parser carry sequence"
    );
    assert_eq!(
        (
            report.diff.unchanged,
            report.diff.added.len(),
            report.diff.removed.len()
        ),
        (4, 0, 0),
        "a replay of the aligned rewrite reads only its new record"
    );
}

#[devkit::test]
async fn a_later_material_read_failure_rolls_back_the_entire_replay_sequence(
    db: devkit::pg::TestDatabase,
) {
    let mut f = fixture(db).await;
    let path = history(&f.dir, ": 1:0;first\n");
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    std::fs::write(&path, ": 1:0;first\n: 2:0;second\n").unwrap();
    ingest(&mut f.ctx, &path, &mut state).await;
    let before = live_ids(&mut f.ctx.read_conn).await;
    assert_eq!(before.len(), 2);
    let parts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM raw.materials ORDER BY part_index")
        .fetch_all(&mut f.ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(parts.len(), 2);
    // The first part is read and sent normally. The next part cannot supply
    // its declared range, after the replay transaction has begun.
    sqlx::query("UPDATE raw.materials SET manifest=jsonb_set(manifest,'{records}','[[0,99999]]') WHERE id=$1")
        .bind(parts[1]).execute(&mut f.ctx.read_conn).await.unwrap();
    let error = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), false),
    )
    .await
    .unwrap_err();
    assert!(error.contains("record range exceeds material"), "{error}");
    assert_eq!(live_ids(&mut f.ctx.read_conn).await, before);
    assert_eq!(
        live_commands(&mut f.ctx.read_conn).await,
        vec!["first", "second"]
    );
    let archived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit.archived WHERE reason='replay'")
            .fetch_one(&mut f.ctx.read_conn)
            .await
            .unwrap();
    let operations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='replay'")
            .fetch_one(&mut f.ctx.read_conn)
            .await
            .unwrap();
    assert_eq!((archived, operations), (0, 0));
}

#[devkit::test]
async fn aligned_rewrite_replay_uses_the_same_previous_carry_as_capture(
    db: devkit::pg::TestDatabase,
) {
    let mut f = fixture(db).await;
    let path = history(
        &f.dir,
        ": 1700000000:0;duplicate\n: 1700000000:1;duplicate\n",
    );
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    std::fs::write(&path,"prefix-other\n: 1700000000:0;duplicate\n: 1700000000:1;duplicate\n: 1700000000:2;duplicate\n").unwrap();
    ingest(&mut f.ctx, &path, &mut state).await;
    let before:Vec<(String,Value,String,Vec<u8>)>=sqlx::query_as("SELECT e.source,e.payload,e.equivalence_key,e.record_hash FROM core.events e JOIN core.live_equivalence l ON l.event_id=e.id ORDER BY e.payload::text").fetch_all(&mut f.ctx.read_conn).await.unwrap();
    let seen = before
        .iter()
        .filter(|(_, payload, _, _)| payload["command"] == "duplicate")
        .map(|(_, payload, _, _)| payload["n"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(seen, vec![0, 1, 2]);
    let report = run(
        &mut f.ctx.read_conn,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), false),
    )
    .await
    .unwrap();
    assert_eq!(report.sequences, 1);
    let after:Vec<(String,Value,String,Vec<u8>)>=sqlx::query_as("SELECT e.source,e.payload,e.equivalence_key,e.record_hash FROM core.events e JOIN core.live_equivalence l ON l.event_id=e.id ORDER BY e.payload::text").fetch_all(&mut f.ctx.read_conn).await.unwrap();
    assert_eq!(
        after, before,
        "a physical rewrite continues the same parser stream without duplicating or renumbering old occurrences"
    );
}
