//! Replay through the real writer and the zsh driver, on a real database.
use super::*;
use crate::context::{Context, Sink};
use crate::writer::Priority;
use sinex_sources::drivers::append_file::AppendState;
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
    ctx: Context,
    dir: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn fixture() -> Fixture {
    let (db, url, _) = crate::testdb::fresh().await;
    let dir = std::env::temp_dir().join(format!("sinexd-replay-{}", Uuid::now_v7()));
    let ctx = Context::open(&url, &dir.join("store")).await.unwrap();
    Fixture { _db: db, ctx, dir }
}

/// Read `path` through the zsh driver to the end, as backfill does.
async fn ingest(ctx: &mut Context, path: &Path, state: &mut AppendState) {
    let source = zsh::source("h".into());
    let driver = zsh::driver(path.to_owned(), "h".into());
    let sink = Sink(ctx.blobs.clone());
    loop {
        let (cursor, carry) = positions::load(&mut ctx.aux, "zsh@h")
            .await
            .unwrap()
            .unwrap_or((Value::Null, Value::Null));
        let acquired = driver.poll(&cursor, state).unwrap();
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
    }
}

#[tokio::test]
async fn replay_reads_every_record_again_with_new_ids_and_the_same_content() {
    let mut f = fixture().await;
    let path = history(&f.dir, ": 1:0;ls\n: 2:0;echo a\\\nb\n");
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    std::fs::write(&path, ": 1:0;ls\n: 2:0;echo a\\\nb\n: 3:0;make\n").unwrap();
    ingest(&mut f.ctx, &path, &mut state).await;
    let before = live_commands(&mut f.ctx.aux).await;
    let ids_before = live_ids(&mut f.ctx.aux).await;
    assert_eq!(before, vec!["ls", "echo a\nb", "make"]);

    let compared = run(
        &mut f.ctx.aux,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), true),
    )
    .await
    .unwrap();
    assert_eq!(
        (
            compared.chains,
            compared.materials,
            compared.archived,
            compared.admitted
        ),
        (1, 2, 3, 3)
    );
    assert_eq!(compared.diff.unchanged, 3);
    assert!(compared.diff.changed.is_empty() && compared.diff.removed.is_empty());
    assert_eq!(
        live_ids(&mut f.ctx.aux).await,
        ids_before,
        "compare keeps nothing"
    );
    let ops: i64 = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.operations"#)
        .fetch_one(&mut f.ctx.aux)
        .await
        .unwrap();
    assert_eq!(ops, 0);

    let applied = run(
        &mut f.ctx.aux,
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
    assert_eq!(live_commands(&mut f.ctx.aux).await, before);
    let ids_after = live_ids(&mut f.ctx.aux).await;
    assert!(
        ids_after.iter().all(|id| !ids_before.contains(id)),
        "replay mints new IDs"
    );
    let archived: i64 = sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM audit.archived WHERE reason = 'replay' AND operation_id = $1"#,
        applied.operation.0
    )
    .fetch_one(&mut f.ctx.aux)
    .await
    .unwrap();
    assert_eq!(archived, 3);

    // Resuming the finished operation skips its chain.
    let resumed = run(
        &mut f.ctx.aux,
        &f.ctx.blobs,
        &f.ctx.writer,
        Options {
            resume: Some(applied.operation),
            ..options(Scope::default(), false)
        },
    )
    .await
    .unwrap();
    assert_eq!((resumed.chains, resumed.chains_skipped), (0, 1));
}

#[tokio::test]
async fn a_narrowed_replay_touches_only_its_window_and_hidden_stays_hidden() {
    let mut f = fixture().await;
    let path = history(&f.dir, ": 100:0;one\n: 200:0;two\n: 300:0;three\n");
    ingest(&mut f.ctx, &path, &mut AppendState::default()).await;
    let two: Uuid =
        sqlx::query_scalar!(r#"SELECT id FROM core.events WHERE payload->>'command' = 'two'"#)
            .fetch_one(&mut f.ctx.aux)
            .await
            .unwrap();
    hide(&f.ctx.writer, sinex_model::EventId(two)).await;
    let report = run(
        &mut f.ctx.aux,
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
    assert_eq!(live_commands(&mut f.ctx.aux).await, vec!["one", "three"]);
}

#[tokio::test]
async fn a_rewritten_history_file_is_aligned_and_replays_without_duplicates() {
    let mut f = fixture().await;
    let path = history(&f.dir, ": 1:0;a\n: 2:0;b\n: 3:0;a\n");
    let mut state = AppendState::default();
    ingest(&mut f.ctx, &path, &mut state).await;
    // zsh expires the older duplicate and appends a new command.
    std::fs::write(&path, ": 2:0;b\n: 3:0;a\n: 4:0;c\n").unwrap();
    // The driver restarts: its state comes back from the stored bytes.
    let cursor = positions::load(&mut f.ctx.aux, "zsh@h")
        .await
        .unwrap()
        .unwrap()
        .0;
    let chain = crate::context::stored_chain(&mut f.ctx.aux, &f.ctx.blobs, &cursor)
        .await
        .unwrap();
    let mut rebuilt =
        AppendState::rebuild(chain.iter().map(|(id, b)| (*id, b.as_slice())), zsh::frame);
    assert_eq!(rebuilt, state);
    ingest(&mut f.ctx, &path, &mut rebuilt).await;
    assert_eq!(
        live_commands(&mut f.ctx.aux).await,
        vec!["a", "b", "a", "c"],
        "only the new command was read from the rewrite"
    );
    let report = run(
        &mut f.ctx.aux,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), true),
    )
    .await
    .unwrap();
    assert_eq!(report.chains, 2, "the first revision and the rewrite");
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

#[tokio::test]
async fn an_excised_record_stays_out_of_a_replay() {
    let mut f = fixture().await;
    let path = history(&f.dir, ": 1:0;ls\n: 2:0;cat secret\n: 3:0;pwd\n");
    ingest(&mut f.ctx, &path, &mut AppendState::default()).await;
    let secret: Uuid = sqlx::query_scalar!(
        r#"SELECT id FROM core.events WHERE payload->>'command' = 'cat secret'"#
    )
    .fetch_one(&mut f.ctx.aux)
    .await
    .unwrap();
    let out = crate::operate::excise_with(
        &mut f.ctx.aux,
        &f.ctx.blobs,
        &f.ctx.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(secret)),
        "t",
    )
    .await
    .unwrap();
    assert!(
        out.starts_with("excised events 1 | materials rewritten 1"),
        "{out}"
    );
    let blob: String = sqlx::query_scalar!("SELECT blob FROM raw.materials")
        .fetch_one(&mut f.ctx.aux)
        .await
        .unwrap();
    let bytes = f.ctx.blobs.read(&blob).unwrap();
    assert!(!bytes.windows(6).any(|w| w == b"secret"));
    assert_eq!(bytes.len(), ": 1:0;ls\n: 2:0;cat secret\n: 3:0;pwd\n".len());

    let report = run(
        &mut f.ctx.aux,
        &f.ctx.blobs,
        &f.ctx.writer,
        options(whole_binding(), false),
    )
    .await
    .unwrap();
    assert_eq!(
        (report.archived, report.admitted, report.failures),
        (2, 2, 0)
    );
    assert_eq!(live_commands(&mut f.ctx.aux).await, vec!["ls", "pwd"]);
}
