//! A restart over a long sequence of parts: the reader rebuilds what it needs
//! from its saved memory and the parts after it, and the time the rebuild
//! takes is printed for comparison.
use super::*;
use sinex_sources::sources::{atuin, notes};
use std::time::Instant;

/// Parts each binding stores before the restart.
const PARTS: usize = 300;

async fn turn(ctx: &mut ReaderContext, readers: &mut [BindingReader]) {
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = watch::channel(false);
    for (_, result) in read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        readers,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await
    {
        result.unwrap();
    }
}

async fn parts(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM raw.materials")
        .fetch_one(conn)
        .await
        .unwrap()
}

/// The binding's saved memories: one, replaced as the reader saves again.
async fn saved_memories(conn: &mut PgConnection, binding: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM raw.reader_memory WHERE binding = $1")
        .bind(binding)
        .fetch_one(conn)
        .await
        .unwrap()
}

/// Rebuild a fresh reader's memory the way a restart does; how long it took.
async fn restore(ctx: &mut ReaderContext, readers: &mut BindingReader) -> std::time::Duration {
    let (_stop, stop) = watch::channel(false);
    let name = readers.binding.name.clone();
    let started = Instant::now();
    restore_reader(
        &mut ctx.read_conn,
        &ctx.blobs,
        polled(readers),
        &name,
        None,
        &stop,
    )
    .await
    .unwrap();
    started.elapsed()
}

#[devkit::test]
async fn a_restarted_row_reader_remembers_every_stored_row(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE history (id TEXT PRIMARY KEY, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT, deleted_at INTEGER)",
        )
        .unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let binding = Binding::of(&atuin::SPEC, &path, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&atuin::FORMAT, binding.clone()).unwrap()];
    for n in 0..PARTS {
        sqlite
            .execute(
                "INSERT INTO history VALUES (?1, ?2, 1000, 0, ?3, '/', 's', 'h', NULL)",
                rusqlite::params![
                    format!("row{n:06}"),
                    1_600_000_000_000_000_000i64 + n as i64 * 1_000_000_000,
                    format!("command {n}")
                ],
            )
            .unwrap();
        turn(&mut ctx, &mut readers).await;
    }
    assert_eq!(parts(&mut ctx.read_conn).await, PARTS as i64);
    assert_eq!(saved_memories(&mut ctx.read_conn, &binding.name).await, 1);

    let mut restarted = BindingReader::new(&atuin::FORMAT, binding.clone()).unwrap();
    let took = restore(&mut ctx, &mut restarted).await;
    eprintln!("restored a row reader over {PARTS} parts in {took:?}");
    // The first poll after a start reads every row; none differs from what
    // was stored, so nothing new is stored.
    let (cursor, _) = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    let acquired = polled(&mut restarted).poll(&cursor).unwrap();
    assert!(
        acquired.material.is_none(),
        "a restart stored rows it had already stored"
    );
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn a_restarted_directory_reader_stores_nothing_unchanged_and_sees_old_removals(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("vault");
    std::fs::create_dir_all(&root).unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let binding = Binding::of(&notes::SPEC, &root, Settings::new("synthetic"));
    let mut readers = vec![BindingReader::new(&notes::FORMAT, binding.clone()).unwrap()];
    for n in 0..PARTS {
        std::fs::write(root.join(format!("{n:06}.md")), format!("note {n}")).unwrap();
        turn(&mut ctx, &mut readers).await;
    }
    assert_eq!(parts(&mut ctx.read_conn).await, PARTS as i64);
    assert_eq!(saved_memories(&mut ctx.read_conn, &binding.name).await, 1);
    drop(readers);

    let mut restarted = BindingReader::new(&notes::FORMAT, binding.clone()).unwrap();
    let took = restore(&mut ctx, &mut restarted).await;
    eprintln!("restored a directory reader over {PARTS} parts in {took:?}");
    let (cursor, _) = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    let unchanged = polled(&mut restarted).poll(&cursor).unwrap();
    assert!(
        unchanged.material.is_none(),
        "a restart stored files it had already stored"
    );

    // Only the first part names this file.
    std::fs::remove_file(root.join("000000.md")).unwrap();
    let acquired = polled(&mut restarted).poll(&cursor).unwrap();
    let material = acquired.material.expect("the removal is recorded");
    let members = &material.manifest.members;
    let removed: Vec<&str> = members
        .iter()
        .filter(|member| member.details.contains_key("removed_at_ns"))
        .map(|member| member.path.as_str())
        .collect();
    assert_eq!(removed, ["000000.md"]);
    assert_eq!(members.len(), 1, "no unchanged file is stored again");
    ctx.close().await.unwrap();
}
