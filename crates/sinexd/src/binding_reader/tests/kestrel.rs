use super::*;
use sinex_model::query::{Context, bind};
use sinex_sources::sources::{activitywatch, atuin, goodreads, zsh};
use sinex_store::query::{self, Answer, Page};
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kestrel")
        .join(name)
}

fn binding(
    format: &InputFormat,
    spec: &sinex_source_core::registry::SourceSpec,
    path: &Path,
) -> BindingReader {
    BindingReader::new(format, Binding::of(spec, path, Settings::new("kestrel"))).unwrap()
}

async fn ingest(ctx: &mut ReaderContext, readers: &mut [BindingReader]) {
    let (_stop, stop) = watch::channel(false);
    let results = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &Sink(ctx.blobs.clone()),
        readers,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await;
    for (source, result) in results {
        result.unwrap_or_else(|error| panic!("{source}: {error}"));
    }
}

async fn ask(ctx: &mut ReaderContext, text: &str) -> Answer {
    let context = Context::new(
        "2026-09-16T10:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let parsed = bind(text, &context).unwrap_or_else(|error| panic!("{}", error.render(text)));
    query::run(
        &mut ctx.read_conn,
        text,
        &parsed,
        &context,
        &Page {
            size: Some(100),
            cursor: None,
        },
    )
    .await
    .unwrap()
}

async fn replay(ctx: &mut ReaderContext) -> crate::replay::Report {
    crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope::default(),
            compare: false,
            resume: None,
            actor: "synthetic-kestrel".into(),
            unlock: None,
        },
    )
    .await
    .unwrap()
}

#[devkit::test]
async fn kestrel_shell_and_books_keep_their_records_after_replay(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let shell = dir.path().join("atuin.db");
    let history = dir.path().join("zsh_history");
    let books = dir.path().join("goodreads.csv");
    for (from, to) in [
        ("atuin.db", &shell),
        ("zsh_history", &history),
        ("goodreads.csv", &books),
    ] {
        std::fs::copy(fixture(from), to).unwrap();
    }
    let mut readers = [
        binding(&atuin::FORMAT, &atuin::SPEC, &shell),
        binding(&zsh::FORMAT, &zsh::SPEC, &history),
        binding(&goodreads::FORMAT, &goodreads::SPEC, &books),
    ];
    ingest(&mut ctx, &mut readers).await;
    let commands = ask(
        &mut ctx,
        "events where event_type = command.imported sort when",
    )
    .await;
    assert_eq!(commands.total, 5);
    let texts: Vec<_> = commands
        .rows
        .iter()
        .map(|r| r["payload"]["command"].as_str().unwrap())
        .collect();
    assert_eq!(
        texts,
        [
            "cd ~/src/tidepool",
            "echo \"zażółć gęślą jaźń\" > notatki.txt",
            "git status",
            "for f in src/*.rs; do\n  wc -l \"$f\"\ndone",
            "cargo fmt --check"
        ]
    );
    let day = ask(
        &mut ctx,
        "events where event_type = command.imported since 2026-09-13 until 2026-09-14",
    )
    .await;
    assert_eq!(
        day.total, 5,
        "the 01:30 command belongs to Sunday's logical day"
    );
    let failed = ask(
        &mut ctx,
        "events where event_type = command.executed and payload.exit_code = 101",
    )
    .await;
    assert_eq!(failed.total, 1);
    let deleted = ask(
        &mut ctx,
        "events where event_type = command.executed and state = removed and payload.command = \"rm notes-old.txt\"",
    )
    .await;
    assert_eq!(deleted.total, 1);
    let present = ask(
        &mut ctx,
        "events where event_type = command.executed and state = present and payload.command = \"rm notes-old.txt\"",
    ).await;
    assert_eq!(present.total, 0);
    let books_before = ask(&mut ctx, "events where event_type = book.status sort when").await;
    assert_eq!(books_before.total, 3);
    let before: Vec<_> = commands
        .rows
        .iter()
        .map(|r| {
            (
                r["material"].clone(),
                r["offset"].clone(),
                r["payload"].clone(),
            )
        })
        .collect();
    let old_ids: Vec<_> = commands.rows.iter().map(|r| r["id"].clone()).collect();
    std::fs::remove_file(shell).unwrap();
    std::fs::remove_file(history).unwrap();
    std::fs::remove_file(books).unwrap();
    let report = replay(&mut ctx).await;
    assert_eq!(report.failures, 0);
    let after = ask(
        &mut ctx,
        "events where event_type = command.imported sort when",
    )
    .await;
    assert_eq!(
        after
            .rows
            .iter()
            .map(|r| (
                r["material"].clone(),
                r["offset"].clone(),
                r["payload"].clone()
            ))
            .collect::<Vec<_>>(),
        before
    );
    assert!(after.rows.iter().all(|r| !old_ids.contains(&r["id"])));
    assert_eq!(
        ask(&mut ctx, "events where event_type = book.status")
            .await
            .total,
        3
    );
    let archived = ask(
        &mut ctx,
        "archive where event_type = command.imported and reason = replay",
    )
    .await;
    assert_eq!(archived.total, 5);
    ctx.close().await.unwrap();
}

fn activity_rows(sqlite: &rusqlite::Connection, name: &str) {
    let rows: Value =
        serde_json::from_slice(&std::fs::read(fixture(&format!("activitywatch/{name}"))).unwrap())
            .unwrap();
    for row in rows.as_array().unwrap() {
        let start = row["timestamp"]
            .as_str()
            .unwrap()
            .parse::<jiff::Timestamp>()
            .unwrap();
        let duration_ns = (row["duration"].as_f64().unwrap() * 1_000_000_000.0) as i64;
        sqlite
            .execute(
                "INSERT OR REPLACE INTO events VALUES (?1,1,?2,?3,?4)",
                rusqlite::params![
                    row["id"].as_i64().unwrap(),
                    start.as_nanosecond().to_string(),
                    (start.as_nanosecond() + i128::from(duration_ns)).to_string(),
                    row["data"].to_string(),
                ],
            )
            .unwrap();
    }
}

#[devkit::test]
async fn kestrel_presence_updates_replace_readings_and_replay_keeps_the_latest(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("activitywatch.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch("CREATE TABLE buckets(id INTEGER PRIMARY KEY, name TEXT, type TEXT);
        INSERT INTO buckets VALUES (1,'aw-watcher-afk_kestrel','afkstatus');
        CREATE TABLE events(id INTEGER PRIMARY KEY, bucketrow INTEGER, starttime TEXT, endtime TEXT, data TEXT);").unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let mut readers = [binding(&activitywatch::FORMAT, &activitywatch::SPEC, &path)];
    activity_rows(&sqlite, "2026-09-14T08-00-00Z.json");
    ingest(&mut ctx, &mut readers).await;
    query::set_grants(&mut ctx.read_conn, "guarded")
        .await
        .unwrap();
    let first = ask(
        &mut ctx,
        "events where event_type = presence.span and payload.status = afk",
    )
    .await;
    assert_eq!(first.total, 1);
    assert_eq!(first.rows[0]["payload"]["ended_at"], "2026-09-14T08:00:00Z");
    activity_rows(&sqlite, "2026-09-14T10-00-00Z.json");
    readers = [binding(&activitywatch::FORMAT, &activitywatch::SPEC, &path)];
    ingest(&mut ctx, &mut readers).await;
    let extended = ask(
        &mut ctx,
        "events where event_type = presence.span and payload.status = afk",
    )
    .await;
    assert_eq!(extended.total, 1);
    assert_eq!(
        extended.rows[0]["payload"]["ended_at"],
        "2026-09-14T09:27:30Z"
    );
    assert_ne!(first.rows[0]["id"], extended.rows[0]["id"]);
    activity_rows(&sqlite, "2026-09-15T07-00-00Z.json");
    readers = [binding(&activitywatch::FORMAT, &activitywatch::SPEC, &path)];
    ingest(&mut ctx, &mut readers).await;
    let latest = ask(
        &mut ctx,
        "events where event_type = presence.span sort when",
    )
    .await;
    assert_eq!(latest.total, 5);
    let payloads: Vec<_> = latest.rows.iter().map(|r| r["payload"].clone()).collect();
    let archived = ask(
        &mut ctx,
        "archive where event_type = presence.span and reason = newer_reading",
    )
    .await;
    assert_eq!(archived.total, 2);
    drop(sqlite);
    std::fs::remove_file(path).unwrap();
    let report = replay(&mut ctx).await;
    assert_eq!(report.failures, 0);
    let after = ask(
        &mut ctx,
        "events where event_type = presence.span sort when",
    )
    .await;
    assert_eq!(
        after
            .rows
            .iter()
            .map(|r| r["payload"].clone())
            .collect::<Vec<_>>(),
        payloads
    );
    ctx.close().await.unwrap();
}
