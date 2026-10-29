use super::*;
use sinex_model::{BatchKind, RunId};
use sinex_source_core::binding;
use sinex_sources::sources::atuin;

struct FailedSink;
impl drivers::MaterialSink for FailedSink {
    fn put(&self, _: &[u8]) -> Result<String, String> {
        Err("synthetic storage failure".into())
    }
    fn put_file(&self, _: &std::path::Path) -> Result<String, String> {
        Err("synthetic storage failure".into())
    }
}

#[devkit::test]
async fn sqlite_rows_keep_failed_and_partial_snapshots_until_writer_commits(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-row-retry-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("history.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch("CREATE TABLE history(id TEXT, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT);
        INSERT INTO history VALUES ('a',1,-1,-1,'first','/synthetic','s','h'), ('b',2,-1,-1,'second','/synthetic','s','h');").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let source = atuin::source();
    let mut reader = binding::rows(atuin::driver(path, "synthetic".into()))
        .into_polled()
        .unwrap();
    let acquired = reader.poll(&Value::Null).unwrap();
    let id = acquired.material.as_ref().unwrap().id;
    assert!(
        drivers::prepare(
            &source,
            "row-retry",
            RunId::new(),
            acquired,
            Value::Null,
            &FailedSink,
            Limits::default()
        )
        .is_err()
    );
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "row-retry")
            .await
            .unwrap()
            .is_none()
    );
    sqlite
        .execute_batch("UPDATE history SET command='changed' WHERE id='b'")
        .unwrap();
    let acquired = reader.poll(&Value::Null).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, id);
    let prepared = drivers::prepare(
        &source,
        "row-retry",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            max_bytes: 1024,
        },
    )
    .unwrap();
    let mut batches = prepared.batches().unwrap();
    let first = batches.next().unwrap().unwrap();
    ctx.writer
        .submit(crate::writer::Priority::Bulk, first)
        .await
        .unwrap();
    let second = batches.next().unwrap().unwrap();
    let BatchKind::Source(progress) = &second.kind else {
        panic!()
    };
    let final_cursor = progress.cursor.clone();
    // Admission failure occurs after preparation, with the first record committed.
    sqlx::query("CREATE FUNCTION reject_synthetic_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic admission failure'; END $$").execute(&mut ctx.read_conn).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_synthetic_row BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION reject_synthetic_row()").execute(&mut ctx.read_conn).await.unwrap();
    assert!(
        ctx.writer
            .submit(crate::writer::Priority::Bulk, second)
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER reject_synthetic_row ON core.events")
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    drop(batches);
    drop(prepared);
    let (cursor, carry) = sinex_store::positions::load(&mut ctx.read_conn, "row-retry")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(cursor, final_cursor);
    let acquired = reader.poll(&cursor).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, id);
    assert_eq!(acquired.material.as_ref().unwrap().records.len(), 1);
    let prepared = drivers::prepare(
        &source,
        "row-retry",
        RunId::new(),
        acquired,
        carry,
        &sink,
        Limits::default(),
    )
    .unwrap();
    for batch in prepared.batches().unwrap() {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch.unwrap())
            .await
            .unwrap();
    }
    drop(prepared);
    let payloads: Vec<Value> = sqlx::query_scalar(
        "SELECT payload FROM core.events WHERE source='terminal.atuin-history' ORDER BY anchor",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0]["command"], "first");
    assert_eq!(payloads[1]["command"], "second");
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "row-retry")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor, final_cursor);
    let next = reader.poll(&cursor).unwrap();
    assert_ne!(next.material.as_ref().unwrap().id, id);
    let prepared = drivers::prepare(
        &source,
        "row-retry",
        RunId::new(),
        next,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap();
    for batch in prepared.batches().unwrap() {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch.unwrap())
            .await
            .unwrap();
    }
    drop(prepared);
    let commands: Vec<String> = sqlx::query_scalar("SELECT e.payload->>'command' FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.source='terminal.atuin-history' ORDER BY e.payload->>'native_id'").fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(commands, vec!["first", "changed"]);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
