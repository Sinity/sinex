use super::*;
use uuid::Uuid;

#[tokio::test]
async fn a_reread_source_leaves_the_excised_record_out_of_its_new_bytes() {
    use sinex_sources::{Source, drivers};
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.sqlite");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT); INSERT INTO history VALUES('native-1',1700000000000000000,1,0,'synthetic removed value','/','s','h'),('native-2',1700000001000000000,1,0,'synthetic kept value','/','s','h')").unwrap();
    let mut ctx = crate::one_shot::OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let source = sinex_sources::sources::atuin::source();
    let binding = sinex_sources::Binding {
        name: "atuin@h".into(),
        source: source.spec().name.into(),
        parser: source.spec().parser.into(),
        input: path.clone(),
        settings: sinex_sources::Settings::new("h"),
    };
    crate::backfill::record(&mut ctx.read_conn, &binding)
        .await
        .unwrap();
    let driver = sinex_sources::sources::atuin::driver(path, "h".into());
    let mut state = drivers::sqlite_rows::RowState::default();
    let acquired = driver.poll(&serde_json::Value::Null, &mut state).unwrap();
    let sink = crate::one_shot::Sink(ctx.blobs.clone());
    for batch in drivers::batches(
        &source,
        &binding.name,
        sinex_model::RunId::new(),
        acquired,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch)
            .await
            .unwrap();
    }
    let selected: Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE payload->>'native_id'='native-1'")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    crate::excision::excise_with(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(selected)),
        "synthetic",
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE history SET cwd='/updated-private-stage-control' WHERE id='native-2'",
        [],
    )
    .unwrap();
    let exclusions = Exclusions::load(&mut ctx.read_conn, source.spec().name)
        .await
        .unwrap();
    let mut restarted = drivers::sqlite_rows::RowState::default();
    let acquired = driver
        .poll(&serde_json::Value::Null, &mut restarted)
        .unwrap();
    let filtered = exclusions
        .filter(
            &source,
            acquired,
            &serde_json::Value::Null,
            source.spec().class,
            false,
        )
        .unwrap();
    let material = filtered.material.as_ref().unwrap();
    let id = material.id;
    let Some(drivers::StoredBytes::FramedFile(stored)) = &material.stored else {
        panic!("the blanked bytes are a private file until stored");
    };
    let mut kept = Vec::new();
    std::io::Read::read_to_end(&mut &**stored, &mut kept).unwrap();
    assert!(!String::from_utf8_lossy(&kept).contains("synthetic removed value"));
    assert!(String::from_utf8_lossy(&kept).contains("synthetic kept value"));
    let prepared = drivers::prepare(
        &source,
        &binding.name,
        sinex_model::RunId::new(),
        filtered,
        serde_json::Value::Null,
        &sink,
        sinex_sources::Limits::default(),
    )
    .unwrap()
    .with_exclusions(exclusions.version().clone());
    for batch in prepared.batches().unwrap() {
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batch.unwrap())
            .await
            .unwrap();
    }
    let body: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
        .bind(id.0)
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(ctx.blobs.read(&body).unwrap(), kept);
    crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some(binding.name),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: None,
        },
    )
    .await
    .unwrap();
    let commands:Vec<String>=sqlx::query_scalar("SELECT e.payload->>'command' FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.source=$1 ORDER BY e.payload->>'native_id'").bind(source.spec().name).fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(commands, ["synthetic kept value"]);
    let tombstone: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE event_id=$1)")
            .bind(selected)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert!(tombstone);
    ctx.close().await.unwrap();
}
