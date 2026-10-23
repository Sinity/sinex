//! Synthetic whole-image capture through the actual row reader and writer.
use super::*;
use sinex_model::{Class, RunId};
use sinex_sources::{Settings, binding, drivers, sources::atuin};
use std::io::{Read, Write};

struct FailRowsAfterImage<'a>(&'a Sink);
impl drivers::MaterialSink for FailRowsAfterImage<'_> {
    fn put(&self, _: &[u8]) -> Result<String, String> {
        Err("synthetic row storage failure".into())
    }
    fn put_file(&self, _: &std::path::Path) -> Result<String, String> {
        Err("synthetic row storage failure".into())
    }
    fn put_image(
        &self,
        image: &drivers::sqlite_image::Image,
    ) -> Result<sinex_model::SqliteImagePublication, String> {
        drivers::MaterialSink::put_image(self.0, image)
    }
}

fn database(path: &std::path::Path) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch("PRAGMA page_size=1024; PRAGMA journal_mode=WAL;
      CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,_from TEXT,_rowid TEXT);
      INSERT INTO history VALUES ('a',1,-1,-1,'first','/synthetic','s','h','native origin','native row'),('b',2,-1,-1,'second','/synthetic','s','h',NULL,NULL);
      CREATE TABLE unselected(x BLOB); INSERT INTO unselected VALUES(zeroblob(20000));").unwrap();
    c
}
fn reader(path: &std::path::Path, class: Class) -> Box<dyn sinex_sources::Reader> {
    let mut r = binding::rows(atuin::driver(path.to_path_buf(), "synthetic".into()));
    let mut settings = Settings::new("synthetic");
    settings.options = serde_json::json!({"sqlite_images":{"scope":"whole_database","class":class,"interval_secs":3600,"turn_pages":1}});
    r.configure(&settings).unwrap();
    r
}
fn poll(reader: &mut dyn sinex_sources::Reader, cursor: &Value) -> drivers::Acquired {
    loop {
        match reader.poll(cursor) {
            Err(drivers::DriverError::Pending) => {}
            other => return other.unwrap(),
        }
    }
}
async fn submit(
    ctx: &mut OneShot,
    binding: &str,
    acquired: drivers::Acquired,
    limit: usize,
) -> Vec<sinex_model::Batch> {
    let source = atuin::source();
    let sink = Sink(ctx.blobs.clone());
    let prepared = drivers::prepare(
        &source,
        binding,
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: limit,
            max_bytes: usize::MAX,
        },
    )
    .unwrap();
    prepared.batches().unwrap().map(Result::unwrap).collect()
}
async fn admit(ctx: &mut OneShot, batch: sinex_model::Batch) {
    ctx.writer
        .submit(crate::writer::Priority::Bulk, batch)
        .await
        .unwrap();
}
async fn image(
    ctx: &mut OneShot,
    id: sinex_model::MaterialId,
) -> (sinex_model::SqliteImageMeta, Vec<u8>) {
    let (meta, extents, _) = sinex_store::sqlite_images::load(&mut ctx.read_conn, id)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    ctx.blobs
        .assemble(&extents)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    (meta, bytes)
}
fn read_image(bytes: &[u8], dir: &std::path::Path) -> String {
    let mut f = tempfile::NamedTempFile::new_in(dir).unwrap();
    f.write_all(bytes).unwrap();
    let c =
        rusqlite::Connection::open_with_flags(f.path(), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    c.query_row("SELECT command FROM history WHERE id='b'", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn sqlite_physical_images_publish_atomically_and_retry_exact_pending_rows_after_restart() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let provider = database(&path);
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let mut original = reader(&path, Class::Guarded);
    let acquired = poll(original.as_mut(), &Value::Null);
    let row_id = acquired.material.as_ref().unwrap().id;
    let image_id = acquired.image.as_ref().unwrap().metadata.material;
    let sink = Sink(ctx.blobs.clone());
    assert!(
        drivers::prepare(
            &atuin::source(),
            "images",
            RunId::new(),
            acquired,
            Value::Null,
            &FailRowsAfterImage(&sink),
            Limits::default()
        )
        .is_err()
    );
    provider
        .execute_batch("UPDATE history SET command='edited before retry' WHERE id='b'")
        .unwrap();
    let acquired = poll(original.as_mut(), &Value::Null);
    assert_eq!(acquired.material.as_ref().unwrap().id, row_id);
    assert_eq!(acquired.image.as_ref().unwrap().metadata.material, image_id);
    let mut batches = submit(&mut ctx, "images", acquired, 1).await;
    assert_eq!(batches.len(), 2);
    let first = batches.remove(0);
    for wrong_count in [false, true] {
        let mut bad = first.clone();
        let sinex_model::BatchKind::Source(p) = &mut bad.kind else {
            panic!()
        };
        let image = p.image.as_mut().unwrap();
        if wrong_count {
            image.metadata.page_count += 1;
        } else {
            image.metadata.map_offset += u64::from(image.metadata.page_size);
        }
        image.material.manifest["sqlite_image"] = serde_json::to_value(&image.metadata).unwrap();
        assert!(
            ctx.writer
                .submit(crate::writer::Priority::Bulk, bad)
                .await
                .is_err()
        );
    }
    // An admission failure must leave neither a companion image nor a cursor.
    sqlx::query("CREATE FUNCTION reject_image_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic image failure'; END $$").execute(&mut ctx.read_conn).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_image_row BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION reject_image_row()").execute(&mut ctx.read_conn).await.unwrap();
    assert!(
        ctx.writer
            .submit(crate::writer::Priority::Bulk, first.clone())
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.sqlite_images")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "images")
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("DROP TRIGGER reject_image_row ON core.events")
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    provider
        .execute_batch("UPDATE history SET command='edited after capture' WHERE id='b'")
        .unwrap();
    admit(&mut ctx, first).await;
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "images")
        .await
        .unwrap()
        .unwrap();
    let (meta, bytes) = image(&mut ctx, image_id).await;
    assert_eq!(read_image(&bytes, dir.path()), "second");
    assert!(meta.evidence_bytes > meta.image_bytes);
    // A fresh reader restores the entire retained JSONL part, not source bytes.
    let mut resumed = reader(&path, Class::Guarded);
    let reference = sinex_store::material_bytes::reference(&mut ctx.read_conn, row_id)
        .await
        .unwrap()
        .unwrap();
    let mut file = tempfile::tempfile().unwrap();
    ctx.blobs.copy_to(&reference.blob, &mut file).unwrap();
    resumed
        .restore_rows(
            reference.clone(),
            drivers::RetainedRecords {
                blob: reference.blob,
                bytes: std::sync::Arc::new(crate::retained::Retained::new(file, reference.length)),
            },
            &cursor,
        )
        .unwrap();
    let acquired = poll(resumed.as_mut(), &cursor);
    assert_eq!(acquired.material.as_ref().unwrap().id, row_id);
    let pending = submit(&mut ctx, "images", acquired, 1).await;
    assert_eq!(pending.len(), 1);
    admit(&mut ctx, pending.into_iter().next().unwrap()).await;
    let commands: Vec<String> =
        sqlx::query_scalar("SELECT payload->>'command' FROM core.events ORDER BY anchor")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(commands, vec!["first", "second"]);
    let extras: Value = sqlx::query_scalar(
        "SELECT payload->'extra' FROM core.events WHERE payload->>'native_id'='a'",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(extras["_from"], "native origin");
    assert_eq!(extras["_rowid"], "native row");
    // Excision remains refused before any mutation, including the row without
    // an exact-image pointer that a later logical-only poll produces.
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "images")
        .await
        .unwrap()
        .unwrap();
    resumed.restore_image(drivers::sqlite_image::Previous {
        metadata: meta.clone(),
        extents: sinex_store::sqlite_images::load(&mut ctx.read_conn, image_id)
            .await
            .unwrap()
            .unwrap()
            .1,
        map: None,
    });
    let next = poll(resumed.as_mut(), &cursor);
    assert!(next.image.is_none());
    let id = next.material.as_ref().unwrap().id;
    for batch in submit(&mut ctx, "images", next, 1).await {
        admit(&mut ctx, batch).await;
    }
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    let err = sinex_store::excision::plan(
        &mut ctx.read_conn,
        sinex_store::excision::Target::Material(id),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("database lineage"));
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(before, after);
    let replay=crate::replay::run(&mut ctx.read_conn,&ctx.blobs,&ctx.writer,crate::replay::Options {
        scope:crate::replay::Scope {binding:Some("images".into()),..Default::default()},compare:true,resume:None,actor:"synthetic".into(),authority:None,
    }).await.unwrap();
    assert_eq!(replay.materials,2,"only the retained row parts are parsed");
    assert_eq!(replay.failures,0);assert!(replay.diff.changed.is_empty());assert!(replay.diff.added.is_empty());assert!(replay.diff.removed.is_empty());
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_image_branches_preserve_capture_order_when_an_older_pending_view_commits_late() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let provider = database(&path);
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let mut seed = reader(&path, Class::Open);
    let captured = poll(seed.as_mut(), &Value::Null);
    let seed_id = captured.image.as_ref().unwrap().metadata.material;
    let map = captured.image.as_ref().unwrap().map.clone();
    for batch in submit(&mut ctx, "binding-base", captured, 20).await {
        admit(&mut ctx, batch).await;
    }
    let (metadata, extents, _) = sinex_store::sqlite_images::load(&mut ctx.read_conn, seed_id)
        .await
        .unwrap()
        .unwrap();
    let previous = drivers::sqlite_image::Previous {
        metadata,
        extents,
        map,
    };
    let mut a = reader(&path, Class::Open);
    a.restore_image(previous.clone());
    assert!(matches!(
        a.poll(&Value::Null),
        Err(drivers::DriverError::Pending)
    ));
    provider
        .execute_batch("UPDATE history SET command='new view' WHERE id='b'")
        .unwrap();
    let mut b = reader(&path, Class::Open);
    b.restore_image(previous);
    let pending_b = poll(b.as_mut(), &Value::Null);
    let b_id = pending_b.image.as_ref().unwrap().metadata.material;
    for batch in submit(&mut ctx, "binding-b", pending_b, 20).await {
        admit(&mut ctx, batch).await;
    }
    let pending_a = poll(a.as_mut(), &Value::Null);
    let a_id = pending_a.image.as_ref().unwrap().metadata.material;
    assert!(!pending_a.image.as_ref().unwrap().metadata.full);
    for batch in submit(&mut ctx, "binding-a", pending_a, 20).await {
        admit(&mut ctx, batch).await;
    }
    let (_, a_bytes) = image(&mut ctx, a_id).await;
    let (_, b_bytes) = image(&mut ctx, b_id).await;
    assert_eq!(read_image(&a_bytes, dir.path()), "second");
    assert_eq!(read_image(&b_bytes, dir.path()), "new view");
    let canonical = path.canonicalize().unwrap().display().to_string();
    assert_eq!(
        sinex_store::sqlite_images::latest(&mut ctx.read_conn, &canonical, "synthetic")
            .await
            .unwrap(),
        Some(b_id)
    );
    let live:String=sqlx::query_scalar("SELECT e.payload->>'command' FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.payload->>'native_id'='b'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(
        live, "new view",
        "the older view cannot become newer by finishing or committing late"
    );
    // A separate binding's logical rows share the excision guard.
    let row:uuid::Uuid=sqlx::query_scalar("SELECT m.id FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding='binding-a' AND m.manifest->>'role'='records'").fetch_one(&mut ctx.read_conn).await.unwrap();
    assert!(
        sinex_store::excision::plan(
            &mut ctx.read_conn,
            sinex_store::excision::Target::Material(sinex_model::MaterialId(row))
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("database lineage")
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn sealed_sqlite_image_and_row_keys_commit_together_without_plain_digests() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let _provider = database(&path);
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let mut r = reader(&path, Class::Sealed);
    let acquired = poll(r.as_mut(), &Value::Null);
    let identity = sinex_store::sealed::Identity::generate();
    let sink = SealingSink::new(ctx.blobs.clone(), identity.to_public());
    let source = atuin::source();
    let prepared = drivers::prepare(
        &source,
        "sealed-images",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap();
    let mut batches: Vec<_> = prepared.batches().unwrap().map(Result::unwrap).collect();
    sink.seal(&mut batches).unwrap();
    let sinex_model::BatchKind::Source(progress) = &batches[0].kind else {
        panic!()
    };
    let published = progress.image.as_ref().unwrap().clone();
    assert_eq!(progress.material_keys.len(), 1);
    assert_ne!(
        progress.material_keys[0].key_ref,
        published.key.as_ref().unwrap().key_ref
    );
    assert!(published.metadata.full);
    assert!(ctx.blobs.read(&published.material.blob).is_err());
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.material_keys")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(keys, 0);
    sqlx::query("CREATE FUNCTION reject_sealed_image() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic sealed image failure'; END $$").execute(&mut ctx.read_conn).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_sealed_image BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION reject_sealed_image()").execute(&mut ctx.read_conn).await.unwrap();
    assert!(
        ctx.writer
            .submit(crate::writer::Priority::Bulk, batches[0].clone())
            .await
            .is_err()
    );
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.material_keys")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(keys, 0);
    sqlx::query("DROP TRIGGER reject_sealed_image ON core.events")
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    for batch in batches {
        admit(&mut ctx, batch).await;
    }
    let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.material_keys")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(keys, 2);
    let wrapped = published.key.unwrap();
    let key = sinex_store::sealed::DataKey {
        key_ref: wrapped.key_ref,
        key: sinex_store::sealed::unwrap(&wrapped.wrapped, &identity).unwrap(),
    };
    let mut input = ctx
        .blobs
        .read_sealed(
            &published.material.blob,
            Some(published.material.length),
            &key,
            crate::sealed_access::read_limits(published.material.length).unwrap(),
            || Ok(()),
        )
        .unwrap();
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes).unwrap();
    let meta = published.metadata;
    assert_eq!(
        read_image(&bytes[..meta.image_bytes as usize], dir.path()),
        "second"
    );
    assert_ne!(
        meta.image_hash,
        format!(
            "blake3:{}",
            blake3::hash(&bytes[..meta.image_bytes as usize]).to_hex()
        )
    );
    assert_ne!(
        &bytes[meta.map_offset as usize..meta.map_offset as usize + 32],
        blake3::hash(&bytes[..meta.page_size as usize]).as_bytes()
    );
    // Locked restart remains a refusal; it cannot borrow the test's identity.
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "sealed-images")
        .await
        .unwrap()
        .unwrap();
    assert!(
        stored_chain(&mut ctx.read_conn, &ctx.blobs, &cursor)
            .await
            .is_err()
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn empty_sqlite_table_can_commit_an_image_without_fabricating_a_row_or_recapturing_each_poll()
{
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let provider = database(&path);
    provider.execute_batch("DELETE FROM history;").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let mut r = reader(&path, Class::Guarded);
    let acquired = poll(r.as_mut(), &Value::Null);
    assert!(acquired.material.is_none());
    assert!(acquired.image.is_some());
    let batches = submit(&mut ctx, "empty-images", acquired, 1).await;
    assert_eq!(batches.len(), 1);
    assert!(batches[0].records.is_empty());
    admit(&mut ctx, batches.into_iter().next().unwrap()).await;
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "empty-images")
        .await
        .unwrap()
        .unwrap();
    assert!(!cursor["image"].is_null());
    let next = poll(r.as_mut(), &cursor);
    assert!(next.image.is_none());
    assert!(next.material.is_none());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    let images: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.sqlite_images")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(images, 1);
    ctx.close().await.unwrap();
}
