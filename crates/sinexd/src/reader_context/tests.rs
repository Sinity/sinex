use super::*;
use crate::material_access::Sink;
use serde_json::Value;
use sinex_source_core::{Limits, drivers};

#[devkit::test]
async fn directory_resumes_from_admitted_cursor_with_original_bytes(db: devkit::pg::TestDatabase) {
    use sinex_model::{BatchKind, RunId};
    use sinex_source_core::binding;
    use sinex_sources::sources::notes;
    let url = db.url(None);
    let dir = std::env::temp_dir().join(format!("sinex-directory-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let root = dir.join("vault");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("a.md"), "").unwrap();
    std::fs::write(root.join("b.md"), "beta").unwrap();
    let mut ctx = ReaderContext::open(
        &url,
        &dir.join("store"),
        sinex_model::time_settings::TimeSettings::operator_default(),
    )
    .await
    .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let source = notes::source();
    let mut reader = binding::walked(notes::driver(root.clone(), "synthetic".into()))
        .into_polled()
        .unwrap();
    let acquired = reader.poll(&Value::Null).unwrap();
    let material = acquired.material.as_ref().unwrap();
    let id = material.id;
    let first_end = material.records[0].1;
    let last_end = material.records.last().unwrap().1;
    let Some(drivers::StoredBytes::FramedFile(file)) = &material.stored else {
        panic!()
    };
    let mut original = vec![0; file.metadata().unwrap().len() as usize];
    std::os::unix::fs::FileExt::read_exact_at(&**file, &mut original, 0).unwrap();
    let prepared = drivers::prepare(
        &source,
        "synthetic-vault",
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
    let first = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = &first.kind else {
        panic!()
    };
    let blob = progress.material.as_ref().unwrap().blob.clone();
    let expected_cursor = progress.cursor.clone();
    ctx.writer
        .submit(crate::writer::Priority::Bulk, first)
        .await
        .unwrap();
    drop(prepared);
    let (cursor, carry) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-vault")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor, expected_cursor);
    assert_eq!(cursor["through"], first_end);
    assert_eq!(ctx.blobs.read(&blob).unwrap(), original);
    std::fs::write(root.join("b.md"), "later bytes").unwrap();
    let acquired = reader.poll(&cursor).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().id, id);
    let prepared = drivers::prepare(
        &source,
        "synthetic-vault",
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
        "SELECT payload FROM core.events WHERE event_type='note.document' ORDER BY anchor",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0]["text"], "");
    assert_eq!(payloads[1]["text"], "beta");
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, "synthetic-vault")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor["through"], last_end);
    let next = reader.poll(&cursor).unwrap();
    assert_ne!(next.material.as_ref().unwrap().id, id);
    ctx.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

mod sqlite_rows;

mod startup;
