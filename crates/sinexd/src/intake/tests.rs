use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::forecast::{ForecastRecord, Issued};
use sinex_sources::sources::forecast;

fn acquisition(id: &str) -> drivers::Acquired {
    let at = Timestamp::from_unix_secs(1_700_000_000).unwrap();
    forecast::request_material(
        ForecastRecord::Issued(Issued {
            forecast: id.into(),
            target: "synthetic event".into(),
            probability: 0.5,
            criteria: "synthetic completion".into(),
            horizon: Timestamp::from_unix_secs(1_800_000_000).unwrap(),
            information_cutoff: at,
            information: Vec::new(),
            information_position: 0,
            by: "untrusted author".into(),
            at,
        }),
        "operator",
        "synthetic",
        at,
    )
}

#[devkit::test]
async fn request_refuses_all_records_when_one_does_not_parse(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let mut acquired = acquisition("first");
    let material = acquired.material.as_mut().unwrap();
    let start = material.bytes.len();
    material
        .bytes
        .extend_from_slice(b"{\"record\":\"not_a_forecast\"}\n");
    material.records.push((start, material.bytes.len()));
    let error = request(
        &mut ctx.read_conn,
        &ctx.writer,
        &ctx.blobs,
        Arc::new(forecast::request_source()),
        "synthetic-request",
        acquired,
    )
    .await
    .err()
    .unwrap();
    assert!(error.contains("failed to read back"), "{error}");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "synthetic-request")
            .await
            .unwrap()
            .is_none()
    );
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn request_commits_its_records_together_with_the_source_class(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let mut acquired = acquisition("first");
    let second = acquisition("second").material.unwrap();
    let material = acquired.material.as_mut().unwrap();
    let start = material.bytes.len();
    material.bytes.extend(second.bytes);
    material.records.push((start, material.bytes.len()));
    let submitted = request(
        &mut ctx.read_conn,
        &ctx.writer,
        &ctx.blobs,
        Arc::new(forecast::request_source()),
        "synthetic-request",
        acquired,
    )
    .await
    .unwrap();
    assert_eq!(submitted.outcome.admitted, 2);
    let rows: Vec<(uuid::Uuid, String, serde_json::Value)> =
        sqlx::query_as("SELECT material_id, class::text, payload FROM core.events ORDER BY anchor")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    for (material, class, payload) in rows {
        assert_eq!(Some(MaterialId(material)), submitted.material);
        assert_eq!(class, "guarded");
        assert_eq!(payload["by"], "operator");
    }
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn submission_checks_again_inside_the_writer(db: devkit::pg::TestDatabase) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(&db.url(None), dir.path())
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let check_calls = calls.clone();
    let material = crate::sealing::Materials::new(
        ctx.blobs.clone(),
        Class::Guarded,
        crate::sealing::Access::default(),
    )
    .unwrap();
    let result = Intake {
        writer: &ctx.writer,
        source: &forecast::request_source(),
        binding: "synthetic-check",
        run: RunId::new(),
        material: &material,
        check: Arc::new(move || {
            if check_calls.fetch_add(1, Ordering::SeqCst) >= 3 {
                Err("capture stopped".into())
            } else {
                Ok(())
            }
        }),
        priority: Priority::Bulk,
        atomic: false,
        registered: None,
    }
    .submit(acquisition("first"), Value::Null, &mut Totals::default())
    .await;
    assert!(result.err().unwrap().contains("capture stopped"));
    assert!(calls.load(Ordering::SeqCst) >= 4);
    assert!(
        sinex_store::positions::load(&mut ctx.read_conn, "synthetic-check")
            .await
            .unwrap()
            .is_none()
    );
    ctx.close().await.unwrap();
}
