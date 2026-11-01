use super::*;
use serde_json::json;

async fn connect(db: &devkit::pg::TestDatabase) -> PgConnection {
    <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap()
}

fn unparsed(locator: &str, blob: &str) -> Unparsed {
    Unparsed {
        id: MaterialId::new(),
        binding: format!("stage:{locator}@fixture-host"),
        input: InputRef {
            kind: "file".into(),
            locator: locator.into(),
            host: "fixture-host".into(),
        },
        observation: ObservationFacts {
            size: Some(5),
            changed: true,
            ..ObservationFacts::default()
        },
        blob: blob.into(),
        length: 5,
        manifest: Manifest::from_json(json!({"path": locator, "candidates": ["first", "second"]})),
    }
}

#[devkit::test]
async fn a_stage_is_logged_from_begin_to_end(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let operation = OperationId::new();
    let scope = json!({"items": [{"path": "/synthetic/export.zip"}]});
    begin(
        &mut conn,
        operation,
        "operator",
        &scope,
        &json!({"files": []}),
    )
    .await
    .unwrap();
    let running = list(&mut conn, true).await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].operation, operation);
    assert_eq!(running[0].result, None);
    assert_eq!(running[0].scope, scope);

    progress(
        &mut conn,
        operation,
        &json!({"files": [{"path": "/synthetic/export.zip"}]}),
    )
    .await
    .unwrap();
    end(
        &mut conn,
        operation,
        OperationResult::Ok,
        &json!({"new": 3}),
        &json!({"files": ["done"]}),
        &json!([]),
    )
    .await
    .unwrap();
    assert!(list(&mut conn, true).await.unwrap().is_empty());
    let shown = show(&mut conn, operation).await.unwrap().unwrap();
    assert_eq!(shown.result.as_deref(), Some("ok"));
    assert_eq!(shown.counts, json!({"new": 3}));
    assert_eq!(shown.inventory, json!({"files": ["done"]}));
    assert!(shown.end_seq.is_some());
    // An ended stage takes no more progress.
    assert!(progress(&mut conn, operation, &json!({})).await.is_err());
    assert!(show(&mut conn, OperationId::new()).await.unwrap().is_none());
}

/// law: sx.stage.without-parser
#[devkit::test]
async fn an_unparsed_material_has_no_parser_and_keeps_its_candidates(db: devkit::pg::TestDatabase) {
    let mut conn = connect(&db).await;
    let blob = format!("blake3:{}", "ab".repeat(32));
    let material = unparsed("/synthetic/unknown.bin", &blob);
    let mut tx = sqlx::Connection::begin(&mut conn).await.unwrap();
    register_unparsed(&mut tx, &material).await.unwrap();
    tx.commit().await.unwrap();
    let row = sqlx::query!(
        r#"SELECT m.parser, m.parser_version, m.manifest, m.length, i.locator, r.kind AS revision
           FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id
           JOIN raw.revisions r ON r.id = m.revision_id WHERE m.id = $1"#,
        material.id.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(row.parser, None);
    assert_eq!(row.parser_version, None);
    assert_eq!(row.manifest["candidates"], json!(["first", "second"]));
    assert_eq!(row.length, 5);
    assert_eq!(row.locator, "/synthetic/unknown.bin");
    assert_eq!(row.revision, "snapshot");

    // A parser and its version come together or not at all.
    let half = sqlx::query(
        "INSERT INTO raw.materials (id, blob, length, manifest, parser) VALUES ($1, $2, 5, '{}', 'synthetic')",
    )
    .bind(MaterialId::new().0)
    .bind(&blob)
    .execute(&mut conn)
    .await;
    assert!(half.is_err(), "{half:?}");
}
