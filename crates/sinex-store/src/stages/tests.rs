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
    let inventory = Inventory {
        files: vec![sinex_model::stage::File {
            path: "/synthetic/export.zip".into(),
            container: Some(sinex_model::stage::Container::Zip),
            size: Some(5),
            reading: sinex_model::stage::FileReading::Unparsed { candidates: vec![] },
            members: vec![],
            products: Default::default(),
            material: None,
        }],
    };
    begin(&mut conn, operation, "operator", &scope, &inventory)
        .await
        .unwrap();
    let running = list(&mut conn, true).await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].operation, operation);
    assert_eq!(running[0].result, None);
    assert_eq!(running[0].counts, None);
    assert_eq!(running[0].scope, scope);
    let committed = Checkpoint {
        accounting: sinex_model::Accounting::default(),
    };
    checkpoint(&mut conn, operation, "synthetic", &committed)
        .await
        .unwrap();
    progress(&mut conn, operation, &inventory).await.unwrap();
    let saved = show(&mut conn, operation).await.unwrap().unwrap();
    assert_eq!(saved.inventory.checkpoints["synthetic"], committed);
    let counts = Counts {
        files: 1,
        stored_without_parser: 0,
        accounting: sinex_model::Accounting::default(),
    };
    let failures = vec!["synthetic source failed".to_owned()];
    end(
        &mut conn,
        operation,
        OperationResult::StoppedEarly,
        &counts,
        &inventory,
        &failures,
    )
    .await
    .unwrap();
    assert!(list(&mut conn, true).await.unwrap().is_empty());
    let shown = show(&mut conn, operation).await.unwrap().unwrap();
    assert_eq!(shown.result, Some(OperationResult::StoppedEarly));
    assert_eq!(shown.counts, Some(counts.clone()));
    assert_eq!(shown.inventory.inventory, inventory);
    assert_eq!(shown.inventory.checkpoints["synthetic"], committed);
    assert_eq!(shown.failures, failures);
    let report: sinex_model::stage::Report =
        serde_json::from_value(serde_json::to_value(&shown).unwrap()).unwrap();
    assert_eq!(report.failures, failures);
    assert_eq!(report.counts, Some(counts.clone()));
    assert!(shown.end_seq.is_some());
    assert!(progress(&mut conn, operation, &inventory).await.is_err());
    reopen(&mut conn, operation).await.unwrap();
    end(
        &mut conn,
        operation,
        OperationResult::Ok,
        &counts,
        &inventory,
        &[],
    )
    .await
    .unwrap();
    let ended = show(&mut conn, operation).await.unwrap().unwrap();
    assert!(ended.inventory.checkpoints.is_empty());
    assert!(ended.failures.is_empty());
    assert!(show(&mut conn, OperationId::new()).await.unwrap().is_none());
}

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
