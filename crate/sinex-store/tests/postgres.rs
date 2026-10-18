//! Runs only against the throwaway PostgreSQL 18 database that `xtask test` provides.
#[path = "support/event_contract.rs"]
mod event_contract;
use sinex_store::{
    Error, MaterialEvent, RuntimeIdentity, Store, bootstrap, check_schema,
    prepare_admission_statements,
};
use sqlx::{Connection, Executor, PgConnection, Row};

#[tokio::test]
#[ignore = "needs the PostgreSQL roles, URLs and blob root that xtask test provides"]
async fn roles_writer_lock_synthetic_admission_and_schema_checks()
-> Result<(), Box<dyn std::error::Error>> {
    let admin = std::env::var("SINEX_TEST_ADMIN_URL")?;
    let writer = std::env::var("SINEX_TEST_WRITER_URL")?;
    let reader = std::env::var("SINEX_TEST_READER_URL")?;
    let state = std::path::PathBuf::from(std::env::var("SINEX_TEST_STORE_DIR")?);
    let first = bootstrap(&admin).await?;
    assert_eq!(first.fingerprint, bootstrap(&admin).await?.fingerprint);
    prepare_admission_statements(&admin).await?;
    let mut read_conn = PgConnection::connect(&reader).await?;
    let settings_error = read_conn
        .execute("SHOW shared_preload_libraries")
        .await
        .expect_err("reader must not receive broad settings visibility");
    assert_eq!(
        settings_error
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("42501")
    );
    let preloaded: bool = sqlx::query_scalar("SELECT sinex_schemas.timescale_preloaded()")
        .fetch_one(&mut read_conn)
        .await?;
    assert!(preloaded);
    check_schema(&reader).await?;

    for statement in [
        "UPDATE core.commit_counter SET seq=seq+1 WHERE one",
        "DELETE FROM core.events",
        "INSERT INTO raw.source_cursors (binding,input,position,carry) VALUES ('synthetic.denied','fixture','{}','{}')",
    ] {
        let error = read_conn
            .execute(statement)
            .await
            .expect_err("reader must be denied");
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("42501")
        );
    }
    let mut writer_conn = PgConnection::connect(&writer).await?;
    let error = writer_conn
        .execute("UPDATE sinex_schemas.definition SET catalog='{}'::jsonb")
        .await
        .expect_err("writer cannot rewrite the stored catalog");
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501")
    );
    writer_conn.close().await?;
    let store = Store::connect(&writer, &reader, state.clone(), 2, identity()?, None).await?;
    assert!(matches!(
        Store::connect(&writer, &reader, state.clone(), 2, identity()?, None).await,
        Err(Error::WriterOwned)
    ));
    let receipt = store
        .admission
        .submit_synthetic(MaterialEvent {
            source: "synthetic.foundation".into(),
            event_type: "synthetic.observed".into(),
            bytes: b"synthetic retained evidence".to_vec(),
            anchor_byte: 0,
            ordinal: 0,
            payload: serde_json::json!({"fixture":"foundation"}),
        })
        .await?;
    assert_eq!(receipt.event_id.get_version_num(), 7);
    let row = sqlx::query("SELECT e.id,e.ts_orig IS NULL AS unknown_time,e.ts_quality,e.ts_coided::text AS minted,e.ts_persisted::text AS persisted,e.commit_seq,p.position,m.total_bytes,b.blake3,l.event_id AS logged,r.effective_config_hash,manifest.artifact_digest FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id JOIN core.blobs b ON b.id=m.blob_id AND b.status='present' JOIN raw.source_cursors p ON (p.position->>'material')::uuid=m.id JOIN core.commit_log l ON l.commit_seq=e.commit_seq JOIN core.runs r ON r.id=e.run_id JOIN core.manifests manifest ON manifest.id=r.manifest_id WHERE e.id=$1").bind(receipt.event_id).fetch_one(&mut read_conn).await?;
    assert_eq!(row.get::<String, _>("ts_quality"), "unknown");
    assert!(row.get::<bool, _>("unknown_time"));
    assert!(!row.get::<String, _>("minted").is_empty());
    assert!(!row.get::<String, _>("persisted").is_empty());
    assert_eq!(row.get::<i64, _>("commit_seq"), receipt.commit_seq);
    let position: serde_json::Value = row.get("position");
    assert_eq!(
        position["byte_offset"].as_i64(),
        Some(row.get::<i64, _>("total_bytes"))
    );
    assert_eq!(row.get::<uuid::Uuid, _>("logged"), receipt.event_id);
    let blobs = hashed_blobs::Store::open(state.clone(), vec![], 0o600)?;
    use std::io::Read;
    let mut bytes = Vec::new();
    blobs
        .read(&hashed_blobs::Hash::of(b"synthetic retained evidence"))?
        .read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"synthetic retained evidence");
    let closed = store.admission.clone();
    store.shutdown().await?;
    assert!(
        closed
            .submit_synthetic(MaterialEvent {
                source: "synthetic.closed".into(),
                event_type: "synthetic.observed".into(),
                bytes: vec![0],
                anchor_byte: 0,
                ordinal: 0,
                payload: serde_json::json!({})
            })
            .await
            .is_err()
    );
    let restarted = Store::connect(&writer, &reader, state, 2, identity()?, None).await?;
    restarted.shutdown().await?;
    let mut helper_admin = PgConnection::connect(&admin).await?;
    helper_admin
        .execute("GRANT EXECUTE ON FUNCTION sinex_schemas.timescale_preloaded() TO PUBLIC")
        .await?;
    let acl_drift = check_schema(&reader).await;
    helper_admin
        .execute("REVOKE EXECUTE ON FUNCTION sinex_schemas.timescale_preloaded() FROM PUBLIC")
        .await?;
    assert!(matches!(acl_drift, Err(Error::Schema(_))));
    check_schema(&reader).await?;
    let original: String = sqlx::query_scalar(
        "SELECT pg_get_functiondef('sinex_schemas.timescale_preloaded()'::regprocedure)",
    )
    .fetch_one(&mut helper_admin)
    .await?;
    helper_admin.execute("CREATE OR REPLACE FUNCTION sinex_schemas.timescale_preloaded() RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS 'SELECT 1/0=0'").await?;
    let body_drift = check_schema(&reader).await;
    helper_admin.execute(original.as_str()).await?;
    // A division-by-zero database error would mean the changed function body ran.
    assert!(matches!(body_drift, Err(Error::Schema(_))));
    check_schema(&reader).await?;
    // PostgreSQL 18 records NOT NULL in pg_constraint as well as attnotnull. The
    // constraint check skips those rows, so the column check must catch a dropped NOT NULL.
    let mut admin_conn = PgConnection::connect(&admin).await?;
    admin_conn
        .execute("ALTER TABLE core.events ALTER COLUMN payload DROP NOT NULL")
        .await?;
    let nullability = check_schema(&admin).await;
    admin_conn
        .execute("ALTER TABLE core.events ALTER COLUMN payload SET NOT NULL")
        .await?;
    assert!(matches!(nullability, Err(Error::Schema(_))));
    check_schema(&admin).await?;
    // A space inside a SQL literal changes its meaning even when stripping all
    // whitespace would make the old and new constraint texts look identical.
    admin_conn.execute("ALTER TABLE core.blobs DROP CONSTRAINT blob_status, ADD CONSTRAINT blob_status CHECK (status = 'writing '::text OR status = 'present'::text)").await?;
    let literal_drift = check_schema(&admin).await;
    admin_conn.execute("ALTER TABLE core.blobs DROP CONSTRAINT blob_status, ADD CONSTRAINT blob_status CHECK (status = 'writing'::text OR status = 'present'::text)").await?;
    assert!(matches!(literal_drift, Err(Error::Schema(_))));
    check_schema(&admin).await?;
    // An extra column is reported, not silently accepted.
    admin_conn
        .execute("ALTER TABLE core.events ADD COLUMN undeclared_fixture text")
        .await?;
    let checked = check_schema(&admin).await;
    admin_conn
        .execute("ALTER TABLE core.events DROP COLUMN undeclared_fixture")
        .await?;
    assert!(matches!(checked, Err(Error::Schema(_))));
    check_schema(&admin).await?;
    event_contract::verify(&admin, &reader, receipt.event_id, receipt.material_id).await?;
    Ok(())
}

fn identity() -> Result<RuntimeIdentity, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut exe = std::fs::File::open(std::env::current_exe()?)?;
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 65536];
    loop {
        let count = exe.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(RuntimeIdentity {
        service_name: "synthetic-foundation-test".into(),
        host: "synthetic-host".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        executable_blake3: format!("blake3:{}", hash.finalize().to_hex()),
        config_blake3: format!(
            "blake3:{}",
            blake3::hash(b"synthetic fixed test configuration").to_hex()
        ),
    })
}
