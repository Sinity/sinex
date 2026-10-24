use super::*;
use sqlx::Executor;

// Restore the published predecessor representation and note definitions so
// explicit application exercises the complete ordered upgrade.
async fn predecessor(conn: &mut PgConnection) {
    conn.execute("DROP TABLE raw.pending_native_materials; ALTER TABLE raw.materials DROP COLUMN erased_replay_blob; ALTER TABLE privacy.material_keys DROP COLUMN role; DELETE FROM sinex_schema.applied WHERE version=3").await.unwrap();
    let old: Value = serde_json::from_str(include_str!(
        "../../../../sinex-schema/src/tests/v0-declarations.json"
    ))
    .unwrap();
    let guard = old["statements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .find(|s| s.contains("CREATE FUNCTION core.event_guard"))
        .unwrap();
    conn.execute(
        guard
            .replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
            .as_str(),
    )
    .await
    .unwrap();
    let row: (String, Value, String, Vec<String>, String) = serde_json::from_str(include_str!(
        "../../../../sinex-schema/src/changes/notes/v1-registry.json"
    ))
    .unwrap();
    sqlx::query("UPDATE registry.event_types SET payload_schema=$1,key_kind=$2,key_fields=$3,replace_rule=$4 WHERE name=$5")
        .bind(row.1).bind(row.2).bind(row.3).bind(row.4).bind(row.0)
        .execute(&mut *conn).await.unwrap();
    conn.execute("DROP VIEW event_types.note_document")
        .await
        .unwrap();
    let view = include_str!("../../../../sinex-schema/src/changes/notes/v1-view.sql");
    conn.execute(format!("CREATE VIEW event_types.note_document AS {view}").as_str())
        .await
        .unwrap();
    conn.execute("GRANT SELECT ON event_types.note_document TO sinex_reader,sinex_writer; DELETE FROM sinex_schema.applied WHERE version=2; INSERT INTO core.blobs(hash,size,status) VALUES('blake3:0000000000000000000000000000000000000000000000000000000000000000',17,'present')").await.unwrap();
}

async fn state(conn: &mut PgConnection) -> Vec<Value> {
    sqlx::query_scalar("SELECT to_jsonb(t) FROM sinex_schema.applied t UNION ALL SELECT to_jsonb(t) FROM registry.event_types t UNION ALL SELECT to_jsonb(t) FROM core.blobs t ORDER BY 1")
        .fetch_all(conn).await.unwrap()
}

async fn refuse_old(derive: bool) {
    let (_db, url, mut conn) = crate::testdb::fresh().await;
    predecessor(&mut conn).await;
    let before = state(&mut conn).await;
    let store = tempfile::tempdir().unwrap();
    let error = if derive {
        match crate::local_driver(&url, "startup-fixture", Some(store.path())).await {
            Err(error) => Some(error),
            Ok((driver, task)) => {
                drop(driver);
                task.await.unwrap();
                None
            }
        }
    } else {
        match OneShot::open(&url, store.path()).await {
            Err(error) => Some(error),
            Ok(ctx) => {
                ctx.close().await.unwrap();
                None
            }
        }
    };
    assert!(
        error
            .as_deref()
            .is_some_and(|e| e.contains("version 1") && e.contains("apply explicitly")),
        "ordinary startup must refuse the actual predecessor: {error:?}"
    );
    assert_eq!(state(&mut conn).await, before);
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 1,
            version: sinex_schema::VERSION
        }
    );
    let ctx = OneShot::open(&url, store.path()).await.unwrap();
    ctx.close().await.unwrap();
    let (driver, task) = crate::local_driver(&url, "startup-fixture", Some(store.path()))
        .await
        .unwrap();
    drop(driver);
    task.await.unwrap();
}

#[tokio::test]
async fn one_shot_refuses_an_upgradeable_predecessor_without_applying_it() {
    refuse_old(false).await;
}

#[tokio::test]
async fn derivation_refuses_an_upgradeable_predecessor_without_applying_it() {
    refuse_old(true).await;
}

#[tokio::test]
async fn derivation_claims_the_writer_for_its_entire_lifetime() {
    let (_db, url, mut competing) = crate::testdb::fresh().await;
    let (driver, task) = crate::local_driver(&url, "startup-fixture", None)
        .await
        .unwrap();
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .fetch_one(&mut competing)
        .await
        .unwrap();
    if acquired {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .execute(&mut competing)
            .await
            .unwrap();
    }
    drop(driver);
    task.await.unwrap();
    assert!(
        !acquired,
        "the derivation writer must retain its advisory claim"
    );
    writer::claim(&mut competing).await.unwrap();
    let startup = crate::local_driver(&url, "startup-fixture", None);
    tokio::pin!(startup);
    tokio::select! {
        result = &mut startup => {
            if let Ok((driver,task))=result { drop(driver);task.await.unwrap(); }
            panic!("standalone derivations must wait for the active writer");
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    drop(competing);
    let (driver, task) = tokio::time::timeout(std::time::Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap();
    drop(driver);
    task.await.unwrap();
}

#[tokio::test]
async fn empty_database_needs_explicit_initialization_before_ordinary_startup() {
    let (_db, url, mut conn) = crate::testdb::fresh().await;
    conn.execute("DROP SCHEMA sinex_schema,core,audit,raw,registry,derivation,privacy,telemetry,event_types,analysis CASCADE").await.unwrap();
    let store = tempfile::tempdir().unwrap();
    let refused = match OneShot::open(&url, store.path()).await {
        Err(_) => true,
        Ok(ctx) => {
            ctx.close().await.unwrap();
            false
        }
    };
    assert!(
        refused,
        "ordinary startup must not initialize an empty database"
    );
    assert!(
        crate::local_driver(&url, "startup-fixture", None)
            .await
            .is_err()
    );
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='sinex_schema')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(!exists);
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Created {
            version: sinex_schema::VERSION
        }
    );
    let ctx = OneShot::open(&url, store.path()).await.unwrap();
    ctx.close().await.unwrap();
}
