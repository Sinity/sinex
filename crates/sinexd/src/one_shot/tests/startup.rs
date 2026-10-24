use super::*;
use sqlx::Executor;

// A database whose recorded schema version differs from this build's.
async fn other_version(conn: &mut PgConnection) {
    conn.execute("UPDATE sinex_schema.applied SET version = version + 1")
        .await
        .unwrap();
    conn.execute("INSERT INTO core.blobs(hash,size,status) VALUES('blake3:0000000000000000000000000000000000000000000000000000000000000000',17,'present')").await.unwrap();
}

async fn state(conn: &mut PgConnection) -> (Vec<String>, Vec<Value>) {
    let catalog = sinex_schema::catalog(conn).await.unwrap();
    let tables: Vec<(String,String)> = sqlx::query_as("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema=ANY($1) AND table_type='BASE TABLE' ORDER BY table_schema,table_name")
        .bind(sinex_schema::schemas()).fetch_all(&mut *conn).await.unwrap();
    let mut rows = Vec::new();
    for (schema, table) in tables {
        let relation = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        );
        let sql = format!(
            "SELECT jsonb_build_object('table',$1::text,'row',to_jsonb(t)) FROM {relation} t ORDER BY 1"
        );
        let data: Vec<Value> = sqlx::query_scalar(&sql)
            .bind(relation)
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        rows.extend(data);
    }
    (catalog, rows)
}

async fn refuse_other_version(derive: bool) {
    let (_db, url, mut conn) = crate::testdb::fresh().await;
    other_version(&mut conn).await;
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
            .is_some_and(|e| e.contains("schema version 1")),
        "ordinary startup must refuse another schema version: {error:?}"
    );
    assert_eq!(state(&mut conn).await, before);
    assert!(sinex_schema::apply(&mut conn).await.is_err());
    assert_eq!(state(&mut conn).await, before);
}

#[tokio::test]
async fn one_shot_refuses_another_schema_version_without_changing_it() {
    refuse_other_version(false).await;
}

#[tokio::test]
async fn derivation_refuses_another_schema_version_without_changing_it() {
    refuse_other_version(true).await;
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
