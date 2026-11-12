use crate::{Applied, VERSION, apply, catalog};
use sqlx::{Connection, Executor, PgConnection};

async fn predecessor(conn: &mut PgConnection) {
    conn.execute("DROP INDEX core.ingress_frames_material, core.ingress_frames_refused_at, core.ingress_frames_malformed_producer")
        .await.unwrap();
    sqlx::query("UPDATE sinex_schema.applied SET version=$1")
        .bind(crate::version_before("receipt_read_indexes").unwrap())
        .execute(conn)
        .await
        .unwrap();
}

async fn populate(conn: &mut PgConnection) {
    conn.execute("INSERT INTO core.ingress_frames(producer,frame,material_id,outcome,refusal,family,source_name,source_version)
        SELECT 'synthetic','frame-'||n,CASE WHEN n%3=0 THEN NULL ELSE uuidv7() END,
          CASE WHEN n%2=0 THEN '{\"result\":\"admitted\"}'::jsonb ELSE '{\"result\":\"refused\",\"kind\":\"malformed\",\"source\":null,\"event_types\":[]}'::jsonb END,
          CASE WHEN n%2=0 THEN NULL ELSE 'synthetic refusal' END,
          CASE WHEN n%3=0 THEN NULL ELSE 'synthetic' END,
          CASE WHEN n%3=0 THEN NULL ELSE 'example.source' END,
          CASE WHEN n%3=0 THEN NULL ELSE 1 END
        FROM generate_series(1,64)n").await.unwrap();
}

async fn rows(conn: &mut PgConnection) -> Vec<(serde_json::Value, String)> {
    sqlx::query_as("SELECT to_jsonb(f),ctid::text FROM core.ingress_frames f ORDER BY number")
        .fetch_all(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn receipt_index_upgrade_preserves_every_value_and_row_location(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    populate(&mut conn).await;
    let fresh = catalog(&mut conn).await.unwrap();
    let before = rows(&mut conn).await;
    predecessor(&mut conn).await;
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: crate::version_before("receipt_read_indexes").unwrap(),
            to: VERSION
        }
    );
    assert_eq!(rows(&mut conn).await, before);
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
}

#[devkit::test]
async fn a_failed_last_index_rolls_back_the_whole_upgrade_and_restarts(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    populate(&mut conn).await;
    let fresh = catalog(&mut conn).await.unwrap();
    let before = rows(&mut conn).await;
    predecessor(&mut conn).await;
    conn.execute("CREATE INDEX ingress_frames_malformed_producer ON core.ingress_frames(frame)")
        .await
        .unwrap();
    let failed_catalog = catalog(&mut conn).await.unwrap();
    assert!(apply(&mut conn).await.is_err());
    assert_eq!(catalog(&mut conn).await.unwrap(), failed_catalog);
    assert_eq!(rows(&mut conn).await, before);
    conn.execute("DROP INDEX core.ingress_frames_malformed_producer")
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    apply(&mut conn).await.unwrap();
    assert_eq!(rows(&mut conn).await, before);
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
}
