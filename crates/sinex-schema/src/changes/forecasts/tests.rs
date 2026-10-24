use super::*;
use crate::{Applied, VERSION, catalog};
use sqlx::Connection;

async fn version_three(conn: &mut PgConnection) {
    crate::tests::upgrades::version_zero(conn).await;
    super::super::version_two(conn).await;
    super::super::sealed_erasure(conn).await.unwrap();
    conn.execute("INSERT INTO sinex_schema.applied(version) VALUES(3)")
        .await
        .unwrap();
}

#[tokio::test]
async fn forecast_upgrade_adds_only_declarations_and_reader_views() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    version_three(&mut conn).await;
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,ts_quality,parents,commit_seq,payload,reading_order,record_hash) VALUES('019a1234-1234-7000-8000-000000000001','command.executed','fixture','00000000-0000-0000-0000-000000000002','unknown',ARRAY['019a1234-1234-7000-8000-000000000003'::uuid],1,'{\"command\":\"retained exact text\"}',1,decode(repeat('ab',32),'hex'))").await.unwrap();
    let before: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 3,
            version: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(before, after);
    conn.execute("SET ROLE sinex_reader").await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM event_types.forecast_issued")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    conn.execute("RESET ROLE").await.unwrap();
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
}

#[tokio::test]
async fn forecast_upgrade_refuses_colliding_declarations_atomically() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_three(&mut conn).await;
    conn.execute("CREATE VIEW event_types.forecast_resolved AS SELECT 'external'::text AS value")
        .await
        .unwrap();
    let before = catalog(&mut conn).await.unwrap();
    assert!(matches!(
        crate::apply(&mut conn).await,
        Err(Error::Shape(3))
    ));
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        3
    );
}
