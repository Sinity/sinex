use super::*;
use crate::{Applied, VERSION, catalog};
use sqlx::Connection;

async fn version_four(conn: &mut PgConnection) {
    crate::tests::upgrades::version_zero(conn).await;
    super::super::version_two(conn).await;
    super::super::sealed_erasure(conn).await.unwrap();
    conn.execute("INSERT INTO sinex_schema.applied(version) VALUES(3)")
        .await
        .unwrap();
    super::super::forecasts::apply(conn).await.unwrap();
    conn.execute("INSERT INTO sinex_schema.applied(version) VALUES(4)")
        .await
        .unwrap();
}

#[tokio::test]
async fn producer_custody_upgrade_matches_fresh_catalog_and_preserves_existing_archive_rows() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    version_four(&mut conn).await;
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,ts_quality,parents,commit_seq,payload,reading_order,record_hash) VALUES('019a1234-1234-7000-8000-000000000001','command.executed','fixture','00000000-0000-0000-0000-000000000002','unknown',ARRAY['019a1234-1234-7000-8000-000000000003'::uuid],1,'{\"command\":\"retained exact text\"}',1,decode(repeat('ab',32),'hex')); INSERT INTO core.ingress_frames(producer,frame,admitted,outcome) VALUES('fixture','old-ack',ARRAY['019a1234-1234-7000-8000-000000000001'::uuid],'{\"admitted\":1}'); INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key) VALUES('00000000-0000-0000-0000-000000000004','legacy-key','sealed',decode('00ff10','hex'))").await.unwrap();
    let before:Vec<serde_json::Value>=sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e UNION ALL SELECT to_jsonb(f) FROM core.ingress_frames f UNION ALL SELECT to_jsonb(k) FROM privacy.material_keys k ORDER BY 1")
        .fetch_all(&mut conn).await.unwrap();
    assert!(matches!(
        crate::require_current(&mut conn).await,
        Err(crate::Error::Upgrade { found: 4, .. })
    ));
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 4,
            version: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let after:Vec<serde_json::Value>=sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e UNION ALL SELECT to_jsonb(f) FROM core.ingress_frames f UNION ALL SELECT to_jsonb(k) FROM privacy.material_keys k ORDER BY 1")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
    conn.execute("SET ROLE sinex_writer").await.unwrap();
    conn.execute("INSERT INTO raw.pending_producer_bytes(producer,frame,declaration) VALUES('fixture','new','{}'); DELETE FROM raw.pending_producer_bytes WHERE producer='fixture'").await.unwrap();
}

#[tokio::test]
async fn producer_upgrade_refuses_a_colliding_pending_owner_without_partial_ddl_or_version_change()
{
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_four(&mut conn).await;
    conn.execute("CREATE TABLE raw.pending_producer_bytes(unrelated text)")
        .await
        .unwrap();
    let before = catalog(&mut conn).await.unwrap();
    assert!(matches!(
        crate::apply(&mut conn).await,
        Err(crate::Error::Shape(4))
    ));
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        4
    );
    let mut other = PgConnection::connect(&db.url(None)).await.unwrap();
    other.execute("SET lock_timeout='500ms'").await.unwrap();
    // A standalone transaction can take the schema compatibility lock after refusal.
    let mut tx = other.begin().await.unwrap();
    tx.execute("LOCK TABLE sinex_schema.applied IN ACCESS SHARE MODE")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
}
