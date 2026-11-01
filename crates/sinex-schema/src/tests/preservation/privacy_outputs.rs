use super::*;

/// A database that ran the emptied privacy-output and model-accounting steps
/// loses only what they created and ends equal to a fresh one.
#[devkit::test]
async fn removing_privacy_outputs_keeps_events_and_rules(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    for (position, class) in [(1_i64, "open"), (2, "guarded")] {
        sqlx::query("INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class) VALUES (uuidv7(),'command.imported','test',uuidv7(),uuidv7(),$1,'unknown',$1,'{\"command\":\"ls\"}',1,decode(repeat('01',32),'hex'),$2)")
            .bind(position)
            .bind(class)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    sqlx::raw_sql(include_str!("../privacy_outputs.sql"))
        .execute(&mut conn)
        .await
        .unwrap();
    conn.execute("INSERT INTO privacy.rules (id,name,source,matcher,class,action,author) VALUES (uuidv7(),'synthetic','test','all','guarded','classify','synthetic-test')")
        .await
        .unwrap();
    let events: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: VERSION - 1,
            to: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT to_jsonb(e) FROM core.events e ORDER BY id"
        )
        .fetch_all(&mut conn)
        .await
        .unwrap(),
        events
    );
    let rules: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.rules")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(rules, 1);
    let counter: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.counters WHERE name='privacy_outputs'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(counter, 0);
    let payloads: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM core.visible_events ORDER BY id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        payloads,
        [
            serde_json::json!({"command": "ls"}),
            serde_json::json!({"withheld": "guarded"})
        ]
    );
}
