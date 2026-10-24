use super::*;
use crate::tests::upgrades::version_zero;
use serde_json::Value;

// Forward construction from frozen v0 and the unchanged released 0→1 step.
// Its note row/view were independently captured from the e9ca v1 binary.
async fn version_one(conn: &mut PgConnection) {
    version_zero(conn).await;
    let mut tx = conn.begin().await.unwrap();
    archival_history(&mut tx).await.unwrap();
    tx.execute("INSERT INTO sinex_schema.applied(version) VALUES(1)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn preserved(conn: &mut PgConnection) -> Vec<Value> {
    sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e UNION ALL SELECT to_jsonb(m)-'erased_replay_blob' FROM raw.materials m UNION ALL SELECT to_jsonb(k)-'role' FROM privacy.material_keys k UNION ALL SELECT to_jsonb(b) FROM core.blobs b UNION ALL SELECT to_jsonb(t) FROM registry.event_types t WHERE name NOT IN ('note.document','forecast.issued','forecast.resolved') ORDER BY 1")
        .fetch_all(conn).await.unwrap()
}

async fn seed_old_note(conn: &mut PgConnection) {
    conn.execute("INSERT INTO core.blobs(hash,size,status) VALUES('blake3:0000000000000000000000000000000000000000000000000000000000000000',17,'present'); INSERT INTO raw.materials(id,blob,durable_bytes,manifest,parser,parser_version) VALUES('00000000-0000-0000-0000-000000000001','blake3:0000000000000000000000000000000000000000000000000000000000000000',17,'{}','notes-vault',2); INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key) VALUES('00000000-0000-0000-0000-000000000002','neutral-key','sealed',decode('00ff10','hex')); INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,commit_seq,payload,reading_order,record_hash,ts_quality) VALUES('019a1234-1234-7000-8000-000000000001','note.document','notes-fixture','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000001',0,2,'{\"vault\":\"Neutral\",\"path\":\"a.md\",\"text\":\"retained text\",\"size\":13,\"modified_at\":null,\"vanished_at\":null}',1,decode(repeat('ab',32),'hex'),'unknown'); UPDATE core.counters SET value=2 WHERE name='commit_seq'").await.unwrap();
}

#[tokio::test]
async fn notes_upgrade_updates_only_its_registry_and_view_and_matches_fresh() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = crate::catalog(&mut conn).await.unwrap();
    version_one(&mut conn).await;
    seed_old_note(&mut conn).await;
    let before = preserved(&mut conn).await;
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 1,
            version: VERSION
        }
    );
    assert_eq!(preserved(&mut conn).await, before);
    assert_eq!(crate::catalog(&mut conn).await.unwrap(), fresh);
    let schema: Value = sqlx::query_scalar(
        "SELECT payload_schema FROM registry.event_types WHERE name='note.document'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let kind = crate::event_types::ALL
        .iter()
        .find(|k| k.event_type == "note.document")
        .unwrap();
    assert_eq!(schema, (kind.json_schema)());
    let identity:(String,Vec<String>,String)=sqlx::query_as("SELECT key_kind,key_fields,replace_rule FROM registry.event_types WHERE name='note.document'").fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        identity,
        (
            "fields".into(),
            vec!["vault".into(), "path".into()],
            "replace_when_newer".into()
        )
    );
    conn.execute("SET ROLE sinex_reader").await.unwrap();
    let fields: (Option<Value>, Option<Value>, Option<Value>, Option<Value>) =
        sqlx::query_as("SELECT title,note_id,front_matter,text FROM event_types.note_document")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        fields,
        (
            None,
            None,
            None,
            Some(Value::String("retained text".into()))
        )
    );
    conn.execute("RESET ROLE").await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT value FROM core.counters WHERE name='commit_seq'")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
    conn.execute("UPDATE registry.event_types SET payload_schema=jsonb_set(payload_schema,'{properties,operator_sentinel}','{\"type\":\"string\"}') WHERE name='note.document'").await.unwrap();
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
    assert!(sqlx::query_scalar::<_,bool>("SELECT payload_schema->'properties' ? 'operator_sentinel' FROM registry.event_types WHERE name='note.document'").fetch_one(&mut conn).await.unwrap());
}

#[tokio::test]
async fn notes_upgrade_refuses_unknown_metadata_and_preserves_external_view_dependencies() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_one(&mut conn).await;
    conn.execute(
        "UPDATE registry.event_types SET key_fields=ARRAY['unexpected'] WHERE name='note.document'",
    )
    .await
    .unwrap();
    assert!(matches!(
        crate::apply(&mut conn).await,
        Err(Error::Shape(1))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        1
    );
    conn.execute("UPDATE registry.event_types SET key_fields=ARRAY['vault','path'] WHERE name='note.document'; CREATE VIEW public.operator_notes AS SELECT path FROM event_types.note_document").await.unwrap();
    seed_old_note(&mut conn).await;
    let before: Vec<Option<Value>> = sqlx::query_scalar("SELECT path FROM public.operator_notes")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(before, vec![Some(Value::String("a.md".into()))]);
    let oid: u32 = sqlx::query_scalar::<_, sqlx::postgres::types::Oid>(
        "SELECT 'public.operator_notes'::regclass::oid",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap()
    .0;
    assert_eq!(
        crate::apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 1,
            version: VERSION
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Value>>("SELECT path FROM public.operator_notes")
            .fetch_all(&mut conn)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, sqlx::postgres::types::Oid>(
            "SELECT 'public.operator_notes'::regclass::oid"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
        .0,
        oid
    );
}
