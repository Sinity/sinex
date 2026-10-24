use super::*;
use sqlx::Executor;

#[tokio::test]
async fn whole_bodylessness_is_operation_owned_while_unrelated_telemetry_keeps_a_fresh_data_capability()
 {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("store");
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let material = capture(
        &mut ctx,
        root.path(),
        "history",
        b": 1700000000:0;synthetic_erased_body\n",
        &recipient,
    )
    .await;
    let old = authority
        .material_key(&mut ctx.read_conn, material)
        .await
        .unwrap();
    let unrelated = serde_json::json!({"synthetic":"unrelated telemetry value"});
    let encrypted = crypto::seal_value(&unrelated, &old).unwrap();
    let telemetry = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO telemetry.events(id,event_type,source,run_id,material_id,anchor,ordinal,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,equivalence_key,reading_order,record_hash,record_state,class,support) SELECT $2,event_type,source,run_id,material_id,anchor,ordinal,parents,ts_orig,ts_quality,ts_persisted,commit_seq,$3,equivalence_key,reading_order,record_hash,record_state,class,support FROM core.events WHERE material_id=$1 LIMIT 1")
        .bind(material.0).bind(telemetry).bind(&encrypted).execute(&mut ctx.read_conn).await.unwrap();
    let facts: Value =
        sqlx::query_scalar("SELECT to_jsonb(e)-'payload' FROM telemetry.events e WHERE id=$1")
            .bind(telemetry)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    remove(&ctx, &url, Target::Material(material), authority.clone())
        .await
        .unwrap();
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &old.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    let fresh = authority
        .material_key(&mut ctx.read_conn, material)
        .await
        .unwrap();
    assert_ne!(fresh.key_ref, old.key_ref);
    let (payload, after): (Value, Value) =
        sqlx::query_as("SELECT payload,to_jsonb(e)-'payload' FROM telemetry.events e WHERE id=$1")
            .bind(telemetry)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(after, facts);
    assert_eq!(
        crypto::unseal_payload(&payload, &fresh.key).unwrap(),
        unrelated
    );
    assert!(crypto::unseal_payload(&payload, &old.key).is_err());
    let opened = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        zsh::SPEC.name,
        material,
        Some(&authority),
    )
    .await
    .unwrap();
    assert!(opened.body.is_none());
    assert_eq!(opened.metadata.len(), 1);
    let original_marker = opened.reference.manifest["excised_whole"].clone();
    // A provider flag, or a valid operation for some other target, cannot
    // change whether the archive permits reading original body bytes.
    sqlx::query("UPDATE raw.materials SET manifest=jsonb_set(manifest,'{excised_whole}',to_jsonb($2::text)) WHERE id=$1")
        .bind(material.0).bind(uuid::Uuid::now_v7().to_string()).execute(&mut ctx.read_conn).await.unwrap();
    assert!(
        crate::retained::open(
            &mut ctx.read_conn,
            &ctx.blobs,
            zsh::SPEC.name,
            material,
            Some(&authority)
        )
        .await
        .err()
        .unwrap()
        .contains("archive operation")
    );
    sqlx::query(
        "UPDATE raw.materials SET manifest=jsonb_set(manifest,'{excised_whole}',$2) WHERE id=$1",
    )
    .bind(material.0)
    .bind(original_marker)
    .execute(&mut ctx.read_conn)
    .await
    .unwrap();
    ctx.close().await.unwrap();
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![material.0],
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            authority: Some(authority.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(report.events_read, 0);
    let payload: Value = sqlx::query_scalar("SELECT payload FROM telemetry.events WHERE id=$1")
        .bind(telemetry)
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(
        crypto::unseal_payload(&payload, &fresh.key).unwrap(),
        unrelated
    );
    // A second removal rotates only surviving values; it never reopens the
    // retired original body or resurrects either old data capability.
    remove(&ctx, &url, Target::Material(material), authority.clone())
        .await
        .unwrap();
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &fresh.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        crate::retained::open(
            &mut ctx.read_conn,
            &ctx.blobs,
            zsh::SPEC.name,
            material,
            Some(&authority)
        )
        .await
        .unwrap()
        .body
        .is_none()
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn final_carry_failure_rolls_back_body_events_keys_and_operation_then_the_writer_can_retry() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let mut ctx = OneShot::open(&url, &root.path().join("store"))
        .await
        .unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let raw = b": 1700000000:0;synthetic_removed\n: 1700000001:0;synthetic_retained\n";
    let material = capture(&mut ctx, root.path(), "history", raw, &recipient).await;
    let before = event_rows(&mut ctx).await;
    let target = Target::Event(EventId(before.iter().find(|e| e.2 == 0).unwrap().0));
    let key = authority
        .material_key(&mut ctx.read_conn, material)
        .await
        .unwrap();
    let state:(Value,Value,Value)=sqlx::query_as("SELECT to_jsonb(m),to_jsonb(k),to_jsonb(p) FROM raw.materials m JOIN privacy.material_keys k ON k.material_id=m.id JOIN raw.inputs i ON i.id=m.input_id JOIN raw.positions p ON p.binding=i.binding WHERE m.id=$1").bind(material.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    ctx.read_conn.execute("CREATE FUNCTION public.reject_final_carry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic final phase refusal'; END $$; CREATE TRIGGER reject_final_carry AFTER UPDATE ON raw.positions FOR EACH ROW EXECUTE FUNCTION public.reject_final_carry()").await.unwrap();
    assert!(
        remove(&ctx, &url, target, authority.clone())
            .await
            .unwrap_err()
            .contains("synthetic final phase refusal")
    );
    assert_eq!(event_rows(&mut ctx).await, before);
    let after:(Value,Value,Value)=sqlx::query_as("SELECT to_jsonb(m),to_jsonb(k),to_jsonb(p) FROM raw.materials m JOIN privacy.material_keys k ON k.material_id=m.id JOIN raw.inputs i ON i.id=m.input_id JOIN raw.positions p ON p.binding=i.binding WHERE m.id=$1").bind(material.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(after, state);
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &key.key_ref)
            .await
            .unwrap()
            .is_some()
    );
    let operations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='excise'")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(operations, 0);
    ctx.read_conn.execute("DROP TRIGGER reject_final_carry ON raw.positions; DROP FUNCTION public.reject_final_carry()").await.unwrap();
    let expired = Authority::new(crypto::Identity::generate(), Duration::ZERO).unwrap();
    assert!(
        remove(&ctx, &url, target, expired)
            .await
            .unwrap_err()
            .contains("expired")
    );
    assert_eq!(event_rows(&mut ctx).await, before);
    remove(&ctx, &url, target, authority).await.unwrap();
    assert_eq!(event_rows(&mut ctx).await.len(), 1);
    ctx.close().await.unwrap();
}
