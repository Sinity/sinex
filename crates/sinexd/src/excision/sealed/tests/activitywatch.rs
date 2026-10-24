use super::*;
use sinex_sources::sources::activitywatch as aw;

async fn turn(
    ctx: &mut OneShot,
    bound: &mut [crate::binding_reader::Bound],
    authority: &Authority,
) {
    let recipient = authority.recipient();
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = tokio::sync::watch::channel(false);
    for (_, result) in crate::binding_reader::read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        sinex_model::RunId::new(),
        &sink,
        bound,
        stop,
        crate::sealed_access::Access {
            recipient: Some(&recipient),
            authority: Some(authority),
        },
    )
    .await
    {
        result.unwrap();
    }
}
async fn plain_events(ctx: &mut OneShot, authority: &Authority) -> Vec<(uuid::Uuid, Value)> {
    let rows: Vec<(uuid::Uuid, uuid::Uuid, Value)> =
        sqlx::query_as("SELECT id,material_id,payload FROM core.events ORDER BY commit_seq")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    let mut out = Vec::new();
    for (id, material, payload) in rows {
        let key = authority
            .material_key(&mut ctx.read_conn, MaterialId(material))
            .await
            .unwrap();
        out.push((id, crypto::unseal_payload(&payload, &key.key).unwrap()));
    }
    out
}

#[tokio::test]
async fn activitywatch_partial_purge_sanitizes_only_affected_carry_then_restart_replay_and_future_rows_keep_span_identity()
 {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("store");
    let path = root.path().join("source.db");
    let provider = rusqlite::Connection::open(&path).unwrap();
    provider.execute_batch("CREATE TABLE buckets(id INTEGER PRIMARY KEY,name TEXT,type TEXT,client TEXT,hostname TEXT,created TEXT,data TEXT);CREATE TABLE events(id INTEGER PRIMARY KEY,bucketrow INTEGER,starttime INTEGER,endtime INTEGER,data TEXT); INSERT INTO buckets VALUES(1,'aw-watcher-window_h','currentwindow','c','h','x','{}'),(2,'aw-watcher-window_other','currentwindow','c','h','x','{}')").unwrap();
    let now = sinex_model::Timestamp::now().unix_ns() as i64;
    for (id, bucket, start, app, title) in [
        (1, 1, 0, "retained-app", "retained-window"),
        (2, 1, 3, "private-app", "synthetic_erased_title"),
        (3, 2, 4, "other-app", "other-window"),
    ] {
        provider
            .execute(
                "INSERT INTO events VALUES(?1,?2,?3,?4,?5)",
                rusqlite::params![
                    id,
                    bucket,
                    now + start * 1_000_000_000,
                    now + (start + 1) * 1_000_000_000,
                    serde_json::json!({"app":app,"title":title}).to_string()
                ],
            )
            .unwrap();
    }
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        aw::SPEC.name,
        Class::Sealed,
        "synthetic",
    )
    .await
    .unwrap();
    let identity = crypto::Identity::generate();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let binding = Binding::of(&aw::SPEC, &path, Settings::new("h"));
    let mut bounds = vec![crate::binding_reader::Bound::new(&aw::KIND, binding.clone()).unwrap()];
    turn(&mut ctx, &mut bounds, &authority).await;
    // A later, unrelated record owns the durable carry. Its private body must
    // remain byte-identical while its key is retired with the selected state.
    provider
        .execute(
            "INSERT INTO events VALUES(4,2,?1,?2,?3)",
            rusqlite::params![
                now + 6_000_000_000,
                now + 7_000_000_000,
                serde_json::json!({"app":"other-app","title":"other-window"}).to_string()
            ],
        )
        .unwrap();
    turn(&mut ctx, &mut bounds, &authority).await;
    let before = plain_events(&mut ctx, &authority).await;
    assert_eq!(before.len(), 4);
    let removed = before
        .iter()
        .find(|(_, v)| v["title"] == "synthetic_erased_title")
        .unwrap()
        .0;
    let retained = before
        .iter()
        .filter(|(id, _)| *id != removed)
        .cloned()
        .collect::<Vec<_>>();
    let material: uuid::Uuid =
        sqlx::query_scalar("SELECT material_id FROM core.events WHERE id=$1")
            .bind(removed)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    let old = authority
        .material_key(&mut ctx.read_conn, MaterialId(material))
        .await
        .unwrap();
    let carry_owner: uuid::Uuid = sqlx::query_scalar(
        "SELECT (cursor->>'material')::uuid FROM raw.positions WHERE binding=$1",
    )
    .bind(&binding.name)
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_ne!(carry_owner, material);
    let carry_key = authority
        .material_key(&mut ctx.read_conn, MaterialId(carry_owner))
        .await
        .unwrap();
    let unchanged = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        aw::SPEC.name,
        MaterialId(carry_owner),
        Some(&authority),
    )
    .await
    .unwrap();
    let mut unchanged_bytes = vec![0; unchanged.reference.length as usize];
    sinex_sources::drain::RecordRead::read_exact_at(
        &**unchanged.body.as_ref().unwrap(),
        0,
        &mut unchanged_bytes,
    )
    .unwrap();
    drop(unchanged);
    remove(
        &ctx,
        &url,
        Target::Event(EventId(removed)),
        authority.clone(),
    )
    .await
    .unwrap();
    assert_eq!(plain_events(&mut ctx, &authority).await, retained);
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &old.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    let (cursor, carry) = sinex_store::positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &carry_key.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    let rewritten = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        aw::SPEC.name,
        MaterialId(carry_owner),
        Some(&authority),
    )
    .await
    .unwrap();
    let mut actual = vec![0; unchanged_bytes.len()];
    sinex_sources::drain::RecordRead::read_exact_at(
        &**rewritten.body.as_ref().unwrap(),
        0,
        &mut actual,
    )
    .unwrap();
    assert_eq!(actual, unchanged_bytes);
    assert!(
        rewritten.metadata.is_empty(),
        "unchanged-body rekey must not erase unrelated ranges"
    );
    let fresh = authority
        .material_key(&mut ctx.read_conn, MaterialId(carry_owner))
        .await
        .unwrap();
    let carry = crypto::unseal_payload(&carry, &fresh.key).unwrap();
    assert!(!carry.to_string().contains("synthetic_erased_title"));
    assert!(!carry.to_string().contains("private-app"));
    assert!(carry.to_string().contains("other-window"));
    assert!(carry["erased"].as_object().is_some_and(|m| !m.is_empty()));
    drop(bounds);
    ctx.close().await.unwrap();
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![material],
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
    assert_eq!(report.events_read, 2);
    let replayed = plain_events(&mut ctx, &authority).await;
    assert!(
        replayed
            .iter()
            .all(|(_, v)| v["title"] != "synthetic_erased_title")
    );
    // Row five belongs to the erased window span. Row six starts a distinct
    // span and must keep its own native ID despite that erased transition.
    for (id, start, title) in [
        (5, 4, "synthetic_erased_title"),
        (6, 7, "later-retained-window"),
    ] {
        provider.execute("INSERT INTO events VALUES(?1,1,?2,?3,?4)",rusqlite::params![id,now+start*1_000_000_000,now+(start+1)*1_000_000_000,serde_json::json!({"app":if id==5 {"private-app"}else{"later-app"},"title":title}).to_string()]).unwrap();
    }
    let mut bounds = vec![crate::binding_reader::Bound::new(&aw::KIND, binding.clone()).unwrap()];
    turn(&mut ctx, &mut bounds, &authority).await;
    let future = plain_events(&mut ctx, &authority).await;
    assert!(
        future
            .iter()
            .all(|(_, v)| v["title"] != "synthetic_erased_title")
    );
    let later = future
        .iter()
        .find(|(_, v)| v["title"] == "later-retained-window")
        .unwrap();
    assert_eq!(later.1["span_id"], 6);
    let after = sinex_store::positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(after.0, cursor);
    ctx.close().await.unwrap();
}
