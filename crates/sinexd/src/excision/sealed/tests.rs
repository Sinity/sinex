use super::*;
use crate::one_shot::{OneShot, Sink};
use sinex_model::{Class, EventId};
use sinex_sources::{Binding, Settings, sources::zsh};
use std::time::Duration;

async fn capture(
    ctx: &mut OneShot,
    root: &std::path::Path,
    name: &str,
    raw: &[u8],
    recipient: &crypto::Recipient,
) -> MaterialId {
    capture_scoped(ctx, root, name, raw, recipient, None)
        .await
        .unwrap()
}
async fn capture_scoped(
    ctx: &mut OneShot,
    root: &std::path::Path,
    name: &str,
    raw: &[u8],
    recipient: &crypto::Recipient,
    authority: Option<&Authority>,
) -> Result<MaterialId, String> {
    let path = root.join(name);
    std::fs::write(&path, raw).unwrap();
    let binding = Binding::of(&zsh::SPEC, &path, Settings::new("synthetic"));
    let mut bound = crate::binding_reader::Bound::new(&zsh::KIND, binding).unwrap();
    let mut source = zsh::source("synthetic".into());
    source.spec.class = Class::Sealed;
    bound.source = Box::new(source);
    let sink = Sink(ctx.blobs.clone());
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let result = crate::binding_reader::read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        sinex_model::RunId::new(),
        &sink,
        &mut [bound],
        stop,
        crate::sealed_access::Access {
            recipient: Some(recipient),
            authority,
        },
    )
    .await;
    for (_, result) in result {
        result?;
    }
    let id: uuid::Uuid = sqlx::query_scalar(
        "SELECT m.id FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.locator=$1 ORDER BY m.part_index DESC,m.id DESC LIMIT 1",
    )
    .bind(path.to_string_lossy().to_string())
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    Ok(MaterialId(id))
}

async fn remove(
    ctx: &OneShot,
    url: &str,
    target: Target,
    authority: Authority,
) -> Result<String, String> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .unwrap();
    let connection = pool.acquire().await.unwrap();
    let result = super::super::excise_pooled(
        connection,
        &ctx.blobs,
        &ctx.writer,
        target,
        "synthetic",
        Some(authority),
    )
    .await;
    pool.close().await;
    result
}

async fn event_rows(ctx: &mut OneShot) -> Vec<(uuid::Uuid, uuid::Uuid, i64, Value)> {
    sqlx::query_as("SELECT id,material_id,anchor,to_jsonb(e) FROM core.events e ORDER BY material_id,anchor,id")
        .fetch_all(&mut ctx.read_conn).await.unwrap()
}

#[tokio::test]
async fn partial_sealed_purge_rewrites_two_epochs_keeps_exact_unrelated_facts_and_replays_after_restart()
 {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("store");
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let raw = b": 1700000000:0;synthetic_removed_secret\n: 1700000001:0;synthetic_retained_value\n";
    let a = capture(&mut ctx, root.path(), "epoch-a", raw, &recipient).await;
    let b = capture(&mut ctx, root.path(), "epoch-b", raw, &recipient).await;
    let before = event_rows(&mut ctx).await;
    assert_eq!(before.len(), 4);
    let target = Target::Event(EventId(
        before.iter().find(|e| e.1 == a.0 && e.2 == 0).unwrap().0,
    ));
    let mut old_keys = BTreeMap::new();
    let mut retained = BTreeMap::new();
    for material in [a, b] {
        old_keys.insert(
            material,
            authority
                .material_key(&mut ctx.read_conn, material)
                .await
                .unwrap(),
        );
    }
    for (id, material, anchor, row) in &before {
        if *anchor > 0 {
            let plain =
                crypto::unseal_payload(&row["payload"], &old_keys[&MaterialId(*material)].key)
                    .unwrap();
            let mut metadata = row.clone();
            metadata.as_object_mut().unwrap().remove("payload");
            retained.insert(*id, (plain, metadata));
        }
    }
    let summary = remove(&ctx, &url, target, authority.clone()).await.unwrap();
    assert!(summary.contains("excised events 2"), "{summary}");
    for (material, old) in &old_keys {
        assert!(
            crypto::wrapped_key(&mut ctx.read_conn, &old.key_ref)
                .await
                .unwrap()
                .is_none()
        );
        let fresh = authority
            .material_key(&mut ctx.read_conn, *material)
            .await
            .unwrap();
        assert_ne!(old.key_ref, fresh.key_ref);
        let opened = crate::retained::open(
            &mut ctx.read_conn,
            &ctx.blobs,
            zsh::SPEC.name,
            *material,
            Some(&authority),
        )
        .await
        .unwrap();
        let body = opened.body.as_ref().unwrap();
        use sinex_sources::drain::RecordRead;
        let mut actual = vec![0; raw.len()];
        body.read_exact_at(0, &mut actual).unwrap();
        let end = raw.iter().position(|&b| b == b'\n').unwrap() + 1;
        let mut expected = raw.to_vec();
        sinex_store::excision::blank(&mut expected, &[(0, end)]);
        assert_eq!(actual, expected);
        assert_eq!(opened.metadata.len(), 1);
        let token = serde_json::to_string(&opened.metadata[0].transition).unwrap();
        assert!(!token.contains("synthetic_removed_secret"));
        let reference = opened.reference;
        let mut wrong = ctx.blobs.read_sealed(
            &reference.blob,
            Some(reference.length),
            old,
            crate::sealed_access::read_limits(reference.length).unwrap(),
            || Ok(()),
        );
        assert!(
            wrong.as_mut().is_err()
                || std::io::copy(wrong.as_mut().unwrap(), &mut std::io::sink()).is_err()
        );
    }
    for (id, material, _, row) in event_rows(&mut ctx).await {
        let key = authority
            .material_key(&mut ctx.read_conn, MaterialId(material))
            .await
            .unwrap();
        assert_eq!(
            crypto::unseal_payload(&row["payload"], &key.key).unwrap(),
            retained[&id].0
        );
        let mut metadata = row;
        metadata.as_object_mut().unwrap().remove("payload");
        assert_eq!(metadata, retained[&id].1);
    }
    ctx.close().await.unwrap();
    let mut ctx = OneShot::open(&url, &store).await.unwrap();
    let report = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![a.0, b.0],
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
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE anchor=0")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(actual, 0);
    let filtered = capture_scoped(
        &mut ctx,
        root.path(),
        "future-epoch",
        raw,
        &recipient,
        Some(&authority),
    )
    .await
    .unwrap();
    let opened = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        zsh::SPEC.name,
        filtered,
        Some(&authority),
    )
    .await
    .unwrap();
    let mut bytes = vec![0; raw.len()];
    sinex_sources::drain::RecordRead::read_exact_at(
        &**opened.body.as_ref().unwrap(),
        0,
        &mut bytes,
    )
    .unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("synthetic_removed_secret"));
    assert!(String::from_utf8_lossy(&bytes).contains("synthetic_retained_value"));
    assert_eq!(opened.metadata.len(), 1);
    assert!(
        capture_scoped(
            &mut ctx,
            root.path(),
            "locked-future",
            raw,
            &recipient,
            None
        )
        .await
        .unwrap_err()
        .contains("scoped unlock")
    );
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn whole_sealed_purge_retains_minimal_replay_and_sanitized_carry_without_old_data_capability()
{
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let mut ctx = OneShot::open(&url, &root.path().join("store"))
        .await
        .unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let raw = b": 1700000000:0;synthetic_whole_secret\n";
    let material = capture(&mut ctx, root.path(), "history", raw, &recipient).await;
    let old = authority
        .material_key(&mut ctx.read_conn, material)
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
    let (_, names) = authority
        .material_secrets(&mut ctx.read_conn, material)
        .await
        .unwrap();
    assert_eq!(names.role(), crypto::SecretRole::RecordNamingOnly);
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
    let result = crate::replay::run(
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
            authority: Some(authority),
        },
    )
    .await
    .unwrap();
    assert_eq!(result.events_read, 0);
    ctx.close().await.unwrap();
}

mod activitywatch;
mod dependents;
mod prefix;
mod security;
