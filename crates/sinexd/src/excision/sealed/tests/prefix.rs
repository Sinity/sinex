use super::*;

async fn unique_prefix(whole: bool) {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let mut ctx = OneShot::open(&url, &root.path().join("store"))
        .await
        .unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let original = b": 100:0;synthetic_A\n: 101:0;synthetic_B\n: 102:0;synthetic_C\n";
    let rewritten = b": 100:0;synthetic_A\n: 102:0;synthetic_C\n: 103:0;synthetic_D\n";
    let first = capture(&mut ctx, root.path(), "history", original, &recipient).await;
    let second = capture_scoped(
        &mut ctx,
        root.path(),
        "history",
        rewritten,
        &recipient,
        Some(&authority),
    )
    .await
    .unwrap();
    assert_ne!(first, second);
    let reference = sinex_store::material_bytes::reference(&mut ctx.read_conn, second)
        .await
        .unwrap()
        .unwrap();
    let active: Vec<(usize, usize)> =
        serde_json::from_value(reference.manifest["records"].clone()).unwrap();
    assert_eq!(active.len(), 1);
    assert!(active[0].0 > 0);
    let before = event_rows(&mut ctx).await;
    let mut old_keys = BTreeMap::new();
    for material in [first, second] {
        old_keys.insert(
            material,
            authority
                .material_key(&mut ctx.read_conn, material)
                .await
                .unwrap(),
        );
    }
    let mut target = None;
    let mut unrelated = BTreeMap::new();
    for (id, material, anchor, row) in &before {
        let plain =
            crypto::unseal_payload(&row["payload"], &old_keys[&MaterialId(*material)].key).unwrap();
        if *material == first.0 && *anchor == 0 {
            target = Some(Target::Event(EventId(*id)));
        }
        let selected = if whole {
            plain["command"] != "synthetic_B"
        } else {
            plain["command"] == "synthetic_A"
        };
        if !selected {
            let mut facts = row.clone();
            facts.as_object_mut().unwrap().remove("payload");
            unrelated.insert(*id, (plain, facts));
        }
    }
    remove(
        &ctx,
        &url,
        if whole {
            Target::Material(second)
        } else {
            target.unwrap()
        },
        authority.clone(),
    )
    .await
    .unwrap();
    let after = event_rows(&mut ctx).await;
    assert_eq!(after.len(), unrelated.len());
    for (id, material, _, row) in after {
        let fresh = authority
            .material_key(&mut ctx.read_conn, MaterialId(material))
            .await
            .unwrap();
        assert_eq!(
            crypto::unseal_payload(&row["payload"], &fresh.key).unwrap(),
            unrelated[&id].0
        );
        let mut facts = row;
        facts.as_object_mut().unwrap().remove("payload");
        assert_eq!(facts, unrelated[&id].1);
    }
    for old in old_keys.values() {
        assert!(
            crypto::wrapped_key(&mut ctx.read_conn, &old.key_ref)
                .await
                .unwrap()
                .is_none()
        );
    }
    let opened = crate::retained::open(
        &mut ctx.read_conn,
        &ctx.blobs,
        zsh::SPEC.name,
        second,
        Some(&authority),
    )
    .await
    .unwrap();
    assert_eq!(
        opened.metadata.iter().filter(|r| !r.is_active()).count(),
        if whole { 2 } else { 1 }
    );
    assert!(
        opened
            .metadata
            .iter()
            .filter(|r| !r.is_active())
            .all(|r| r.transition.is_none())
    );
    assert_eq!(
        opened.metadata.iter().filter(|r| r.is_active()).count(),
        usize::from(whole)
    );
    if whole {
        assert!(opened.body.is_none());
    } else {
        let mut bytes = vec![0; rewritten.len()];
        sinex_sources::drain::RecordRead::read_exact_at(
            &**opened.body.as_ref().unwrap(),
            0,
            &mut bytes,
        )
        .unwrap();
        let mut expected = rewritten.to_vec();
        excision::blank(
            &mut expected,
            &[(0, rewritten.iter().position(|&b| b == b'\n').unwrap() + 1)],
        );
        assert_eq!(bytes, expected);
    }
    drop(opened);
    // A distinct later occurrence has different header bytes. It remains n=1
    // after replay; the physical prefix must never become a second counter step.
    let mut later = rewritten.to_vec();
    later.extend_from_slice(b": 100:7;synthetic_A\n");
    let third = capture_scoped(
        &mut ctx,
        root.path(),
        "history",
        &later,
        &recipient,
        Some(&authority),
    )
    .await
    .unwrap();
    let row = event_rows(&mut ctx)
        .await
        .into_iter()
        .find(|row| row.1 == third.0)
        .unwrap();
    let key = authority
        .material_key(&mut ctx.read_conn, third)
        .await
        .unwrap();
    assert_eq!(
        crypto::unseal_payload(&row.3["payload"], &key.key).unwrap()["n"],
        1
    );
    crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![first.0, second.0, third.0],
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
    for (_, material, _, row) in event_rows(&mut ctx)
        .await
        .into_iter()
        .filter(|row| row.1 == third.0)
    {
        let key = authority
            .material_key(&mut ctx.read_conn, MaterialId(material))
            .await
            .unwrap();
        assert_eq!(
            crypto::unseal_payload(&row["payload"], &key.key).unwrap()["n"],
            1
        );
    }
    ctx.close().await.unwrap();
}
#[tokio::test]
async fn partial_purge_erases_unique_inactive_copies_without_changing_other_occurrences_or_replay_counts()
 {
    unique_prefix(false).await;
}
#[tokio::test]
async fn whole_purge_accounts_for_inactive_copies_and_retains_only_their_zero_effect_metadata() {
    unique_prefix(true).await;
}

#[tokio::test]
async fn indistinguishable_selected_and_unselected_prefix_occurrences_refuse_before_any_key_or_reading_changes()
 {
    let (_db, url, _) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let mut ctx = OneShot::open(&url, &root.path().join("store"))
        .await
        .unwrap();
    let identity = crypto::Identity::generate();
    let recipient = identity.to_public();
    let authority = Authority::new(identity, Duration::from_secs(300)).unwrap();
    let first = capture(
        &mut ctx,
        root.path(),
        "history",
        b": 100:0;synthetic_equal\n: 100:0;synthetic_equal\n: 101:0;old_tail\n",
        &recipient,
    )
    .await;
    capture_scoped(
        &mut ctx,
        root.path(),
        "history",
        b": 100:0;synthetic_equal\n: 100:0;synthetic_equal\n: 102:0;new_tail\n",
        &recipient,
        Some(&authority),
    )
    .await
    .unwrap();
    let before = event_rows(&mut ctx).await;
    let target = Target::Event(EventId(
        before
            .iter()
            .find(|row| row.1 == first.0 && row.2 == 0)
            .unwrap()
            .0,
    ));
    let old = authority
        .material_key(&mut ctx.read_conn, first)
        .await
        .unwrap();
    let error = remove(&ctx, &url, target, authority).await.unwrap_err();
    assert!(error.contains("ambiguous"), "{error}");
    assert_eq!(event_rows(&mut ctx).await, before);
    assert!(
        crypto::wrapped_key(&mut ctx.read_conn, &old.key_ref)
            .await
            .unwrap()
            .is_some()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='excise'")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    ctx.close().await.unwrap();
}
