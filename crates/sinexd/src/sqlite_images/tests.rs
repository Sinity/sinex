use super::*;
use sinex_sources::drivers::sqlite_image::{Policy, Scan};
use std::os::unix::fs::FileExt;

#[test]
fn sqlite_image_publication_checks_dense_map_bytes_and_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(&dir.path().join("store")).unwrap();
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES ('synthetic'); BEGIN; SELECT count(*) FROM sqlite_master;").unwrap();
    let mut scan = Scan::start(
        connection,
        Policy {
            scope: "whole_database".into(),
            class: Class::Guarded,
            interval_secs: 3600,
            turn_pages: 1,
            rebase_threshold: 1.0,
        },
        None,
        "synthetic".into(),
        "test".into(),
        sinex_model::Timestamp::now(),
    )
    .unwrap();
    while !scan.turn().unwrap() {}
    let image = scan.finish().unwrap();
    image
        .set_check(Arc::new(|| Err("synthetic cancellation".into())))
        .unwrap();
    assert!(
        store(&blobs, &image, None)
            .unwrap_err()
            .contains("cancellation")
    );
    assert!(image.stored().unwrap().is_none());
    image.set_check(Arc::new(|| Ok(()))).unwrap();
    let map = image.map.as_ref().unwrap();
    let mut original = [0; 32];
    map.read_exact_at(&mut original, 0).unwrap();
    map.write_all_at(&[0; 32], 0).unwrap();
    assert!(
        store(&blobs, &image, None)
            .unwrap_err()
            .contains("page map differs")
    );
    map.write_all_at(&original, 0).unwrap();
    let publication = store(&blobs, &image, None).unwrap();
    publication.validate().unwrap();
    assert!(publication.metadata.evidence_bytes > publication.metadata.image_bytes);
}

#[tokio::test]
async fn native_image_costs_stay_unresolved_until_installed_verification_and_use_actual_dedup_encoding()
 {
    use sinex_model::{InputRef, RevisionChange, SourceProgress};
    use sinex_store::native_materials::{self, Pending, State};
    let (_db, url, mut conn) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let blobs = Blobs::open(&root).unwrap();
    let database = rusqlite::Connection::open_in_memory().unwrap();
    database.execute_batch("CREATE TABLE t(x TEXT); INSERT INTO t VALUES ('synthetic image row'); BEGIN; SELECT count(*) FROM sqlite_master;").unwrap();
    let mut scan = Scan::start(
        database,
        Policy {
            scope: "whole_database".into(),
            class: Class::Guarded,
            interval_secs: 3600,
            turn_pages: 1,
            rebase_threshold: 1.0,
        },
        None,
        "synthetic-db".into(),
        "h".into(),
        sinex_model::Timestamp::now(),
    )
    .unwrap();
    while !scan.turn().unwrap() {}
    let image = scan.finish().unwrap();
    let (prepared, stage) = prepare_plain(&blobs, image.clone(), "p", 1).unwrap();
    let name = stage.name();
    assert!(!blobs.contains(&name).unwrap());
    sinex_store::bindings::save(
        &mut conn,
        &sinex_store::bindings::Stored {
            binding: "image-binding".into(),
            source: "synthetic-source".into(),
            input: "synthetic-db".into(),
            settings: serde_json::json!({"host":"h"}),
        },
    )
    .await
    .unwrap();
    let basis = sinex_store::excision::operations_basis(&mut conn, "synthetic-source")
        .await
        .unwrap();
    let progress = SourceProgress {
        native_pending: None,
        erased_replay: None,
        excision_basis: Some(basis),
        binding: "image-binding".into(),
        image: None,
        image_revision: Some(image.metadata.revision),
        material_keys: Vec::new(),
        input: InputRef {
            kind: "sqlite".into(),
            locator: "synthetic-db".into(),
            host: "h".into(),
        },
        observation: Default::default(),
        revision: RevisionChange::Snapshot,
        material: None,
        cursor: serde_json::Value::Null,
        carry: serde_json::Value::Null,
    };
    let pending = Pending {
        material: image.metadata.material,
        source: "synthetic-source".into(),
        binding: "image-binding".into(),
        progress,
        removed: Vec::new(),
        image: Some(prepared.plan().unwrap()),
        base_cursor: serde_json::Value::Null,
        base_carry: serde_json::Value::Null,
        start_cursor: serde_json::Value::Null,
        end_cursor: serde_json::Value::Null,
        state: State::Reserved,
    };
    let mut guard = crate::source_excision::AcquisitionGuard::acquire(&url, "synthetic-source")
        .await
        .unwrap();
    let mut tx = sqlx::Connection::begin(guard.connection()).await.unwrap();
    native_materials::reserve(&mut tx, &pending).await.unwrap();
    tx.commit().await.unwrap();
    let stored: serde_json::Value =
        sqlx::query_scalar("SELECT publication FROM raw.pending_native_materials")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(stored["image"]["costs"].is_null());
    assert!(
        stored["image"]["metadata"]
            .get("base_stored_bytes")
            .is_none()
    );
    assert!(
        stored["image"]["material"]["manifest"]["sqlite_image"]
            .get("read_stored_bytes")
            .is_none()
    );
    assert!(
        native_materials::installed(&mut conn, pending.material)
            .await
            .is_err()
    );
    // An earlier CAS writer uses a different compression setting. Installation
    // must reuse that representation and measure it rather than its own stage.
    let other = hashed_blobs::Store::open(&root, vec![], 0o600)
        .unwrap()
        .with_frame_size(512);
    let winner = other
        .put(image.reader(Arc::new(|h| *h), false).unwrap(), 19)
        .unwrap();
    assert_eq!(format!("blake3:{}", winner.hex()), name);
    let winner_size = blobs.stored_size(&name).unwrap();
    assert_eq!(stage.install().unwrap(), name);
    let completed = prepared.verify().unwrap();
    drop(image);
    let restored = native_materials::load(&mut conn, pending.material)
        .await
        .unwrap()
        .unwrap();
    let replayed = verify_retained_plan(
        &blobs,
        restored.image.as_ref().unwrap(),
        None,
        None,
        Arc::new(|| Ok(())),
    )
    .unwrap();
    assert_eq!(
        replayed, completed,
        "lost pinned view recovers exact frozen image, without reopening provider"
    );
    assert_eq!(completed.metadata.base_stored_bytes, winner_size);
    assert_eq!(completed.metadata.read_stored_bytes, winner_size);
    assert_eq!(completed.metadata.patch_stored_bytes, 0);
    let mut wrong = completed.clone();
    wrong.metadata.database = "different-database".into();
    wrong.material.manifest["sqlite_image"] = serde_json::to_value(&wrong.metadata).unwrap();
    wrong.material.manifest["sqlite_database"] = serde_json::json!("different-database");
    assert!(
        native_materials::finalize_image(&mut conn, pending.material, wrong)
            .await
            .is_err()
    );
    let mut tx = sqlx::Connection::begin(guard.connection()).await.unwrap();
    native_materials::finalize_image(&mut tx, pending.material, completed.clone())
        .await
        .unwrap();
    native_materials::installed(&mut tx, pending.material)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let still = native_materials::load(&mut conn, pending.material)
        .await
        .unwrap()
        .unwrap();
    assert!(still.image.is_some() && still.state == State::Reserved);
    let mut tx = sqlx::Connection::begin(guard.connection()).await.unwrap();
    native_materials::finalize_image(&mut tx, pending.material, completed.clone())
        .await
        .unwrap();
    native_materials::installed(&mut tx, pending.material)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let ready = native_materials::load(&mut conn, pending.material)
        .await
        .unwrap()
        .unwrap();
    assert!(ready.image.is_none() && ready.state == State::Installed);
    assert_eq!(ready.progress.image.as_ref().unwrap(), &completed);
    guard.release().await.unwrap();
    sinex_store::bindings::save(
        &mut conn,
        &sinex_store::bindings::Stored {
            binding: "other-table".into(),
            source: "other-table-source".into(),
            input: "synthetic-db".into(),
            settings: serde_json::json!({"host":"h"}),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO raw.inputs(id,binding,kind,locator,host) VALUES($1,'other-table','sqlite','synthetic-db','h')")
        .bind(uuid::Uuid::now_v7()).execute(&mut conn).await.unwrap();
    sqlx::query(
        "INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic',$2)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(serde_json::json!({"sources":["other-table-source"]}))
    .execute(&mut conn)
    .await
    .unwrap();
    let error = native_materials::reserve(&mut conn, &pending)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("excluded database lineage"),
        "{error}"
    );
    let retained = native_materials::load(&mut conn, pending.material)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.progress.image.unwrap(),
        completed,
        "refusal replaced retained pending evidence"
    );
}

#[test]
fn sealed_image_preparation_freezes_keyed_digest_and_wrapped_key_before_cas_install() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(&dir.path().join("store")).unwrap();
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES('synthetic keyed image'); BEGIN; SELECT count(*) FROM sqlite_master;").unwrap();
    let mut scan = Scan::start(
        connection,
        Policy {
            scope: "whole_database".into(),
            class: Class::Sealed,
            interval_secs: 3600,
            turn_pages: 1,
            rebase_threshold: 1.0,
        },
        None,
        "synthetic".into(),
        "h".into(),
        sinex_model::Timestamp::now(),
    )
    .unwrap();
    while !scan.turn().unwrap() {}
    let image = scan.finish().unwrap();
    let mut exact = Vec::new();
    image
        .reader(Arc::new(|h| *h), true)
        .unwrap()
        .take(image.metadata.image_bytes)
        .read_to_end(&mut exact)
        .unwrap();
    let identity = sealed::Identity::generate();
    let (prepared, stage) =
        prepare_sealed(&blobs, image.clone(), "p", 1, &identity.to_public()).unwrap();
    let plan = prepared.plan().unwrap();
    let key = prepared.key.as_ref().unwrap().clone();
    let wrapped = prepared.expected.key.as_ref().unwrap().wrapped.clone();
    let expected_hash = prepared.expected.metadata.image_hash.clone();
    let naming = blake3::derive_key("sinex 2026 SQLite image naming key", &key.key);
    assert_eq!(
        expected_hash,
        format!("blake3:{}", blake3::keyed_hash(&naming, &exact))
    );
    assert_ne!(expected_hash, format!("blake3:{}", blake3::hash(&exact)));
    assert!(!blobs.contains(&stage.name()).unwrap());
    stage.install().unwrap();
    let completed = prepared.verify().unwrap();
    assert_eq!(
        completed.key.as_ref().unwrap().wrapped,
        wrapped,
        "verification changed the reserved capability envelope"
    );
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(30)).unwrap();
    let restart = |authority: Option<&crate::sealed_access::Authority>| {
        verify_retained_plan(&blobs, &plan, None, authority, Arc::new(|| Ok(())))
    };
    assert!(restart(None).is_err());
    let wrong = crate::sealed_access::Authority::new(
        sealed::Identity::generate(),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    assert!(restart(Some(&wrong)).is_err());
    assert_eq!(restart(Some(&authority)).unwrap(), completed);
    let priced = sinex_store::native_materials::PendingImage::AwaitingCosts(completed.clone());
    assert_eq!(
        verify_retained_plan(&blobs, &priced, None, Some(&authority), Arc::new(|| Ok(()))).unwrap(),
        completed
    );
    assert!(
        verify_retained_plan(
            &blobs,
            &plan,
            None,
            Some(&authority),
            Arc::new(|| Err("capture canceled".into()))
        )
        .is_err()
    );
    authority.revoke();
    assert!(restart(Some(&authority)).is_err());
    assert_eq!(completed.metadata.image_hash, expected_hash);
    assert_eq!(completed.metadata.patch_stored_bytes, 0);
    assert_eq!(
        completed.metadata.base_stored_bytes,
        blobs.stored_size(&completed.material.blob).unwrap()
    );
    let mut decoded = Vec::new();
    blobs
        .read_sealed(
            &completed.material.blob,
            Some(completed.material.length),
            &key,
            crate::sealed_access::read_limits(completed.material.length).unwrap(),
            || Ok(()),
        )
        .unwrap()
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(&decoded[..exact.len()], exact);
    let page = &exact[..completed.metadata.page_size as usize];
    let map = completed.metadata.map_offset as usize;
    assert_eq!(
        &decoded[map..map + 32],
        blake3::keyed_hash(&naming, blake3::hash(page).as_bytes()).as_bytes()
    );
}
