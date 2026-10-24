use super::*;
use crate::{
    Blobs,
    admission::{NoDerivations, admit},
    testdb,
};
use sinex_model::{Batch, BatchKind, InputRef, ObservationFacts, RevisionChange, RunId};
use sqlx::{Connection, Executor};

async fn pending(conn: &mut PgConnection, source: &str, blob: &str, length: u64) -> Pending {
    sqlx::query(
        "INSERT INTO raw.bindings(binding,source,input,settings) VALUES($1,$1,'synthetic','{}')",
    )
    .bind(source)
    .execute(&mut *conn)
    .await
    .unwrap();
    let material = MaterialId::new();
    let cursor = serde_json::json!({"material":material,"through":length});
    let progress = SourceProgress {
        native_pending: None,
        erased_replay: None,
        excision_basis: Some(
            crate::excision::operations_basis(conn, source)
                .await
                .unwrap(),
        ),
        binding: source.into(),
        image: None,
        image_revision: None,
        material_keys: vec![],
        input: InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "h".into(),
        },
        observation: ObservationFacts {
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Snapshot,
        material: Some(MaterialRef {
            id: material,
            input: None,
            blob: blob.into(),
            length,
            part_index: 0,
            previous_part: None,
            parser: "lines".into(),
            parser_version: 1,
            manifest: serde_json::json!({}),
        }),
        cursor: cursor.clone(),
        carry: Value::Null,
    };
    Pending {
        material,
        source: source.into(),
        binding: source.into(),
        progress,
        removed: Vec::new(),
        cited: Vec::new(),
        image: None,
        base_cursor: Value::Null,
        base_carry: Value::Null,
        start_cursor: Value::Null,
        end_cursor: cursor,
        state: State::Reserved,
    }
}
async fn reservation(conn: &mut PgConnection, p: &Pending) {
    let mut tx = conn.begin().await.unwrap();
    reserve(&mut tx, p).await.unwrap();
    tx.commit().await.unwrap();
}
async fn admission(conn: &mut PgConnection, p: &Pending) -> Result<()> {
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Source(Box::new(
            p.first_progress(p.end_cursor.clone(), Value::Null)?,
        )),
        records: vec![],
        archive: p.first_archive(),
    };
    let mut tx = conn.begin().await?;
    match admit(&mut tx, &batch, &NoDerivations).await {
        Ok(_) => tx.commit().await.map_err(Into::into),
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

#[tokio::test]
async fn two_sources_same_hash_reservation_survives_cleanup_refusal_and_first_admission_retry() {
    let (db, mut conn) = testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = b"unrelated exact pending version\n";
    let stage = blobs.prepare_plain(bytes.as_slice()).unwrap();
    let name = stage.name();
    let a = pending(&mut conn, "source-a", &name, bytes.len() as u64).await;
    let mut b = pending(&mut conn, "source-b", &name, bytes.len() as u64).await;
    b.removed = vec![sinex_model::RecordLocation {
        material: a.material,
        anchor: 0,
    }];
    // A previously owned the same name. Its purge marked it for deletion.
    blobs.put(bytes).unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting')")
        .bind(&name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    reserve(&mut tx, &b).await.unwrap();
    let mut cleanup = PgConnection::connect(&db.url(None)).await.unwrap();
    cleanup
        .execute("SET default_transaction_isolation='repeatable read'")
        .await
        .unwrap();
    let cleanup_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut cleanup)
        .await
        .unwrap();
    let worker_blobs = blobs.clone();
    let worker = tokio::spawn(async move {
        let result = crate::excision::finish_deletions(&mut cleanup, &worker_blobs).await;
        (cleanup, result)
    });
    let mut observer = PgConnection::connect(&db.url(None)).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2),async {
        loop {
            let blocked:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(cleanup_pid).fetch_one(&mut observer).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }).await.expect("cleanup did not reach the reserved blob lock");
    assert_eq!(blobs.read(&name).unwrap(), bytes);
    tx.commit().await.unwrap();
    let (mut cleanup, result) = worker.await.unwrap();
    assert!(
        result.unwrap().0.is_empty(),
        "waiting cleanup failed to recheck the newly committed owner"
    );
    stage.install().unwrap();
    installed(&mut conn, b.material).await.unwrap();
    let recovered = load(&mut conn, b.material).await.unwrap().unwrap();
    assert_eq!(removal_plan(&recovered.first_archive()).unwrap(), b.removed);
    let missing_plan = Batch {
        run: RunId::new(),
        kind: BatchKind::Source(Box::new(
            recovered
                .first_progress(recovered.end_cursor.clone(), Value::Null)
                .unwrap(),
        )),
        records: vec![],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    let rejected = admit(&mut tx, &missing_plan, &NoDerivations)
        .await
        .unwrap_err();
    tx.rollback().await.unwrap();
    assert!(
        rejected
            .to_string()
            .contains("frozen installed publication")
    );
    assert!(
        crate::positions::load(&mut conn, &b.binding)
            .await
            .unwrap()
            .is_none()
    );

    conn.execute("CREATE FUNCTION public.reject_pending() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic refusal'; END $$; CREATE TRIGGER reject_pending BEFORE INSERT ON raw.positions FOR EACH ROW EXECUTE FUNCTION public.reject_pending()").await.unwrap();
    assert!(admission(&mut conn, &b).await.is_err());
    assert!(load(&mut conn, b.material).await.unwrap().is_some());
    assert!(
        crate::excision::finish_deletions(&mut cleanup, &blobs)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    conn.execute(
        "DROP TRIGGER reject_pending ON raw.positions; DROP FUNCTION public.reject_pending()",
    )
    .await
    .unwrap();
    admission(&mut conn, &b).await.unwrap();
    assert!(load(&mut conn, b.material).await.unwrap().is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw.pending_native_blobs")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        0
    );
    assert_eq!(blobs.read(&name).unwrap(), bytes);
    assert!(
        crate::positions::load(&mut conn, &a.binding)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn deletion_winning_before_reservation_leaves_explicit_missing_custody_until_exact_install() {
    let (_db, mut conn) = testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = b"exact source-b version\n";
    let stage = blobs.prepare_plain(bytes.as_slice()).unwrap();
    let name = stage.name();
    let _a = pending(&mut conn, "source-a", &name, bytes.len() as u64).await;
    let b = pending(&mut conn, "source-b", &name, bytes.len() as u64).await;
    blobs.put(bytes).unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting')")
        .bind(&name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        crate::excision::finish_deletions(&mut conn, &blobs)
            .await
            .unwrap()
            .0,
        vec![name.clone()]
    );
    reservation(&mut conn, &b).await;
    assert!(!blobs.contains(&name).unwrap());
    assert!(load(&mut conn, b.material).await.unwrap().unwrap().state == State::Reserved);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM core.blobs WHERE hash=$1")
            .bind(&name)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        "reserved"
    );
    assert!(admission(&mut conn, &b).await.is_err());
    assert!(
        crate::positions::load(&mut conn, &b.binding)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        crate::excision::finish_deletions(&mut conn, &blobs)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    stage.install().unwrap();
    installed(&mut conn, b.material).await.unwrap();
    // Derived references cannot be independently edited to authorize admission.
    sqlx::query("DELETE FROM raw.pending_native_blobs WHERE material_id=$1")
        .bind(b.material.0)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(load(&mut conn, b.material).await.is_err());
    reservation(&mut conn, &b).await;
    admission(&mut conn, &b).await.unwrap();
    assert_eq!(blobs.read(&name).unwrap(), bytes);
}

#[tokio::test]
async fn citation_publication_versions_and_projection_fail_closed_without_changing_legacy_ownership()
 {
    let (_db, mut conn) = testdb::fresh().await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(directory.path()).unwrap();
    let body = blobs.put(b"record\n").unwrap();
    let mut current = pending(&mut conn, "current", &body, 7).await;
    let media = format!("blake3:{}", blake3::hash(b"media"));
    current.cited.push(sinex_model::Cited {
        blob: media.clone(),
        size: 5,
    });
    reservation(&mut conn, &current).await;
    let frozen: Value = sqlx::query_scalar(
        "SELECT publication FROM raw.pending_native_materials WHERE material_id=$1",
    )
    .bind(current.material.0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(frozen["version"], 2);
    assert_eq!(
        load(&mut conn, current.material)
            .await
            .unwrap()
            .unwrap()
            .cited,
        current.cited
    );
    for mutate in [
        "unknown",
        "legacy-citations",
        "missing-citations",
        "wrong-size",
    ] {
        let mut changed = frozen.clone();
        match mutate {
            "unknown" => changed["version"] = serde_json::json!(99),
            "legacy-citations" => changed["version"] = serde_json::json!(1),
            "missing-citations" => {
                changed.as_object_mut().unwrap().remove("cited");
            }
            "wrong-size" => changed["cited"][0]["size"] = serde_json::json!(6),
            _ => unreachable!(),
        }
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("UPDATE raw.pending_native_materials SET publication=$2 WHERE material_id=$1")
            .bind(current.material.0)
            .bind(changed)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(
            load(&mut tx, current.material).await.is_err(),
            "accepted {mutate}"
        );
        tx.rollback().await.unwrap();
    }
    installed(&mut conn, current.material).await.unwrap();
    assert!(
        admission(&mut conn, &current).await.is_err(),
        "first admission discarded frozen media citations"
    );
    assert!(load(&mut conn, current.material).await.unwrap().is_some());
    let legacy = pending(&mut conn, "legacy", &body, 7).await;
    reservation(&mut conn, &legacy).await;
    let old: Value = sqlx::query_scalar(
        "SELECT publication FROM raw.pending_native_materials WHERE material_id=$1",
    )
    .bind(legacy.material.0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(old["version"], 1);
    assert!(old.get("cited").is_none());
    assert!(
        load(&mut conn, legacy.material)
            .await
            .unwrap()
            .unwrap()
            .cited
            .is_empty()
    );
    assert!(
        citation_set(&[sinex_model::Cited {
            blob: format!("blake3:{}", "é".repeat(32)),
            size: 0
        }])
        .is_err()
    );
}
