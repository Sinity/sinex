use super::{
    CasFsckOptions, CasFsckStopReason, CasStatus, LOCAL_BLAKE3_CAS_BACKEND, check_cas,
    check_cas_with_options, check_cas_with_options_and_control,
};
use crate::runtime::content_store::{
    CasWalkCheckpoint, ContentStoreConfig, MaterialContentStore,
    gc::{sweep_orphans, sweep_orphans_detailed},
};
use crate::runtime::work_control::WorkCancellation;
use camino::Utf8PathBuf;
use serde_json::json;
use sinex_db::models::Blob;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::{MaterialManifestV1, Timestamp, Uuid};
use std::time::{Duration, SystemTime};
use xtask::sandbox::prelude::*;

#[test]
fn default_fsck_options_do_not_impose_arbitrary_limits() {
    let options = CasFsckOptions::default();
    assert_eq!(options.max_runtime, None);
    assert_eq!(options.max_entries, None);
    assert_eq!(options.verify_bytes_per_sec, None);
}

#[sinex_test]
async fn cancelled_fsck_reports_incomplete_without_scanning(ctx: TestContext) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path,
        ..Default::default()
    })?;
    let cancellation = WorkCancellation::new();
    cancellation.cancel();

    let (report, statuses, checkpoint) = check_cas_with_options_and_control(
        ctx.pool(),
        &content_store,
        false,
        CasFsckOptions::default(),
        None,
        cancellation,
    )
    .await?;
    assert!(report.incomplete);
    assert_eq!(report.stop_reason, Some(CasFsckStopReason::Cancelled));
    assert!(statuses.is_empty());
    assert_eq!(checkpoint, CasWalkCheckpoint::default());
    Ok(())
}

#[sinex_test]
async fn pending_cas_delete_survives_failure_and_resumes(_ctx: TestContext) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let source = root_path.join("delete-me.txt");
    tokio::fs::write(&source, b"pending delete").await?;
    let key = content_store.store_file(&source).await?;
    let original_path = content_store
        .path_if_local(&key.key)?
        .expect("local CAS key must resolve to its object");

    let pending = content_store
        .quarantine_local_cas(&key)
        .await?
        .expect("existing CAS object must be quarantined");
    assert!(!original_path.exists());
    assert!(pending.quarantine_path.exists());
    assert_eq!(content_store.list_pending_deletions().await?.len(), 1);

    MaterialContentStore::fail_next_pending_delete_for_tests();
    assert!(
        content_store
            .finalize_pending_deletion(&pending)
            .await
            .is_err()
    );
    assert!(pending.quarantine_path.exists());
    assert_eq!(content_store.list_pending_deletions().await?.len(), 1);

    content_store.drop_content(&key.key, true).await?;
    assert!(!pending.quarantine_path.exists());
    assert!(content_store.list_pending_deletions().await?.is_empty());
    Ok(())
}

#[sinex_test]
async fn referenced_cas_quarantine_is_restored_by_apply_reconciliation(
    ctx: TestContext,
) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let source = root_path.join("restore-me.txt");
    tokio::fs::write(&source, b"restore after reference").await?;
    let key = content_store.store_file(&source).await?;
    let original_path = content_store
        .path_if_local(&key.key)?
        .expect("local CAS key must resolve to its object");
    let pending = content_store
        .quarantine_local_cas(&key)
        .await?
        .expect("existing CAS object must be quarantined");

    ctx.pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(LOCAL_BLAKE3_CAS_BACKEND.to_string())
                .content_hash(key.digest.clone())
                .size_bytes(key.size as i64)
                .checksum_blake3(key.digest.clone())
                .build(),
        )
        .await?;

    let report = sweep_orphans(ctx.pool(), &content_store, true).await?;
    assert_eq!(report.restored, 1);
    assert_eq!(report.dropped, 0);
    assert!(original_path.exists());
    assert!(!pending.quarantine_path.exists());
    assert!(content_store.list_pending_deletions().await?.is_empty());
    Ok(())
}

#[sinex_test]
async fn live_source_material_manifest_cas_reference_survives_apply_orphan_sweep(
    ctx: TestContext,
) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    tokio::fs::create_dir_all(&root_path).await?;

    let manifest_source = root_path.join("source-material-manifest.json");
    tokio::fs::write(&manifest_source, br#"{"version":1,"segments":[]}"#).await?;
    let manifest_key = content_store.store_file(&manifest_source).await?;
    let manifest_path = content_store
        .path_if_local(&manifest_key.key)?
        .expect("fixture manifest must use the local BLAKE3 CAS");

    std::fs::File::open(manifest_path.as_std_path())?.set_times(
        std::fs::FileTimes::new().set_modified(
            SystemTime::now()
                .checked_sub(Duration::from_secs(11 * 60))
                .expect("fixture timestamp must be representable"),
        ),
    )?;

    let material_id = Uuid::now_v7();
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some(&format!("test://source-material-manifest/{material_id}")),
            json!({"material_manifest": {"content_key": manifest_key.key}}),
            Timestamp::now(),
        )
        .await?;

    let (fsck_report, statuses) = check_cas(ctx.pool(), &content_store, false).await?;
    assert_eq!(fsck_report.referenced, 1);
    assert_eq!(fsck_report.orphaned, 0);
    assert_eq!(fsck_report.missing, 0);
    assert!(fsck_report.bytes_verified > 0);
    let manifest_status = statuses
        .iter()
        .find(|status| status.hash == manifest_key.digest)
        .expect("fsck must inspect the source-material manifest CAS file");
    assert_eq!(manifest_status.status, CasStatus::Referenced);
    assert_eq!(
        manifest_status.blob_id.as_deref(),
        Some(format!("material-manifest:{material_id}").as_str()),
        "the source-material manifest reference must be the fsck authority"
    );

    let sweep_report = sweep_orphans(ctx.pool(), &content_store, true).await?;
    assert_eq!(sweep_report.db_backed, 1);
    assert_eq!(sweep_report.orphaned, 0);
    assert_eq!(sweep_report.dropped, 0);
    assert!(
        manifest_path.exists(),
        "anti-vacuity: removing raw.source_material_registry material_manifest content_key references from load_sinexblake3_hashes makes this stale CAS file orphaned and the apply-mode sweep deletes it"
    );

    Ok(())
}

#[sinex_test]
async fn manifest_encoded_bytes_are_reconciled_as_cas_authority(
    ctx: TestContext,
) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let payload = b"encoded bytes retained independently of the manifest object";
    let payload_path = root_path.join("source.bin");
    tokio::fs::write(&payload_path, payload).await?;
    let payload_key = content_store.store_file(&payload_path).await?;

    let material_id = Uuid::now_v7();
    let manifest = MaterialManifestV1::from_capture(
        material_id,
        "source.bin",
        "chunk",
        payload_key.digest.clone(),
        payload.len() as u64,
        json!({
            "material_type": "chunk",
            "pack_member_key": "observed-member-7",
            "logical_source_identifier": "test.pack",
        }),
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );
    let manifest_path = root_path.join("manifest.json");
    tokio::fs::write(&manifest_path, manifest.canonical_bytes()?).await?;
    let manifest_key = content_store.store_file(&manifest_path).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "chunk",
            Some("chunk://test.pack#7"),
            json!({"material_manifest": {"content_key": manifest_key.key}}),
            Timestamp::now(),
        )
        .await?;

    let (report, statuses) = check_cas(ctx.pool(), &content_store, false).await?;
    assert_eq!(report.referenced, 2);
    assert_eq!(report.orphaned, 0);
    assert_eq!(report.missing, 0);
    assert!(statuses.iter().any(|status| {
        status.hash == payload_key.digest
            && status.blob_id.as_deref()
                == Some(format!("material-bytes:{material_id}").as_str())
            && status.status == CasStatus::Referenced
    }));

    let sweep = sweep_orphans(ctx.pool(), &content_store, true).await?;
    assert_eq!(sweep.orphaned, 0);
    assert!(
        content_store
            .path_if_local(&payload_key.key)?
            .expect("payload CAS path")
            .exists(),
        "manifest encoded digest must keep the exact source bytes out of orphan cleanup"
    );
    Ok(())
}

#[sinex_test]
async fn detailed_cas_sweep_returns_orphan_identity_for_operator_output(
    ctx: TestContext,
) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let source = root_path.join("orphan.txt");
    tokio::fs::write(&source, b"operator-visible orphan").await?;
    let key = content_store.store_file(&source).await?;
    let orphan_path = content_store
        .path_if_local(&key.key)?
        .expect("local CAS key must resolve to the stored orphan path");
    std::fs::File::open(orphan_path.as_std_path())?.set_times(
        std::fs::FileTimes::new().set_modified(
            SystemTime::now()
                .checked_sub(Duration::from_secs(11 * 60))
                .expect("test clock must support an eleven-minute subtraction"),
        ),
    )?;

    // Apply-mode fsck refuses to delete when the paired database has no known
    // local-CAS rows. Keep one synthetic authority row so this test reaches
    // the orphan deletion and operator-detail path.
    let retained_hash = blake3::hash(b"retained authority").to_hex().to_string();
    ctx.pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(LOCAL_BLAKE3_CAS_BACKEND.to_string())
                .content_hash(retained_hash.clone())
                .size_bytes(17)
                .checksum_blake3(retained_hash)
                .build(),
        )
        .await?;

    let (report, entries) = sweep_orphans_detailed(ctx.pool(), &content_store, true).await?;
    assert_eq!(report.orphaned, 1);
    assert_eq!(report.dropped, 0);
    assert_eq!(report.quarantined, 1);
    assert_eq!(report.pending_deletes, 1);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, key);
    assert_eq!(entries[0].number, 1);
    assert_eq!(
        entries[0].key.key, key.key,
        "anti-vacuity: local-CAS detailed sweeps must preserve the exact orphan key for operator auditing"
    );
    assert!(
        !orphan_path.exists(),
        "anti-vacuity: apply-mode sweep must move an orphan out of the live CAS tree before retryable deletion"
    );

    let mut pending = content_store
        .list_pending_deletions()
        .await?
        .pop()
        .expect("quarantined orphan must have a durable retry record");
    pending.created_at_unix_secs = pending.created_at_unix_secs.saturating_sub(11 * 60);
    tokio::fs::write(&pending.record_path, serde_json::to_vec(&pending)?).await?;
    let retry_report = sweep_orphans(ctx.pool(), &content_store, true).await?;
    assert_eq!(retry_report.dropped, 1);
    assert_eq!(retry_report.pending_deletes, 0);
    assert!(content_store.list_pending_deletions().await?.is_empty());
    Ok(())
}

#[sinex_test]
async fn apply_fsck_refuses_to_delete_after_a_partial_budgeted_pass(
    ctx: TestContext,
) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let source = root_path.join("orphan.txt");
    tokio::fs::write(&source, b"orphan").await?;
    let key = content_store.store_file(&source).await?;
    let path = content_store
        .path_if_local(&key.key)?
        .expect("fixture must use local CAS");
    std::fs::File::open(path.as_std_path())?.set_times(
        std::fs::FileTimes::new().set_modified(
            SystemTime::now()
                .checked_sub(Duration::from_secs(11 * 60))
                .expect("fixture timestamp must be representable"),
        ),
    )?;
    let authority_hash = blake3::hash(b"database-authority").to_hex().to_string();
    ctx.pool
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(LOCAL_BLAKE3_CAS_BACKEND.to_string())
                .content_hash(authority_hash.clone())
                .size_bytes(18)
                .checksum_blake3(authority_hash)
                .build(),
        )
        .await?;

    let result = check_cas_with_options(
        ctx.pool(),
        &content_store,
        true,
        CasFsckOptions {
            max_runtime: None,
            max_entries: Some(0),
            verify_bytes_per_sec: Some(1024.0),
        },
    )
    .await;
    assert!(result.is_err(), "apply must fail closed on a partial pass");
    assert!(path.exists(), "partial fsck must not delete any candidate");
    Ok(())
}

#[sinex_test]
async fn in_flight_cas_staging_file_survives_apply_fsck(ctx: TestContext) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;

    let authority_source = root_path.join("authority.txt");
    tokio::fs::write(&authority_source, b"authority").await?;
    let authority_key = content_store.store_file(&authority_source).await?;
    let material_id = Uuid::now_v7();
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some(&format!("test://cas-staging/{material_id}")),
            json!({"material_manifest": {"content_key": authority_key.key}}),
            Timestamp::now(),
        )
        .await?;

    let staged_dir = root_path.join("sinex-cas").join("aa").join("bb");
    tokio::fs::create_dir_all(&staged_dir).await?;
    let staged_path = staged_dir.join(format!("{}.tmp-in-flight", authority_key.digest));
    tokio::fs::write(&staged_path, b"partially copied").await?;

    let (report, statuses) = check_cas(ctx.pool(), &content_store, true).await?;
    assert_eq!(report.staged, 1);
    assert_eq!(report.removed, 0);
    assert!(
        staged_path.exists(),
        "anti-vacuity: an in-flight .tmp object must survive apply fsck"
    );
    assert!(
        statuses
            .iter()
            .any(|status| status.status == CasStatus::Staged)
    );
    Ok(())
}

#[sinex_test]
async fn missing_cas_report_uses_configured_hash_path(ctx: TestContext) -> TestResult<()> {
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    let hash = blake3::hash(Uuid::now_v7().as_bytes()).to_hex().to_string();
    let blob = ctx
        .pool()
        .blobs()
        .insert(
            Blob::builder()
                .storage_backend(LOCAL_BLAKE3_CAS_BACKEND.to_string())
                .content_hash(hash.clone())
                .size_bytes(42)
                .checksum_blake3(hash.clone())
                .build(),
        )
        .await?;

    let (report, statuses) = check_cas(ctx.pool(), &content_store, false).await?;
    assert_eq!(report.missing, 1);
    let status = statuses
        .iter()
        .find(|status| status.blob_id.as_deref() == Some(blob.id.to_string().as_str()))
        .expect("missing blob must be reported");
    let expected_path = content_store
        .path_if_local(&format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{hash}"))?
        .expect("valid local CAS key must resolve to a path");
    assert_eq!(status.status, CasStatus::Missing);
    assert_eq!(status.path, expected_path.as_str());
    assert!(
        !status.path.contains("XX/YY"),
        "anti-vacuity: missing-CAS diagnostics must identify the actual configured path"
    );
    Ok(())
}
