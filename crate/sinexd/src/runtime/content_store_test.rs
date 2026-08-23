// Small inline tests are used here because the parser helper is private
// and tightly coupled to git-annex output semantics.
use super::*;
use crate::runtime::work_control::WorkCancellation;
use xtask::sandbox::{sinex_test, timing::WaitHelpers};

async fn test_cas_root(repo_path: &Utf8Path) -> ::xtask::sandbox::TestResult<()> {
    tokio::fs::create_dir_all(repo_path.join(LOCAL_BLAKE3_CAS_DIR)).await?;
    Ok(())
}

#[sinex_test]
async fn default_blob_retrieval_cap_matches_default_material_assembly_cap()
-> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        ContentStoreConfig::default().max_blob_size,
        512 * 1024 * 1024,
        "anti-vacuity: restoring the old 100 MiB default recreates the accepted-but-unreadable material band"
    );
    Ok(())
}

#[sinex_test]
async fn content_store_default_honors_deployment_size_override() -> ::xtask::sandbox::TestResult<()>
{
    let mut env = xtask::sandbox::EnvGuard::new();
    env.set("SINEX_CONTENT_STORE_MAX_BLOB_SIZE", "123456");
    assert_eq!(
        configured_max_blob_size(),
        123456,
        "the Nix-exported retrieval cap must reach default content-store clients"
    );
    Ok(())
}

#[sinex_test]
async fn parse_unused_output_extracts_numbered_unused_entries() -> ::xtask::sandbox::TestResult<()>
{
    let entries = parse_unused_output(
        br#"{"unused-list":{"2":"SHA256E-s4--beef.txt","1":"SHA256E-s5--deadbeef.dat"}}"#,
    )
    .expect("valid unused output should parse");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].number, 1);
    assert_eq!(entries[0].key.key, "SHA256E-s5--deadbeef.dat");
    assert_eq!(entries[1].number, 2);
    assert_eq!(entries[1].key.digest, "beef.txt");
    Ok(())
}

#[sinex_test]
async fn parse_unused_output_rejects_non_numeric_entry_numbers() -> ::xtask::sandbox::TestResult<()>
{
    let error = parse_unused_output(br#"{"unused-list":{"oops":"SHA256E-s5--deadbeef.dat"}}"#)
        .expect_err("non-numeric unused entry number must fail honestly");

    assert!(error.contains("valid u32"));
    Ok(())
}

#[sinex_test]
async fn local_cas_key_parse_requires_canonical_blake3_digest() -> ::xtask::sandbox::TestResult<()>
{
    let digest = "a".repeat(64);
    let parsed = ContentStoreKey::parse(&format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{digest}"))?;

    assert_eq!(parsed.storage_backend(), LOCAL_BLAKE3_CAS_BACKEND);
    assert_eq!(parsed.size, 42);
    assert_eq!(parsed.digest, digest);

    for key in [
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{}", "a".repeat(63)),
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{}", "a".repeat(65)),
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{}", "A".repeat(64)),
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--{}", "g".repeat(64)),
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--ab/../../target"),
        format!("{LOCAL_BLAKE3_CAS_BACKEND}-s42--/absolute/path"),
    ] {
        assert!(
            ContentStoreKey::parse(&key).is_err(),
            "malformed local CAS key should fail: {key}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn path_if_local_does_not_resolve_malformed_local_cas_key() -> ::xtask::sandbox::TestResult<()>
{
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temp path should be valid utf-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path.clone(),
        num_copies: None,
        large_files: None,
        ..Default::default()
    })?;
    let malicious_key = format!("{LOCAL_BLAKE3_CAS_BACKEND}-s1--ab/../../outside");

    assert!(
        content_store.path_if_local(&malicious_key)?.is_none(),
        "malformed local CAS keys must not resolve to filesystem paths"
    );

    let valid_digest = "0".repeat(64);
    let valid_path = content_store
        .path_if_local(&format!("{LOCAL_BLAKE3_CAS_BACKEND}-s1--{valid_digest}"))?
        .expect("valid local CAS key should resolve");
    assert!(valid_path.starts_with(repo_path.join(LOCAL_BLAKE3_CAS_DIR)));
    assert_eq!(valid_path.file_name(), Some(valid_digest.as_str()));

    Ok(())
}

#[sinex_test]
async fn small_files_use_local_cas_without_content_store_process()
-> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temp path should be valid utf-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path.clone(),
        num_copies: None,
        large_files: None,
        ..Default::default()
    })?;
    reset_content_store_process_counters();

    let source_path = repo_path.join("small-material.jsonl");
    tokio::fs::write(&source_path, br#"{"event":"small"}"#).await?;

    let key = content_store.store_file(&source_path).await?;
    assert_eq!(key.storage_backend(), LOCAL_BLAKE3_CAS_BACKEND);
    assert_eq!(key.size, 17);
    let counters = content_store_process_counters_snapshot();
    assert_eq!(
        counters.git_annex_commands, 0,
        "small-file storage should stay on local CAS and avoid git-annex subprocesses"
    );

    let content_path = content_store
        .path_if_local(&key.key)?
        .expect("local CAS key should resolve to a local path");
    assert!(content_path.exists());
    content_store.ensure_content_local(&key.key).await?;

    let verification = content_store
        .verify_key(false, false, Some(&key.key))
        .await?;
    assert!(verification.success);

    content_store.drop_content(&key.key, true).await?;
    assert!(!content_path.exists());
    Ok(())
}

#[sinex_test]
async fn direct_store_file_enforces_configured_size_limit() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temp path should be valid utf-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path.clone(),
        max_blob_size: 1,
        ..Default::default()
    })?;
    let source_path = repo_path.join("too-large.bin");
    tokio::fs::write(&source_path, b"12").await?;

    let error = content_store
        .store_file(&source_path)
        .await
        .expect_err("direct content-store writes must enforce max_blob_size");
    assert!(error.to_string().contains("exceeds limit"));
    Ok(())
}

#[sinex_test]
async fn bounded_content_store_reads_reject_oversized_files_before_buffering()
-> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    test_cas_root(&repo_path).await?;
    let source_path = repo_path.join("oversized.bin");
    tokio::fs::write(&source_path, b"1234").await?;

    let error = MaterialContentStore::read_file_with_limit(&source_path, 3)
        .await
        .expect_err("bounded reads must reject content beyond the configured limit");
    assert!(error.to_string().contains("exceeds limit"));
    Ok(())
}

#[sinex_test]
async fn direct_content_store_inputs_must_remain_under_configured_root()
-> ::xtask::sandbox::TestResult<()> {
    let parent_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(parent_dir.path().join("content-store"))
        .expect("temporary content-store path should be UTF-8");
    let outside_path = Utf8PathBuf::from_path_buf(parent_dir.path().join("outside.bin"))
        .expect("temporary outside path should be UTF-8");
    tokio::fs::write(&outside_path, b"outside content").await?;
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;

    for input in [Utf8PathBuf::from("../outside.bin"), outside_path.clone()] {
        let store_error = content_store
            .store_file(&input)
            .await
            .expect_err("store_file must reject an input outside the configured root");
        assert!(
            store_error.to_string().contains("escapes configured root"),
            "unexpected store_file error: {store_error}"
        );

        let lookup_error = content_store
            .lookup_content_key(&input)
            .await
            .expect_err("lookup_content_key must reject an input outside the configured root");
        assert!(
            lookup_error.to_string().contains("escapes configured root"),
            "unexpected lookup_content_key error: {lookup_error}"
        );
    }

    #[cfg(unix)]
    {
        let symlink_path = root_path.join("linked-outside.bin");
        std::os::unix::fs::symlink(&outside_path, &symlink_path)?;
        let error = content_store
            .store_file(&symlink_path)
            .await
            .expect_err("symlinked inputs must not bypass root containment");
        assert!(
            error.to_string().contains("escapes configured root"),
            "unexpected symlink error: {error}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn async_content_store_commands_are_serialized_through_process_exit()
-> ::xtask::sandbox::TestResult<()> {
    let temp_dir = tempfile::tempdir()?;
    let marker = Utf8PathBuf::from_path_buf(temp_dir.path().join("command-order.log"))
        .expect("temporary marker path should be UTF-8");

    let first_marker = marker.clone();
    let first = tokio::spawn(async move {
        let mut command = AsyncCommand::new("sh");
        command.args([
            "-c",
            "printf 'first-start\\n' > \"$1\"; sleep 0.2; printf 'first-end\\n' >> \"$1\"",
            "content-store-test",
            first_marker.as_str(),
        ]);
        run_command_async(command, "first serialization probe").await
    });

    WaitHelpers::wait_for_condition(
        || {
            let marker = marker.clone();
            async move { Ok::<_, std::io::Error>(marker.exists()) }
        },
        2,
    )
    .await?;

    let second_marker = marker.clone();
    let second = tokio::spawn(async move {
        let mut command = AsyncCommand::new("sh");
        command.args([
            "-c",
            "printf 'second\\n' >> \"$1\"",
            "content-store-test",
            second_marker.as_str(),
        ]);
        run_command_async(command, "second serialization probe").await
    });

    first.await??;
    second.await??;
    let order = tokio::fs::read_to_string(&marker).await?;
    assert_eq!(
        order, "first-start\nfirst-end\nsecond\n",
        "anti-vacuity: the process lock must cover subprocess execution, not only invocation counting"
    );
    Ok(())
}

#[sinex_test]
async fn local_cas_paths_reject_symlink_escape() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let outside_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    test_cas_root(&repo_path).await?;
    let outside_path = outside_dir.path().join("outside");
    tokio::fs::write(&outside_path, b"outside").await?;
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path,
        ..Default::default()
    })?;
    let digest = "a".repeat(64);
    let cas_path = content_store
        .path_if_local(&format!("{LOCAL_BLAKE3_CAS_BACKEND}-s7--{digest}"))?
        .expect("valid local CAS key should resolve");
    tokio::fs::create_dir_all(cas_path.parent().expect("CAS path has a parent")).await?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside_path, &cas_path)?;

    #[cfg(unix)]
    assert!(
        content_store
            .canonicalize_local_cas_path(&cas_path)
            .await
            .is_err(),
        "CAS symlinks must not resolve outside the configured root"
    );
    Ok(())
}

#[sinex_test]
async fn annex_path_arguments_reject_traversal() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temp path should be valid utf-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path,
        legacy_annex_enabled: true,
        ..Default::default()
    })?;

    let error = content_store
        .resolve_argument("../outside")
        .await
        .expect_err("annex path arguments must not escape the configured root");
    assert!(error.to_string().contains("root-contained"));
    Ok(())
}

#[sinex_test]
async fn cas_walker_batches_and_resumes_at_completed_prefixes() -> ::xtask::sandbox::TestResult<()>
{
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    test_cas_root(&repo_path).await?;
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path.clone(),
        ..Default::default()
    })?;
    let first_dir = repo_path.join(LOCAL_BLAKE3_CAS_DIR).join("aa").join("bb");
    let second_dir = repo_path.join(LOCAL_BLAKE3_CAS_DIR).join("cc").join("dd");
    tokio::fs::create_dir_all(&first_dir).await?;
    tokio::fs::create_dir_all(&second_dir).await?;
    tokio::fs::write(first_dir.join("first"), b"first").await?;
    tokio::fs::write(second_dir.join("second"), b"second").await?;

    let mut walker = content_store.cas_walker(None).await?;
    let first_batch = walker.next_batch(1).await?;
    assert_eq!(first_batch.entries.len(), 1);
    assert_eq!(first_batch.entries[0].0, "first");
    assert_eq!(first_batch.checkpoint, CasWalkCheckpoint::default());
    assert!(!first_batch.complete);

    let second_batch = walker.next_batch(1).await?;
    assert_eq!(second_batch.entries[0].0, "second");
    assert_eq!(
        second_batch.checkpoint,
        CasWalkCheckpoint {
            prefix_a: Some("aa".to_owned()),
            prefix_b: Some("bb".to_owned()),
            complete: false,
        }
    );

    let mut resumed = content_store
        .cas_walker(Some(second_batch.checkpoint))
        .await?;
    let resumed_batch = resumed.next_batch(1).await?;
    assert_eq!(resumed_batch.entries[0].0, "second");
    let completion = resumed.next_batch(1).await?;
    assert!(completion.complete);
    assert!(completion.entries.is_empty());
    Ok(())
}

#[sinex_test]
async fn cas_faults_leave_staged_file_and_lease_recoverable() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    test_cas_root(&repo_path).await?;
    let source = repo_path.join("source.bin");
    tokio::fs::write(&source, b"staged fault").await?;
    MaterialContentStore::init_with_config(&repo_path, None, false).await?;
    let injector = FaultInjector::default();
    injector.fail_once(FaultPoint::CasStagedFile);
    let store = MaterialContentStore::new_with_fault_injector(
        ContentStoreConfig {
            root_path: repo_path.clone(),
            ..Default::default()
        },
        injector,
    );

    assert!(store.store_file_with_lease(&source).await.is_err());
    assert_eq!(store.list_write_leases().await?.len(), 1);
    let entries = store.walk_cas().await?;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].0.contains(".tmp-"));
    Ok(())
}

#[sinex_test]
async fn cas_publish_fault_preserves_published_object_until_commit_cleanup()
-> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    let source = repo_path.join("source.bin");
    tokio::fs::write(&source, b"publish fault").await?;
    MaterialContentStore::init_with_config(&repo_path, None, false).await?;
    let injector = FaultInjector::default();
    injector.fail_once(FaultPoint::CasPublish);
    let store = MaterialContentStore::new_with_fault_injector(
        ContentStoreConfig {
            root_path: repo_path,
            ..Default::default()
        },
        injector,
    );

    assert!(store.store_file_with_lease(&source).await.is_err());
    let leases = store.list_write_leases().await?;
    assert_eq!(leases.len(), 1);
    let target = store
        .path_if_local(&leases[0].key.key)?
        .expect("published local CAS key must resolve");
    assert!(
        target.exists(),
        "publish interruption must leave the object recoverable"
    );
    store.release_write_lease(&leases[0]).await?;
    assert!(
        target.exists(),
        "lease cleanup must not delete published bytes"
    );
    Ok(())
}

#[sinex_test]
async fn post_rename_directory_sync_failure_is_not_acknowledged_and_reingests()
-> ::xtask::sandbox::TestResult<()> {
    fn fail_directory_sync(_: &std::path::Path) -> std::io::Result<()> {
        Err(std::io::Error::other("injected directory sync failure"))
    }

    let repo_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temp path should be valid UTF-8");
    let source_path = root_path.join("publish-boundary.jsonl");
    let payload = br#"{"event":"publish-boundary"}"#;
    tokio::fs::write(&source_path, payload).await?;

    let mut faulted_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root_path.clone(),
        ..Default::default()
    })?;
    faulted_store.sync_parent_directory = fail_directory_sync;

    let error = faulted_store
        .store_file(&source_path)
        .await
        .expect_err("post-rename directory sync failure must not acknowledge ingestion");
    assert!(
        error
            .to_string()
            .contains("injected directory sync failure")
    );

    let digest = blake3::hash(payload).to_hex().to_string();
    let key = format!("{LOCAL_BLAKE3_CAS_BACKEND}-s{}--{digest}", payload.len());
    let published_path = faulted_store
        .path_if_local(&key)?
        .expect("fixture must use local CAS");
    assert!(
        published_path.exists(),
        "failure must occur after rename while caller observes no success"
    );

    let restarted_store = MaterialContentStore::new(ContentStoreConfig {
        root_path,
        ..Default::default()
    })?;
    let recovered = restarted_store.store_file(&source_path).await?;
    assert_eq!(recovered.key, key);
    restarted_store.ensure_content_local(&key).await?;
    Ok(())
}

#[sinex_test]
async fn cas_quarantine_and_delete_faults_are_resumable() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    let source = repo_path.join("source.bin");
    tokio::fs::write(&source, b"quarantine fault").await?;
    MaterialContentStore::init_with_config(&repo_path, None, false).await?;
    let injector = FaultInjector::default();
    let store = MaterialContentStore::new_with_fault_injector(
        ContentStoreConfig {
            root_path: repo_path,
            ..Default::default()
        },
        injector.clone(),
    );
    let key = store.store_file(&source).await?;
    injector.fail_once(FaultPoint::CasQuarantine);
    assert!(store.quarantine_local_cas(&key).await.is_err());
    let pending = store.list_pending_deletions().await?;
    assert_eq!(pending.len(), 1);
    let pending = pending[0].clone();
    assert!(!pending.source_path.exists());
    injector.fail_once(FaultPoint::CasPendingDelete);
    assert!(store.finalize_pending_deletion(&pending).await.is_err());
    assert_eq!(store.list_pending_deletions().await?.len(), 1);
    store.finalize_pending_deletion(&pending).await?;
    assert!(store.list_pending_deletions().await?.is_empty());
    Ok(())
}

#[sinex_test]
async fn cas_walker_cancellation_interrupts_directory_enumeration()
-> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
    let store = MaterialContentStore::new(ContentStoreConfig {
        root_path: repo_path,
        ..Default::default()
    })?;
    tokio::fs::create_dir_all(store.root_path().join(LOCAL_BLAKE3_CAS_DIR).join("aa")).await?;
    let cancellation = WorkCancellation::new();
    cancellation.cancel();
    assert!(
        store
            .cas_walker_with_control(None, Some(cancellation))
            .await
            .is_err()
    );
    Ok(())
}
