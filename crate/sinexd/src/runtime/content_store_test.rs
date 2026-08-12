// Small inline tests are used here because the parser helper is private
// and tightly coupled to git-annex output semantics.
use super::*;
use xtask::sandbox::{sinex_test, timing::WaitHelpers};

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
async fn content_store_default_honors_deployment_size_override()
-> ::xtask::sandbox::TestResult<()> {
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
    assert_eq!(order, "first-start\nfirst-end\nsecond\n", "anti-vacuity: the process lock must cover subprocess execution, not only invocation counting");
    Ok(())
}

#[sinex_test]
async fn local_cas_paths_reject_symlink_escape() -> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let outside_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
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
async fn cas_walker_batches_and_resumes_at_completed_prefixes()
-> ::xtask::sandbox::TestResult<()> {
    let repo_dir = tempfile::tempdir()?;
    let repo_path = Utf8PathBuf::from_path_buf(repo_dir.path().to_path_buf())
        .expect("temporary path should be valid utf-8");
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
