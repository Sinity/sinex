// Small inline tests are used here because the parser helper is private
// and tightly coupled to git-annex output semantics.
use super::*;
use xtask::sandbox::sinex_test;

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
