// Small inline tests are used here because the parser helper is private
// and tightly coupled to git-annex output semantics.
use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn parse_unused_output_extracts_numbered_unused_entries()
-> ::xtask::sandbox::TestResult<()> {
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
async fn parse_unused_output_rejects_non_numeric_entry_numbers()
-> ::xtask::sandbox::TestResult<()> {
    let error = parse_unused_output(br#"{"unused-list":{"oops":"SHA256E-s5--deadbeef.dat"}}"#)
        .expect_err("non-numeric unused entry number must fail honestly");

    assert!(error.contains("valid u32"));
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
