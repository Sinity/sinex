// # External System Integration Tests
//
// Integration tests with external systems and services:
// - Content-store git-annex backend
// - PostgreSQL with TimescaleDB extensions
//
// ## Test Categories
//
// - **Content Store Integration**: File storage, retrieval, and deduplication
// - **Database Integration**: External database service integration
//
// ## Performance Expectations
//
// - **Individual tests**: 10-60 seconds
// - **Resource usage**: Significant disk I/O, external process spawning
// - **Dependencies**: git-annex backend, external command tools, filesystem access

use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::{ContentStoreConfig, ContentStoreKey, MaterialContentStore};
use sqlx::Row;
use tempfile::TempDir;
use tokio::fs;
use xtask::sandbox::TestResult;
use xtask::sandbox::prelude::*;

// ==================== CONTENT STORE INTEGRATION TESTS ====================

async fn setup_test_content_store() -> TestResult<(MaterialContentStore, TempDir)> {
    let temp_dir = TempDir::new()?;
    let repo_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("content-store root path must be valid UTF-8"))?;

    MaterialContentStore::init_with_config(&repo_path, Some("test-repo"), false).await?;

    let config = ContentStoreConfig {
        root_path: repo_path.clone(),
        num_copies: Some(1),
        large_files: None,
        legacy_annex_enabled: false,
        ..Default::default()
    };

    let content_store = MaterialContentStore::new(config)?;

    Ok((content_store, temp_dir))
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_file_add_and_retrieve(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Create a test file
    let test_file = temp_dir.path().join("test.txt");
    let content = b"Hello, content store!";
    fs::write(&test_file, content).await?;

    // Add file to content store
    let content_key = content_store.store_file(&test_file).await?;

    // Verify key was generated
    assert!(!content_key.key.is_empty());
    assert_eq!(content_key.size, content.len() as u64);

    assert_local_cas_content(&content_store, &content_key, content).await?;

    // Verify file still exists and is a symlink
    assert!(test_file.exists());

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_large_file_handling(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Create 1MB of data
    let content = vec![0u8; 1024 * 1024];
    let large_file = temp_dir.path().join("large.bin");
    fs::write(&large_file, &content).await?;

    // Add large file to content store
    let content_key = content_store.store_file(&large_file).await?;

    assert_eq!(content_key.size, content.len() as u64);
    assert_local_cas_content(&content_store, &content_key, &content).await?;

    // Check status
    let status = content_store.status().await?;
    assert!(status.contains("Files: 1"));
    assert!(status.contains(&format!("Total size: {} bytes", content.len())));

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_content_key_lookup(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Create a test file with known content
    let test_file = temp_dir.path().join("lookup_test.txt");
    let content = b"Content for key lookup test";
    fs::write(&test_file, content).await?;

    // Add to content store
    let original_key = content_store.store_file(&test_file).await?;

    // Look up the key again
    let looked_up_key = content_store.lookup_content_key(&test_file).await?;

    // Keys should match
    assert_eq!(original_key.key, looked_up_key.key);
    assert_eq!(original_key.size, looked_up_key.size);
    assert_eq!(
        original_key.storage_backend(),
        looked_up_key.storage_backend()
    );
    assert_local_cas_content(&content_store, &looked_up_key, content).await?;

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_drop_content(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Create and add a file
    let test_file = temp_dir.path().join("drop_test.txt");
    fs::write(&test_file, b"Content to drop").await?;

    let key = content_store.store_file(&test_file).await?;
    let content_path = assert_local_cas_content(&content_store, &key, b"Content to drop").await?;

    // Try to drop content without force; local CAS requires explicit deletion.
    let drop_result = content_store.drop_content(&key.key, false).await;
    assert!(drop_result.is_err());

    // Force drop
    content_store.drop_content(&key.key, true).await?;
    assert!(!content_path.exists());
    assert!(content_store.ensure_content_local(&key.key).await.is_err());

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_fsck(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Add some files
    let mut keys = Vec::new();
    for i in 0..3 {
        let file = temp_dir.path().join(format!("file_{i}.txt"));
        let content = format!("Content {i}");
        fs::write(&file, &content).await?;
        let key = content_store.store_file(&file).await?;
        assert_local_cas_content(&content_store, &key, content.as_bytes()).await?;
        keys.push(key);
    }

    for key in keys {
        let fsck_output = content_store
            .verify_key(true, false, Some(&key.key))
            .await?;
        assert!(fsck_output.success);
        assert!(fsck_output.output.contains("local CAS verification"));
    }

    Ok(())
}

#[ignore = "requires external content-store infrastructure (legacy git-annex)"]
#[sinex_test]
async fn test_git_annex_configuration(_ctx: TestContext) -> TestResult<()> {
    system_test_preflight()?;

    let temp_dir = TempDir::new()?;
    let repo_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("content-store root path must be valid UTF-8"))?;

    // Initialize with configuration
    MaterialContentStore::init_with_config(&repo_path, Some("configured-repo"), true).await?;

    let config = ContentStoreConfig {
        root_path: repo_path.clone(),
        num_copies: Some(2),
        large_files: Some("*.bin".to_string()),
        legacy_annex_enabled: true,
        ..Default::default()
    };

    let content_store = MaterialContentStore::new(config)?;
    content_store.configure().await?;

    // Verify configuration was applied
    let num_copies_output = tokio::process::Command::new("git")
        .args(["config", "annex.numcopies"])
        .current_dir(&repo_path)
        .output()
        .await?;
    assert!(
        num_copies_output.status.success(),
        "git config annex.numcopies should succeed"
    );

    let num_copies = String::from_utf8_lossy(&num_copies_output.stdout)
        .trim()
        .to_string();
    assert_eq!(num_copies, "2");

    let large_files_output = tokio::process::Command::new("git")
        .args(["config", "annex.largefiles"])
        .current_dir(&repo_path)
        .output()
        .await?;
    assert!(
        large_files_output.status.success(),
        "git config annex.largefiles should succeed"
    );

    let large_files = String::from_utf8_lossy(&large_files_output.stdout)
        .trim()
        .to_string();
    assert_eq!(large_files, "*.bin");

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test(timeout = 30)]
async fn test_concurrent_file_operations(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;
    let content_store = std::sync::Arc::new(content_store);
    let mut handles = vec![];

    // Spawn multiple concurrent operations
    for i in 0..5 {
        let content_store = content_store.clone();
        let temp_path = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let file_path = temp_path.join(format!("concurrent_{i}.txt"));
            let content = format!("Concurrent content {i}");

            // Write file
            fs::write(&file_path, content.as_bytes()).await?;

            // Add to content store
            let key = content_store.store_file(&file_path).await?;

            Ok::<_, color_eyre::eyre::Error>(key)
        });

        handles.push(handle);
    }

    // Wait for all operations
    let mut keys = vec![];
    for handle in handles {
        let key = handle.await??;
        keys.push(key);
    }

    // Verify all files were added
    assert_eq!(keys.len(), 5);
    for key in keys {
        assert!(!key.key.is_empty());
        assert!(key.is_local_blake3_cas());
    }

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test]
async fn test_files_in_subdirectories(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    // Create subdirectory structure
    let sub_dir = temp_dir.path().join("nested").join("path");
    fs::create_dir_all(&sub_dir).await?;

    // Create file in subdirectory
    let nested_file = sub_dir.join("data.json");
    let content = br#"{"nested": "json", "data": true}"#;
    fs::write(&nested_file, content).await?;

    // Add to content store
    let key = content_store.store_file(&nested_file).await?;

    // Verify path structure
    assert!(nested_file.exists());
    assert_eq!(key.size, content.len() as u64);

    assert_local_cas_content(&content_store, &key, content).await?;

    Ok(())
}

#[ignore = "requires external content-store infrastructure"]
#[sinex_test(timeout = 30)]
async fn test_content_store_deduplication(_ctx: TestContext) -> TestResult<()> {
    let (content_store, temp_dir) = setup_test_content_store().await?;

    let content = b"Duplicate content for dedup test";

    // Create two files with same content
    let file1 = temp_dir.path().join("dup1.txt");
    let file2 = temp_dir.path().join("dup2.txt");

    fs::write(&file1, content).await?;
    fs::write(&file2, content).await?;

    // Add both to content store
    let key1 = content_store.store_file(&file1).await?;
    let key2 = content_store.store_file(&file2).await?;

    // Both files should exist
    assert!(file1.exists());
    assert!(file2.exists());

    // Keys should be identical (git-annex deduplicates by content)
    assert_eq!(key1.key, key2.key);
    assert_eq!(key1.digest, key2.digest);

    let content_path1 = assert_local_cas_content(&content_store, &key1, content).await?;
    let content_path2 = assert_local_cas_content(&content_store, &key2, content).await?;
    assert_eq!(content_path1, content_path2);

    Ok(())
}

async fn assert_local_cas_content(
    content_store: &MaterialContentStore,
    key: &ContentStoreKey,
    expected_content: &[u8],
) -> TestResult<Utf8PathBuf> {
    assert!(
        key.is_local_blake3_cas(),
        "store_file should produce local BLAKE3 CAS keys"
    );

    let content_path = content_store
        .path_if_local(&key.key)?
        .ok_or_else(|| color_eyre::eyre::eyre!("local CAS key did not resolve: {}", key.key))?;
    assert!(
        content_path.exists(),
        "local CAS path should exist: {content_path}"
    );

    let stored = fs::read(&content_path).await?;
    assert_eq!(stored, expected_content);

    content_store.ensure_content_local(&key.key).await?;
    let verification = content_store
        .verify_key(false, false, Some(&key.key))
        .await?;
    assert!(
        verification.success,
        "local CAS verification should succeed: {}",
        verification.output
    );

    Ok(content_path)
}

// ==================== DATABASE INTEGRATION TESTS ====================

#[ignore = "requires external database infrastructure"]
#[sinex_test]
async fn test_external_database_timescaledb_functions(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    let extension = sqlx::query("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
        .fetch_optional(&pool)
        .await?;
    assert!(
        extension.is_some(),
        "TimescaleDB extension must be installed in the sandbox database"
    );

    let bucket = sqlx::query(
        "
        SELECT
            EXTRACT(MINUTE FROM time_bucket('1 minute', TIMESTAMPTZ '2026-05-06 12:34:56+00'))::int AS minute,
            EXTRACT(SECOND FROM time_bucket('1 minute', TIMESTAMPTZ '2026-05-06 12:34:56+00'))::int AS second
        ",
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(bucket.get::<i32, _>("minute"), 34);
    assert_eq!(bucket.get::<i32, _>("second"), 0);

    Ok(())
}

#[ignore = "requires external database infrastructure"]
#[sinex_test]
async fn test_external_database_extensions(ctx: TestContext) -> TestResult<()> {
    // Test that required database extensions are available
    let pool = ctx.pool().clone();

    // Validate UUIDv7 function expected by canonical schema.
    let uuid_test = sqlx::query("SELECT uuidv7()::text as test_uuid")
        .fetch_one(&pool)
        .await?;
    let uuid_str = uuid_test.get::<String, _>("test_uuid");
    assert_eq!(uuid_str.len(), 36);

    Ok(())
}
