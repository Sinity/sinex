//! BlobManager Integration Tests
//!
//! Comprehensive test suite for BlobManager component focusing on:
//! 1. Content-based deduplication using BLAKE3 hashing
//! 2. Git-annex integration (add, get, verify operations)
//! 3. Graceful degradation when git-annex unavailable
//! 4. Large blob handling and performance characteristics
//! 5. Concurrent operations and thread safety
//! 6. Error handling and edge cases
//! 7. Integrity verification and corruption detection
//!
//! IMPORTANT: These tests require git-annex to be available. If git-annex
//! is not installed, tests will be skipped with appropriate warnings.

use sinex_test_utils::prelude::*;
use sinex_test_utils::resources::{create_test_file, temp_dir};
use futures;
use sinex_satellite_sdk::annex::{AnnexConfig, BlobManager, GitAnnex};
use sinex_types::events::{sources, event_types};
use camino::Utf8Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

/// Test fixture for BlobManager tests with isolated git-annex repository
struct BlobManagerTest {
    manager: BlobManager,
    _temp_dir: TempDir,
    annex_path: std::path::PathBuf,
}

impl BlobManagerTest {
    /// Create a new test fixture with BlobManager and temporary git-annex repo
    async fn new(pool: &DbPool) -> color_eyre::Result<Self> {
        let temp_dir = temp_dir()?;
        let annex_path = temp_dir.path().join("test-annex");

        // Initialize git-annex repository
        Self::init_test_annex(&annex_path).await?;

        let annex_config = AnnexConfig {
            repo_path: annex_path.clone(),
            num_copies: Some(1),
            large_files: Some("anything".to_string()), // Accept all files as large
        };

        let manager = BlobManager::new(annex_config, pool.clone())?;

        Ok(Self {
            manager,
            _temp_dir: temp_dir,
            annex_path,
        })
    }

    /// Initialize a test git-annex repository with proper setup
    async fn init_test_annex(path: &std::path::Path) -> color_eyre::Result<()> {
        fs::create_dir_all(path).await?;

        // Check if git-annex is available
        if !Self::is_git_annex_available().await {
            return Err(color_eyre::eyre::eyre!("git-annex not available - skipping test"));
        }

        // Initialize git repository
        let output = tokio::process::Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(color_eyre::eyre::eyre!("Failed to initialize git repository"));
        }

        // Set git config to avoid warnings
        tokio::process::Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(path)
            .output()
            .await?;

        tokio::process::Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(path)
            .output()
            .await?;

        // Initialize git-annex
        let output = tokio::process::Command::new("git")
            .args(["annex", "init", "test-repo"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(color_eyre::eyre::eyre!(
                "Failed to initialize git-annex repository: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(())
    }

    /// Check if git-annex is available on the system
    async fn is_git_annex_available() -> bool {
        tokio::process::Command::new("git")
            .args(["annex", "version"])
            .output()
            .await
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

/// Helper macro to skip tests if git-annex is not available
macro_rules! skip_if_no_git_annex {
    () => {
        if !BlobManagerTest::is_git_annex_available().await {
            eprintln!("Skipping test: git-annex not available");
            return Ok(());
        }
    };
}

// ============================================================================
// Content Deduplication Tests
// ============================================================================

#[sinex_test]
async fn test_blake3_content_deduplication(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Identical content for deduplication testing";

    // Ingest same content twice with different filenames
    let metadata1 = fixture
        .manager
        .ingest_from_bytes(content, "file1.txt", "text/plain")
        .await?;

    let metadata2 = fixture
        .manager
        .ingest_from_bytes(content, "file2.txt", "text/plain")
        .await?;

    // Should have same BLAKE3 hash (deduplication)
    assert_eq!(metadata1.checksum_blake3, metadata2.checksum_blake3);
    assert_eq!(metadata1.blob_id, metadata2.blob_id);
    assert_eq!(metadata1.annex_key, metadata2.annex_key);

    // Verify BLAKE3 hash is computed correctly
    let temp_file = temp_dir()?.path().join("temp_for_hash.bin");
    tokio::fs::write(&temp_file, content).await?;
    let expected_blake3 = GitAnnex::compute_blake3_hash(&temp_file).await?;
    assert_eq!(
        metadata1.checksum_blake3.as_ref().unwrap(),
        &expected_blake3
    );

    // Both should reference the same blob in database
    let blob1 = fixture
        .manager
        .get_blob_metadata(&metadata1.blob_id)
        .await?;
    let blob2 = fixture
        .manager
        .get_blob_metadata(&metadata2.blob_id)
        .await?;
    assert_eq!(blob1.blob_id, blob2.blob_id);

    Ok(())
}

#[sinex_test]
async fn test_different_content_separate_blobs(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    let content1 = b"First unique content";
    let content2 = b"Second unique content";

    // Ingest different content
    let metadata1 = fixture
        .manager
        .ingest_from_bytes(content1, "file1.txt", "text/plain")
        .await?;

    let metadata2 = fixture
        .manager
        .ingest_from_bytes(content2, "file2.txt", "text/plain")
        .await?;

    // Should have different hashes and blob IDs
    assert_ne!(metadata1.checksum_blake3, metadata2.checksum_blake3);
    assert_ne!(metadata1.blob_id, metadata2.blob_id);
    assert_ne!(metadata1.annex_key, metadata2.annex_key);

    // Verify both blobs exist separately
    let blob1 = fixture
        .manager
        .get_blob_metadata(&metadata1.blob_id)
        .await?;
    let blob2 = fixture
        .manager
        .get_blob_metadata(&metadata2.blob_id)
        .await?;
    assert_ne!(blob1.blob_id, blob2.blob_id);

    Ok(())
}

#[sinex_test]
async fn test_file_vs_bytes_deduplication(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let temp_dir = temp_dir()?;

    let content = "Content that should deduplicate between file and bytes";

    // Create a test file
    let file_path = create_test_file(temp_dir.path(), "test.txt", content)?;

    // Ingest from file
    let metadata1 = fixture
        .manager
        .ingest_file(&file_path, Some("from_file.txt"))
        .await?;

    // Ingest same content from bytes
    let metadata2 = fixture
        .manager
        .ingest_from_bytes(content.as_bytes(), "from_bytes.txt", "text/plain")
        .await?;

    // Should deduplicate (same BLAKE3 hash)
    assert_eq!(metadata1.checksum_blake3, metadata2.checksum_blake3);
    assert_eq!(metadata1.blob_id, metadata2.blob_id);

    // Verify content can be retrieved identically
    let retrieved1 = fixture
        .manager
        .retrieve_content(&metadata1.annex_key)
        .await?;
    let retrieved2 = fixture
        .manager
        .retrieve_content(&metadata2.annex_key)
        .await?;
    assert_eq!(retrieved1, retrieved2);
    assert_eq!(retrieved1, content.as_bytes());

    Ok(())
}

#[sinex_test]
async fn test_large_content_deduplication(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Create large content (10MB)
    let large_content = vec![b'A'; 10 * 1024 * 1024];

    // Ingest large content twice
    let metadata1 = fixture
        .manager
        .ingest_from_bytes(&large_content, "large1.bin", "application/octet-stream")
        .await?;

    let metadata2 = fixture
        .manager
        .ingest_from_bytes(&large_content, "large2.bin", "application/octet-stream")
        .await?;

    // Should deduplicate
    assert_eq!(metadata1.checksum_blake3, metadata2.checksum_blake3);
    assert_eq!(metadata1.blob_id, metadata2.blob_id);
    assert_eq!(metadata1.size_bytes, 10 * 1024 * 1024);

    Ok(())
}

// ============================================================================
// Git-annex Integration Tests
// ============================================================================

#[sinex_test]
async fn test_git_annex_add_and_lookup(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let temp_dir = temp_dir()?;

    let content = "Test content for git-annex operations";
    let file_path = create_test_file(temp_dir.path(), "test.txt", content)?;

    // Ingest file and verify git-annex key generation
    let metadata = fixture
        .manager
        .ingest_file(&file_path, Some("test.txt"))
        .await?;

    // Verify annex key format (should be SHA256E-s<size>--<hash>.ext)
    assert!(metadata.annex_key.starts_with("SHA256"));
    assert!(metadata.annex_key.contains(&format!("s{}", content.len())));

    // Verify blob is registered in database
    let db_metadata = fixture.manager.get_blob_metadata(&metadata.blob_id).await?;
    assert_eq!(db_metadata.annex_key, metadata.annex_key);
    assert_eq!(db_metadata.storage_backend, "git-annex");

    Ok(())
}

#[sinex_test]
async fn test_git_annex_get_content(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let original_content = b"Content for git-annex get testing";

    // Ingest content
    let metadata = fixture
        .manager
        .ingest_from_bytes(original_content, "get_test.txt", "text/plain")
        .await?;

    // Retrieve content via annex key
    let retrieved_content = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;

    // Verify content matches exactly
    assert_eq!(retrieved_content, original_content);

    // Verify get_blob_path works
    let blob_path = fixture.manager.get_blob_path(&metadata.blob_id).await?;
    assert!(blob_path.exists());

    let file_content = fs::read(&blob_path).await?;
    assert_eq!(file_content, original_content);

    Ok(())
}

#[sinex_test]
async fn test_git_annex_verification(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Content for verification testing";

    // Ingest content
    let metadata = fixture
        .manager
        .ingest_from_bytes(content, "verify_test.txt", "text/plain")
        .await?;

    // Verify blob integrity
    let is_verified = fixture.manager.verify_blob(&metadata.blob_id).await?;
    assert!(is_verified);

    // Check verification status was updated in database
    let updated_metadata = fixture.manager.get_blob_metadata(&metadata.blob_id).await?;
    assert_eq!(
        updated_metadata.verification_status.as_deref(),
        Some("verified")
    );

    Ok(())
}

#[sinex_test]
async fn test_annex_key_parsing(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Content for key parsing test";

    // Ingest content
    let metadata = fixture
        .manager
        .ingest_from_bytes(content, "key_test.txt", "text/plain")
        .await?;

    // Parse annex key and verify structure
    let annex_key = sinex_satellite_sdk::annex::AnnexKey::parse(&metadata.annex_key)?;
    assert!(annex_key.backend.contains("SHA256"));
    assert_eq!(annex_key.size, content.len() as u64);
    assert!(!annex_key.hash.is_empty());

    Ok(())
}

// ============================================================================
// Graceful Degradation Tests
// ============================================================================

#[sinex_test]
async fn test_blob_manager_creation_without_git_annex(ctx: TestContext) -> color_eyre::Result<()> {
    // This test runs when git-annex is NOT available
    if BlobManagerTest::is_git_annex_available().await {
        eprintln!("Skipping test: git-annex is available");
        return Ok(());
    }

    let temp_dir = temp_dir()?;
    let annex_path = temp_dir.path().join("no-annex");

    let annex_config = AnnexConfig {
        repo_path: annex_path,
        num_copies: Some(1),
        large_files: None,
    };

    // Should fail gracefully when git-annex is not available
    let result = BlobManager::new(annex_config, ctx.pool().clone());
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("git-annex not found"));

    Ok(())
}

#[sinex_test]
async fn test_invalid_repository_path(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let annex_config = AnnexConfig {
        repo_path: "/nonexistent/path".into(),
        num_copies: Some(1),
        large_files: None,
    };

    // Should fail when repository path doesn't exist
    let result = BlobManager::new(annex_config, ctx.pool().clone());
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("does not exist"));

    Ok(())
}

// ============================================================================
// Large Blob Handling and Performance Tests
// ============================================================================

#[sinex_test]
async fn test_large_blob_handling(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Create large blob (50MB)
    let large_size = 50 * 1024 * 1024;
    let large_content: Vec<u8> = (0..large_size).map(|i| (i % 256) as u8).collect();

    let start_time = std::time::Instant::now();

    // Ingest large blob
    let metadata = fixture
        .manager
        .ingest_from_bytes(&large_content, "large_blob.bin", "application/octet-stream")
        .await?;

    let ingest_duration = start_time.elapsed();

    // Verify metadata
    assert_eq!(metadata.size_bytes, large_size as i64);
    assert_eq!(metadata.storage_backend, "git-annex");

    // Test retrieval performance
    let retrieve_start = std::time::Instant::now();
    let retrieved_content = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;
    let retrieve_duration = retrieve_start.elapsed();

    // Verify content integrity
    assert_eq!(retrieved_content.len(), large_content.len());
    assert_eq!(retrieved_content, large_content);

    // Performance logging (informational)
    eprintln!(
        "Large blob ({} MB) - Ingest: {:?}, Retrieve: {:?}",
        large_size / (1024 * 1024),
        ingest_duration,
        retrieve_duration
    );

    Ok(())
}

#[sinex_test]
async fn test_multiple_blob_sizes(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Test various blob sizes
    let test_sizes = vec![
        0,               // Empty
        1,               // Single byte
        1024,            // 1KB
        1024 * 1024,     // 1MB
        5 * 1024 * 1024, // 5MB
    ];

    for size in test_sizes {
        let content: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let filename = format!("size_test_{}.bin", size);

        let metadata = fixture
            .manager
            .ingest_from_bytes(&content, &filename, "application/octet-stream")
            .await?;

        assert_eq!(metadata.size_bytes, size as i64);
        assert_eq!(metadata.original_filename, filename);

        // Verify retrieval works for all sizes
        let retrieved = fixture
            .manager
            .retrieve_content(&metadata.annex_key)
            .await?;
        assert_eq!(retrieved.len(), content.len());

        if size > 0 {
            assert_eq!(retrieved, content);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_concurrent_blob_operations(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = Arc::new(BlobManagerTest::new(ctx.pool()).await?);

    // Create multiple different blobs concurrently
    let mut tasks = Vec::new();

    for i in 0..10 {
        let fixture = Arc::clone(&fixture);
        let content = format!("Concurrent blob content {}", i).into_bytes();
        let filename = format!("concurrent_{}.txt", i);

        tasks.push(async move {
            fixture
                .manager
                .ingest_from_bytes(&content, &filename, "text/plain")
                .await
        });
    }

    // Wait for all ingestions to complete
    let results: Vec<_> = futures::future::try_join_all(tasks).await?;

    // Verify all succeeded and got unique blob IDs
    assert_eq!(results.len(), 10);
    let mut unique_blobs = std::collections::HashSet::new();

    for metadata in results {
        assert!(unique_blobs.insert(metadata.blob_id));
        assert_eq!(metadata.storage_backend, "git-annex");
    }

    assert_eq!(unique_blobs.len(), 10); // All should be unique

    Ok(())
}

// ============================================================================
// Error Handling and Edge Cases
// ============================================================================

#[sinex_test]
async fn test_retrieve_nonexistent_content(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Try to retrieve with invalid annex key
    let result = fixture.manager.retrieve_content("invalid-key-format").await;
    assert!(result.is_err());

    // Try to retrieve with valid format but non-existent key
    let result = fixture
        .manager
        .retrieve_content("SHA256E-s100--nonexistent.dat")
        .await;
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_get_metadata_nonexistent_blob(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let nonexistent_id = Ulid::new();

    // Should fail for non-existent blob ID
    let result = fixture.manager.get_blob_metadata(&nonexistent_id).await;
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_verify_nonexistent_blob(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let nonexistent_id = Ulid::new();

    // Should fail for non-existent blob ID
    let result = fixture.manager.verify_blob(&nonexistent_id).await;
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_empty_content_handling(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let empty_content = b"";

    // Ingest empty content
    let metadata = fixture
        .manager
        .ingest_from_bytes(empty_content, "empty.txt", "text/plain")
        .await?;

    assert_eq!(metadata.size_bytes, 0);
    assert_eq!(metadata.original_filename, "empty.txt");

    // Verify retrieval works for empty content
    let retrieved = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;
    assert_eq!(retrieved, empty_content);

    // Verify verification works for empty content
    let is_verified = fixture.manager.verify_blob(&metadata.blob_id).await?;
    assert!(is_verified);

    Ok(())
}

#[sinex_test]
async fn test_binary_content_integrity(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Create binary content with all possible byte values
    let mut binary_content = Vec::new();
    for i in 0..=255u8 {
        binary_content.push(i);
    }
    // Repeat pattern to ensure uniqueness
    let pattern = binary_content.clone();
    binary_content.extend_from_slice(&pattern);

    // Ingest binary content
    let metadata = fixture
        .manager
        .ingest_from_bytes(
            &binary_content,
            "binary_test.bin",
            "application/octet-stream",
        )
        .await?;

    // Retrieve and verify exact binary match
    let retrieved = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;
    assert_eq!(retrieved, binary_content);

    // Verify no data corruption occurred
    for (i, &byte) in retrieved.iter().enumerate() {
        let expected = (i % 256) as u8;
        assert_eq!(byte, expected, "Byte mismatch at position {}", i);
    }

    Ok(())
}

#[sinex_test]
async fn test_unicode_filename_handling(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = "Unicode content test: こんにちは世界 🌍";

    // Test various Unicode filenames
    let unicode_filenames = vec![
        "简体中文.txt",
        "русский.txt",
        "العربية.txt",
        "हिन्दी.txt",
        "emoji_😀_file.txt",
        "spaces and symbols!@#$.txt",
    ];

    for filename in unicode_filenames {
        let metadata = fixture
            .manager
            .ingest_from_bytes(content.as_bytes(), filename, "text/plain; charset=utf-8")
            .await?;

        assert_eq!(metadata.original_filename, filename);

        // Verify retrieval works with Unicode filenames
        let retrieved = fixture
            .manager
            .retrieve_content(&metadata.annex_key)
            .await?;
        assert_eq!(retrieved, content.as_bytes());
    }

    Ok(())
}

// ============================================================================
// Metadata and Database Integration Tests
// ============================================================================

#[sinex_test]
async fn test_blob_metadata_completeness(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Test content for metadata validation";
    let filename = "metadata_test.txt";
    let content_type = "text/plain";

    // Ingest content
    let metadata = fixture
        .manager
        .ingest_from_bytes(content, filename, content_type)
        .await?;

    // Verify all metadata fields are populated correctly
    assert!(!metadata.blob_id.to_string().is_empty());
    assert!(!metadata.annex_key.is_empty());
    assert_eq!(metadata.original_filename, filename);
    assert_eq!(metadata.size_bytes, content.len() as i64);
    assert_eq!(metadata.mime_type.as_deref(), Some(content_type));
    assert!(!metadata.checksum_sha256.is_empty());
    assert!(metadata.checksum_blake3.is_some());
    assert_eq!(metadata.storage_backend, "git-annex");
    assert_eq!(metadata.verification_status.as_deref(), Some("verified"));

    // Verify database record matches
    let db_metadata = fixture.manager.get_blob_metadata(&metadata.blob_id).await?;
    assert_eq!(db_metadata.blob_id, metadata.blob_id);
    assert_eq!(db_metadata.annex_key, metadata.annex_key);
    assert_eq!(db_metadata.original_filename, metadata.original_filename);
    assert_eq!(db_metadata.size_bytes, metadata.size_bytes);
    assert_eq!(db_metadata.checksum_sha256, metadata.checksum_sha256);
    assert_eq!(db_metadata.checksum_blake3, metadata.checksum_blake3);

    Ok(())
}

#[sinex_test]
async fn test_storage_statistics_emission(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;

    // Ingest several blobs of different sizes
    let small_content = b"Small content";
    let medium_content = vec![b'X'; 1024];
    let large_content = vec![b'Y'; 10240];

    let test_blobs = vec![
        (small_content.as_slice(), "small.txt"),
        (medium_content.as_slice(), "medium.bin"),
        (large_content.as_slice(), "large.bin"),
    ];

    for (content, filename) in test_blobs {
        fixture
            .manager
            .ingest_from_bytes(content, filename, "application/octet-stream")
            .await?;
    }

    // Emit storage statistics
    fixture.manager.emit_storage_stats().await?;

    // Verify statistics event was created in core.events
    let recent_events = ctx.get_recent_events(10).await?;
    let stats_events: Vec<_> = recent_events
        .iter()
        .filter(|e| e.source == sources::BLOB_STORAGE && e.event_type == event_types::metrics::BLOB_STORAGE_STATISTICS)
        .collect();

    assert!(
        !stats_events.is_empty(),
        "Storage statistics event should be created"
    );

    let stats_event = &stats_events[0];
    assert!(stats_event.payload["total_blobs"].as_i64().unwrap() >= 3);
    assert!(stats_event.payload["total_size_bytes"].as_i64().unwrap() > 0);
    assert_eq!(stats_event.payload["storage_backend"], "git-annex");

    Ok(())
}

#[sinex_test]
async fn test_metrics_emission_during_operations(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Content for metrics testing";

    // Count events before operation
    let events_before = ctx.get_recent_events(100).await?;
    let metrics_before = events_before
        .iter()
        .filter(|e| e.source == sources::BLOB_STORAGE && e.event_type == event_types::metrics::BLOB_STORAGE_OPERATION)
        .count();

    // Perform blob operations
    let metadata = fixture
        .manager
        .ingest_from_bytes(content, "metrics_test.txt", "text/plain")
        .await?;

    let _retrieved = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;

    // Check that operation metrics were emitted
    let events_after = ctx.get_recent_events(100).await?;
    let metrics_after = events_after
        .iter()
        .filter(|e| e.source == sources::BLOB_STORAGE && e.event_type == event_types::metrics::BLOB_STORAGE_OPERATION)
        .count();

    // Should have at least ingest and retrieve metrics
    assert!(
        metrics_after > metrics_before,
        "Operation metrics should be emitted during blob operations"
    );

    // Verify metrics contain expected fields
    let operation_events: Vec<_> = events_after
        .iter()
        .filter(|e| e.source == sources::BLOB_STORAGE && e.event_type == event_types::metrics::BLOB_STORAGE_OPERATION)
        .collect();

    for event in operation_events.iter().take(2) {
        // Check recent metrics
        let payload = &event.payload;
        assert!(payload.get("operation").is_some());
        assert!(payload.get("result").is_some());
        assert!(payload.get("size_bytes").is_some());
        assert!(payload.get("duration_ms").is_some());
    }

    Ok(())
}

// ============================================================================
// Integration with ContentService Tests
// ============================================================================

#[sinex_test]
async fn test_blob_manager_content_service_integration(ctx: TestContext) -> color_eyre::Result<()> {
    skip_if_no_git_annex!();

    let fixture = BlobManagerTest::new(ctx.pool()).await?;
    let content = b"Integration test content";
    let filename = "integration_test.txt";

    // Test that BlobManager works correctly as a backend for ContentService
    let metadata = fixture
        .manager
        .ingest_from_bytes(content, filename, "text/plain")
        .await?;

    // Verify database state is compatible with ContentService expectations
    let blob_record = fixture.manager.get_blob_metadata(&metadata.blob_id).await?;

    // Check that all required fields for ContentService integration are present
    assert!(!blob_record.annex_key.is_empty());
    assert!(!blob_record.checksum_sha256.is_empty());
    assert!(blob_record.checksum_blake3.is_some());
    assert_eq!(blob_record.storage_backend, "git-annex");

    // Test path resolution (required for ContentService)
    let blob_path = fixture.manager.get_blob_path(&metadata.blob_id).await?;
    assert!(blob_path.exists());

    // Verify content retrieval (required for ContentService)
    let retrieved = fixture
        .manager
        .retrieve_content(&metadata.annex_key)
        .await?;
    assert_eq!(retrieved, content);

    Ok(())
}