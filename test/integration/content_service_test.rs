// ContentService Integration Tests
//
// Tests the ContentService wrapper around BlobManager, focusing on:
// 1. Content storage from various sources (files, byte arrays)
// 2. Content retrieval and verification
// 3. Metadata extraction and validation
// 4. Deduplication behavior
// 5. Error handling and edge cases
// 6. Integration with artifact storage
//
// IMPORTANT: These tests require git-annex to be available. If git-annex
// is not installed, tests will be skipped with appropriate warnings.

use crate::common::prelude::*;

use crate::common::prelude::*;
use crate::common::resources::{create_test_file, temp_dir};
use futures;
use sinex_annex::{AnnexConfig, BlobManager};
use sinex_db::artifacts;
use sinex_services::{ContentService, ServiceError};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

/// Test fixture for ContentService tests
struct ContentServiceTest {
    service: ContentService,
    _temp_dir: TempDir,
    annex_path: std::path::PathBuf,
}

impl ContentServiceTest {
    /// Create a new test fixture with ContentService and temporary git-annex repo
    async fn new(pool: &DbPool) -> AnyhowResult<Self> {
        let temp_dir = temp_dir()?;
        let annex_path = temp_dir.path().join("test-annex");

        // Initialize git-annex repository
        Self::init_test_annex(&annex_path).await?;

        let annex_config = AnnexConfig {
            repo_path: annex_path.clone(),
            num_copies: Some(1),
            large_files: None,
        };

        let blob_manager = Arc::new(BlobManager::new(annex_config, pool.clone())?);
        let service = ContentService::new(pool.clone(), blob_manager);

        Ok(Self {
            service,
            _temp_dir: temp_dir,
            annex_path,
        })
    }

    /// Initialize a test git-annex repository
    async fn init_test_annex(path: &Path) -> AnyhowResult<()> {
        fs::create_dir_all(path).await?;

        // Check if git-annex is available
        if !Self::is_git_annex_available().await {
            return Err(anyhow::anyhow!("git-annex not available - skipping test").into());
        }

        let output = tokio::process::Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to initialize git repository").into());
        }

        let output = tokio::process::Command::new("git")
            .args(["annex", "init", "test-repo"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to initialize git-annex repository").into());
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

/// Helper to skip tests if git-annex is not available
macro_rules! skip_if_no_git_annex {
    () => {
        if !ContentServiceTest::is_git_annex_available().await {
            eprintln!("Skipping test: git-annex not available");
            return Ok(());
        }
    };
}

#[sinex_test]
async fn test_store_content_from_bytes(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Hello, World! This is test content.";
    let filename = "test.txt";
    let content_type = "text/plain";
    let source = "test";

    // Store content
    let annex_key = fixture
        .service
        .store_large_content(content, filename, content_type, source)
        .await?;

    // Verify annex key format
    assert!(!annex_key.is_empty());
    assert!(annex_key.starts_with("SHA256"));

    // Verify artifact was created
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    assert!(!artifacts.is_empty());

    let artifact = &artifacts[0];
    assert_eq!(artifact.artifact_type, "blob");
    assert_eq!(artifact.title, filename);
    assert_eq!(artifact.mime_type.as_deref(), Some(content_type));
    assert_eq!(artifact.size_bytes, Some(content.len() as i64));
    assert!(artifact.blob_id.is_some());
    assert!(artifact.checksum.is_some());

    // Verify metadata contains annex key and source
    let metadata = &artifact.metadata;
    assert_eq!(metadata["annex_key"], annex_key);
    assert_eq!(metadata["source"], source);

    Ok(())
}

#[sinex_test]
async fn test_store_content_from_file(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let temp_dir = temp_dir()?;

    // Create a test file
    let content = "This is a test file with some content that should be stored in git-annex.";
    let file_path = create_test_file(temp_dir.path(), "testfile.txt", content)?;

    // Read file content to compare later
    let file_content = fs::read(&file_path).await?;
    let filename = "stored_file.txt";
    let content_type = "text/plain";
    let source = "file_test";

    // Store content from file
    let annex_key = fixture
        .service
        .store_large_content(&file_content, filename, content_type, source)
        .await?;

    // Verify storage
    assert!(!annex_key.is_empty());

    // Verify artifact creation
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    assert_eq!(artifact.size_bytes, Some(content.len() as i64));
    assert_eq!(artifact.title, filename);

    Ok(())
}

#[sinex_test]
async fn test_retrieve_content(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let original_content = b"Test content for retrieval verification";
    let filename = "retrieve_test.txt";
    let content_type = "text/plain";
    let source = "retrieve_test";

    // Store content first
    let annex_key = fixture
        .service
        .store_large_content(original_content, filename, content_type, source)
        .await?;

    // Retrieve content
    let retrieved_content = fixture.service.retrieve_content(&annex_key).await?;

    // Verify content matches
    assert_eq!(retrieved_content, original_content);

    Ok(())
}

#[sinex_test]
async fn test_content_deduplication(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Duplicate content for deduplication testing";
    let source = "dedup_test";

    // Store same content twice with different filenames
    let annex_key1 = fixture
        .service
        .store_large_content(content, "file1.txt", "text/plain", source)
        .await?;

    let annex_key2 = fixture
        .service
        .store_large_content(content, "file2.txt", "text/plain", source)
        .await?;

    // Both should have the same annex key (deduplication)
    assert_eq!(annex_key1, annex_key2);

    // Should have multiple artifacts but same blob
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let blob_artifacts: Vec<_> = artifacts
        .iter()
        .filter(|a| a.artifact_type == "blob")
        .collect();

    // Should have created artifacts for both files
    assert!(blob_artifacts.len() >= 2);

    // Both artifacts should reference the same blob ID (if blob_id is used for deduplication)
    // Note: The exact deduplication behavior depends on BlobManager implementation

    Ok(())
}

#[sinex_test]
async fn test_empty_content_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let empty_content = b"";
    let filename = "empty.txt";
    let content_type = "text/plain";
    let source = "empty_test";

    // Store empty content
    let annex_key = fixture
        .service
        .store_large_content(empty_content, filename, content_type, source)
        .await?;

    // Verify storage succeeded
    assert!(!annex_key.is_empty());

    // Verify artifact creation
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    assert_eq!(artifact.size_bytes, Some(0));

    // Retrieve and verify empty content
    let retrieved = fixture.service.retrieve_content(&annex_key).await?;
    assert_eq!(retrieved, empty_content);

    Ok(())
}

#[sinex_test]
async fn test_large_content_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;

    // Create large content (1MB)
    let large_content = vec![b'x'; 1024 * 1024];
    let filename = "large_file.bin";
    let content_type = "application/octet-stream";
    let source = "large_test";

    // Store large content
    let annex_key = fixture
        .service
        .store_large_content(&large_content, filename, content_type, source)
        .await?;

    // Verify storage
    assert!(!annex_key.is_empty());

    // Verify artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    assert_eq!(artifact.size_bytes, Some(large_content.len() as i64));

    // Retrieve and verify (only check size to avoid memory issues)
    let retrieved = fixture.service.retrieve_content(&annex_key).await?;
    assert_eq!(retrieved.len(), large_content.len());

    Ok(())
}

#[sinex_test]
async fn test_various_content_types(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let source = "content_type_test";

    let test_cases = vec![
        (b"plain text content".as_slice(), "test.txt", "text/plain"),
        (
            b"{\"json\": \"data\"}".as_slice(),
            "data.json",
            "application/json",
        ),
        (
            b"<html><body>HTML</body></html>".as_slice(),
            "page.html",
            "text/html",
        ),
        (b"\x89PNG\r\n\x1a\n".as_slice(), "image.png", "image/png"), // PNG header
        (b"\xFF\xD8\xFF".as_slice(), "photo.jpg", "image/jpeg"),     // JPEG header
    ];

    for (content, filename, content_type) in test_cases {
        let annex_key = fixture
            .service
            .store_large_content(content, filename, content_type, source)
            .await?;

        // Verify storage
        assert!(!annex_key.is_empty());

        // Verify retrieval
        let retrieved = fixture.service.retrieve_content(&annex_key).await?;
        assert_eq!(retrieved, content);
    }

    Ok(())
}

#[sinex_test]
async fn test_error_handling_invalid_annex_key(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;

    // Try to retrieve with invalid annex key
    let result = fixture.service.retrieve_content("invalid-key-format").await;

    // Should return an error
    assert!(result.is_err());

    // Verify error type
    match result.unwrap_err() {
        ServiceError::OperationFailed(msg) => {
            assert!(msg.contains("Content retrieval failed"));
        }
        other => panic!("Expected OperationFailed error, got: {:?}", other),
    }

    Ok(())
}

#[sinex_test]
async fn test_unicode_content_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;

    // Test various Unicode content
    let unicode_tests = vec![
        "Hello, 世界! 🌍",
        "Тест русского текста",
        "العربية النص",
        "🚀🔥💻✨🎉",
        "Ĵőźëłĺĺẹ ñǖæńçé",
    ];

    for (i, content) in unicode_tests.iter().enumerate() {
        let content_bytes = content.as_bytes();
        let filename = format!("unicode_{}.txt", i);
        let content_type = "text/plain; charset=utf-8";
        let source = "unicode_test";

        // Store Unicode content
        let annex_key = fixture
            .service
            .store_large_content(content_bytes, &filename, content_type, source)
            .await?;

        // Retrieve and verify
        let retrieved = fixture.service.retrieve_content(&annex_key).await?;
        let retrieved_str = String::from_utf8(retrieved)?;
        assert_eq!(&retrieved_str, content);
    }

    Ok(())
}

#[sinex_test]
async fn test_binary_content_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;

    // Create binary content with all byte values
    let mut binary_content = Vec::new();
    for i in 0..=255u8 {
        binary_content.push(i);
    }

    let filename = "binary_test.bin";
    let content_type = "application/octet-stream";
    let source = "binary_test";

    // Store binary content
    let annex_key = fixture
        .service
        .store_large_content(&binary_content, filename, content_type, source)
        .await?;

    // Retrieve and verify exact binary match
    let retrieved = fixture.service.retrieve_content(&annex_key).await?;
    assert_eq!(retrieved, binary_content);

    // Verify no data corruption
    assert_eq!(retrieved.len(), 256);
    for (i, &byte) in retrieved.iter().enumerate() {
        assert_eq!(byte, i as u8);
    }

    Ok(())
}

#[sinex_test]
async fn test_content_metadata_accuracy(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Metadata accuracy test content";
    let filename = "metadata_test.txt";
    let content_type = "text/plain";
    let source = "metadata_test";

    // Store content
    let annex_key = fixture
        .service
        .store_large_content(content, filename, content_type, source)
        .await?;

    // Get artifact and verify all metadata fields
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];

    // Verify basic metadata
    assert_eq!(artifact.artifact_type, "blob");
    assert_eq!(artifact.title, filename);
    assert_eq!(artifact.mime_type.as_deref(), Some(content_type));
    assert_eq!(artifact.size_bytes, Some(content.len() as i64));
    assert!(artifact.checksum.is_some());
    assert!(artifact.blob_id.is_some());

    // Verify custom metadata
    let metadata = &artifact.metadata;
    assert_eq!(metadata["annex_key"], annex_key);
    assert_eq!(metadata["source"], source);

    // Verify timestamps
    assert!(artifact.created_at <= chrono::Utc::now());
    assert!(artifact.updated_at <= chrono::Utc::now());
    assert!(artifact.deleted_at.is_none());

    Ok(())
}

#[sinex_test]
async fn test_concurrent_content_storage(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let source = "concurrent_test";

    // Create multiple different content pieces
    let contents: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("Concurrent test content {}", i).into_bytes())
        .collect();

    // Store all content concurrently
    let mut tasks = Vec::new();
    for (i, content) in contents.iter().enumerate() {
        let service = &fixture.service;
        let filename = format!("concurrent_{}.txt", i);
        let content_type = "text/plain";

        tasks.push(async move {
            service
                .store_large_content(content, &filename, content_type, source)
                .await
        });
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::try_join_all(tasks).await?;

    // Verify all succeeded and got different keys
    assert_eq!(results.len(), 5);
    let mut unique_keys = std::collections::HashSet::new();
    for key in results {
        assert!(!key.is_empty());
        unique_keys.insert(key);
    }
    assert_eq!(unique_keys.len(), 5); // All should be unique

    // Verify all artifacts were created
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let blob_artifacts: Vec<_> = artifacts
        .iter()
        .filter(|a| a.artifact_type == "blob")
        .collect();
    assert!(blob_artifacts.len() >= 5);

    Ok(())
}

#[sinex_test]
async fn test_content_service_without_git_annex() -> TestResult {
    // This test runs when git-annex is NOT available
    if ContentServiceTest::is_git_annex_available().await {
        eprintln!("Skipping test: git-annex is available");
        return Ok(());
    }

    // Test that attempting to create ContentService fails gracefully
    // when git-annex is not available
    let pool = crate::common::create_test_db_pool().await?;
    let temp_dir = temp_dir()?;
    let annex_path = temp_dir.path().join("no-annex");

    let annex_config = AnnexConfig {
        repo_path: annex_path,
        num_copies: Some(1),
        large_files: None,
    };

    // This should fail because git-annex is not available
    let result = BlobManager::new(annex_config, pool);
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_get_content_metadata(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Metadata test content";
    let filename = "metadata_test.txt";
    let content_type = "text/plain";
    let source = "metadata_test";

    // Store content
    let _annex_key = fixture
        .service
        .store_large_content(content, filename, content_type, source)
        .await?;

    // Get the blob ID from the artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    let blob_id = artifact.blob_id.expect("Blob ID should be present");

    // Get metadata via ContentService
    let metadata = fixture.service.get_content_metadata(blob_id).await?;

    // Verify metadata
    assert_eq!(metadata.blob_id, blob_id);
    assert_eq!(metadata.original_filename, filename);
    assert_eq!(metadata.size_bytes, content.len() as i64);
    assert_eq!(metadata.mime_type.as_deref(), Some(content_type));
    assert!(metadata.checksum_sha256.len() > 0);
    assert!(metadata.checksum_blake3.is_some());
    assert_eq!(metadata.storage_backend, "git-annex");

    Ok(())
}

#[sinex_test]
async fn test_verify_content_integrity(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Content for integrity verification";
    let filename = "verify_test.txt";
    let content_type = "text/plain";
    let source = "verify_test";

    // Store content
    let _annex_key = fixture
        .service
        .store_large_content(content, filename, content_type, source)
        .await?;

    // Get the blob ID from the artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    let blob_id = artifact.blob_id.expect("Blob ID should be present");

    // Verify content integrity
    let is_valid = fixture.service.verify_content(blob_id).await?;

    // Content should be valid immediately after storage
    assert!(is_valid);

    Ok(())
}

#[sinex_test]
async fn test_get_metadata_invalid_blob_id(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let invalid_blob_id = sinex_ulid::Ulid::new(); // Random ULID that doesn't exist

    // Try to get metadata for non-existent blob
    let result = fixture.service.get_content_metadata(invalid_blob_id).await;

    // Should return an error
    assert!(result.is_err());

    match result.unwrap_err() {
        ServiceError::OperationFailed(msg) => {
            assert!(msg.contains("Failed to get blob metadata"));
        }
        other => panic!("Expected OperationFailed error, got: {:?}", other),
    }

    Ok(())
}

#[sinex_test]
async fn test_verify_invalid_blob_id(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let invalid_blob_id = sinex_ulid::Ulid::new(); // Random ULID that doesn't exist

    // Try to verify non-existent blob
    let result = fixture.service.verify_content(invalid_blob_id).await;

    // Should return an error
    assert!(result.is_err());

    match result.unwrap_err() {
        ServiceError::OperationFailed(msg) => {
            assert!(msg.contains("Content verification failed"));
        }
        other => panic!("Expected OperationFailed error, got: {:?}", other),
    }

    Ok(())
}

#[sinex_test]
async fn test_content_storage_and_metadata_consistency(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Consistency test content with special chars: !@#$%^&*()";
    let filename = "consistency_test.txt";
    let content_type = "text/plain; charset=utf-8";
    let source = "consistency_test";

    // Store content
    let annex_key = fixture
        .service
        .store_large_content(content, filename, content_type, source)
        .await?;

    // Get artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 1).await?;
    let artifact = &artifacts[0];
    let blob_id = artifact.blob_id.expect("Blob ID should be present");

    // Get metadata via ContentService
    let metadata = fixture.service.get_content_metadata(blob_id).await?;

    // Retrieve content
    let retrieved_content = fixture.service.retrieve_content(&annex_key).await?;

    // Verify consistency between artifact, metadata, and retrieved content
    assert_eq!(artifact.size_bytes, Some(content.len() as i64));
    assert_eq!(metadata.size_bytes, content.len() as i64);
    assert_eq!(retrieved_content.len(), content.len());
    assert_eq!(retrieved_content, content);

    // Verify metadata consistency
    assert_eq!(artifact.title, metadata.original_filename);
    assert_eq!(artifact.mime_type.as_deref(), metadata.mime_type.as_deref());
    assert_eq!(
        artifact.checksum.as_deref(),
        Some(metadata.checksum_sha256.as_str())
    );

    // Verify annex key consistency
    let artifact_metadata = &artifact.metadata;
    assert_eq!(artifact_metadata["annex_key"], annex_key);
    assert_eq!(metadata.annex_key, annex_key);

    Ok(())
}
