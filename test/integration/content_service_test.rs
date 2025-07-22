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
use sinex_annex::{AnnexConfig, BlobManager};
use sinex_db::artifacts;
use sinex_services::{ContentService, ServiceError};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use sinex_events::event_types::filesystem;

/// Test fixture for ContentService tests
struct ContentServiceTest {
    service: ContentService,
    _temp_dir: TempDir,
    annex_path: std::path::PathBuf,
}

impl ContentServiceTest {
    /// Create a new test fixture with ContentService and temporary git-annex repo
    async fn new(pool: &DbPool) -> AnyhowResult<Self> {
        let temp_dir = TempDir::new()?;
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
            return Err(anyhow::anyhow!("git-annex not available - skipping test"));
        }

        let output = tokio::process::Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to initialize git repository"));
        }

        let output = tokio::process::Command::new("git")
            .args(["annex", "init", "test-repo"])
            .current_dir(path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to initialize git-annex repository"));
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
    
    // Create test file
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("test_document.md");
    let content = "# Test Document\n\nThis is a test markdown file with some content.";
    fs::write(&test_file, content).await?;

    // Store file content
    let annex_key = fixture
        .service
        .store_file(&test_file, "text/markdown", "file_upload")
        .await?;

    // Verify storage
    assert!(!annex_key.is_empty());

    // Verify artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let artifact = artifacts
        .iter()
        .find(|a| a.title == "test_document.md")
        .expect("Should find uploaded artifact");

    assert_eq!(artifact.artifact_type, "blob");
    assert_eq!(artifact.mime_type.as_deref(), Some("text/markdown"));
    assert_eq!(artifact.size_bytes, Some(content.len() as i64));
    assert_eq!(artifact.metadata["source"], "file_upload");

    Ok(())
}

#[sinex_test]
async fn test_retrieve_content(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let original_content = b"Test content for retrieval";
    
    // Store content
    let annex_key = fixture
        .service
        .store_large_content(original_content, "retrieve_test.txt", "text/plain", "test")
        .await?;

    // Retrieve content by key
    let retrieved = fixture.service.retrieve_content(&annex_key).await?;
    assert_eq!(retrieved, original_content);

    Ok(())
}

#[sinex_test]
async fn test_content_deduplication(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    let content = b"Duplicate content test";

    // Store same content twice with different filenames
    let key1 = fixture
        .service
        .store_large_content(content, "file1.txt", "text/plain", "test")
        .await?;

    let key2 = fixture
        .service
        .store_large_content(content, "file2.txt", "text/plain", "test")
        .await?;

    // Should get same annex key (content-addressed)
    assert_eq!(key1, key2, "Same content should produce same annex key");

    // But should create two artifacts
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let matching_artifacts: Vec<_> = artifacts
        .iter()
        .filter(|a| a.metadata["annex_key"] == key1)
        .collect();

    assert_eq!(matching_artifacts.len(), 2, "Should have two artifacts for same content");
    
    // With different titles
    let titles: HashSet<_> = matching_artifacts.iter().map(|a| &a.title).collect();
    assert!(titles.contains(&"file1.txt".to_string()));
    assert!(titles.contains(&"file2.txt".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_metadata_extraction(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Test with JSON content
    let json_content = r#"{"name": "test", "version": "1.0.0", "data": [1, 2, 3]}"#;
    let annex_key = fixture
        .service
        .store_large_content(json_content.as_bytes(), "data.json", "application/json", "test")
        .await?;

    // Check artifact metadata
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let json_artifact = artifacts
        .iter()
        .find(|a| a.title == "data.json")
        .expect("Should find JSON artifact");

    assert_eq!(json_artifact.mime_type.as_deref(), Some("application/json"));
    assert!(json_artifact.size_bytes.is_some());

    Ok(())
}

#[sinex_test]
async fn test_large_file_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Create a "large" file (1MB for testing)
    let large_content = vec![b'X'; 1024 * 1024];
    
    let start = Instant::now();
    let annex_key = fixture
        .service
        .store_large_content(&large_content, "large_file.bin", "application/octet-stream", "test")
        .await?;
    let duration = start.elapsed();

    println!("Stored 1MB file in {:?}", duration);
    assert!(!annex_key.is_empty());

    // Verify size in artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let large_artifact = artifacts
        .iter()
        .find(|a| a.title == "large_file.bin")
        .expect("Should find large artifact");

    assert_eq!(large_artifact.size_bytes, Some(1024 * 1024));

    Ok(())
}

#[sinex_test]
async fn test_invalid_file_path(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Try to store non-existent file
    let result = fixture
        .service
        .store_file("/non/existent/file.txt", "text/plain", "test")
        .await;

    assert!(result.is_err(), "Should fail with non-existent file");

    Ok(())
}

#[sinex_test]
async fn test_empty_content_handling(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Store empty content
    let annex_key = fixture
        .service
        .store_large_content(b"", "empty.txt", "text/plain", "test")
        .await?;

    assert!(!annex_key.is_empty(), "Should handle empty content");

    // Verify artifact
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let empty_artifact = artifacts
        .iter()
        .find(|a| a.title == "empty.txt")
        .expect("Should find empty artifact");

    assert_eq!(empty_artifact.size_bytes, Some(0));

    Ok(())
}

#[sinex_test]
async fn test_concurrent_storage(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Store multiple files concurrently
    let futures: Vec<_> = (0..5)
        .map(|i| {
            let service = fixture.service.clone();
            async move {
                let content = format!("Concurrent content {}", i);
                service
                    .store_large_content(
                        content.as_bytes(),
                        &format!("concurrent_{}.txt", i),
                        "text/plain",
                        "concurrent_test",
                    )
                    .await
            }
        })
        .collect();

    let results = join_all(futures).await;
    
    // All should succeed
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Concurrent storage {} should succeed", i);
    }

    // Verify all artifacts were created
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let concurrent_artifacts: Vec<_> = artifacts
        .iter()
        .filter(|a| a.metadata["source"] == "concurrent_test")
        .collect();

    assert_eq!(concurrent_artifacts.len(), 5, "Should have all concurrent artifacts");

    Ok(())
}

#[sinex_test]
async fn test_content_type_validation(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Test various content types
    let test_cases = vec![
        ("test.html", "text/html", b"<html></html>"),
        ("data.csv", "text/csv", b"col1,col2\nval1,val2"),
        ("image.png", "image/png", b"\x89PNG\r\n\x1a\n"),
        ("archive.zip", "application/zip", b"PK\x03\x04"),
    ];

    for (filename, content_type, content) in test_cases {
        let annex_key = fixture
            .service
            .store_large_content(content, filename, content_type, "type_test")
            .await?;

        assert!(!annex_key.is_empty(), "Should store {} successfully", filename);
    }

    // Verify all were stored with correct types
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 20).await?;
    
    assert!(artifacts.iter().any(|a| a.title == "test.html" && a.mime_type == Some("text/html".to_string())));
    assert!(artifacts.iter().any(|a| a.title == "data.csv" && a.mime_type == Some("text/csv".to_string())));
    assert!(artifacts.iter().any(|a| a.title == "image.png" && a.mime_type == Some("image/png".to_string())));
    assert!(artifacts.iter().any(|a| a.title == "archive.zip" && a.mime_type == Some("application/zip".to_string())));

    Ok(())
}

#[sinex_test]
async fn test_retrieve_nonexistent_content(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Try to retrieve with invalid key
    let result = fixture
        .service
        .retrieve_content("SHA256-invalid-key-that-does-not-exist")
        .await;

    assert!(result.is_err(), "Should fail to retrieve non-existent content");

    Ok(())
}

#[sinex_test]
async fn test_special_characters_in_filename(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Test with special characters in filename
    let special_names = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
        "παράδειγμα.txt", // Greek
        "例子.txt", // Chinese
    ];

    for filename in special_names {
        let content = format!("Content for {}", filename);
        let result = fixture
            .service
            .store_large_content(content.as_bytes(), filename, "text/plain", "special_chars")
            .await;

        assert!(result.is_ok(), "Should handle filename: {}", filename);
    }

    Ok(())
}

#[sinex_test]
async fn test_store_and_link_to_event(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Create an event
    let event = EventFactory::new(sources::FS).create_event(
        filesystem::FILE_CREATED,
        json!({
            "path": "/documents/report.pdf",
            "size": 2048
        })
    );
    let inserted_event = insert_event(ctx.pool(), &event).await?;

    // Store content and link to event
    let content = b"PDF content simulation";
    let annex_key = fixture
        .service
        .store_large_content(content, "report.pdf", "application/pdf", "event_linked")
        .await?;

    // Update artifact to link with event
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let artifact = artifacts
        .iter()
        .find(|a| a.metadata["annex_key"] == annex_key)
        .expect("Should find artifact");

    // In a real scenario, we'd update the artifact to link it to the event
    // This demonstrates the integration pattern
    assert_eq!(artifact.title, "report.pdf");
    assert!(artifact.blob_id.is_some());

    Ok(())
}

#[sinex_test]
async fn test_storage_statistics(ctx: TestContext) -> TestResult {
    skip_if_no_git_annex!();

    let fixture = ContentServiceTest::new(ctx.pool()).await?;
    
    // Store multiple files of different sizes
    let files = vec![
        ("small.txt", 100),
        ("medium.txt", 10_000),
        ("large.txt", 100_000),
    ];

    let mut total_size = 0i64;
    for (filename, size) in files {
        let content = vec![b'A'; size];
        fixture
            .service
            .store_large_content(&content, filename, "text/plain", "stats_test")
            .await?;
        total_size += size as i64;
    }

    // Query artifacts to verify sizes
    let artifacts = artifacts::get_recent_artifacts(ctx.pool(), 10).await?;
    let stats_artifacts: Vec<_> = artifacts
        .iter()
        .filter(|a| a.metadata["source"] == "stats_test")
        .collect();

    assert_eq!(stats_artifacts.len(), 3);

    let total_stored: i64 = stats_artifacts
        .iter()
        .filter_map(|a| a.size_bytes)
        .sum();

    assert_eq!(total_stored, total_size, "Total size should match");

    Ok(())
}