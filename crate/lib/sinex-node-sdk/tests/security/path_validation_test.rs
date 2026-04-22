//! Path validation security tests
//!
//! This module tests security improvements to path handling in database
//! and migration code, ensuring protection against path traversal attacks.

use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::{
    ContentStoreConfig, ContentStoreManager, VerifiedPath, manager::BLOB_EVENT_CHANNEL_CAPACITY,
};
use sinex_primitives::{Event, JsonValue, validate_path};
use std::path::Path;
use tempfile::TempDir;
use tokio::fs;
use tokio::process::Command;
use tokio::sync::mpsc;
use xtask::sandbox::TestResult;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_path_validation_rejects_traversal_attacks(ctx: TestContext) -> TestResult<()> {
    // Test various path traversal attack patterns
    let dangerous_paths = vec![
        "../../../etc/passwd",
        "..\\..\\..\\windows\\system32\\config\\sam",
        "%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "%252e%252e%252f%252e%252e%252fetc%252fpasswd",
        "..%2f..%2fetc%2fpasswd",
        "..%5c..%5cwindows%5csystem32",
        "..%c0%af..%c0%afetc%c0%afpasswd",
        "..%c1%9c..%c1%9cetc%c1%9cpasswd",
        "/etc/passwd\0.txt",
        "normal/path/../../../sensitive/data",
    ];

    for dangerous_path in dangerous_paths {
        // Validate path function should reject all these
        let result = validate_path(dangerous_path);
        ctx.assert("path validation should reject dangerous paths")
            .that(
                result.is_err(),
                &format!("Path '{dangerous_path}' should be rejected"),
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn test_path_validation_allows_safe_paths(ctx: TestContext) -> TestResult<()> {
    // Test various safe path patterns
    let safe_paths = vec![
        "/home/user/document.txt",
        "/tmp/safe_file.log",
        "relative/path/to/file.json",
        "simple_filename.txt",
        "/var/log/application.log",
        "/opt/app/config/settings.toml",
        "./current/directory/file.txt",
        "unicode/测试/файл.txt",
        "path/with spaces/file.txt",
        "path-with-hyphens/file_with_underscores.txt",
    ];

    for safe_path in safe_paths {
        // Validate path function should accept all these
        let result = validate_path(safe_path);
        ctx.assert("path validation should accept safe paths")
            .that(
                result.is_ok(),
                &format!("Path '{safe_path}' should be accepted"),
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn test_manager_path_validation(ctx: TestContext) -> TestResult<()> {
    system_test_preflight()?;
    // Create temporary directory and files for testing
    let temp_dir = TempDir::new()?;
    let content_store_path = temp_dir.path().join("test-content-store");
    init_content_store_root(&content_store_path).await?;

    let temp_file = temp_dir.path().join("test_file.txt");
    fs::write(&temp_file, b"test content").await?;

    let repo_utf8 = Utf8PathBuf::from_path_buf(content_store_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("content-store path not valid UTF-8"))?;

    // Create content-store manager
    let content_store_config = ContentStoreConfig {
        root_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
    };

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(BLOB_EVENT_CHANNEL_CAPACITY);
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    let manager = ContentStoreManager::new(content_store_config, ctx.pool().clone(), Some(event_tx))?;

    // Test with safe path - should work if file exists
    let safe_path = camino::Utf8PathBuf::try_from(temp_file)?;

    // This might fail due to git-annex setup, but it should fail with a different error
    // than path validation (it should pass path validation)
    let verified = VerifiedPath::parse(safe_path.as_str())?;
    let result = manager.ingest_file(&verified, None).await;

    // Check that the error is NOT a path validation error
    if let Err(e) = result {
        let error_msg = e.to_string();
        ctx.assert("content-store manager error should not be path validation")
            .that(
                !error_msg.contains("Invalid file path") && !error_msg.contains("path traversal"),
                "Error should not be about path validation",
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn manager_rejects_percent_encoded_traversal(ctx: TestContext) -> TestResult<()> {
    system_test_preflight()?;
    let temp_dir = TempDir::new()?;
    let content_store_path = temp_dir.path().join("percent-encoded-content-store");

    let repo_utf8 = Utf8PathBuf::from_path_buf(content_store_path)
        .map_err(|_| color_eyre::eyre::eyre!("content-store path not valid UTF-8"))?;

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(BLOB_EVENT_CHANNEL_CAPACITY);
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    let content_store_config = ContentStoreConfig {
        root_path: repo_utf8,
        num_copies: None,
        large_files: None,
    };

    let _manager = ContentStoreManager::new(content_store_config, ctx.pool().clone(), Some(event_tx))?;

    let encoded_path = Utf8PathBuf::from("%2e%2e%2fetc%2fpasswd");
    let verification = VerifiedPath::parse(encoded_path.as_str());
    assert!(
        verification.as_ref().map_or_else(
            |err| err.to_string().contains("Path validation failed"),
            |_| false
        ),
        "Percent-encoded traversal paths must be rejected before ingestion"
    );

    Ok(())
}

async fn init_content_store_root(path: &Path) -> TestResult<()> {
    fs::create_dir_all(path).await?;

    let output = Command::new("git")
        .arg("init")
        .current_dir(path)
        .output()
        .await?;
    if !output.status.success() {
        return Err(color_eyre::eyre::eyre!(
            "Failed to initialize git repository"
        ));
    }

    Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(path)
        .output()
        .await?;
    Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(path)
        .output()
        .await?;

    let output = Command::new("git")
        .args(["annex", "init", "security-tests"])
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

#[sinex_test]
async fn test_unicode_and_null_byte_handling() -> TestResult<()> {
    // Test various dangerous unicode and null byte patterns
    let dangerous_strings = vec![
        "/path/with/null\0byte",
        "/path/with\u{0000}explicit/null",
        "/path\u{200B}zero/width/space",
        "/path\u{202E}right-to-left/override",
        "/path\u{FEFF}bom/character",
    ];

    for dangerous_string in dangerous_strings {
        // Path validation should handle these appropriately
        let result = validate_path(dangerous_string);

        if dangerous_string.contains('\0') {
            // Null bytes should be rejected
            assert!(
                result.is_err(),
                "Path with null bytes should be rejected: '{dangerous_string:?}'"
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_path_length_limits() -> TestResult<()> {
    // Test extremely long paths
    let very_long_path = "a".repeat(5000);
    let result = validate_path(&very_long_path);

    assert!(result.is_err(), "Extremely long path should be rejected");

    // Test reasonable length path
    let reasonable_path = "reasonable/length/path.txt";
    let result = validate_path(reasonable_path);

    assert!(result.is_ok(), "Reasonable length path should be accepted");

    Ok(())
}
