//! Path validation security tests
//!
//! This module tests security improvements to path handling in database
//! and migration code, ensuring protection against path traversal attacks.

use camino::Utf8PathBuf;
use sinex_core::db::security::{SecurityError, SecurityValidator};
use sinex_core::types::validate_path;
use sinex_core::{Event, JsonValue};
use sinex_satellite_sdk::annex::{
    blob_manager::BLOB_EVENT_CHANNEL_CAPACITY, AnnexConfig, BlobManager, VerifiedPath,
};
use sinex_test_utils::prelude::*;
use std::path::Path;
use tempfile::TempDir;
use tokio::fs;
use tokio::process::Command;
use tokio::sync::mpsc;

#[sinex_test]
async fn test_path_validation_rejects_traversal_attacks(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
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
                &format!("Path '{}' should be rejected", dangerous_path),
            )?;

        // SecurityValidator should also reject these
        let sanitize_result = SecurityValidator::sanitize_path(dangerous_path);
        ctx.assert("security validator should reject dangerous paths")
            .that(
                sanitize_result.is_err(),
                &format!("SecurityValidator should reject '{}'", dangerous_path),
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn test_path_validation_allows_safe_paths(ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
                &format!("Path '{}' should be accepted", safe_path),
            )?;

        // SecurityValidator should also accept these
        let sanitize_result = SecurityValidator::sanitize_path(safe_path);
        ctx.assert("security validator should accept safe paths")
            .that(
                sanitize_result.is_ok(),
                &format!("SecurityValidator should accept '{}'", safe_path),
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn test_blob_manager_path_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    if !git_annex_available().await {
        eprintln!("Skipping test: git-annex not available");
        return Ok(());
    }

    // Create temporary directory and files for testing
    let temp_dir = TempDir::new()?;
    let annex_path = temp_dir.path().join("test-annex");
    init_annex_repo(&annex_path).await?;

    let temp_file = temp_dir.path().join("test_file.txt");
    fs::write(&temp_file, b"test content").await?;

    let repo_utf8 = Utf8PathBuf::from_path_buf(annex_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path not valid UTF-8"))?;

    // Create blob manager
    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: Some(1),
        large_files: Some("anything".to_string()),
    };

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(BLOB_EVENT_CHANNEL_CAPACITY);
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    let blob_manager = BlobManager::new(annex_config, ctx.pool().clone(), Some(event_tx))?;

    // Test with safe path - should work if file exists
    let safe_path = camino::Utf8PathBuf::try_from(temp_file)?;

    // This might fail due to git-annex setup, but it should fail with a different error
    // than path validation (it should pass path validation)
    let verified = VerifiedPath::parse(safe_path.as_str())?;
    let result = blob_manager.ingest_file(&verified, None).await;

    // Check that the error is NOT a path validation error
    if let Err(e) = result {
        let error_msg = e.to_string();
        ctx.assert("blob manager error should not be path validation")
            .that(
                !error_msg.contains("Invalid file path") && !error_msg.contains("path traversal"),
                "Error should not be about path validation",
            )?;
    }

    Ok(())
}

#[sinex_test]
async fn blob_manager_rejects_percent_encoded_traversal(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let temp_dir = TempDir::new()?;
    let annex_path = temp_dir.path().join("percent-encoded-annex");

    let repo_utf8 = Utf8PathBuf::from_path_buf(annex_path.clone())
        .map_err(|_| color_eyre::eyre::eyre!("annex path not valid UTF-8"))?;

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(BLOB_EVENT_CHANNEL_CAPACITY);
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: None,
        large_files: None,
    };

    let blob_manager = BlobManager::new(annex_config, ctx.pool().clone(), Some(event_tx))?;

    let encoded_path = Utf8PathBuf::from("%2e%2e%2fetc%2fpasswd");
    let verification = VerifiedPath::parse(encoded_path.as_str());
    assert!(
        verification
            .as_ref()
            .map(|_| false)
            .unwrap_or_else(|err| err.to_string().contains("Path validation failed")),
        "Percent-encoded traversal paths must be rejected before ingestion"
    );

    Ok(())
}

async fn git_annex_available() -> bool {
    Command::new("git")
        .args(["annex", "version"])
        .output()
        .await
        .map(|output| output.status.success())
        .unwrap_or(false)
}

async fn init_annex_repo(path: &Path) -> color_eyre::eyre::Result<()> {
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
async fn test_unicode_and_null_byte_handling() -> color_eyre::eyre::Result<()> {
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
                "Path with null bytes should be rejected: '{:?}'",
                dangerous_string
            );
        }

        // SecurityValidator should handle these
        let sanitized = SecurityValidator::sanitize_unicode(dangerous_string);

        if dangerous_string.contains('\0') {
            // Null bytes should be removed
            assert!(
                !sanitized.contains('\0'),
                "Null bytes should be removed from: '{:?}'",
                dangerous_string
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_path_length_limits() -> color_eyre::eyre::Result<()> {
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

#[sinex_test]
async fn test_security_validator_rejects_dangerous_paths() -> color_eyre::eyre::Result<()> {
    let samples = vec![
        "../../../etc/passwd",
        "..\\..\\windows\\system32\\config\\sam",
        "%2e%2e%2fetc%2fpasswd",
        "%252e%252e%252f%252e%252e%252fetc%252fpasswd",
    ];

    for sample in samples {
        let sanitized = SecurityValidator::sanitize_path(sample);
        assert!(
            matches!(sanitized, Err(SecurityError::PathTraversal(_))),
            "SecurityValidator should reject dangerous input: {sample}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_security_validator_accepts_safe_paths() -> color_eyre::eyre::Result<()> {
    let samples = vec![
        "/opt/sinex/data/file.txt",
        "relative/path/to/file.log",
        "C:/sinex/watcher/history",
    ];

    for sample in samples {
        let sanitized = SecurityValidator::sanitize_path(sample)?;
        assert!(
            !sanitized.contains(".."),
            "sanitized path should not contain traversal sequences"
        );
    }

    Ok(())
}
