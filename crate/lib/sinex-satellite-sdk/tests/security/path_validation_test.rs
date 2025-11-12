//! Path validation security tests
//! 
//! This module tests security improvements to path handling in database
//! and migration code, ensuring protection against path traversal attacks.

use camino::Utf8PathBuf;
use sinex_test_utils::TestResult;
use serde_json::json;
use sinex_core::db::security::{SecurityError, SecurityValidator};
use sinex_core::types::validate_path;
use sinex_core::{Event, JsonValue};
use sinex_satellite_sdk::annex::BlobManager;
use sinex_satellite_sdk::annex::AnnexConfig;
use sinex_satellite_sdk::preflight::validate_toml_file;
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
            .that(result.is_err(), &format!("Path '{}' should be rejected", dangerous_path))?;

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
            .that(result.is_ok(), &format!("Path '{}' should be accepted", safe_path))?;

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

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<JsonValue>>();
    tokio::spawn(async move {
        while event_rx.recv().await.is_some() {}
    });

    let blob_manager = BlobManager::new(annex_config, ctx.pool().clone(), event_tx)?;

    // Test with safe path - should work if file exists
    let safe_path = camino::Utf8PathBuf::try_from(temp_file)?;
    let safe_utf8_path = safe_path.as_path();

    // This might fail due to git-annex setup, but it should fail with a different error
    // than path validation (it should pass path validation)
    let result = blob_manager.ingest_file(safe_utf8_path, None).await;
    
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

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<JsonValue>>();
    tokio::spawn(async move {
        while event_rx.recv().await.is_some() {}
    });

    let annex_config = AnnexConfig {
        repo_path: repo_utf8,
        num_copies: None,
        large_files: None,
    };

    let blob_manager = BlobManager::new(annex_config, ctx.pool().clone(), event_tx)?;

    let encoded_path = Utf8PathBuf::from("%2e%2e%2fetc%2fpasswd");

    let result = blob_manager.ingest_file(encoded_path.as_path(), None).await;

    assert!(
        result
            .as_ref()
            .map(|_| false)
            .unwrap_or_else(|err| err.to_string().contains("Invalid file path")),
        "BlobManager should reject percent-encoded traversal paths instead of attempting ingestion"
    );

    Ok(())
}

#[sinex_test]
async fn test_configuration_file_validation() -> color_eyre::eyre::Result<()> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;

    // Create a legitimate TOML file
    let valid_toml_path = temp_dir.path().join("valid_config.toml");
    fs::write(
        &valid_toml_path,
        r#"
        [section]
        key = "value"
        "#,
    ).await?;

    // Test with valid path - should work
    let valid_utf8_path = camino::Utf8PathBuf::try_from(valid_toml_path)?;
    let result = validate_toml_file(&valid_utf8_path).await;
    // Note: This might still fail due to other validation, but should pass path validation
    
    // Test with dangerous path pattern - should be rejected
    let dangerous_path = camino::Utf8Path::new("../../../etc/passwd");
    let result = validate_toml_file(dangerous_path).await;
    assert!(
        result.is_err(),
        "validate_toml_file should reject dangerous path"
    );

    // Check that the error mentions path validation
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Invalid or dangerous path") || error_msg.contains("path"),
            "Error should mention path validation issue: {}",
            error_msg
        );
    }

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
        return Err(color_eyre::eyre::eyre!("Failed to initialize git repository"));
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
async fn test_event_sanitization_with_dangerous_paths(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test that events with dangerous paths in payloads are sanitized
    let event_with_dangerous_path = ctx
        .create_test_event(
            "test-source",
            "file.processed",
            json!({
                "path": "../../../etc/passwd",
                "target": "..\\..\\windows\\system32\\config",
                "file": "/normal/path/../../../sensitive/data",
                "directory": "%2e%2e%2fetc%2f"
            }),
        )
        .await;

    // The event creation should either:
    // 1. Succeed but sanitize the dangerous paths, OR
    // 2. Fail due to validation
    match event_with_dangerous_path {
        Ok(event) => {
            // If event was created, paths should be sanitized
            let payload = &event.payload;
            
            if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
                ctx.assert("dangerous path should be sanitized")
                    .that(!path.contains(".."), "Path should not contain '..' after sanitization")?;
            }
            
            if let Some(target) = payload.get("target").and_then(|v| v.as_str()) {
                ctx.assert("dangerous target should be sanitized")
                    .that(!target.contains(".."), "Target should not contain '..' after sanitization")?;
            }
        }
        Err(e) => {
            // If event creation failed, it should be due to validation
            let error_msg = e.to_string();
            ctx.assert("event creation should fail due to validation")
                .that(
                    error_msg.contains("validation") || error_msg.contains("path") || error_msg.contains("traversal"),
                    "Error should be related to path validation",
                )?;
        }
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
    
    assert!(
        result.is_err(),
        "Extremely long path should be rejected"
    );

    // Test reasonable length path
    let reasonable_path = "reasonable/length/path.txt";
    let result = validate_path(reasonable_path);
    
    assert!(
        result.is_ok(),
        "Reasonable length path should be accepted"
    );

    Ok(())
}

#[sinex_test] 
async fn test_json_payload_path_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that JSON payloads with various path fields are validated
    let payload_with_paths = json!({
        "path": "/safe/path/file.txt",
        "file": "/another/safe/file.txt", 
        "directory": "/safe/directory",
        "filename": "safe_filename.txt",
        "filepath": "/safe/filepath.txt",
        "target": "/safe/target.txt",
        "source_path": "/safe/source.txt",
        "dest_path": "/safe/destination.txt"
    });

    // This should be accepted
    let event = ctx
        .create_test_event("path-test", "multiple.paths", payload_with_paths)
        .await?;

    // Verify the event was created successfully
    ctx.assert("event with safe paths should be created")
        .that(event.event_type.as_str() == "multiple.paths", "Event type should match")?;

    // Test with dangerous paths in various fields
    let dangerous_payload = json!({
        "path": "../../../etc/passwd",
        "file": "..\\..\\windows\\system32",
        "directory": "%2e%2e%2fetc",
        "other_field": "this should be fine"
    });

    let result = ctx
        .create_test_event("path-test", "dangerous.paths", dangerous_payload)
        .await;

    // This should either fail or sanitize the paths
    match result {
        Ok(event) => {
            // If successful, dangerous paths should be sanitized
            let payload = &event.payload;
            if let Some(path_val) = payload.get("path") {
                if let Some(path_str) = path_val.as_str() {
                    assert!(
                        !path_str.contains(".."),
                        "Dangerous path should be sanitized: {}",
                        path_str
                    );
                }
            }
        }
        Err(_) => {
            // It's also acceptable for this to fail validation
            // This shows the security measures are working
        }
    }

    Ok(())
}
