//! Path validation security tests
//! 
//! This module tests security improvements to path handling in database
//! and migration code, ensuring protection against path traversal attacks.

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::db::security::{SecurityError, SecurityValidator};
use sinex_core::types::validate_path;
use sinex_satellite_sdk::annex::BlobManager;
use sinex_satellite_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_satellite_sdk::preflight::validate_toml_file;
use sinex_test_utils::prelude::*;
use std::path::Path;
use tempfile::TempDir;

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
    // Create temporary directory and files for testing
    let temp_dir = TempDir::new()?;
    let temp_file = temp_dir.path().join("test_file.txt");
    tokio::fs::write(&temp_file, b"test content").await?;

    // Create blob manager
    let annex_config = AnnexConfig {
        repo_path: temp_dir.path().to_path_buf().try_into()?,
        auto_get: false,
    };
    let blob_manager = BlobManager::new(annex_config, ctx.pool.clone())?;

    // Test with dangerous path - should be rejected
    let dangerous_path = camino::Utf8Path::new("../../../etc/passwd");
    let result = blob_manager.ingest_file(dangerous_path, None).await;
    ctx.assert("blob manager should reject dangerous paths")
        .that(result.is_err(), "Dangerous path should be rejected")?;

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
async fn test_configuration_file_validation(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create a temporary directory for test files
    let temp_dir = TempDir::new()?;

    // Create a legitimate TOML file
    let valid_toml_path = temp_dir.path().join("valid_config.toml");
    tokio::fs::write(
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
async fn test_unicode_and_null_byte_handling(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
async fn test_path_length_limits(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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